package h2conn

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

const (
	defaultStreamWindow   = 65535
	defaultConnWindow     = 65535
	initialConnRecvWin    = 64 << 20
	connWinUpdateThresh   = 1 << 20
	streamWinUpdateThresh = 32768
	maxReadQueue          = 8 << 20
	settingsTimeout       = 5 * time.Second
	requestTimeout        = 10 * time.Second
	maxFrameSize          = 16384

	packetHeaderSizeVal  = 8
	packetPayloadSizeVal = 1400
	PacketDataSize       = packetHeaderSizeVal + packetPayloadSizeVal
)

func PacketHeaderSize() int  { return packetHeaderSizeVal }
func PacketPayloadSize() int { return packetPayloadSizeVal }

type session struct {
	rawConn net.Conn
	framer  *http2.Framer

	mu               sync.RWMutex
	streams          map[uint32]*stream
	connSendWin      int32
	connRecvConsumed int32
	writeCond        *sync.Cond
	nextID           uint32
	closed           bool
	initWin          int32

	framerMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type stream struct {
	id uint32
	s  *session

	mu        sync.Mutex
	readQ     []readChunk
	readBytes int
	readErr   error
	readCond  *sync.Cond
	spaceCond *sync.Cond

	sendWin int32
	closed  atomic.Bool

	connectCh    chan error
	connectOnce  sync.Once
	recvConsumed int32
}

type readChunk struct {
	buf []byte
	off int
}

type Conn struct {
	st          *stream
	ownsSession bool
	closeOnce   sync.Once
}

var dataFrameBufPool = sync.Pool{
	New: func() any {
		return make([]byte, maxFrameSize)
	},
}

func newSession(proxyAddr string) (*session, error) {
	rawConn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("tcp dial: %w", err)
	}

	tlsConn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
		ServerName:         "127.0.0.1",
	})

	if err := tlsConn.Handshake(); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("tls handshake: %w", err)
	}

	state := tlsConn.ConnectionState()
	if state.NegotiatedProtocol != "h2" {
		tlsConn.Close()
		return nil, fmt.Errorf("proxy did not negotiate h2, got %q", state.NegotiatedProtocol)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &session{
		rawConn:     tlsConn,
		framer:      http2.NewFramer(tlsConn, tlsConn),
		streams:     make(map[uint32]*stream),
		connSendWin: defaultConnWindow,
		nextID:      1,
		ctx:         ctx,
		cancel:      cancel,
		initWin:     defaultStreamWindow,
	}
	s.writeCond = sync.NewCond(&s.mu)

	if _, err := io.WriteString(tlsConn, http2.ClientPreface); err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("write preface: %w", err)
	}

	if err := s.framer.WriteSettings(
		http2.Setting{ID: http2.SettingInitialWindowSize, Val: 1 << 20},
		http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: 1000},
	); err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("write settings: %w", err)
	}

	gotSettings := false
	gotSettingsAck := false
	deadline := time.Now().Add(settingsTimeout)

	for !gotSettings || !gotSettingsAck {
		if time.Now().After(deadline) {
			tlsConn.Close()
			return nil, fmt.Errorf("timeout waiting for settings exchange")
		}
		tlsConn.SetReadDeadline(deadline)

		f, err := s.framer.ReadFrame()
		if err != nil {
			tlsConn.Close()
			return nil, fmt.Errorf("read settings: %w", err)
		}
		switch ff := f.(type) {
		case *http2.SettingsFrame:
			if ff.IsAck() {
				gotSettingsAck = true
			} else {
				ff.ForeachSetting(func(st http2.Setting) error {
					if st.ID == http2.SettingInitialWindowSize {
						s.mu.Lock()
						s.initWin = int32(st.Val)
						s.mu.Unlock()
					}
					return nil
				})
				gotSettings = true
				s.framer.WriteSettingsAck()
			}
		case *http2.WindowUpdateFrame:
			if ff.StreamID == 0 {
				s.mu.Lock()
				s.connSendWin += int32(ff.Increment)
				s.mu.Unlock()
			}
		case *http2.PingFrame:
			if !ff.IsAck() {
				s.framer.WritePing(true, ff.Data)
			}
		}
	}
	tlsConn.SetReadDeadline(time.Time{})

	s.framer.WriteWindowUpdate(0, initialConnRecvWin-defaultConnWindow)

	s.wg.Add(1)
	go s.readLoop()

	return s, nil
}

func (s *session) readLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		f, err := s.framer.ReadFrame()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			if !isTimeout(err) {
				s.mu.Lock()
				for _, st := range s.streams {
					st.mu.Lock()
					if st.readErr == nil {
						st.readErr = err
					}
					st.readCond.Broadcast()
					st.spaceCond.Broadcast()
					st.mu.Unlock()
				}
				s.mu.Unlock()
				return
			}
			continue
		}

		switch ff := f.(type) {
		case *http2.DataFrame:
			data := ff.Data()
			dataLen := len(data)

			s.mu.RLock()
			st := s.streams[ff.StreamID]
			s.mu.RUnlock()

			if st == nil {
				continue
			}

			st.mu.Lock()
			for st.readBytes+dataLen > maxReadQueue && st.readErr == nil && !st.closed.Load() && s.ctx.Err() == nil {
				st.spaceCond.Wait()
			}
			if st.readErr != nil || st.closed.Load() || s.ctx.Err() != nil {
				st.mu.Unlock()
				continue
			}
			buf := getDataFrameBuf(dataLen)
			copy(buf, data)
			st.readQ = append(st.readQ, readChunk{buf: buf})
			st.readBytes += dataLen
			st.readCond.Broadcast()
			st.mu.Unlock()

		case *http2.HeadersFrame:
			s.mu.RLock()
			st := s.streams[ff.StreamID]
			s.mu.RUnlock()
			if st == nil || st.connectCh == nil {
				continue
			}

			hd := hpack.NewDecoder(4096, nil)
			hf, err := hd.DecodeFull(ff.HeaderBlockFragment())
			if err != nil {
				st.connectOnce.Do(func() { st.connectCh <- err })
				continue
			}
			status := ""
			for _, h := range hf {
				if h.Name == ":status" {
					status = h.Value
				}
			}
			if status == "200" {
				st.connectOnce.Do(func() { close(st.connectCh) })
			} else {
				st.connectOnce.Do(func() { st.connectCh <- fmt.Errorf("proxy returned status %s", status) })
			}

		case *http2.WindowUpdateFrame:
			s.mu.Lock()
			if ff.StreamID == 0 {
				s.connSendWin += int32(ff.Increment)
			} else {
				if st := s.streams[ff.StreamID]; st != nil {
					st.sendWin += int32(ff.Increment)
				}
			}
			s.writeCond.Broadcast()
			s.mu.Unlock()

		case *http2.SettingsFrame:
			if !ff.IsAck() {
				ff.ForeachSetting(func(st http2.Setting) error {
					if st.ID == http2.SettingInitialWindowSize {
						s.mu.Lock()
						delta := int32(st.Val) - s.initWin
						s.initWin = int32(st.Val)
						for _, str := range s.streams {
							str.sendWin += delta
						}
						s.writeCond.Broadcast()
						s.mu.Unlock()
					}
					return nil
				})
				s.framerMu.Lock()
				s.framer.WriteSettingsAck()
				s.framerMu.Unlock()
			}

		case *http2.PingFrame:
			if !ff.IsAck() {
				s.framerMu.Lock()
				s.framer.WritePing(true, ff.Data)
				s.framerMu.Unlock()
			}

		case *http2.GoAwayFrame:
			s.mu.Lock()
			for _, st := range s.streams {
				st.mu.Lock()
				if st.readErr == nil {
					st.readErr = fmt.Errorf("GOAWAY: %v", ff.ErrCode)
				}
				st.readCond.Broadcast()
				st.spaceCond.Broadcast()
				st.mu.Unlock()
			}
			s.mu.Unlock()
			return

		case *http2.RSTStreamFrame:
			s.mu.Lock()
			if st := s.streams[ff.StreamID]; st != nil {
				st.mu.Lock()
				if st.readErr == nil {
					st.readErr = fmt.Errorf("RST_STREAM: %v", ff.ErrCode)
				}
				st.readCond.Broadcast()
				st.spaceCond.Broadcast()
				st.mu.Unlock()
			}
			s.mu.Unlock()
		}
	}
}

func (s *session) dialStream(targetAddr string) (*stream, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("session closed")
	}
	streamID := s.nextID
	initWin := s.initWin
	s.nextID += 2
	s.mu.Unlock()

	st := &stream{
		id:        streamID,
		s:         s,
		connectCh: make(chan error, 1),
		sendWin:   initWin,
	}
	st.readCond = sync.NewCond(&st.mu)
	st.spaceCond = sync.NewCond(&st.mu)

	s.mu.Lock()
	s.streams[streamID] = st
	s.mu.Unlock()

	var hbuf bytes.Buffer
	enc := hpack.NewEncoder(&hbuf)
	enc.WriteField(hpack.HeaderField{Name: ":method", Value: "CONNECT"})
	enc.WriteField(hpack.HeaderField{Name: ":authority", Value: targetAddr})

	s.framerMu.Lock()
	err := s.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		EndStream:     false,
		EndHeaders:    true,
		BlockFragment: hbuf.Bytes(),
	})
	s.framerMu.Unlock()
	if err != nil {
		s.mu.Lock()
		delete(s.streams, streamID)
		s.mu.Unlock()
		return nil, fmt.Errorf("write connect headers: %w", err)
	}

	select {
	case err := <-st.connectCh:
		if err != nil {
			s.mu.Lock()
			delete(s.streams, streamID)
			s.mu.Unlock()
			return nil, err
		}
	case <-s.ctx.Done():
		s.mu.Lock()
		delete(s.streams, streamID)
		s.mu.Unlock()
		return nil, s.ctx.Err()
	case <-time.After(requestTimeout):
		s.mu.Lock()
		delete(s.streams, streamID)
		s.mu.Unlock()
		return nil, fmt.Errorf("timeout waiting for CONNECT response")
	}

	return st, nil
}

func (s *session) close() error {
	s.cancel()

	s.mu.Lock()
	s.closed = true
	s.writeCond.Broadcast()
	for _, st := range s.streams {
		st.mu.Lock()
		st.readCond.Broadcast()
		st.spaceCond.Broadcast()
		st.mu.Unlock()
	}
	s.mu.Unlock()

	s.rawConn.Close()
	s.wg.Wait()
	return nil
}

func (c *Conn) Read(b []byte) (int, error) {
	st := c.st

	st.mu.Lock()

	for st.readBytes == 0 && st.readErr == nil && st.s.ctx.Err() == nil && !st.closed.Load() {
		st.readCond.Wait()
	}

	if st.readBytes == 0 {
		if st.readErr != nil {
			st.mu.Unlock()
			return 0, st.readErr
		}
		if st.closed.Load() {
			st.mu.Unlock()
			return 0, fmt.Errorf("stream closed")
		}
		err := st.s.ctx.Err()
		st.mu.Unlock()
		return 0, err
	}

	n := 0
	for n < len(b) && len(st.readQ) > 0 {
		head := &st.readQ[0]
		copied := copy(b[n:], head.buf[head.off:])
		n += copied
		st.readBytes -= copied
		head.off += copied

		if head.off == len(head.buf) {
			putDataFrameBuf(head.buf)
			st.readQ[0] = readChunk{}
			st.readQ = st.readQ[1:]
		}
	}
	st.spaceCond.Broadcast()
	st.mu.Unlock()

	c.addRecvWindow(n)
	return n, nil
}

func (c *Conn) Write(b []byte) (int, error) {
	st := c.st
	s := st.s
	total := 0

	for len(b) > 0 {
		s.mu.Lock()
		for st.sendWin <= 0 || s.connSendWin <= 0 {
			if st.closed.Load() || s.closed {
				s.mu.Unlock()
				return total, fmt.Errorf("connection closed")
			}
			s.writeCond.Wait()
		}
		toSend := int32(len(b))
		if toSend > st.sendWin {
			toSend = st.sendWin
		}
		if toSend > s.connSendWin {
			toSend = s.connSendWin
		}
		if toSend > maxFrameSize {
			toSend = maxFrameSize
		}
		st.sendWin -= toSend
		s.connSendWin -= toSend
		s.mu.Unlock()

		s.framerMu.Lock()
		err := s.framer.WriteData(st.id, false, b[:toSend])
		s.framerMu.Unlock()
		if err != nil {
			return total, err
		}
		total += int(toSend)
		b = b[toSend:]
	}

	return total, nil
}

func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		st := c.st
		s := st.s

		st.closed.Store(true)
		st.mu.Lock()
		st.readCond.Broadcast()
		st.spaceCond.Broadcast()
		st.mu.Unlock()

		s.mu.Lock()
		s.writeCond.Broadcast()
		delete(s.streams, st.id)
		s.mu.Unlock()

		s.framerMu.Lock()
		s.framer.WriteData(st.id, true, nil)
		s.framerMu.Unlock()

		if c.ownsSession {
			err = s.close()
		}
	})
	return err
}

func (c *Conn) addRecvWindow(n int) {
	if n <= 0 {
		return
	}
	st := c.st
	s := st.s

	var streamInc uint32
	st.mu.Lock()
	st.recvConsumed += int32(n)
	if st.recvConsumed >= streamWinUpdateThresh {
		streamInc = uint32(st.recvConsumed)
		st.recvConsumed = 0
	}
	st.mu.Unlock()

	var connInc uint32
	s.mu.Lock()
	s.connRecvConsumed += int32(n)
	if s.connRecvConsumed >= connWinUpdateThresh {
		connInc = uint32(s.connRecvConsumed)
		s.connRecvConsumed = 0
	}
	s.mu.Unlock()

	if connInc > 0 || streamInc > 0 {
		s.framerMu.Lock()
		if connInc > 0 {
			s.framer.WriteWindowUpdate(0, connInc)
		}
		if streamInc > 0 {
			s.framer.WriteWindowUpdate(st.id, streamInc)
		}
		s.framerMu.Unlock()
	}
}

func getDataFrameBuf(size int) []byte {
	if size > maxFrameSize {
		return make([]byte, size)
	}
	return dataFrameBufPool.Get().([]byte)[:size]
}

func putDataFrameBuf(buf []byte) {
	if cap(buf) < maxFrameSize {
		return
	}
	dataFrameBufPool.Put(buf[:maxFrameSize])
}

func Dial(proxyAddr, targetAddr string) (*Conn, error) {
	s, err := newSession(proxyAddr)
	if err != nil {
		return nil, err
	}

	st, err := s.dialStream(targetAddr)
	if err != nil {
		s.close()
		return nil, err
	}

	return &Conn{st: st, ownsSession: true}, nil
}

func isTimeout(err error) bool {
	if e, ok := err.(net.Error); ok && e.Timeout() {
		return true
	}
	return false
}
