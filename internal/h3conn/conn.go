package h3conn

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

const (
	settingsTimeout = 5 * time.Second
	requestTimeout  = 10 * time.Second

	PacketDataSize = 1408
	hdrSize        = 8
	payloadSize    = PacketDataSize - hdrSize
)

func PacketHeaderSize() int  { return hdrSize }
func PacketPayloadSize() int { return payloadSize }

type Conn struct {
	rstr       *http3.RequestStream
	quicConn   *quic.Conn
	transport  *quic.Transport
	clientConn *http3.ClientConn

	mu     sync.Mutex
	closed bool
}

type session struct {
	transport  *quic.Transport
	quicConn   *quic.Conn
	clientConn *http3.ClientConn
	initDone   <-chan struct{}
}

func dialQUIC(proxyAddr string) (*session, error) {
	udpConn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, fmt.Errorf("listen udp: %w", err)
	}

	tr := &quic.Transport{Conn: udpConn}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
		ServerName:         "127.0.0.1",
	}

	ctx, cancel := context.WithTimeout(context.Background(), settingsTimeout)
	defer cancel()

	quicConn, err := tr.Dial(ctx, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3128}, tlsConf, &quic.Config{
		MaxIdleTimeout: 30 * time.Second,
	})
	if err != nil {
		tr.Close()
		return nil, fmt.Errorf("quic dial: %w", err)
	}

	h3t := &http3.Transport{}
	cc := h3t.NewClientConn(quicConn)

	done := make(chan struct{})
	s := &session{
		transport:  tr,
		quicConn:   quicConn,
		clientConn: cc,
		initDone:   done,
	}

	go func() {
		defer close(done)
		select {
		case <-cc.ReceivedSettings():
		case <-quicConn.Context().Done():
		}
	}()

	return s, nil
}

func (s *session) dialStream(targetAddr string) (*Conn, error) {
	select {
	case <-s.initDone:
	case <-s.quicConn.Context().Done():
		return nil, s.quicConn.Context().Err()
	case <-time.After(settingsTimeout):
		return nil, fmt.Errorf("timeout waiting for settings exchange")
	}

	ctx, cancel := context.WithTimeout(s.quicConn.Context(), requestTimeout)
	defer cancel()

	req, err := http.NewRequest("CONNECT", "https://"+targetAddr, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req = req.WithContext(ctx)

	rstr, err := s.clientConn.OpenRequestStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("open request stream: %w", err)
	}

	if err := rstr.SendRequestHeader(req); err != nil {
		rstr.CancelRead(0)
		rstr.CancelWrite(0)
		return nil, fmt.Errorf("send request header: %w", err)
	}

	resp, err := rstr.ReadResponse()
	if err != nil {
		rstr.CancelRead(0)
		rstr.CancelWrite(0)
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		rstr.CancelRead(0)
		rstr.CancelWrite(0)
		return nil, fmt.Errorf("proxy returned status %d", resp.StatusCode)
	}

	return &Conn{
		rstr:       rstr,
		quicConn:   s.quicConn,
		transport:  s.transport,
		clientConn: s.clientConn,
	}, nil
}

func (s *session) close() error {
	err := s.quicConn.CloseWithError(0, "")
	s.transport.Close()
	return err
}

func (c *Conn) Read(b []byte) (int, error) {
	return c.rstr.Read(b)
}

func (c *Conn) Write(b []byte) (int, error) {
	return c.rstr.Write(b)
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.rstr.CancelRead(0)
	return c.rstr.Close()
}

func Dial(proxyAddr, targetAddr string) (*Conn, error) {
	s, err := dialQUIC(proxyAddr)
	if err != nil {
		return nil, err
	}

	conn, err := s.dialStream(targetAddr)
	if err != nil {
		s.close()
		return nil, err
	}
	return conn, nil
}
