package tunnel

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"stresstest/internal/h2conn"
	"stresstest/internal/h3conn"
)

type Config struct {
	ProxyAddr  string
	TargetAddr string
	RateMbps   float64
	ID         int
	Conn       *h2conn.Conn
	Conn3      *h3conn.Conn
	UseH3      bool
}

func NewConfig(id int, proxyAddr, targetAddr string, rateMbps float64) Config {
	return Config{
		ID:         id,
		ProxyAddr:  proxyAddr,
		TargetAddr: targetAddr,
		RateMbps:   rateMbps,
	}
}

type Tunnel struct {
	id     int
	config Config
	stats  *Stats
}

func NewTunnel(config Config) *Tunnel {
	return &Tunnel{
		id:     config.ID,
		config: config,
		stats: &Stats{
			RTTs: make([]float64, 0, 10000),
		},
	}
}

func (t *Tunnel) Stats() *Stats {
	return t.stats
}

type tunnelConn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}

const maxRTTEntries = 65536

func (t *Tunnel) Run(ctx context.Context) error {
	t.stats.Start = time.Now()
	defer func() { t.stats.End = time.Now() }()

	var conn tunnelConn
	var packetSize, hdrSize int

	if t.config.Conn3 != nil {
		conn = t.config.Conn3
		packetSize = h3conn.PacketDataSize
		hdrSize = h3conn.PacketHeaderSize()
	} else if t.config.UseH3 {
		c, err := h3conn.Dial(t.config.ProxyAddr, t.config.TargetAddr)
		if err != nil {
			t.stats.RecordError()
			return fmt.Errorf("tunnel %d dial: %w", t.id, err)
		}
		conn = c
		packetSize = h3conn.PacketDataSize
		hdrSize = h3conn.PacketHeaderSize()
	} else if t.config.Conn != nil {
		conn = t.config.Conn
		packetSize = h2conn.PacketDataSize
		hdrSize = h2conn.PacketHeaderSize()
	} else {
		c, err := h2conn.Dial(t.config.ProxyAddr, t.config.TargetAddr)
		if err != nil {
			t.stats.RecordError()
			return fmt.Errorf("tunnel %d dial: %w", t.id, err)
		}
		conn = c
		packetSize = h2conn.PacketDataSize
		hdrSize = h2conn.PacketHeaderSize()
	}

	payload := make([]byte, packetSize-hdrSize)
	rng := rand.New(rand.NewSource(int64(t.id*1234567 + 1)))
	for i := range payload {
		payload[i] = byte(rng.Intn(256))
	}

	packet := make([]byte, packetSize)
	copy(packet[hdrSize:], payload)

	bytesPerSec := int(t.config.RateMbps * 125000)
	packetInterval := time.Second / time.Duration(bytesPerSec/packetSize)
	if packetInterval < 50*time.Microsecond {
		packetInterval = 50 * time.Microsecond
	}

	ticker := time.NewTicker(packetInterval)
	defer ticker.Stop()

	var seq uint32
	rttMap := make(map[uint32]time.Time, maxRTTEntries)
	var rttMu sync.Mutex

	readBuf := make([]byte, packetSize*4)
	readPos := 0

	sendDone := make(chan struct{})
	recvDone := make(chan struct{})

	var sendErr, recvErr error

	go func() {
		defer close(sendDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			now := time.Now()
			binary.BigEndian.PutUint32(packet[0:4], seq)
			binary.BigEndian.PutUint32(packet[4:8], uint32(now.UnixMilli()))

			rttMu.Lock()
			if len(rttMap) < maxRTTEntries {
				rttMap[seq] = now
			}
			rttMu.Unlock()
			seq++

			n, err := conn.Write(packet)
			if n > 0 {
				t.stats.AddSent(int64(n))
			}
			if err != nil {
				sendErr = err
				return
			}
		}
	}()

	go func() {
		defer close(recvDone)
		tmp := make([]byte, packetSize)
		for {
			n, err := conn.Read(tmp)
			if err != nil {
				recvErr = err
				// Process any remaining complete packets before exit
				rttMu.Lock()
				for readPos >= packetSize {
					rcvSeq := binary.BigEndian.Uint32(readBuf[0:4])
					if sentTime, ok := rttMap[rcvSeq]; ok {
						rttMu.Unlock()
						rtt := time.Since(sentTime)
						t.stats.RecordRTT(rtt)
						rttMu.Lock()
						delete(rttMap, rcvSeq)
					}
					copy(readBuf, readBuf[packetSize:readPos])
					readPos -= packetSize
				}
				rttMu.Unlock()
				return
			}
			if n == 0 {
				continue
			}

			copy(readBuf[readPos:], tmp[:n])
			readPos += n
			t.stats.AddRecv(int64(n))

			for readPos >= packetSize {
				rcvSeq := binary.BigEndian.Uint32(readBuf[0:4])

				rttMu.Lock()
				if sentTime, ok := rttMap[rcvSeq]; ok {
					rtt := time.Since(sentTime)
					t.stats.RecordRTT(rtt)
					delete(rttMap, rcvSeq)
				}
				rttMu.Unlock()

				copy(readBuf, readBuf[packetSize:readPos])
				readPos -= packetSize
			}
		}
	}()

	<-ctx.Done()
	ticker.Stop()

	conn.Close()

	<-sendDone
	<-recvDone

	if sendErr != nil && !isConnClosed(sendErr) {
		t.stats.RecordError()
		return fmt.Errorf("tunnel %d send: %w", t.id, sendErr)
	}

	if recvErr != nil && !isConnClosed(recvErr) {
		t.stats.RecordError()
		return fmt.Errorf("tunnel %d recv: %w", t.id, recvErr)
	}

	return nil
}

func isConnClosed(err error) bool {
	if err == nil {
		return true
	}
	if err == context.Canceled {
		return true
	}
	s := err.Error()
	if s == "stream closed" || s == "connection closed" {
		return true
	}
	if len(s) > 10 && s[:10] == "H3 error (" {
		return true
	}
	return false
}
