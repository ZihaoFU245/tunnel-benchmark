package tunnel

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
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
		stats:  &Stats{},
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

type sentSlot struct {
	seq      atomic.Uint64
	unixNano atomic.Int64
}

func (t *Tunnel) Run(ctx context.Context) error {
	t.stats.MarkStart(time.Now())
	defer func() { t.stats.MarkEnd(time.Now()) }()

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

	rng := rand.New(rand.NewSource(int64(t.id*1234567 + 1)))
	packet := make([]byte, packetSize)
	for i := hdrSize; i < len(packet); i++ {
		packet[i] = byte(rng.Intn(256))
	}

	packetsPerSec := t.config.RateMbps * 125000 / float64(packetSize)
	packetInterval, packetsPerTick := pacing(packetsPerSec)

	ticker := time.NewTicker(packetInterval)
	defer ticker.Stop()

	var seq uint32
	sent := make([]sentSlot, maxRTTEntries)

	sendDone := make(chan struct{})
	recvDone := make(chan struct{})

	var sendErr, recvErr error

	go func() {
		defer close(sendDone)
		var duePackets float64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			duePackets += packetsPerTick
			sendCount := int(duePackets)
			if sendCount == 0 {
				continue
			}
			duePackets -= float64(sendCount)

			for i := 0; i < sendCount; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				now := time.Now()
				binary.BigEndian.PutUint32(packet[0:4], seq)

				idx := seq & (maxRTTEntries - 1)
				sent[idx].unixNano.Store(now.UnixNano())
				sent[idx].seq.Store(uint64(seq) + 1)
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
		}
	}()

	go func() {
		defer close(recvDone)
		tmp := make([]byte, packetSize*32)
		partial := make([]byte, packetSize)
		partialLen := 0

		processPacket := func(pkt []byte) {
			rcvSeq := binary.BigEndian.Uint32(pkt[0:4])
			idx := rcvSeq & (maxRTTEntries - 1)
			expected := uint64(rcvSeq) + 1

			if sent[idx].seq.Load() != expected {
				return
			}
			sentAt := sent[idx].unixNano.Load()
			if sentAt == 0 {
				return
			}
			if sent[idx].seq.CompareAndSwap(expected, 0) {
				t.stats.RecordRTT(time.Duration(time.Now().UnixNano() - sentAt))
			}
		}

		for {
			n, err := conn.Read(tmp)
			if n > 0 {
				t.stats.AddRecv(int64(n))
				chunk := tmp[:n]

				for len(chunk) > 0 {
					if partialLen == 0 && len(chunk) >= packetSize {
						processPacket(chunk[:packetSize])
						chunk = chunk[packetSize:]
						continue
					}

					need := packetSize - partialLen
					if need > len(chunk) {
						copy(partial[partialLen:], chunk)
						partialLen += len(chunk)
						break
					}

					copy(partial[partialLen:], chunk[:need])
					processPacket(partial)
					partialLen = 0
					chunk = chunk[need:]
				}
			}
			if err != nil {
				recvErr = err
				return
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

func pacing(packetsPerSec float64) (time.Duration, float64) {
	if packetsPerSec <= 0 {
		return time.Second, 1
	}
	if packetsPerSec < 1000 {
		return time.Duration(float64(time.Second) / packetsPerSec), 1
	}
	interval := time.Millisecond
	if packetsPerSec > 100000 {
		interval = 100 * time.Microsecond
	}
	return interval, packetsPerSec * interval.Seconds()
}

func isConnClosed(err error) bool {
	if err == nil {
		return true
	}
	if err == context.Canceled {
		return true
	}
	if e, ok := err.(net.Error); ok && e.Timeout() {
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
