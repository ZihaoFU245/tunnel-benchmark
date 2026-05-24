package tunnel

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestPacing(t *testing.T) {
	tests := []struct {
		name           string
		packetsPerSec  float64
		wantInterval   time.Duration
		wantPacketsPer float64
	}{
		{name: "zero", packetsPerSec: 0, wantInterval: time.Second, wantPacketsPer: 1},
		{name: "slow", packetsPerSec: 0.5, wantInterval: 2 * time.Second, wantPacketsPer: 1},
		{name: "moderate", packetsPerSec: 2000, wantInterval: time.Millisecond, wantPacketsPer: 2},
		{name: "high", packetsPerSec: 200000, wantInterval: 100 * time.Microsecond, wantPacketsPer: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interval, packets := pacing(tt.packetsPerSec)
			if interval != tt.wantInterval {
				t.Fatalf("interval = %v, want %v", interval, tt.wantInterval)
			}
			if packets != tt.wantPacketsPer {
				t.Fatalf("packets per tick = %v, want %v", packets, tt.wantPacketsPer)
			}
		})
	}
}

func TestAggregateLatencyHistogram(t *testing.T) {
	s := &Stats{}
	for _, rtt := range []time.Duration{
		500 * time.Microsecond,
		2 * time.Millisecond,
		50 * time.Millisecond,
		150 * time.Millisecond,
	} {
		s.RecordRTT(rtt)
	}
	s.AddSent(100)
	s.AddRecv(90)
	s.RecordError()
	start := time.Unix(10, 0)
	end := start.Add(2 * time.Second)
	s.MarkStart(start)
	s.MarkEnd(end)

	agg := Aggregate([]*Stats{s})
	if agg.TotalSamples != 4 {
		t.Fatalf("samples = %d, want 4", agg.TotalSamples)
	}
	if agg.TotalBytesSent != 100 || agg.TotalBytesRecv != 90 || agg.TotalErrors != 1 {
		t.Fatalf("totals = sent:%d recv:%d errors:%d", agg.TotalBytesSent, agg.TotalBytesRecv, agg.TotalErrors)
	}
	if agg.Duration != 2*time.Second {
		t.Fatalf("duration = %v, want 2s", agg.Duration)
	}

	min, max, avg, p50, p95, p98, p99 := agg.Percentiles()
	assertClose(t, "min", min, 0.5, 0.001)
	assertClose(t, "max", max, 150, 0.001)
	assertClose(t, "avg", avg, 50.625, 0.001)
	assertClose(t, "p50", p50, 2.1, 0.001)
	assertClose(t, "p95", p95, 150, 0.001)
	assertClose(t, "p98", p98, 150, 0.001)
	assertClose(t, "p99", p99, 150, 0.001)

	tail5, tail2 := agg.TailPercentiles()
	if tail5 > max || tail2 > max {
		t.Fatalf("tail averages must not exceed max: tail5=%.3f tail2=%.3f max=%.3f", tail5, tail2, max)
	}
}

func TestStatsConcurrentRecordRTT(t *testing.T) {
	const goroutines = 16
	const perGoroutine = 1000

	var s Stats
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				s.RecordRTT(time.Duration(offset+j) * time.Microsecond)
			}
		}(i)
	}
	wg.Wait()

	snap := s.Snapshot()
	if snap.Samples != goroutines*perGoroutine {
		t.Fatalf("samples = %d, want %d", snap.Samples, goroutines*perGoroutine)
	}
}

func assertClose(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %.6f, want %.6f +/- %.6f", name, got, want, tolerance)
	}
}
