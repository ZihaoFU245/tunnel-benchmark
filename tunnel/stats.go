package tunnel

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	fineLatencyBuckets   = 1000
	mediumLatencyBuckets = 990
	coarseLatencyBuckets = 990
	latencyBucketCount   = fineLatencyBuckets + mediumLatencyBuckets + coarseLatencyBuckets + 1
)

type Stats struct {
	latencyHist [latencyBucketCount]atomic.Uint64
	samples     atomic.Uint64
	sumMicros   atomic.Uint64
	minMicros   atomic.Uint64
	maxMicros   atomic.Uint64

	BytesSent atomic.Int64
	BytesRecv atomic.Int64
	Errors    atomic.Int64

	timeMu sync.RWMutex
	Start  time.Time
	End    time.Time
}

func (s *Stats) RecordRTT(rtt time.Duration) {
	us := rtt.Microseconds()
	if us < 0 {
		us = 0
	}
	micros := uint64(us)

	s.latencyHist[latencyBucket(micros)].Add(1)
	s.samples.Add(1)
	s.sumMicros.Add(micros)
	updateAtomicMin(&s.minMicros, micros+1)
	updateAtomicMax(&s.maxMicros, micros)
}

func (s *Stats) RecordError() {
	s.Errors.Add(1)
}

func (s *Stats) AddSent(n int64) {
	s.BytesSent.Add(n)
}

func (s *Stats) AddRecv(n int64) {
	s.BytesRecv.Add(n)
}

func (s *Stats) MarkStart(t time.Time) {
	s.timeMu.Lock()
	s.Start = t
	s.timeMu.Unlock()
}

func (s *Stats) MarkEnd(t time.Time) {
	s.timeMu.Lock()
	s.End = t
	s.timeMu.Unlock()
}

func (s *Stats) Snapshot() StatsSnapshot {
	var hist [latencyBucketCount]uint64
	for i := range hist {
		hist[i] = s.latencyHist[i].Load()
	}

	s.timeMu.RLock()
	start := s.Start
	end := s.End
	s.timeMu.RUnlock()

	min := s.minMicros.Load()
	if min > 0 {
		min--
	}

	return StatsSnapshot{
		LatencyHist: hist,
		Samples:     s.samples.Load(),
		SumMicros:   s.sumMicros.Load(),
		MinMicros:   min,
		MaxMicros:   s.maxMicros.Load(),
		BytesSent:   s.BytesSent.Load(),
		BytesRecv:   s.BytesRecv.Load(),
		Errors:      s.Errors.Load(),
		Start:       start,
		End:         end,
	}
}

type StatsSnapshot struct {
	LatencyHist [latencyBucketCount]uint64
	Samples     uint64
	SumMicros   uint64
	MinMicros   uint64
	MaxMicros   uint64
	BytesSent   int64
	BytesRecv   int64
	Errors      int64
	Start       time.Time
	End         time.Time
}

type AggregateStats struct {
	TunnelCount        int
	LatencyHist        [latencyBucketCount]uint64
	TotalSamples       uint64
	TotalLatencyMicros uint64
	MinLatencyMicros   uint64
	MaxLatencyMicros   uint64
	TotalBytesSent     int64
	TotalBytesRecv     int64
	TotalErrors        int64
	Duration           time.Duration
}

func Aggregate(all []*Stats) *AggregateStats {
	agg := &AggregateStats{TunnelCount: len(all)}
	if len(all) == 0 {
		return agg
	}

	var earliest, latest time.Time
	for _, s := range all {
		if s == nil {
			continue
		}
		snap := s.Snapshot()
		for i, count := range snap.LatencyHist {
			agg.LatencyHist[i] += count
		}
		agg.TotalSamples += snap.Samples
		agg.TotalLatencyMicros += snap.SumMicros
		agg.TotalBytesSent += snap.BytesSent
		agg.TotalBytesRecv += snap.BytesRecv
		agg.TotalErrors += snap.Errors

		if snap.Samples > 0 && (agg.MinLatencyMicros == 0 || snap.MinMicros < agg.MinLatencyMicros) {
			agg.MinLatencyMicros = snap.MinMicros
		}
		if snap.MaxMicros > agg.MaxLatencyMicros {
			agg.MaxLatencyMicros = snap.MaxMicros
		}
		if earliest.IsZero() || (!snap.Start.IsZero() && snap.Start.Before(earliest)) {
			earliest = snap.Start
		}
		if snap.End.After(latest) {
			latest = snap.End
		}
	}
	if !earliest.IsZero() && !latest.IsZero() {
		agg.Duration = latest.Sub(earliest)
	}
	return agg
}

func (agg *AggregateStats) Percentiles() (min, max, avg, p50, p95, p98, p99 float64) {
	if agg.TotalSamples == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	min = microsToMillis(agg.MinLatencyMicros)
	max = microsToMillis(agg.MaxLatencyMicros)
	avg = float64(agg.TotalLatencyMicros) / float64(agg.TotalSamples) / 1000.0
	p50 = agg.percentile(0.50)
	p95 = agg.percentile(0.95)
	p98 = agg.percentile(0.98)
	p99 = agg.percentile(0.99)
	return
}

func (agg *AggregateStats) TailPercentiles() (tail5, tail2 float64) {
	if agg.TotalSamples == 0 {
		return 0, 0
	}
	tail5 = agg.tailAvg(0.05)
	tail2 = agg.tailAvg(0.02)
	return
}

func (agg *AggregateStats) LatencyVariance() float64 {
	if agg.TotalSamples < 2 {
		return 0
	}
	avg := float64(agg.TotalLatencyMicros) / float64(agg.TotalSamples)
	var sumSqDiff float64
	for i, count := range agg.LatencyHist {
		if count == 0 {
			continue
		}
		diff := bucketMidpointMicros(i) - avg
		sumSqDiff += diff * diff * float64(count)
	}
	return math.Sqrt(sumSqDiff/float64(agg.TotalSamples)) / 1000.0
}

func (agg *AggregateStats) percentile(p float64) float64 {
	threshold := uint64(math.Ceil(p * float64(agg.TotalSamples)))
	if threshold < 1 {
		threshold = 1
	}

	var seen uint64
	for i, count := range agg.LatencyHist {
		seen += count
		if seen >= threshold {
			return microsToMillis(minUint64(bucketUpperMicros(i), agg.MaxLatencyMicros))
		}
	}
	return microsToMillis(agg.MaxLatencyMicros)
}

func (agg *AggregateStats) tailAvg(fraction float64) float64 {
	if fraction <= 0 || fraction >= 1 {
		return 0
	}
	target := uint64(math.Ceil(fraction * float64(agg.TotalSamples)))
	if target < 1 {
		target = 1
	}

	var count uint64
	var sum float64
	for i := len(agg.LatencyHist) - 1; i >= 0 && count < target; i-- {
		bucketCount := agg.LatencyHist[i]
		if bucketCount == 0 {
			continue
		}
		take := bucketCount
		if count+take > target {
			take = target - count
		}
		representative := bucketMidpointMicros(i)
		if representative > float64(agg.MaxLatencyMicros) {
			representative = float64(agg.MaxLatencyMicros)
		}
		sum += representative * float64(take)
		count += take
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count) / 1000.0
}

func latencyBucket(micros uint64) int {
	if micros < fineLatencyBuckets {
		return int(micros)
	}
	if micros < 100000 {
		return fineLatencyBuckets + int((micros-fineLatencyBuckets)/100)
	}
	if micros < 10000000 {
		return fineLatencyBuckets + mediumLatencyBuckets + int((micros-100000)/10000)
	}
	return latencyBucketCount - 1
}

func bucketUpperMicros(idx int) uint64 {
	if idx < fineLatencyBuckets {
		return uint64(idx)
	}
	if idx < fineLatencyBuckets+mediumLatencyBuckets {
		return 1000 + uint64(idx-fineLatencyBuckets+1)*100
	}
	if idx < latencyBucketCount-1 {
		return 100000 + uint64(idx-fineLatencyBuckets-mediumLatencyBuckets+1)*10000
	}
	return 10000000
}

func bucketMidpointMicros(idx int) float64 {
	if idx < fineLatencyBuckets {
		return float64(idx)
	}
	if idx < fineLatencyBuckets+mediumLatencyBuckets {
		lower := 1000 + uint64(idx-fineLatencyBuckets)*100
		return float64(lower + 50)
	}
	if idx < latencyBucketCount-1 {
		lower := 100000 + uint64(idx-fineLatencyBuckets-mediumLatencyBuckets)*10000
		return float64(lower + 5000)
	}
	return 10000000
}

func microsToMillis(micros uint64) float64 {
	return float64(micros) / 1000.0
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func updateAtomicMin(dst *atomic.Uint64, val uint64) {
	for {
		old := dst.Load()
		if old != 0 && old <= val {
			return
		}
		if dst.CompareAndSwap(old, val) {
			return
		}
	}
}

func updateAtomicMax(dst *atomic.Uint64, val uint64) {
	for {
		old := dst.Load()
		if old >= val {
			return
		}
		if dst.CompareAndSwap(old, val) {
			return
		}
	}
}
