package tunnel

import (
	"math"
	"sort"
	"sync"
	"time"
)

type Stats struct {
	mu        sync.Mutex
	RTTs      []float64
	BytesSent int64
	BytesRecv int64
	Errors    int64
	Start     time.Time
	End       time.Time
}

func (s *Stats) RecordRTT(rtt time.Duration) {
	s.mu.Lock()
	s.RTTs = append(s.RTTs, float64(rtt.Microseconds())/1000.0)
	s.mu.Unlock()
}

func (s *Stats) RecordError() {
	s.mu.Lock()
	s.Errors++
	s.mu.Unlock()
}

func (s *Stats) AddSent(n int64) {
	s.mu.Lock()
	s.BytesSent += n
	s.mu.Unlock()
}

func (s *Stats) AddRecv(n int64) {
	s.mu.Lock()
	s.BytesRecv += n
	s.mu.Unlock()
}

type AggregateStats struct {
	TunnelCount   int
	TotalRTTs     []float64
	TotalBytesSent int64
	TotalBytesRecv int64
	TotalErrors   int64
	Duration      time.Duration
}

func Aggregate(all []*Stats) *AggregateStats {
	agg := &AggregateStats{TunnelCount: len(all)}
	if len(all) == 0 {
		return agg
	}

	var earliest, latest time.Time
	earliest = all[0].Start
	latest = all[0].End

	for _, s := range all {
		agg.TotalRTTs = append(agg.TotalRTTs, s.RTTs...)
		agg.TotalBytesSent += s.BytesSent
		agg.TotalBytesRecv += s.BytesRecv
		agg.TotalErrors += s.Errors
		if s.Start.Before(earliest) {
			earliest = s.Start
		}
		if s.End.After(latest) {
			latest = s.End
		}
	}
	agg.Duration = latest.Sub(earliest)
	return agg
}

func (agg *AggregateStats) Percentiles() (min, max, avg, p50, p95, p98, p99 float64) {
	if len(agg.TotalRTTs) == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	sorted := make([]float64, len(agg.TotalRTTs))
	copy(sorted, agg.TotalRTTs)
	sort.Float64s(sorted)

	min = sorted[0]
	max = sorted[len(sorted)-1]

	var sum float64
	for _, v := range sorted {
		sum += v
	}
	avg = sum / float64(len(sorted))

	p50 = percentile(sorted, 0.50)
	p95 = percentile(sorted, 0.95)
	p98 = percentile(sorted, 0.98)
	p99 = percentile(sorted, 0.99)

	return
}

func (agg *AggregateStats) TailPercentiles() (tail5, tail2 float64) {
	if len(agg.TotalRTTs) == 0 {
		return 0, 0
	}

	sorted := make([]float64, len(agg.TotalRTTs))
	copy(sorted, agg.TotalRTTs)
	sort.Float64s(sorted)

	tail5 = tailAvg(sorted, 0.05)
	tail2 = tailAvg(sorted, 0.02)
	return
}

func (agg *AggregateStats) LatencyVariance() float64 {
	if len(agg.TotalRTTs) < 2 {
		return 0
	}
	_, _, avg, _, _, _, _ := agg.Percentiles()
	var sumSqDiff float64
	for _, v := range agg.TotalRTTs {
		diff := v - avg
		sumSqDiff += diff * diff
	}
	return math.Sqrt(sumSqDiff / float64(len(agg.TotalRTTs)))
}

func percentile(sorted []float64, p float64) float64 {
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

func tailAvg(sorted []float64, fraction float64) float64 {
	if fraction <= 0 || fraction >= 1 {
		return 0
	}
	count := int(math.Ceil(fraction * float64(len(sorted))))
	if count < 1 {
		count = 1
	}
	tail := sorted[len(sorted)-count:]
	var sum float64
	for _, v := range tail {
		sum += v
	}
	return sum / float64(len(tail))
}
