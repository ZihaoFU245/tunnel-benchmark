package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type sample struct {
	ts      time.Time
	rssKB   int64
	vsizeKB int64
}

func main() {
	pid := flag.Int("pid", 0, "PID of the process to watch")
	duration := flag.Duration("duration", 500*time.Millisecond, "Sampling interval")
	output := flag.String("output", "memory.csv", "Output CSV file path")
	flag.Parse()

	if *pid <= 0 {
		log.Fatal("--pid is required and must be > 0")
	}

	if _, _, err := readProcMem(*pid); err != nil {
		log.Fatalf("cannot read memory for pid %d: %v", *pid, err)
	}

	f, err := os.Create(*output)
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"timestamp", "vm_rss_kb", "vm_size_kb"})
	w.Flush()

	var samples []sample

	ticker := time.NewTicker(*duration)
	defer ticker.Stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		<-sig
		done <- struct{}{}
	}()

loop:
	for {
		select {
		case <-done:
			break loop
		case t := <-ticker.C:
			rss, vsize, err := readProcMem(*pid)
			if err != nil {
				fmt.Fprintf(os.Stderr, "process %d gone: %v\n", *pid, err)
				break loop
			}
			s := sample{ts: t, rssKB: rss, vsizeKB: vsize}
			samples = append(samples, s)
			w.Write([]string{
				t.Format(time.RFC3339Nano),
				strconv.FormatInt(rss, 10),
				strconv.FormatInt(vsize, 10),
			})
			w.Flush()
		}
	}

	printReport(samples, *pid, *duration)
}

func readProcMem(pid int) (rssKB, vsizeKB int64, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, 0, fmt.Errorf("reading proc status: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "VmRSS:":
			rssKB = val
		case "VmSize:":
			vsizeKB = val
		}
	}
	if rssKB == 0 && vsizeKB == 0 {
		return 0, 0, fmt.Errorf("no memory info found for pid %d (process may not exist)", pid)
	}
	return rssKB, vsizeKB, nil
}

func printReport(samples []sample, pid int, interval time.Duration) {
	if len(samples) == 0 {
		fmt.Fprintln(os.Stderr, "no samples collected")
		return
	}

	n := len(samples)
	elapsed := samples[n-1].ts.Sub(samples[0].ts)

	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("  Memory Watch Report\n")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("  PID: %-6d  Samples: %-6d  Interval: %v  Elapsed: %v\n", pid, n, interval, elapsed.Round(time.Millisecond))
	fmt.Println(strings.Repeat("=", 80))

	printMetric("VmRSS (Resident Set)", samples, func(s sample) int64 { return s.rssKB })
	printMetric("VmSize (Virtual Memory)", samples, func(s sample) int64 { return s.vsizeKB })
}

func printMetric(name string, samples []sample, val func(sample) int64) {
	vals := make([]int64, len(samples))
	for i, s := range samples {
		vals[i] = val(s)
	}

	var total int64
	minV := int64(math.MaxInt64)
	maxV := int64(math.MinInt64)
	for _, v := range vals {
		total += v
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	avg := float64(total) / float64(len(vals))
	startV := vals[0]
	endV := vals[len(vals)-1]
	delta := endV - startV

	pctChange := 0.0
	if startV > 0 {
		pctChange = float64(delta) / float64(startV) * 100
	}

	fmt.Println()
	fmt.Printf("  %s:\n", name)
	fmt.Printf("    Min: %s    Max: %s    Avg: %s\n",
		formatKB(minV), formatKB(maxV), formatKB(int64(avg)))
	fmt.Printf("    Start -> End: %s -> %s  Delta: %+s (%+.1f%%)\n",
		formatKB(startV), formatKB(endV), formatDelta(delta), pctChange)

	var peakUp, peakDown float64
	var ratesSum float64
	var rateCount int
	for i := 1; i < len(samples); i++ {
		dV := float64(vals[i] - vals[i-1])
		dT := samples[i].ts.Sub(samples[i-1].ts).Seconds()
		if dT <= 0 {
			continue
		}
		rate := dV / dT
		ratesSum += rate
		rateCount++
		if rate > peakUp {
			peakUp = rate
		}
		if rate < peakDown {
			peakDown = rate
		}
	}
	avgRate := 0.0
	if rateCount > 0 {
		avgRate = ratesSum / float64(rateCount)
	}

	fmt.Printf("    Growth Rate:  Peak +: %s/s    Peak -: %s/s    Avg: %+s/s\n",
		formatKB(int64(peakUp)), formatKB(int64(math.Abs(peakDown))), formatKBRate(avgRate))

	printSparkline(vals, samples[0].ts, samples[len(samples)-1].ts)
}

func printSparkline(vals []int64, start, end time.Time) {
	const width = 60
	const levels = 8

	blockSet := [9]rune{' ', '\u2581', '\u2582', '\u2583', '\u2584', '\u2585', '\u2586', '\u2587', '\u2588'}

	minV := int64(math.MaxInt64)
	maxV := int64(math.MinInt64)
	for _, v := range vals {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}

	scale := func(v int64) int {
		if maxV == minV {
			return 4
		}
		norm := float64(v-minV) / float64(maxV-minV)
		return int(math.Round(norm * float64(levels)))
	}

	cols := make([]int, width)
	chunk := float64(len(vals)) / float64(width)

	for i := range width {
		lo := int(float64(i) * chunk)
		hi := int(float64(i+1) * chunk)
		if hi > len(vals) {
			hi = len(vals)
		}
		if lo >= len(vals) {
			lo = len(vals) - 1
		}
		if hi <= lo {
			hi = lo + 1
		}

		var sum int64
		for j := lo; j < hi; j++ {
			sum += vals[j]
		}
		avg := sum / int64(hi-lo)
		cols[i] = scale(avg)
	}

	fmt.Printf("\n    Sparkline (%s -> %s):\n", formatKB(minV), formatKB(maxV))

	row := make([]rune, width)
	for i, c := range cols {
		row[i] = blockSet[c]
	}
	fmt.Printf("    %s\n", string(row))

	duration := end.Sub(start).Seconds()
	tickCount := 4
	labels := make([]string, tickCount+1)
	for i := 0; i <= tickCount; i++ {
		t := duration * float64(i) / float64(tickCount)
		labels[i] = fmt.Sprintf("%.1fs", t)
	}
	fmt.Printf("    %s\n\n", strings.Join(labels, "  "))
}

func formatKB(kb int64) string {
	if kb < 0 {
		v := -kb
		s := formatKB(v)
		return "-" + s
	}
	if kb >= 1024*1024 {
		return fmt.Sprintf("%.1f GB", float64(kb)/(1024*1024))
	}
	if kb >= 10*1024 {
		return fmt.Sprintf("%.2f MB", float64(kb)/1024)
	}
	if kb >= 1024 {
		return fmt.Sprintf("%.1f MB", float64(kb)/1024)
	}
	return fmt.Sprintf("%d KB", kb)
}

func formatDelta(delta int64) string {
	if delta >= 0 {
		return "+" + formatKB(delta)
	}
	return formatKB(delta)
}

func formatKBRate(rate float64) string {
	if rate >= 0 {
		return "+" + formatKB(int64(rate))
	}
	return formatKB(int64(rate))
}
