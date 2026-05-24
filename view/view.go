package view

import (
	"fmt"
	"os"
	"text/tabwriter"

	"stresstest/tunnel"
)

func PrintReport(agg *tunnel.AggregateStats, transport string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	title := "HTTP/2"
	if transport != "" {
		title = transport
	}

	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "═══════════════════════════════════════════════════════════")
	fmt.Fprintf(w, "  %s CONNECT TUNNEL STRESS TEST REPORT\n", title)
	fmt.Fprintln(w, "═══════════════════════════════════════════════════════════")
	fmt.Fprintln(w, "")

	fmt.Fprintf(w, "  Tunnels:\t%d\n", agg.TunnelCount)
	fmt.Fprintf(w, "  Duration:\t%v\n", agg.Duration)
	fmt.Fprintf(w, "  Samples:\t%d\n", len(agg.TotalRTTs))
	fmt.Fprintf(w, "  Errors:\t%d\n", agg.TotalErrors)
	fmt.Fprintln(w, "")

	fmt.Fprintf(w, "  Bytes Sent:\t%s\n", formatBytes(agg.TotalBytesSent))
	fmt.Fprintf(w, "  Bytes Recv:\t%s\n", formatBytes(agg.TotalBytesRecv))
	if agg.Duration.Seconds() > 0 {
		throughput := float64(agg.TotalBytesSent*8) / agg.Duration.Seconds() / 1e6
		fmt.Fprintf(w, "  Throughput:\t%.2f Mbps\n", throughput)
	}
	fmt.Fprintln(w, "")

	fmt.Fprintln(w, "  ─────────── Latency (ms) ───────────")
	fmt.Fprintln(w, "")

	min, max, avg, p50, p95, p98, p99 := agg.Percentiles()
	tail5, tail2 := agg.TailPercentiles()
	stddev := agg.LatencyVariance()

	fmt.Fprintf(w, "  Min:\t%.3f\n", min)
	fmt.Fprintf(w, "  Max:\t%.3f\n", max)
	fmt.Fprintf(w, "  Avg:\t%.3f\n", avg)
	fmt.Fprintf(w, "  P50:\t%.3f\n", p50)
	fmt.Fprintf(w, "  P95:\t%.3f\n", p95)
	fmt.Fprintf(w, "  P98:\t%.3f\n", p98)
	fmt.Fprintf(w, "  P99:\t%.3f\n", p99)
	fmt.Fprintln(w, "")

	fmt.Fprintf(w, "  Tail 5%% Avg:\t%.3f\n", tail5)
	fmt.Fprintf(w, "  Tail 2%% Avg:\t%.3f\n", tail2)
	fmt.Fprintf(w, "  Latency Variance:\t%.3f\n", stddev)
	fmt.Fprintln(w, "")

	fmt.Fprintln(w, "═══════════════════════════════════════════════════════════")
	w.Flush()
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
