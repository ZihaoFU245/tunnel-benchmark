package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"stresstest/internal/h2conn"
	"stresstest/internal/h3conn"
	"stresstest/tunnel"
	"stresstest/view"
)

func main() {
	var (
		tunnels    int
		rate       float64
		duration   time.Duration
		proxy      string
		target     string
		h2multiplex bool
		h3         bool
		h3multiplex bool
	)

	rootCmd := &cobra.Command{
		Use:   "stresstest",
		Short: "HTTP/2 and HTTP/3 CONNECT tunnel stress test",
		Long: `Stress test HTTP CONNECT tunnels through a proxy.
Supports HTTP/2 (TCP) and HTTP/3 (QUIC) transports.
Creates k tunnels, each pumping m Mbps of data, and measures
round-trip latency through an echo server.`,
		Run: func(cmd *cobra.Command, args []string) {
			if tunnels <= 0 {
				log.Fatal("tunnels must be > 0")
			}
			if rate <= 0 {
				log.Fatal("rate must be > 0")
			}

			transport := "HTTP/2"
			mode := "independent connections"
			if h3 || h3multiplex {
				transport = "QUIC (HTTP/3)"
				if h3multiplex {
					mode = "single QUIC connection (h3 multiplex)"
				}
			} else if h2multiplex {
				mode = "single TCP connection (h2 multiplex)"
			}

			fmt.Printf("Starting stress test\n")
			fmt.Printf("  Tunnels:   %d\n", tunnels)
			fmt.Printf("  Rate:      %.2f Mbps per tunnel\n", rate)
			fmt.Printf("  Duration:  %v\n", duration)
			fmt.Printf("  Proxy:     %s\n", proxy)
			fmt.Printf("  Target:    %s\n", target)
			fmt.Printf("  Transport: %s\n", transport)
			fmt.Printf("  Mode:      %s\n", mode)
			fmt.Println()

			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()

			var wg sync.WaitGroup
			allStats := make([]*tunnel.Stats, tunnels)

			if h3multiplex {
				mux, err := h3conn.NewMultiplexedConn(proxy)
				if err != nil {
					log.Fatalf("h3 multiplexed dial: %v", err)
				}
				defer mux.Close()

				for i := 0; i < tunnels; i++ {
					conn, err := mux.Dial(target)
					if err != nil {
						log.Fatalf("tunnel %d h3 mux dial: %v", i, err)
					}
					wg.Add(1)
					go func(id int, c *h3conn.Conn) {
						defer wg.Done()
						cfg := tunnel.NewConfig(id, proxy, target, rate)
						cfg.Conn3 = c
						t := tunnel.NewTunnel(cfg)
						if err := t.Run(ctx); err != nil {
							log.Printf("tunnel %d: %v", id, err)
						}
						allStats[id] = t.Stats()
					}(i, conn)
				}
			} else if h3 {
				for i := 0; i < tunnels; i++ {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						cfg := tunnel.NewConfig(id, proxy, target, rate)
						cfg.UseH3 = true
						t := tunnel.NewTunnel(cfg)
						if err := t.Run(ctx); err != nil {
							log.Printf("tunnel %d: %v", id, err)
						}
						allStats[id] = t.Stats()
					}(i)
				}
			} else if h2multiplex {
				mux, err := h2conn.NewMultiplexedConn(proxy)
				if err != nil {
					log.Fatalf("h2 multiplexed dial: %v", err)
				}
				defer mux.Close()

				for i := 0; i < tunnels; i++ {
					conn, err := mux.Dial(target)
					if err != nil {
						log.Fatalf("tunnel %d h2 mux dial: %v", i, err)
					}
					wg.Add(1)
					go func(id int, c *h2conn.Conn) {
						defer wg.Done()
						cfg := tunnel.NewConfig(id, proxy, target, rate)
						cfg.Conn = c
						t := tunnel.NewTunnel(cfg)
						if err := t.Run(ctx); err != nil {
							log.Printf("tunnel %d: %v", id, err)
						}
						allStats[id] = t.Stats()
					}(i, conn)
				}
			} else {
				for i := 0; i < tunnels; i++ {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						cfg := tunnel.NewConfig(id, proxy, target, rate)
						t := tunnel.NewTunnel(cfg)
						if err := t.Run(ctx); err != nil {
							log.Printf("tunnel %d: %v", id, err)
						}
						allStats[id] = t.Stats()
					}(i)
				}
			}

			wg.Wait()

			agg := tunnel.Aggregate(allStats)
			view.PrintReport(agg, transport)
		},
	}

	rootCmd.Flags().IntVarP(&tunnels, "tunnels", "k", 10, "Number of concurrent tunnels")
	rootCmd.Flags().Float64VarP(&rate, "rate", "m", 1.0, "Data rate per tunnel in Mbps")
	rootCmd.Flags().DurationVarP(&duration, "duration", "d", 10*time.Second, "Test duration")
	rootCmd.Flags().StringVarP(&proxy, "proxy", "p", "127.0.0.1:3128", "Proxy address (host:port)")
	rootCmd.Flags().StringVarP(&target, "target", "t", "127.0.0.1:8080", "Echo target address (host:port)")
	rootCmd.Flags().BoolVar(&h2multiplex, "h2-multiplex", false, "Send all H2 CONNECT tunnels over a single TCP connection")
	rootCmd.Flags().BoolVar(&h3, "h3", false, "Use HTTP/3 (QUIC) transport")
	rootCmd.Flags().BoolVar(&h3multiplex, "h3-multiplex", false, "Use HTTP/3 with a single QUIC connection for all tunnels")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
