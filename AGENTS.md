The purpose of this is to test HTTP CONNECT method under load.

HTTP/2 dep: golang.org/x/net/http2
CLI dep: cobra

How to construct tests, unlike http GET/POST, test target is different.

1. Construct k tunnels
2. Each tunnel will pump m Mbps data at a fixed rate.
3. Use an echo server, that will send the exact data back

ie. For 100 tunnels, pump 1Mb of data each second.

Lists:

1. CLI for control: Use cobra
2. Master process for spawing the k goroutine for requests
3. HTTP CONNECT tunnel go routines
4. Server for echo back data send

What to measure?

1. In-tunnel Jitter. As the data send and receive are the same, we can
   measure the Round Trip Time. We need to know min/max/avg. Latency Variance.

2. Tail Latency. Check for P95, P98, tail 5% and tail 2% latency.

3. Back pressure. Echo server controls can delibrately control, the latency,
   so utilize this, we can measure how tunnel is handling back presssure.

Requirements:

1. Keep zero copy. We do not want data copy to be a bottleneck. The data we
   send can be pre-initialized, and read only, never copied. How to construct
   such data for measuring, that is up to you. The most suitable data is perferred.

2. Do not over bloat single file size. `main.go` maintains an entry. `tunnel/` contains
   each CONNECT tunnel goroutine. `view/` contains the code that finally print out the report.
   The print out report should present rich data, and formatted cleanly.

3. Avoid `net.http` over head. `net.Dial` is preferred for faster binary. 

4. 2 executables. `main` for client side create tunnels, pump data, and log collection.
   `server` for echoing back data. It can control the rate of response send. How long to
   hold the data, etc.

Given:

You are given a https (h2) proxy server on `127.0.0.1:3128`, self signed certificate. Always use
127.0.0.1 not localhost.
