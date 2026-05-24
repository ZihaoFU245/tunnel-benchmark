package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	delay := flag.Duration("delay", 0, "delay before echoing back data")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	fmt.Printf("echo server listening on %s (delay=%v)\n", *addr, *delay)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn, *delay)
	}
}

func handleConn(conn net.Conn, delay time.Duration) {
	defer conn.Close()
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("read: %v", err)
			}
			return
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		if _, err := conn.Write(buf[:n]); err != nil {
			log.Printf("write: %v", err)
			return
		}
	}
}
