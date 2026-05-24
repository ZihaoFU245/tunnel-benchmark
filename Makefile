.PHONY: all build clean server client

all: build

build: client server watcher

client:
	mkdir -p bin
	go build -o bin/stresstest ./cmd/stresstest/

server:
	mkdir -p bin
	go build -o bin/echoserver ./cmd/server/

watcher:
	mkdir -p bin
	go build -o bin/watcher ./cmd/watcher/

clean:
	rm -rf bin/

vet:
	go vet ./...

tidy:
	go mod tidy
