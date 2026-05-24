.PHONY: all build clean server client

all: build

build: client server

client:
	mkdir -p bin
	go build -o bin/stresstest ./cmd/stresstest/

server:
	mkdir -p bin
	go build -o bin/echoserver ./cmd/server/

clean:
	rm -rf bin/

vet:
	go vet ./...

tidy:
	go mod tidy
