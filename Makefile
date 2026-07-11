.PHONY: all test vet lint

all: vet lint test

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run
