.PHONY: all test vet lint bench

all: vet lint test

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run

bench:
	go test -run '^$$' -bench BenchmarkClaimComplete -benchtime 2000x .
