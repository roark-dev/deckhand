.PHONY: build test cover lint

build:
	go build ./...

test:
	go test -race ./...

# The enforced coverage gate (also run in CI). Floors live in the script.
cover:
	bash scripts/coverage.sh

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"
