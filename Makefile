.PHONY: build test cover lint install

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

# Rebuild and install deckhand over the copy already on PATH (or via `go
# install` if none). Atomic replace so it's safe while the daemon is running.
# Handy after pulling a merge: `git pull && make install`.
install:
	@dst="$$(command -v deckhand)"; \
	if [ -z "$$dst" ]; then go install ./cmd/deckhand && echo "installed via go install"; exit 0; fi; \
	go build -o "$$dst.new" ./cmd/deckhand && mv "$$dst.new" "$$dst" && echo "updated $$dst"
