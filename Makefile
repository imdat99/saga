.PHONY: up down test test-chaos lint

up:
	docker compose up -d

down:
	docker compose down -v

# Integration tests start their own Postgres via testcontainers (Docker required).
test:
	go test -race -count=1 ./...

test-chaos:
	go test -race -count=1 -tags chaos ./test/chaos/...

lint:
	golangci-lint run ./...
