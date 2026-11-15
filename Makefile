.PHONY: up down test test-chaos lint lint-fix fmt

up:
	docker compose up -d --wait

down:
	docker compose down -v

# Integration tests use the Postgres of docker compose (make up), or SAGA_TEST_DSN.
test:
	go test -race -count=1 ./...

test-chaos:
	go test -race -count=1 -tags chaos ./test/chaos/...

lint:
	golangci-lint run ./...

lint-fix:
	golangci-lint run --fix ./...

fmt:
	golangci-lint fmt ./...
