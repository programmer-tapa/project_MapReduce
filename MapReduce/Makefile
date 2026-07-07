.PHONY: build test proto run-coordinator run-worker compose-up compose-down clean

# ── Build ─────────────────────────────────────────────────────────────────────
build:
	go build -o bin/coordinator ./cmd/coordinator
	go build -o bin/worker ./cmd/worker

# ── Test ──────────────────────────────────────────────────────────────────────
test:
	go test -v -race ./internal/...

bench:
	go test -bench=. -benchmem ./internal/...

# ── Protobuf Code Generation ─────────────────────────────────────────────────
proto:
	protoc \
		--go_out=. --go_opt=module=mapreduce \
		--go-grpc_out=. --go-grpc_opt=module=mapreduce \
		proto/mapreduce.proto

# ── Local Development ────────────────────────────────────────────────────────
run-coordinator:
	go run ./cmd/coordinator --addr :9090 --input testdata/*.txt --nreduce 10

run-worker:
	go run ./cmd/worker --coordinator-addr localhost:9090 --app examples/wordcount.py

# ── Docker Compose ───────────────────────────────────────────────────────────
compose-up:
	docker compose -f deployments/docker/docker-compose.yml up --build -d

compose-down:
	docker compose -f deployments/docker/docker-compose.yml down -v

# ── Cleanup ──────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/
	rm -f mr-*
	rm -f mr-out-*
