# Kora

Kora is a Redis-compatible persistent key-value database built in Go with an LSM-tree storage engine.

It includes a write-ahead log, in-memory skip list memtable, SSTable persistence, manifest tracking, flush and compaction pipelines, and a RESP TCP server.

## What It Supports

- `PING`
- `SET key value [EX seconds]`
- `GET key`
- `DEL key`
- `FLUSH`
- `COMPACT`

## Quick Start

```bash
cd /home/cubinguy/idk/Projects/kora
go test ./...
go build ./cmd/kora
./kora
```

In another terminal:

```bash
redis-cli -p 6379
PING
SET demo hello
GET demo
DEL demo
GET demo
```

## Testing

```bash
go test ./...
go test ./... -v
```

The full suite covers:
- engine helpers
- memtable and skip list behavior
- WAL append and recovery
- SSTable write/read paths
- manifest persistence
- flush and compaction
- RESP parsing and TCP server commands

## Notes

- Runtime data is written to `./data` by default.
- The server listens on `:6379` by default.
- `go test ./...` currently passes cleanly across all packages.
