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

## Final Project Layout

```text
cmd/kora/main.go
docs-kora/
	01-implementation-playbook.md
	02-architecture-and-technical-depth.md
	03-project-explainer-for-interviews.md
	07-day-5-bloom.md
	08-days-6-7-sstable.md
	10-day-10-engine-api.md
	12-day-12-compaction-merger.md
	13-day-13-compaction-scheduler.md
	15-interviewer-runbook.md
internal/engine/
	entry.go
	entry_test.go
	bloom/
	compaction/
	core/
	manifest/
	memtable/
	sstable/
	wal/
internal/server/
internal/util/
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

## Interview Runbook

For a complete presenter-style walkthrough, use [docs-kora/15-interviewer-runbook.md](docs-kora/15-interviewer-runbook.md).

## Notes

- Runtime data is written to `./data` by default.
- The server listens on `:6379` by default.
- `go test ./...` currently passes cleanly across all packages.
