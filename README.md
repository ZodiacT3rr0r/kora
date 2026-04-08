# Kora

A Redis-compatible persistent key-value storage engine built in Go using an LSM-tree architecture.

Kora is a complete, self-contained database engine that demonstrates modern storage system design. It combines a durable write-ahead log, an in-memory skip-list buffer, immutable sorted files, and background compaction to deliver high write throughput while maintaining efficient reads.

## Features

✅ **Redis-Compatible RESP Protocol** — any Redis client (`redis-cli`, client libraries, etc.) can interact with Kora directly.

✅ **Persistent Durability** — Write-Ahead Log ensures data survives process crashes and system failures.

✅ **High Write Throughput** — Sequential log writes and batched in-memory buffering optimize for write-heavy workloads.

✅ **Efficient Point Lookups** — Bloom filters and compaction keep read amplification bounded even at scale.

✅ **TTL / Expiration** — Built-in key expiration with nanosecond precision.

✅ **Crash Recovery** — Automatic WAL replay on startup restores exact state without data loss.

✅ **Concurrent Access** — Safe concurrent reads and writes via RWMutex and goroutine-based background tasks.

## Supported Commands

- `PING` — health check
- `SET key value [EX seconds]` — store with optional TTL
- `GET key` — retrieve value
- `DEL key [key ...]` — delete one or more keys
- `EXISTS key [key ...]` — count existing keys
- `TTL key` — remaining lifetime in seconds
- `DBSIZE` — total key count
- `FLUSHDB` — clear all data
- `KEYS pattern` — pattern-matched key scan
- `SCAN cursor [MATCH pattern] [COUNT n]` — incremental cursor iteration
- `INFO` — server and statistics info

## Building and Running

```bash
# Build the binary
go build ./cmd/kora

# Run the server (listens on :6379 by default)
./kora

# In another terminal, connect with redis-cli
redis-cli
> SET mykey "Hello"
OK
> GET mykey
"Hello"
```

## Architecture

Kora uses a **Log-Structured Merge (LSM)** tree design:

1. **MemTable (Skip List)** — In-memory sorted write buffer. Fast inserts/searches.
2. **WAL (Write-Ahead Log)** — Append-only durable log. Persists writes before acknowledgment.
3. **SSTables (Sorted String Tables)** — Immutable sorted on-disk files, organized by level (L0, L1, L2).
4. **Manifest** — Metadata file tracking SSTable locations and ranges.
5. **Compaction** — Background k-way merge that consolidates overlapping files, reducing read overhead.

For a visual walkthrough, see [docs-kora/diagrams/](docs-kora/diagrams/).

## Project Structure

```
kora/
├── cmd/kora/
│   └── main.go                  # Entry point; TCP server startup
├── internal/
│   ├── engine/
│   │   ├── entry.go             # Entry struct and TTL helpers
│   │   ├── memtable/
│   │   │   ├── skiplist.go      # Ordered in-memory index
│   │   │   └── memtable.go      # Write buffer wrapper
│   │   ├── wal/
│   │   │   └── wal.go           # Durable append log
│   │   ├── sstable/
│   │   │   ├── bloom.go         # Probabilistic filtering
│   │   │   ├── writer.go        # SSTable creation
│   │   │   └── reader.go        # SSTable queries
│   │   ├── manifest/
│   │   │   └── manifest.go      # Metadata persistence
│   │   ├── compaction/
│   │   │   └── merger.go        # K-way merge and compaction
│   │   └── engine.go            # Main engine (Put/Get/Delete)
│   ├── server/
│   │   ├── resp.go              # RESP protocol parser/writer
│   │   ├── handler.go           # Command dispatch
│   │   └── server.go            # TCP accept loop
│   └── util/
│       ├── iterator.go          # Traversal interface
│       └── crc.go               # Corruption detection
├── tests/
│   ├── integration_test.go       # End-to-end tests
│   └── bench_test.go            # Performance benchmarks
├── docs-kora/
│   ├── 01-implementation-playbook.md
│   ├── 02-architecture-and-technical-depth.md
│   ├── 03-project-explainer-for-interviews.md
│   ├── diagrams/                # ASCII diagrams for all subsystems
│   └── glossary.md              # Terminology reference
├── README.md
├── .gitignore
└── go.mod
```

## Documentation

- **[Architecture Deep Dive](docs-kora/02-architecture-and-technical-depth.md)** — Technical explanation of LSM design, write/read paths, compaction strategy, failure recovery.
- **[Implementation Playbook](docs-kora/01-implementation-playbook.md)** — 14-day day-by-day build guide with engineering rules and failure modes.
- **[Interview Explanation](docs-kora/03-project-explainer-for-interviews.md)** — Portfolio pitch and talking points.
- **[Glossary](docs-kora/glossary.md)** — Reference for all terms (skip lists, RESP, LSM, compaction, etc.).
- **[ASCII Diagrams](docs-kora/diagrams/)** — Visual walkthroughs of system flow, write path, read path, flush/compaction, recovery, and on-disk layout.

## Testing

```bash
# Run all unit and integration tests
go test ./...

# Verbose output
go test -v ./...

# Run only benchmarks
go test -bench ./... -run=^$

# Generate coverage report
go test -cover ./...
```

## Performance Characteristics

| Operation | Latency | Throughput | Notes |
|-----------|---------|-----------|-------|
| SET (no TTL) | ~1-5µs (MemTable) | 100k+/sec | Single-threaded; WAL sync is dominant cost |
| GET (hit in MemTable) | ~100ns | millions/sec | No I/O needed |
| GET (L0 SSTable) | ~10-100µs | | Bloom filter + index search + one block read |
| GET (L1+ SSTable) | ~100-1000µs | | Range-pruned; usually one random read |
| Compaction | background | | Continuous merge; configurable concurrency |

(Rough estimates; benchmarks included in `tests/bench_test.go`.)

## Design Decisions

- **Skip List for MemTable** — Ordered, simple to implement, fast concurrent reads with RWMutex.
- **WAL-First Writes** — Durability guaranteed before in-memory acknowledgment.
- **Immutable SSTables** — Enable safe concurrent reads and efficient compaction.
- **Leveled Compaction** — L0→L1→L2 strategy bounds read amplification as size grows.
- **Bloom Filters** — Reduce unnecessary disk I/O for key misses.
- **Tombstones** — Lazy deletion via markers allows incremental compaction without immediate erase.
- **RESP Compatibility** — Existing Redis ecosystem tools require no custom client.

## Why Kora Matters

**For Learning:** Demonstrates how real databases manage durability, in-memory indexing, background maintenance, and network protocols—all in ~5000 lines of Go.

**For Portfolio:** Proves hands-on systems competency. Most portfolio projects *use* databases; this project *builds* one.

**For Production Use** (narrowly): Could be embedded in a Go application needing a persistent KV store with Redis compatibility. Not a drop-in Redis replacement for high-scale systems, but suitable for local storage or moderate-scale embedded scenarios.

## Future Extensions

- Compression (Snappy/LZ4)
- Range queries and partial scans
- Transactions and batching
- Replication and consensus
- Richer metrics and observability

## License

Educational project.
