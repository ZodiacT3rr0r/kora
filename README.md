# Kora

A Redis-compatible persistent key-value storage engine built in Go using an LSM-tree architecture.

## Project Overview

Kora demonstrates core database storage engine internals: WAL (Write-Ahead Log), in-memory MemTable with skip list, immutable SSTables, leveled compaction, and RESP protocol server compatibility.

**Technology Stack:**
- Language: Go
- Architecture: LSM-tree (Log-Structured Merge)
- Concurrency: Goroutines and channels
- Protocol: Redis RESP over TCP
- Durability: WAL + atomic manifest updates

## Building and Running

```bash
# Build the binary
go build ./cmd/kora

# Run the server
./kora
```

## Project Structure

```
.
├── cmd/kora/                    # Entry point
├── internal/
│   ├── engine/                  # Core storage engine
│   │   ├── entry.go             # Entry def (Day 1)
│   │   ├── memtable/            # In-memory write buffer
│   │   ├── wal/                 # Write-ahead log
│   │   ├── sstable/             # Immutable sorted files
│   │   ├── manifest/            # Metadata tracking
│   │   ├── compaction/          # Background merge
│   │   └── engine.go            # Main engine
│   ├── server/                  # RESP TCP server
│   └── util/                    # Utilities (CRC, Iterator)
├── tests/                       # Integration tests
└── docs-kora/                   # Architecture documentation
```

## Documentation

- [01-implementation-playbook.md](docs-kora/01-implementation-playbook.md) — 14-day build plan
- [02-architecture-and-technical-depth.md](docs-kora/02-architecture-and-technical-depth.md) — LSM design deep dive
- [03-project-explainer-for-interviews.md](docs-kora/03-project-explainer-for-interviews.md) — Portfolio pitch
- [diagrams/](docs-kora/diagrams/) — Visual architecture ASCII diagrams

## Development Status

**Day 1 (Complete):**
- ✅ Go module scaffolding
- ✅ Core Entry and EntryKind types
- ✅ Iterator interface
- ✅ CRC corruption detection

**Day 2 (Next):**
- Skip list MemTable implementation
- Ordered insert/search/scan

## Learning Path (Beginner-Friendly)

If you're new to databases or Go:
1. Read [docs-kora/glossary.md](docs-kora/glossary.md) for terminology
2. Browse [docs-kora/diagrams/](docs-kora/diagrams/) for visual overviews
3. Follow the 14-day implementation playbook
4. Run tests after each day: `go test ./...`

## Tests

```bash
go test ./...           # Run all tests
go test -v ./...        # Verbose output
go test -bench ./...    # Run benchmarks
```

## License

Educational project.
