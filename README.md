# olive-tree

Hazelcast-inspired distributed data grid playground in Rust.

Library-first project with a runnable example: `book_search_engine`.
The goal is to experiment with cluster membership, partitioned storage, replication, distributed task execution, and full-text search.

## Project Status

Experimental / for fun.
APIs and internals may change quickly.

## Why

I wanted to build a small distributed systems playground in Rust:
- SWIM-like gossip membership over UDP
- partitioned in-memory maps with replication
- distributed task queue/executor
- simple book ingestion + search demo

If you're into distributed systems and Rust, this repo is for you.

## Features

- Cluster membership service (`membership`)
- Partition manager with replication factor (`storage`)
- Distributed key-value maps (`storage`)
- Distributed task queue + worker execution (`executor`)
- Gutenberg ingestion pipeline (`ingestion`)
- Token-based search with score ranking (`search`)
- Health and debug HTTP endpoints

## Architecture

Core library modules:
- `olive_tree::membership`
- `olive_tree::storage`
- `olive_tree::executor`
- `olive_tree::ingestion`
- `olive_tree::search`

Runnable example:
- `examples/book_search_engine.rs`

Networking model:
- `--bind <ip:port>` is UDP gossip address
- HTTP API listens on `bind_port + 1000`

Example:
- gossip `127.0.0.1:5000` => HTTP `127.0.0.1:6000`

## Quick Start

### Requirements

- Rust toolchain (stable)
- `cargo`
- `curl`
- optional: `jq`

### Run a 3-node cluster

Terminal 1:
```bash
cargo run --example book_search_engine -- --bind 127.0.0.1:5000
```

Terminal 2:
```bash
cargo run --example book_search_engine -- --bind 127.0.0.1:5001 --seed 127.0.0.1:5000
```

Terminal 3:
```bash
cargo run --example book_search_engine -- --bind 127.0.0.1:5002 --seed 127.0.0.1:5000
```

## Demo: Book Search Engine

1. Check routes:
```bash
curl -s http://127.0.0.1:6000/health/routes | jq .
```

2. Ingest a Gutenberg book (example: `1342`):
```bash
curl -s -X POST http://127.0.0.1:6000/ingest/1342 | jq .
```

3. Check ingestion status:
```bash
curl -s http://127.0.0.1:6000/ingest/status/1342 | jq .
```

4. Search from another node:
```bash
curl -s "http://127.0.0.1:6001/search?q=darcy&limit=5" | jq .
```

5. Inspect node stats:
```bash
curl -s http://127.0.0.1:6002/health/stats | jq .
```

Note: indexing is async through the distributed queue, so search results can appear with a short delay.

## HTTP API (main endpoints)

- `GET /health/routes`
- `GET /health/stats`
- `POST /ingest/:book_id`
- `GET /ingest/status/:book_id`
- `GET /search?q=<query>&limit=<n>&offset=<n>`
- `POST /books`
- Internal replication/debug endpoints under:
  - `/books/*`
  - `/datalake/*`
  - `/index/*`
  - `/task/*`
  - `/internal/*`

## Configuration

Environment variables:
- `REPLICATION_FACTOR` (default: `2`)
- `MAX_BODY_BYTES` (default: `20971520`, 20 MB)

Example:
```bash
REPLICATION_FACTOR=3 MAX_BODY_BYTES=52428800 cargo run --example book_search_engine -- --bind 127.0.0.1:5000
```

## Development

```bash
cargo fmt
cargo check
cargo test
```

## Roadmap

- Persistent storage backend
- Better consistency and recovery semantics
- Query improvements (ranking, phrase search, filters)
- Better observability/metrics
- Docker Compose local cluster
- CI + benchmarks

## Contributing

PRs, issues, and architecture discussions are welcome.
If you want to contribute, open an issue first for larger changes.

## License

MIT. See `LICENSE`.
