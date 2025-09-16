# AI Agent Instructions for CockroachDB

This guide helps AI coding agents understand CockroachDB's architecture, development patterns, and workflows.

## Key Development Commands

The unified `./dev` script is the primary interface for development:

```bash
./dev doctor     # Verify environment setup
./dev build short  # Quick builds without UI
./dev test pkg/[package]  # Run package tests
./dev generate   # Generate code (after proto/schema changes)
./dev lint --short  # Quick linting
```

## Architecture Overview

CockroachDB follows a layered architecture with strict boundaries:

```
SQL Layer (pkg/sql/) → Distributed KV (pkg/kv/) → Storage (pkg/storage/)
```

Key packages and responsibilities:
- `/pkg/sql/` - SQL processing, PostgreSQL compatibility
- `/pkg/kv/` - Distributed transactions (Serializable Snapshot Isolation)
- `/pkg/storage/` - Storage engine integration (RocksDB/Pebble)
- `/pkg/raft/` - Consensus implementation 
- `/pkg/ccl/` - Commercial features

## Essential Patterns

1. **Range-Based Data Distribution**
   - Data split into ~512MB ranges
   - Each range replicated via Raft
   - See `/pkg/kv/kvserver/` for implementation

2. **Transaction Processing**
   - Lock-free transactions with automatic retries
   - Serializable isolation by default
   - Main flow in `/pkg/kv/kv.go`

3. **Code Generation**
   - Protobuf for RPC/storage (`/pkg/roachpb/`)
   - SQL parser/AST (`/pkg/sql/parser/`)
   - Run `./dev generate` after changes

4. **Testing Strategy**
   - Unit tests alongside code
   - SQL logic tests in `/pkg/sql/logictest/`
   - Distributed tests via `roachtest`

## Common Workflows

1. **Adding SQL Features**
   - Parser changes: `/pkg/sql/parser/`
   - Planner changes: `/pkg/sql/sem/tree/` 
   - Executor changes: `/pkg/sql/execinfra/`
   - Add logic tests under `/pkg/sql/logictest/testdata/`

2. **Storage Layer Changes**
   - Storage interfaces in `/pkg/storage/`
   - Range operations in `/pkg/kv/kvserver/`
   - MVCC handling in `/pkg/storage/mvcc/`

3. **Adding APIs**
   - Define protos in `/pkg/roachpb/`
   - Implement RPCs in relevant packages
   - Add integration tests

## Build System 

- Bazel-based, wrapped by `./dev` script
- Dependencies in `DEPS.bzl` and `go.mod`
- Generated code tracked in git
- Enterprise code in `/pkg/ccl/`