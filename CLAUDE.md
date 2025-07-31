# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## CockroachDB Development Environment

CockroachDB is a distributed SQL database written in Go, built with Bazel and managed through a unified `./dev` tool.

### Essential Commands

**Setup and Environment Check:**
```bash
./dev doctor              # Verify development environment setup
```

**Building:**
```bash
./dev build cockroach     # Build full cockroach binary
./dev build short         # Build cockroach without UI (faster)
./dev build roachtest     # Build integration test runner
./dev build workload      # Build load testing tool
```

**Testing:**
```bash
./dev test pkg/sql                   # Run unit tests for SQL package
./dev test pkg/sql -f=TestParse -v   # Run specific test pattern.
./dev test pkg/sql --race            # Run with race detection
./dev test pkg/sql --stress          # Run repeatedly until failure
./dev testlogic                      # Run all SQL logic tests
./dev testlogic ccl                  # Run enterprise logic tests
./dev testlogic base --config=local --files='prepare|fk' # Run specific test files under a specific configuration
```

Note that when filtering tests via `-f` to include the `-v` flag which
will warn you in the output if your filter didn't match anything. Look
for `testing: warning: no tests to run` in the output.

**Code Generation and Linting:**
```bash
./dev generate            # Generate all code (protobuf, parsers, etc.)
./dev generate go         # Generate Go code only
./dev generate bazel      # Update BUILD.bazel files when dependencies change
./dev generate protobuf   # Generate files based on protocol buffer definitions
./dev lint                # Run all linters (only run this when requested)
./dev lint --short        # Run fast subset of linters (only run this when requested)
```

### Architecture Overview

CockroachDB follows a layered architecture:
```
SQL Layer (pkg/sql/) → Distributed KV (pkg/kv/) → Storage (pkg/storage/)
```

**Key Components:**
- **SQL Engine**: `/pkg/sql/` - Complete PostgreSQL-compatible SQL processing
- **Transaction Layer**: `/pkg/kv/` - Distributed transactions with Serializable Snapshot Isolation
- **Storage Engine**: `/pkg/storage/` - RocksDB/Pebble integration with MVCC
- **Consensus**: `/pkg/raft/` - Raft protocol for data replication
- **Networking**: `/pkg/rpc/`, `/pkg/gossip/` - RPC and cluster coordination
- **Enterprise Features**: `/pkg/ccl/` - Commercial features (backup, restore, multi-tenancy)

**Key Design Patterns:**
- Range-based data partitioning (512MB default ranges)
- Raft consensus per range for strong consistency
- Lock-free transactions with automatic retry handling
- Multi-tenancy with virtual clusters

### Development Workflow

1. **Environment Setup**: Run `./dev doctor` to ensure all dependencies are installed.
2. **Building**: Use `./dev build short` for iterative development, `./dev build cockroach` for full builds.
3. **Testing**: Run package-specific tests with `./dev test pkg/[package]`.
4. **Code Generation**: After schema/proto changes, run `./dev generate go`.
5. **Linting**: Run with `./dev lint` or `./dev lint --short`. This takes a while, so no need to run it regularly.

### Testing Strategy

CockroachDB has comprehensive testing infrastructure:
- **Unit Tests**: Standard Go tests throughout `/pkg/` packages.
- **Logic Tests**: SQL correctness tests using `./dev testlogic`.
- **Roachtests**: Distributed system integration tests.
- **Acceptance Tests**: End-to-end testing in `/pkg/acceptance/`.
- **Stress Testing**: Continuous testing with `--stress` flag.


### Build System

- **Primary Tool**: Bazel (wrapped by `./dev` script)
- **Cross-compilation**: Support for Linux, macOS, Windows via `--cross` flag
- **Caching**: Distributed build caching for faster builds
- **Multiple Binaries**: Produces `cockroach`, `roachprod`, `workload`, `roachtest`, etc.

### Code Organization

**Package Structure:**
- `/pkg/sql/` - SQL layer (parser, optimizer, executor)
- `/pkg/sql/opt` - Query optimizer and planner
- `/pkg/sql/schemachanger` - Declarative schema changer
- `/pkg/kv/` - Key-value layer and transaction management
- `/pkg/storage/` - Storage engine interface
- `/pkg/server/` - Node and cluster management
- `/pkg/ccl/` - Enterprise/commercial features
- `/pkg/util/` - Shared utilities across the codebase
- `/docs/` - Technical documentation and RFCs

**Generated Code:**
Large portions of the codebase are generated, particularly:
- SQL parser from Yacc grammar
- Protocol buffer definitions
- Query optimizer rules
- Various code generators in `/pkg/gen/`

Always run `./dev generate` after modifying `.proto` files, SQL grammar, or optimizer rules.

### Special Considerations

- **Bazel Integration**: All builds must go through Bazel - do not use `go build` or `go test` directly
- **SQL Compatibility**: Maintains PostgreSQL wire protocol compatibility
- **Multi-Version Support**: Handles mixed-version clusters during upgrades
- **Performance Critical**: Many components are highly optimized with careful attention to allocations and CPU usage

### Resources

- **Main Documentation**: https://cockroachlabs.com/docs/stable/
- **Architecture Guide**: https://www.cockroachlabs.com/docs/stable/architecture/overview.html
- **Contributing**: See `/CONTRIBUTING.md` and https://wiki.crdb.io/
- **Design Documents**: `/docs/design.md` and `/docs/tech-notes/`

### When generating PRs and commit records

- Follow the format:
  - Separate the subject from the body with a blank line.
  - Use the body of the commit record to explain what existed before your change, what you changed, and why.
  - Require the user to specify whether or not there should be release notes. Release notes should be specified after the body, following "Release Notes:".
  - When writing release notes, please follow the guidance here: https://cockroachlabs.atlassian.net/wiki/spaces/CRDB/pages/186548364/Release+notes
  - Require the user to specify an epic number (or None) which should be included at the bottom of the commit record following "Epic:".
  - Prefix the subject line with the package in which the bulk of the changes occur.
  - For multi-commit PRs, summarize each commit in the PR record.
  - Do not include a test plan unless explicitly asked by the user.
