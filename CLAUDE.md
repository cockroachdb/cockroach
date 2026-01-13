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

## Coding Guidelines

### Log and Error Redactability

CockroachDB implements redactability to ensure sensitive information (PII, confidential data) is automatically removed or marked in log messages and error outputs. This enables customers to safely share logs with support teams.

#### Core Concepts

**Safe vs Unsafe Data:**
- **Safe data**: Information certainly known to NOT be PII-laden (node IDs, range IDs, error codes)
- **Unsafe data**: Information potentially PII-laden or confidential (user data, SQL statements, keys)

**Redactable Strings:**
- Unsafe data is enclosed in Unicode markers: `‹unsafe_data›`
- Safe data appears without markers
- Log entries show a special indicator: `⋮` (vertical ellipsis)

#### Key Implementation Patterns

**SafeValue Interface** - For types that are always safe:
```go
type NodeID int32
func (n NodeID) SafeValue() {}  // Always safe to log

// Interface verification pattern
var _ redact.SafeValue = NodeID(0)
```

**SafeFormatter Interface** - For complex types mixing safe/unsafe data:
```go
func (s *ComponentStats) SafeFormat(w redact.SafePrinter, _ rune) {
    w.Printf("ComponentStats{ID: %v", s.Component)
    // Use w.Printf(), w.SafeString(), w.SafeRune() to mark safe parts
}
```

#### Common APIs

- `redact.Safe(value)` - Mark a value as safe
- `redact.SafeString(s)` - Mark string literal as safe

#### Redactcheck Linter

The linter in `/pkg/testutils/lint/passes/redactcheck/redactcheck.go`:
- Maintains allowlist of types permitted to implement `SafeValue`
- Validates `RegisterSafeType` calls
- Prevents accidental marking of sensitive types as safe

To add a new safe type:
1. Implement `SafeValue()` method
2. Add interface verification: `var _ redact.SafeValue = TypeName{}`
3. Update redactcheck allowlist if needed

#### Key Files

- `/pkg/util/log/redact.go` - Core redaction logic
- `/docs/RFCS/20200427_log_file_redaction.md` - Design RFC
- `/pkg/testutils/lint/passes/redactcheck/redactcheck.go` - Linter implementation
- `/pkg/util/log/redact_test.go` - Test examples and patterns

### Code Formatting with crlfmt

CockroachDB uses a custom code formatter called `crlfmt` that goes beyond standard Go formatting tools to enforce project-specific style guidelines.

#### What is crlfmt

`crlfmt` is CockroachDB's **custom Go code formatter** (not a wrapper around standard tools) that enforces specific coding standards beyond what `gofmt` and `goimports` provide. It's an external tool developed specifically for CockroachDB's needs.

**Repository**: `github.com/cockroachdb/crlfmt`

#### Key Features

**Enhanced Formatting:**
- **Column wrapping**: Wraps code at 100 columns (vs. Go's typical 80 or unlimited)
- **Tab width**: Uses 2-space tabs by default (`-tab 2`)
- **Import grouping**: Intelligent import organization (`-groupimports`)
- **Line length enforcement**: Strict 100-character line limits

**Operation Modes:**
- **Write mode**: Overwrite files in place (`-w`)
- **Diff mode**: Show differences only (`-diff`, default)
- **Fast mode**: Skip `goimports` for faster operation (`-fast`)

#### Common Usage Patterns

**Basic Formatting:**
```bash
# Format a single file (most common)
crlfmt -w filename.go

# Format with CockroachDB standard settings
crlfmt -w -tab 2 filename.go

# Check formatting without writing changes
crlfmt -diff filename.go
```

**Note:** `crlfmt` only accepts one filename at a time. To format multiple files, use `xargs` or a loop:
```bash
# Format multiple files with xargs
find pkg/sql -name "*.go" | xargs -n1 crlfmt -w

# Or use a loop
for f in pkg/sql/*.go; do crlfmt -w "$f"; done
```

#### Configuration Options

**Standard Settings:**
- **Tab width**: 2 spaces (`-tab 2`)
- **Column wrap**: 100 characters (`-wrap 100`)
- **Import grouping**: Enabled by default
- **Source directory**: For import resolution (`-srcdir`)

**Performance Options:**
- **Fast mode**: Skip `goimports` (`-fast`)
- **Ignore patterns**: Skip matching files (`-ignore regex`)

#### Best Practices

**Development Workflow:**
1. **Always use `-w`**: Write changes to files rather than just showing diffs
2. **Use `-tab 2`**: Maintain consistency with CockroachDB standards
3. **Run after changes**: Format code after making modifications
4. **Run after generation**: Format generated code to match standards
5. **Pre-commit formatting**: Run before committing to avoid CI failures

**When to Use:**
- After making any code changes
- After running code generation
- When lint tests fail due to formatting
- As part of regular development workflow
- Before submitting pull requests

`crlfmt` is essential for maintaining CockroachDB's code quality standards and should be part of every developer's regular workflow. It ensures consistent formatting across the large, complex codebase while handling the specific requirements of a distributed systems project.

### Go Coding Guidelines

CockroachDB follows specific Go coding conventions inspired by the Uber Go style guide with CockroachDB-specific modifications.

#### Pointers and Interfaces

**Pointers to Interfaces:**
- Almost never need a pointer to an interface
- Pass interfaces as values—underlying data can still be a pointer
- Interface contains: type-specific information pointer + data pointer

**Receivers and Interfaces:**
```go
type S struct {
    data string
}

// Value receiver - can be called on pointers and values
func (s S) Read() string {
    return s.data
}

// Pointer receiver - needed to modify data
func (s *S) Write(str string) {
    s.data = str
}

// Interface verification pattern
var _ redact.SafeValue = NodeID(0)
```

#### Memory Management and Concurrency

**Mutexes:**
```go
// Zero-value mutex is valid
var mu sync.Mutex
mu.Lock()

// Embed mutex in struct (preferred for private types)
type smap struct {
    sync.Mutex
    data map[string]string
}

// Named field for exported types
type SMap struct {
    mu   sync.Mutex
    data map[string]string
}
```

**Defer for Cleanup:**
```go
// Always use defer for locks and cleanup
func (m *SMap) Get(k string) string {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.data[k]
}

// For panic protection, use separate function
func myFunc() error {
    doRiskyWork := func() error {
        p.Lock()
        defer p.Unlock()
        return somethingThatCanPanic()
    }
    return doRiskyWork()
}
```

**Slice and Map Copying:**
```go
// Document if API captures by reference
// SetTrips sets the driver's trips.
// Note that the slice is captured by reference, the
// caller should take care of preventing unwanted aliasing.
func (d *Driver) SetTrips(trips []Trip) { d.trips = trips }

// Or make defensive copy
func (d *Driver) SetTrips(trips []Trip) {
    d.trips = make([]Trip, len(trips))
    copy(d.trips, trips)
}

// Return copies for internal state
func (s *Stats) Snapshot() map[string]int {
    s.Lock()
    defer s.Unlock()
    result := make(map[string]int, len(s.counters))
    for k, v := range s.counters {
        result[k] = v
    }
    return result
}
```

#### Performance Guidelines

**String Conversion:**
```go
// strconv is faster than fmt for primitives
s := strconv.Itoa(rand.Int()) // Good
s := fmt.Sprint(rand.Int())   // Slower
```

**String-to-Byte Conversion:**
```go
// Avoid repeated conversion
data := []byte("Hello world")      // Good - once
for i := 0; i < b.N; i++ {
    w.Write(data)
}

// Bad - repeated allocation
for i := 0; i < b.N; i++ {
    w.Write([]byte("Hello world"))
}
```

**Channels:**
- Size should be one or unbuffered (zero)
- Any other size requires high scrutiny

**Enums:**
```go
// Start enums at one unless zero value is meaningful
type Operation int
const (
    Add Operation = iota + 1
    Subtract
    Multiply
)
```

#### Code Style Standards

**Line Length and Wrapping:**
- Code: 100 columns, Comments: 80 columns
- Tab width: 2 characters

**Function Signatures:**
```go
func (s *someType) myFunctionName(
    arg1 somepackage.SomeArgType, arg2 int, arg3 somepackage.SomeOtherType,
) (somepackage.SomeReturnType, error) {
    // ...
}

// One argument per line for long lists
func (s *someType) myFunctionName(
    arg1 somepackage.SomeArgType,
    arg2 int,
    arg3 somepackage.SomeOtherType,
) (somepackage.SomeReturnType, error) {
    // ...
}
```

**Import Grouping:**
```go
import (
    // Standard library
    "fmt"
    "os"

    // Everything else
    "go.uber.org/atomic"
    "golang.org/x/sync/errgroup"
)
```

**Variable Declarations:**
```go
// Top-level: omit type if clear from function return
var _s = F()

// Local: use short declaration
s := "foo"

// Empty slices: prefer var declaration
var filtered []int
// Over: filtered := []int{}

// nil is valid slice
return nil // Not return []int{}

// Check empty with len(), not nil comparison
func isEmpty(s []string) bool {
    return len(s) == 0 // Not s == nil
}
```

**Struct Initialization:**
```go
// Always specify field names
k := User{
    FirstName: "John",
    LastName: "Doe",
    Admin: true,
}

// Use &T{} instead of new(T)
sptr := &T{Name: "bar"}
```

**Bool Parameters:**
```go
// Avoid naked bools - use comments or enums
printInfo("foo", true /* isLocal */, true /* done */)

// Better: custom types
type EndTxnAction bool
const (
    Commit EndTxnAction = false
    Abort  = true
)
func endTxn(action EndTxnAction) {}
```

**Error Handling:**
```go
// Reduce variable scope
if err := f.Close(); err != nil {
    return err
}

// Reduce nesting - handle errors early
for _, v := range data {
    if v.F1 != 1 {
        log.Printf("Invalid v: %v", v)
        continue
    }
    v = process(v)
    if err := v.Call(); err != nil {
        return err
    }
    v.Send()
}
```

**Printf and Formatting:**
```go
// Format strings should be const for go vet
const msg = "unexpected values %v, %v\n"
fmt.Printf(msg, 1, 2)

// Printf-style function names should end with 'f'
func Wrapf(format string, args ...interface{}) error

// Use raw strings to avoid escaping
wantError := `unknown error:"test"`
```

#### SQL Column Naming Standards

**Principles for SHOW statements and virtual tables:**
- **Consistent casing** across all statements
- **Same concept = same word**: `variable`/`value` between different SHOW commands
- **Usable without quotes**: `start_key` not `"Start Key"`, avoid SQL keywords
- **Underscore separation**: `start_key` not `startkey` (consistent with information_schema)
- **Avoid abbreviations**: `id` OK (common), `loc` not OK (use `location`)
- **Disambiguate primary key columns**: `zone_id`, `job_id`, `table_id` not just `id`
- **Specify handle type**: `table_name` vs `table_id` to allow both in future
- **Match information_schema**: Use same labels when possible and appropriate

#### Testing Patterns

**Table-Driven Tests:**
```go
tests := []struct{
    give     string
    wantHost string
    wantPort string
}{{
    give:     "192.0.2.0:8000",
    wantHost: "192.0.2.0",
    wantPort: "8000",
}, {
    give:     ":8000",
    wantHost: "",
    wantPort: "8000",
}}

for _, tt := range tests {
    t.Run(tt.give, func(t *testing.T) {
        host, port, err := net.SplitHostPort(tt.give)
        require.NoError(t, err)
        assert.Equal(t, tt.wantHost, host)
        assert.Equal(t, tt.wantPort, port)
    })
}
```

**Functional Options Pattern:**
```go
type Option interface {
    apply(*options)
}

type optionFunc func(*options)
func (f optionFunc) apply(o *options) { f(o) }

func WithTimeout(t time.Duration) Option {
    return optionFunc(func(o *options) {
        o.timeout = t
    })
}

func Connect(addr string, opts ...Option) (*Connection, error) {
    options := options{
        timeout: defaultTimeout,
        caching: defaultCaching,
    }
    for _, o := range opts {
        o.apply(&options)
    }
    // ...
}
```

### Code Commenting Guidelines

#### Engineering Standards (Enforced in Reviews)

**Requirements:**
- **Full sentences**: Capital letter at beginning, period at end, subject and verb
- **More comments than code** in struct, API, and protobuf definitions
- **Field lifecycle documentation**: What initializes, accesses, when obsolete
- **Function phases**: Separate different processing phases with summary comments
- **Grammar matters**: Make honest attempt, reviewers prefix grammar suggestions with "nit:"

**Prohibited:**
- **Reading code aloud**: Don't write "increments x" for `x++`
- **Obvious iterations**: Don't write "iterate over strings" for `for i := range strs`
- **Over-emphasis on grammar**: Prefix with "nit:" in reviews

#### Comment Types and Examples

**Top-Level Design Comments:**
Explain concepts/abstractions, show how pieces fit together, connect to use cases.

Example from `concurrency/concurrency_control.go`:
```go
// Package concurrency provides a concurrency manager that coordinates
// access to keys and key ranges. The concurrency manager sequences
// concurrent txns that access overlapping keys and ensures that locks
// are respected and txn isolation guarantees are upheld.
//
// The concurrency manager is structured as a two-level hierarchy...
```

**API and Interface Comments:**
```go
// AuthConn is the interface used by the authenticator for interacting with the
// pgwire connection.
type AuthConn interface {
    // SendAuthRequest sends a request for authentication information. After
    // calling this, the authenticator needs to call GetPwdData() quickly, as the
    // connection's goroutine will be blocked on providing us the requested data.
    SendAuthRequest(authType int32, data []byte) error

    // GetPwdData returns authentication info that was previously requested with
    // SendAuthRequest. The call blocks until such data is available.
    // An error is returned if the client connection dropped or if the client
    // didn't respect the protocol.
    GetPwdData() ([]byte, error)
}
```

**Function Comments:**
```go
// Append appends the provided string and any number of query parameters.
// Instead of using normal placeholders (e.g. $1, $2), use meta-placeholder $.
// This method rewrites the query so that it uses proper placeholders.
//
// For example, suppose we have the following calls:
//
//   query.Append("SELECT * FROM foo WHERE a > $ AND a < $ ", arg1, arg2)
//   query.Append("LIMIT $", limit)
//
// The query is rewritten into:
//
//   SELECT * FROM foo WHERE a > $1 AND a < $2 LIMIT $3
//   /* $1 = arg1, $2 = arg2, $3 = limit */
//
// Note that this method does NOT return any errors. Instead, we queue up
// errors, which can later be accessed.
func (q *sqlQuery) Append(s string, params ...interface{}) { /* ... */ }
```

**Struct Field Comments:**
```go
// cliState defines the current state of the CLI during command-line processing.
//
// Note: options customizable via \set and \unset should be defined in
// sqlCtx or cliCtx instead, so that the configuration remains global
// across multiple instances of cliState.
type cliState struct {
    // forwardLines is the array of lookahead lines. This gets
    // populated when there is more than one line of input
    // in the data read by ReadLine(), which can happen
    // when copy-pasting.
    forwardLines []string

    // partialStmtsLen represents the number of entries in partialLines
    // parsed successfully so far. It grows larger than zero whenever 1)
    // syntax checking is enabled and 2) multi-statement entry starts.
    partialStmtsLen int
}
```

**Phase Comments in Function Bodies:**
```go
func (r *Replica) executeAdminCommandWithDescriptor(
    ctx context.Context, updateDesc func(*roachpb.RangeDescriptor) error,
) *roachpb.Error {
    // Retry forever as long as we see errors we know will resolve.
    retryOpts := base.DefaultRetryOptions()

    for retryable := retry.StartWithCtx(ctx, retryOpts); retryable.Next(); {
        // The replica may have been destroyed since the start of the retry loop.
        // We need to explicitly check this condition.
        if _, err := r.IsDestroyed(); err != nil {
            return roachpb.NewError(err)
        }

        // Admin commands always require the range lease to begin, but we may
        // have lost it while in this retry loop. Without the lease, a replica's
        // local descriptor can be arbitrarily stale.
        if _, pErr := r.redirectOnOrAcquireLease(ctx); pErr != nil {
            return pErr
        }
    }
}
```

**Protobuf Message Comments:**
```go
// ReplicaType identifies which raft activities a replica participates in. In
// normal operation, VOTER_FULL and LEARNER are the only used states. However,
// atomic replication changes require a transition through a "joint config"; in
// this joint config, the VOTER_DEMOTING and VOTER_INCOMING types are used as
// well to denote voters which are being downgraded to learners and newly added
// by the change, respectively.
enum ReplicaType {
    // VOTER_FULL indicates a replica that is a voter both in the
    // incoming and outgoing set.
    VOTER_FULL = 0;

    // VOTER_INCOMING indicates a voting replica that will be a
    // VOTER_FULL once the ongoing atomic replication change is finalized;
    // that is, it is in the process of being added.
    VOTER_INCOMING = 2;
}
```

#### Comment Maintenance

**When to Update Comments:**
- Add explanations when you discover valuable missing knowledge
- Fix factually incorrect comments immediately (treat as bugs)
- Fix grammar/spelling that significantly impairs reading
- Avoid cosmetic changes that don't improve understanding

**Review Guidelines:**
- Point out when missing comments would help understanding
- Add comments explaining review discussion outcomes
- Prefix grammar suggestions with "nit:" to indicate low priority

### Error Handling

CockroachDB uses the [github.com/cockroachdb/errors](https://github.com/cockroachdb/errors) library, a superset of Go's standard `errors` package and `pkg/errors`.

#### Creating Errors

```go
// Simple static string errors
errors.New("connection failed")

// Formatted error strings
errors.Newf("invalid value: %d", val)

// Assertion failures for implementation bugs (generates louder alerts)
errors.AssertionFailedf("expected non-nil pointer")
```

#### Wrapping and Adding Context

It can be helpful to add context when propagating errors up the call stack:

```go
// Wrap with context (preferred over fmt.Errorf with %w)
return errors.Wrap(err, "opening file")

// Wrap with formatted context
return errors.Wrapf(err, "connecting to %s", addr)
```

Keep context succinct; avoid phrases like "failed to" which pile up:

```go
// Bad: "failed to x: failed to y: failed to create store: the error"
return errors.Newf("failed to create new store: %s", err)

// Good: "x: y: new store: the error"
return errors.Wrap(err, "new store")
```

#### User-Facing Information

```go
// Add hints for end-users (actionable guidance, excluded from Sentry)
errors.WithHint(err, "check your network connection")

// Add details for developers (contextual info, excluded from Sentry)
errors.WithDetail(err, "request payload was 2.5MB")
```

#### Detecting and Handling Errors

For errors that clients need to detect, use sentinel errors or custom types:

```go
// Sentinel error pattern
var ErrNotFound = errors.New("not found")

func Find(id string) error {
    return ErrNotFound
}

// Caller detection with errors.Is (works across network boundaries!)
if errors.Is(err, ErrNotFound) {
    // handle not found
}
```

For errors with additional information, use custom types:

```go
type NotFoundError struct {
    Resource string
}

func (e *NotFoundError) Error() string {
    return fmt.Sprintf("%s not found", e.Resource)
}

// Caller detection with errors.As
var nfErr *NotFoundError
if errors.As(err, &nfErr) {
    log.Printf("missing: %s", nfErr.Resource)
}
```

#### Error Propagation Options

| Scenario | Approach |
|----------|----------|
| No additional context needed | Return original error |
| Adding context | Use `errors.Wrap` or `errors.Wrapf` |
| Passing through goroutine channel | Use `errors.WithStack` on both ends |
| Callers don't need to detect this error | Use `errors.Newf` |
| Hide original cause | Use `errors.Handled` or `errors.Opaque` |

#### Safe Details for PII Protection

Error messages are redacted by default in Sentry reports. Mark data as safe explicitly:

```go
// Mark specific values as safe for reporting
errors.WithSafeDetails(err, "node_id=%d", nodeID)

// The Safe() wrapper for known-safe values
errors.Newf("processing %s", errors.Safe(operationName))
```

#### Type Assertions

Always use the "comma ok" idiom to avoid panics:

```go
// Bad - panics on wrong type
t := i.(string)

// Good - handles gracefully
t, ok := i.(string)
if !ok {
    return errors.New("expected string type")
}
```

#### Key Files

- `/docs/RFCS/20190318_error_handling.md` - Error handling RFC
- [cockroachdb/errors README](https://raw.githubusercontent.com/cockroachdb/errors/refs/heads/master/README.md) - Full API documentation

## Special Considerations

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

# Interaction Style

* Be direct and honest.
* Skip unnecessary acknowledgments.
* Correct me when I'm wrong and explain why.
* Suggest better alternatives if my ideas can be improved.
* Focus on accuracy and efficiency.
* Challenge my assumptions when needed.
* Prioritize quality information and directness.
