---
globs: ["**/*.go", "**/*.proto"]
---

# CockroachDB Commenting Standards

## Block vs Inline Comments

**Block comments** (standalone line) use full sentences with capitalization and punctuation:
```go
// Bad - panics on wrong type.
t := i.(string)

// Good - handles gracefully.
t, ok := i.(string)
```

**Inline comments** (end of code line) are lowercase without terminal punctuation:
```go
s := strconv.Itoa(rand.Int()) // good
func (n NodeID) SafeValue() {}  // always safe to log
```

## Comment Placement Principles

**Data Structure Comments:**
- Belong at the **data structure declaration**
- Explain the purpose, lifecycle, and invariants of the struct/type
- Document which code initializes fields, which code accesses them, and when they become obsolete
- Do not repeat this information in function comments

**Algorithmic Comments:**
- Belong **inside function bodies**
- Explain the logic, phases, and non-obvious implementation details
- Focus on "why" rather than "what" the code does

**Function Declaration Comments:**
- Focus on **inputs and outputs** - what the function does, not how
- Describe the contract, preconditions, postconditions, and behavior
- Do NOT re-document data structure details

**Overview and Design Comments:**
- Must be **completely understandable with zero knowledge of the code**
- Minimize new terminology; define new terms immediately
- Illustrate with examples to clarify abstract concepts

## Comment Types and Examples

**Top-Level Design Comments:**
Explain concepts/abstractions, show how pieces fit together.

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
    GetPwdData() ([]byte, error)
}
```

**Function Comments:**
Focus on inputs, outputs, and behavior:
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
func (q *sqlQuery) Append(s string, params ...interface{}) { /* ... */ }
```

**Struct Field Comments:**
Document the purpose, lifecycle, and usage of each field:
```go
// cliState defines the current state of the CLI during command-line processing.
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
Separate different processing phases and explain non-obvious logic:
```go
func (r *Replica) executeAdminCommandWithDescriptor(
    ctx context.Context, updateDesc func(*roachpb.RangeDescriptor) error,
) *roachpb.Error {
    // Retry forever as long as we see errors we know will resolve.
    retryOpts := base.DefaultRetryOptions()

    for retryable := retry.StartWithCtx(ctx, retryOpts); retryable.Next(); {
        // The replica may have been destroyed since the start of the retry loop.
        if _, err := r.IsDestroyed(); err != nil {
            return roachpb.NewError(err)
        }

        // Admin commands always require the range lease to begin, but we may
        // have lost it while in this retry loop.
        if _, pErr := r.redirectOnOrAcquireLease(ctx); pErr != nil {
            return pErr
        }
    }
}
```

**Protobuf Message Comments:**
```proto
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

## Comment Maintenance

- Add explanations when you discover valuable missing knowledge.
- Fix factually incorrect comments immediately (treat as bugs).
- Fix grammar/spelling that significantly impairs reading.
- Avoid cosmetic changes that don't improve understanding.
- In reviews, prefix grammar suggestions with "nit:".
