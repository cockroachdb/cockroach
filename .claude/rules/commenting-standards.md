---
paths:
  - "**/*.go"
  - "**/*.proto"
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

## Writing Principles

### Start with invariants, not implementation

State what must always be true before explaining how the code works. Readers
need the contract before the mechanism.

```go
// The following invariants are assumed to hold and are preserved:
// - the list contains no overlapping ranges
// - the list contains no contiguous ranges
// - the list is sorted, with larger seqnums at the end
//
// Additionally, the caller must ensure:
// 1. if the new range overlaps with some range in the list, then it also
//    overlaps with every subsequent range in the list.
// 2. the new range's "end" seqnum is larger or equal to the "end" seqnum of
//    the last element in the list.
```

Mark invariants explicitly with `Invariant:` or a similar prefix so they stand
out during code review.

### Use temporal language for lifecycles

Use "before," "during," "once," "after," and "while" to make guarantees
time-bounded. Vague guarantees are not guarantees.

```go
// During sequencing, conflicts are discovered and any found are resolved
// through a combination of passive queuing and active pushing. Once a request
// has been sequenced, it is free to evaluate without concerns of conflicting
// with other in-flight requests due to the isolation provided by the manager.
// This isolation is guaranteed for the lifetime of the request but terminates
// once the request completes.
```

### Introduce concepts progressively

Start with the simple case, then layer in nuance. Define new terms immediately
on first use. Build understanding through contrast and differentiation when
related concepts could be confused.

```go
// A rangefeed's "resolved timestamp" is defined as the timestamp at which no
// future updates will be emitted to the feed at or before. The resolved
// timestamp is closely tied to a Range's closed timestamp, but these concepts
// are not the same. Fundamentally, a closed timestamp is a property of a Range
// that restricts its state such that no "visible" data mutations are permitted
// at equal or earlier timestamps. On the other hand, a resolved timestamp is a
// property of a rangefeed that restricts its state such that no value
// notifications will be emitted at equal or earlier timestamps.
```

### Prefer concrete examples over abstract descriptions

Use concrete notation (actual values, named requests, state snapshots) to make
abstract behavior tangible. Examples should show exact state transitions, not
hand-wave.

```go
// Example:
//   current list [3 5] [10 20] [22 24]
//   new item:    [8 26]
//   final list:  [3 5] [8 26]
```

```go
// wait-queue: [(r1,txn1,inactive), (r2,txn2,active), ..., (r3,txn1,active)]
```

Use ASCII diagrams to show architecture and request flow when spatial
relationships matter:

```go
// +---------------------+---------------------------------------------+
// | concurrency.Manager |                                             |
// +---------------------+                                             |
// |                                                                   |
// +------------+  acquire  +--------------+        acquire            |
//   Sequence() |--->--->---| latchManager |<---<---<---<---<---<---+  |
// +------------+           +--------------+                        |  |
```

### Document design rationale, not just behavior

Explain *why* a design decision was made, what tradeoffs were considered, and
under what conditions the decision should be revisited.

```go
// For admissions, we've chosen to track individual indices instead of the
// latest index, to allow the system to self correct under inconsistencies
// between the sender (leader) and receiver (replica). A secondary reason to
// track individual indices is that it naturally allows the admitted index to
// advance to stable index without lag in the case where there is continuous
// traffic, but sparseness of indices for a priority.
//
// We can revisit the decision to track individual indices if we find the
// memory or compute overhead to be significant.
```

### Treat edge cases and caveats as first-class

Do not bury limitations or edge cases. Call them out with explicit transitions
like "However," and explain both the limitation and the technical reason for it.

```go
// However, at the time of writing, not all locks are stored directly under the
// manager's control, so not all locks are discoverable during sequencing.
// Specifically, write intents (replicated, exclusive locks) are stored inline
// in the MVCC keyspace, so they are not detectable until request evaluation
// time.
```

When documenting failure modes, explain why handling them matters (e.g., forward
progress guarantees) and enumerate the possible states explicitly.

### Use structured markers for depth without noise

Use `NB:` (nota bene) for important qualifications that modify a prior
statement. Use `NOTE:` for design rationale or context that is not strictly
about the current code's behavior. Use `REQUIRES:` for preconditions. Use
`Invariant:` for properties that must always hold.

```go
// NB: this is called "max_intents_bytes" instead of "max_lock_bytes" because
// it was created before the concept of intents were generalized to locks.

// NOTE: it probably makes sense to maintain a single txnStatusCache across all
// Ranges on a Store instead of an individual cache per Range. For now, we
// don't do this because we don't share any state between separate
// concurrency.Manager instances.

// REQUIRES: g.mu to be locked.
```

### State preconditions and postconditions explicitly

Document what must be true before a method is called, what the caller can
expect afterward, and the meaning of each return value combination — including
the non-error cases.

```go
// WaitForEval seeks admission to evaluate a request at the given priority.
// ...
// In the non-error case, the waited return value is true if the priority was
// subject to replication admission control, and the RangeController was not
// closed during the execution of WaitForEval. If closed, or the priority is
// not subject to replication admission control, a (false, nil) will be
// returned -- this is important for the caller to fall back to waiting on the
// local store.
```

### Organize large interfaces with section headers

Group semantically related methods under comment headers within interface
declarations. This gives the reader a map of the interface's surface area.

```go
type Processor interface {
    // Lifecycle of processor.

    // Start starts the processor...
    Start(ctx context.Context, ...) error

    // Lifecycle of registrations.

    // Register registers the stream...
    Register(ctx context.Context, ...) (*BufferedRegistration, ...)

    // Data flow.
    // ...
}
```

### Document concurrency and locking

State mutex ordering requirements upfront, call out exceptions to the rule, and
document which locks a method expects the caller to hold.

```go
// Almost none of the methods are called with Replica.mu held. The caller and
// callee should order their mutexes before Replica.mu. The exceptions are
// HoldsSendTokensLocked, ForceFlushIndexChangedLocked, which hold both raftMu
// and Replica.mu.
```

### Walk through algorithms with state snapshots

For complex algorithms, show the data structure state at each step. Interleave
narration with snapshots so the reader sees causality, not just the final state.

```go
// Before the optimizer is invoked, the memo group contains a single normalized
// expression:
//
//   memo
//    +-- G1: (select G2 G3)
//    +-- G2: (scan a)
//    +-- G3: (eq 3 2)
//    +-- G4: (variable x)
//    +-- G5: (const 1)
//
// Optimization begins at the root of the memo (group #1)...
//
// Now the same set of steps are applied to group #2...
//
//   memo
//    +-- G1: (select G2 G3)
//    +-- G2: (scan a)
//    |    +-- []
//    |         +-- best: (scan a)
//    |         +-- cost: 100.00
```

### Use lifecycle diagrams with numbered annotations

For state machines with multiple concurrent pathways, combine ASCII diagrams
with numbered annotations. The diagram shows the flow; the footnotes explain
each transition without cluttering the visual.

```go
//          +------>created
//          |         |
//          |         |[1]
//          |         v
//   new    |      propBuf
// proposal |         |
//    [5]   |         |[2]
//          |         v       [4]
//          |    proposal map--->proposal map
//          |         |          and propBuf
//          |         |[3]            |
//          |         |               |
//          |         v               |[4]
//          +---apply goroutine<------+
//                    |
//                    v
//                 finished
//
// [1]: (*Replica).propose calls (*proposalBuf).Insert
// [2]: (*proposalBuf).FlushLockedWithRaftGroup on the handleRaftReady
//      goroutine ...
// [3]: picked up by (*replicaDecoder).retrieveLocalProposals when the entry
//      comes up for application ...
```

### Document known races and accepted imperfections

Do not hide race conditions or imperfect behavior. Document the race, explain
why fixing it would be worse (added complexity, extended lock scope, etc.), and
state the recovery mechanism that makes the race tolerable.

```go
// There is a race here: before q.mu is acquired, the granter could
// experience a reduction in load and call WorkQueue.hasWaitingRequests
// to see if it should grant, but since there is nothing in the queue
// that method will return false. Then the work here queues up even
// though granter has spare capacity. We could add additional
// synchronization (and complexity to the granter interface) to deal
// with this, by keeping the granter's lock locked when returning from
// tryGrant. But it has the downside of extending the scope of
// GrantCoordinator.mu. Instead we tolerate this race in the knowledge
// that GrantCoordinator will periodically, at a high frequency, look
// at the state of the requesters to see if there is any queued work
// that can be granted admission.
```

### Document call-ordering constraints on interfaces

When interface methods have temporal dependencies, document which methods must
be called before others, how many times each can be called, and what happens
if the ordering is violated.

```go
type Processor interface {
    // Run is the main loop of the processor. It can be called only once
    // throughout the processor's lifetime.
    Run(context.Context, RowReceiver)

    // Resume resumes the execution of the processor with the new receiver. It
    // can be called many times but after Run() has already been called.
    Resume(output RowReceiver)

    // Close releases the resources of the processor and possibly its inputs.
    // Must be called at least once on a given Processor and can be called
    // multiple times.
    Close(context.Context)
}
```

### Trace cascade effects for performance decisions

When a design choice prevents a cascade of negative effects, document the full
chain so future maintainers understand why a seemingly minor detail matters.

```go
// Terminating a grant chain immediately typically causes a new one to start
// immediately that can burst up to its maximum initial grant burst. Which
// means frequent terminations followed by new starts impose little control
// over the rate at which tokens are granted. This causes huge spikes in the
// runnable goroutine count, observed at 1ms granularity. This spike causes
// the kvSlotAdjuster to ratchet down the totalSlots for KV work all the way
// down to 1, which later causes the runnable goroutine count to crash down
// to a value close to 0, leading to under-utilization.
```

### Document backward compatibility and version interactions

CockroachDB supports rolling upgrades. When a design spans multiple versions,
document how old and new nodes interact, not just the new behavior.

```go
// Prior versions of Registry used the node's epoch value to determine
// whether or not a job should be stolen. The current implementation
// uses a time-based approach, where a node's last reported expiration
// timestamp is used to calculate a liveness value for the purpose
// of job scheduling.
//
// Mixed-version operation between epoch- and time-based nodes works
// since we still publish epoch information in the leases for time-based
// nodes. From the perspective of a time-based node, an epoch-based
// node simply behaves as though its leniency period is 0. Epoch-based
// nodes will see time-based nodes delay the act of stealing a job.
```

### Document field-level access rules for shared mutable state

When a field's synchronization requirements change over its lifecycle, enumerate
each phase and the corresponding access rule.

```go
// The access rules for this field are as follows:
//   - if the proposal has not yet been passed to Replica.propose,
//     can access freely on the originating goroutine.
//   - otherwise, once the proposal has been seen in retrieveLocalProposals
//     and removed from r.mu.proposals, can access freely under raftMu.
//   - otherwise, must hold r.mu across the access.
```

### Voice and tone

- **Formal but accessible.** Use precise technical language, but avoid jargon
  without definition. Favor "Notice that..." over dense formalism.
- **Impersonal.** Focus on what the system does, not what the developer does.
  Write "The processor stops" not "we stop the processor."
- **Active voice for causation.** "Requests are guaranteed isolation" is clearer
  than "it is guaranteed that requests will have isolation."
- **Hedge only where genuinely uncertain.** "We expect this will not be a real
  performance issue" is fine when speculative. Avoid hedging on documented
  contracts.

## Comment Maintenance

- Add explanations when you discover valuable missing knowledge.
- Fix factually incorrect comments immediately (treat as bugs).
- Fix grammar/spelling that significantly impairs reading.
- Avoid cosmetic changes that don't improve understanding.
- In reviews, prefix grammar suggestions with "nit:".
