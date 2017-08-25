- Feature Name: replica_batch
- Status: completed
- Start Date: 2015-08-11
- RFC PR: [#2340](https://github.com/cockroachdb/cockroach/pull/2340)
- Cockroach Issue:

# Summary

Assuming [#1998](https://github.com/cockroachdb/cockroach/pull/1998) in place, replace `roachpb.Request` by `roachpb.BatchRequest` throughout most of the main execution path starting in `Store.ExecuteCmd()`.

# Motivation

[#1998](https://github.com/cockroachdb/cockroach/pull/1998) introduces gateway
changes after the implementation of which only `BatchRequest` is received by
`(*Store).ExecuteCmd()`. The changes described here allow `BatchRequest` to
be submitted to Raft and executed in bulk, which should give significant
performance improvements, in particular due to the lower amount of Raft-related
round-trip delays.

# Detailed design

The required changes to the code are plenty.

The sections below follow the main request path and outline the necessary
changes in each.

## Store.ExecuteCmd

This carries out

* request verification and clock updates, which generalize in a relatively
  straightforward manner.
* the store retry loop, and particularly, write intent error handling. This
  depends on the request which ran into the intent (and whether it's a read
  or write), so `Replica` needs to return not only an error, but also, for
  example, the associated index of the request in the `Batch`.

## Replica.AddCmd

Control flow currently splits up into read, write and admin paths. For simplicity,
allowing `Admin` commands only as single elements of a `Batch`, we can keep the
admin path intact. Regarding the read/write path, there are two options:

* splitting the `Batch` into sub-batches which are completely read or write only.
  This has the advantage of possibly less changes in the read and write paths,
  but requires multiple Raft proposals when reads and writes mix (in the worst
  case scenario, `len(Batch)-1` of them). Having to bookkeep multiple Raft
  proposals for a single `Batch` is a disadvantage and raises questions about
  atomicity and response cache handling.
* keeping the Batch whole, but merging `(*Replica).add{ReadOnly,Write}Cmd`.
  The idea is that if we need to go through Raft (i.e. if the `Batch` contains
  at least one write) anyway, we propose the whole `Batch` and satisfy the
  reads through `Raft`. If the `Batch` is read-only, it executes directly. It
  should be possible to refactor such that the code which executes reads is
  shared.

Overall, option two seems preferable. As a byproduct, it would make `INCONSISTENT`
reads consistent for free when they're part of a mutating batch anyways, and
(almost) implement `CONSENSUS` reads.

### Timestamp Cache and Command Queue

`(*Replica).{begin,end}Cmd` are changed to operate on `Batch` (instead of
`roachpb.RequestHeader`), obviating the `readOnly` flag (which is determined
from the request type). The entries are added to the command queue in bulk
so that overlaps are resolved gracefully: reading `[a,c)` and then writing
`b` should add `[a,b)` and `[b\x00,c)` for reading, and `b` for writing.
There is likely some potential for refactoring with `intersectIntents()`.

Timestamp cache handling is straightforward, except when commands within
the same `Batch` overlap: In that case, if the former is a read and the latter
a write, the latter command's timestamp must be moved past the former.

Note that there is some special-casing regarding the write timestamp cache with
`Transaction`s: transactional writes are still carried out even if they're
incompatible with prior writes' timestamps. This allows `Txn`s to write over
their own data, and to attempt to push in more cases.

## (\*Replica).proposeRaftCommand

no noteworthy changes.

## (\*Replica).processRaftCommand

`roachpb.ResponseWithError` changes to `roachpb.ResponsesWithError` which also
contains the index of the first error, if any (or, alternatively, by
convention the error occurred at index `len(rwe.Responses)`).

## (\*Replica).applyRaftCommand

Returns `[]roachpb.Response`, one for each successfully executed request (in
`Batch` order).

## (\*Replica).applyRaftCommandInBatch

same as `applyRaftCommand`. This actually unwinds the `Batch`, calling
`(*Replica).executeCmd` sequentially until done or an error occurs.

# Drawbacks

# Alternatives

# Unresolved questions
