- Feature Name: dismantle_multiraft
- Status: completed
- Start Date: 2015-12-13
- RFC PR: [#3431](https://github.com/cockroachdb/cockroach/pull/3431)
- Cockroach Issue:

# Summary

Remove the `multiraft` abstraction and move its functionality directly to the `storage` package. The goal of this change is to simplify concurrency-related problems in the current code.

# Motivation

The `multiraft` abstraction was originally designed to be reusable outside of cockroach, and to insulate the `storage` package from the details of the raft implementation. Experience has shown that instead, the two packages have become very tightly coupled. Certain information like the set of active raft groups is stored redundantly across the two packages, and keeping things in sync has required some very tricky concurrency code (and we have not yet resolved all such bugs).

The move from `raft.MultiNode` to `raft.RawNode` gives us an opportunity to move raft functionality to the `storage.Replica`, making it much easier to synchronize raft operations with other parts of the system.

# Detailed design

The "multi" part of `multiraft` will be moved to methods on `storage.Store`. This includes routing messages to the right `Replica`, and all functionality related to timers and coalesced heartbeats. The `Store.replicas` map becomes the source of truth for looking up raft groups.

`storage.Replica` will contain a `raft.RawNode` directly. Other data contained in the `multiraft.group` struct will be replaced by the corresponding fields of `Replica` (including the range descriptor and the `pendingCmds` map).

This change generally means a move away from special goroutines and channels.
Instead, the existing `Mutexes` in `Store` and `Replica` will be used for concurrency. This may eventually allow for greater concurrency, although the initial implementation will use fairly coarse locking to ensure correctness. The `Store.processRaft` goroutine will still be used for some things, especially the timer loop.

# Drawbacks

This adds complexity to the already-large `storage` package.

# Alternatives

This proposal resolves the tension between the `Mutex`-based `storage` package and the channel-based `multiraft` package by emphasizing locks. There may be another equilibrium based on channels instead, although it is probably a greater distance from our current implementation.

# Unresolved questions
