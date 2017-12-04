- Feature Name: proposer_evaluated_kv
- Status: completed
- Start Date: 2016-04-19
- Authors: Tobias Schottdorf
- RFC PR: [#6166](https://github.com/cockroachdb/cockroach/pull/6166)
- Cockroach Issue: [#6290](https://github.com/cockroachdb/cockroach/pull/6290)

# Summary

Pursue an idea by @spencerkimball brought up in the context of
[#5985](https://github.com/cockroachdb/cockroach/pull/5985): move the bulk of
the logic involved in applying commands to a Replica upstream of Raft.

# Motivation

The [first exploration of
migrations](https://github.com/cockroachdb/cockroach/pull/5985) highlighted
the complexity concomitant with migrations on top of the current Raft execution
model.

As of writing, the leading replica is the only replica that proposes commands
to Raft. However, each replica applies commands individually, which leads to
large swaths of code living "downstream" of Raft. Since all of this code needs
to produce identical output even during migrations, a satisfactory migration
story is almost inachievable, a comment made by several reviewers (and the
author).

By having the proposing replica compute the effects on the key-value state
before proposing the command to Raft, much of this downstream code moves
upstream of Raft; naively what's left are simple writes to key-value pairs
(i.e. all commands move upstream except for a simplified MVCCPut operation).
While some complexity remains, this has promise for a much facilitated
migration story, as well as de-triplicating work previously done by each
individual replica.

# Detailed design

## Execution path

### Proposing commands

The main change in the execution path will be in `proposeRaftCommand`, which
currently takes a `BatchRequest` and creates a `pendingCmd`, which currently
has the form

```go
type pendingCmd struct {
  ctx     context.Context
  idKey   storagebase.CmdIDKey
  done    chan roachpb.ResponseWithError // Used to signal waiting RPC handler
  raftCmd struct {
      RangeID       RangeID
      OriginReplica ReplicaDescriptor
      Cmd           BatchRequest
  }
}
```

and within which the main change is changing the type of `Cmd` from
`BatchRequest` to a new type which carries the effects of the application of
the batch, as computed (by the lease holder) earlier in `proposeRaftCommand`.

For simplicity, in this draft we will assume that the `raftCmd` struct will
be modified according to these requirements.

The "effects" contain

* any key-value writes: For example the outcome of a `Put`, `DeleteRange`, `CPut`, `BeginTransaction`, or `RequestLease`.
  Some alternatives exist: writes could be encoded as `[]MVCCKeyValue`, or even
  as post-MVCC raw (i.e. opaque) key-value pairs. We'll assume the former for
  now.
* parameters for any "triggers" which must run with the *application* of the
  command. In particular, this is necessary on `EndTransaction` with a commit
  trigger, or when the lease holder changes (to apply the new lease).
  There are more of them, but this already shows that these triggers (which are
  downstream of Raft) will require various pieces of data to be passed around
  with them.

Note that for the "triggers" above, a lot of the work they currently do can
move pre-Raft as well. For example, for `RequestLease` below we must change some
in-memory state on `*Replica` (lease proto and timestamp cache) plus call out
to Gossip. Still, the bulk of the code in `(*Replica).RequestLease` (i.e. the
actual logic) will move pre-Raft, and the remaining trigger is conditional on
being on the lease holder (so that one could even remove it completely from the
"effects" and hack their execution into the lease holder role itself).

Thus, we arrive at the following (proto-backed) candidate struct:

```proto
# Pseudo-Go-Protobuf, all naming ad-hoc and TBD.

message RaftCmd {
  optional SomeMetadata # tbd (proposing replica, enginepb.MVCCStats diff, ...)
  repeated oneof effect {
    # Applied in field and slice order.
    optional Writes writes = ...; # see MVCC section for Writes type
    # Carries out the following:
    # * sanity checks
    # * compute stats for new left hand side (LHS)      [move pre-raft]
    # * copy some data to new range (abort cache)       [move pre-raft]
    # * make right-hand side `*Replica`
    # * copy tsCache (in-mem)                           [lease holder only]
    # * r.store.SplitRange
    # * maybe trigger Raft election
    optional Split ...;
    # Carries out the following:
    # * sanity checks
    # * stats computations                              [move pre-raft]
    # * copy some data to new range (abort cache)       [move pre-raft]
    # * remove some metadata                            [move pre-raft]
    #   careful: this is a DeleteRange; could benefit from a ClearRange
    #   Raft effect (all inline values; no versioning needed)
    # * clear tsCache (not really necessary)
    optional Merge ...:
    # Carries out the following:
    # * `(*Replica).setDesc`
    # * maybe add to GC queue                           [lease holder only]
    optional ChangeReplicas ...;
    # Carries out the following:
    # * maybe gossips system config                     [lease holder only]
    optional ModifiedSpan ...;
    # Carries out the following:
    # * sanity checks (in particular FindReplica)
    # * update in-memory lease
    # * on Raft(!) leader: maybe try to elect new leader
    # * on (new) lease holder: update tsCache low water mark
    #   (might be easier to do this unconditionally on all nodes)
    # * on (new) lease holder: maybe gossip system configs
    optional Lease ...;
    # Those below may not be required, but something to think about
    optional GCRange ...;    # allow optimized GC (some logic below of Raft?)
    optional ClearRange ...; # allow erasing key range without huge proposal
  }
}
```

Since this simple structure is easy to reason about, it seems adequate to
strive for the minimum amount of interaction between the various effects
(though the data required for the triggers may benefit from more interaction
between them in some cases)

### Computing the (ordered) write set

Before a lease holder proposes a command, it has the job of translating a
`BatchRequest` into a `RaftCmd`. Note that we assume that writes wait for
both reads and writes in the command queue (so that at the point at which
the `RaftCmd` is constructed, all prior mutations have been carried out and
the timestamp cache updated).

This largely takes over the work which was done in

```
applyRaftCommandInBatch -> executeWriteBatch -> executeBatch
                        -> (ReplicaCommands) -> (MVCC)
```

This is hopefully largely mechanical, with the bulk of work in the MVCC layer,
outlined in the next section.

The work to be carried out in `ReplicaCommands` is mostly isolating the various
side effects (most prominently the commit and range lease triggers). In the
process, it should be possible to remove these methods from `(*Replica)`
(though that is not a stated goal of this RFC).

### MVCC & Engine Layer

Operations in the MVCC layer (`MVCC{Get,Put,Scan,DeleteRange,...}`) currently
return some variation of `([]roachpb.KeyValue, []roachpb.Intent, error)` and
operate on the underlying `engine.Engine` (which in practice is a RocksDB
batch) with heavy use of iterators.

With the new code, we want mostly the same, but with two important changes:

* we do not actually intend to commit the batch (though we could do this on the
  proposing node to avoid overhead)
* we need to construct the `Writes` field, which (in some way or
  another) tracks the key-value pairs written (i.e. created, changed or
  deleted) over the lifetime of the batch.

It's also desirable to avoid invasive changes to the MVCC layer. The following
extension to the `Engine` interface seems adequate (mod changes to improve
buffer reuse):

```go
type Engine interface {
  // ...
  NewBatch() Batch
}

var ErrNotTracking = errors.New("batch is not tracking writes")

type Writes []MVCCKeyValue // actual type see below

type Batch interface {
  Engine
  // GetWrites returns a slice of Write updates which were carried out during
  // batch, or ErrNotTracking if the batch does not support this feature.
  // The ordering of the writes is not guaranteed to be stable and may not
  // correspond to the actual order in which the writes were carried out.
  // However, the result of applying the updates in the returned order *is*
  // stable, i.e. non-commutative (overlapping writes) can't rearrange
  // themselves freely). [i.e. can use a map under the hood]
  // The returned Writes are read-only and must not be used after their Batch
  // is finalized. [i.e. can reclaim the memory at that point, or we add an
  // explicit method to finalize the writes if that is more convenient].
  GetWrites() (Writes, error)
}
```

With this strategy, the MVCC changes are fairly locally scoped and not too
invasive.

Naively, this can be achieved by wrapping a "regular" batch with an appropriate
in-memory map which is populated on writes.

However, using [RocksDB's `WriteBatch`
format](https://github.com/facebook/rocksdb/blob/f38540b12ada4fe06598c42d4c084f9d920289ff/db/write_batch.cc#L10)
seems opportune:

```
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeSingleDeletion varstring
//    kTypeMerge varstring varstring
//    kTypeColumnFamilyValue varint32 varstring varstring
//    kTypeColumnFamilyDeletion varint32 varstring varstring
//    kTypeColumnFamilySingleDeletion varint32 varstring varstring
//    kTypeColumnFamilyMerge varint32 varstring varstring
// varstring :=
//    len: varint32
//    data: uint8[len]
```

* The format is straightforward to implement; the writes need to be serialized
  anyway for the Raft proposal, and this appears to be an efficient format to
  use, even when the underlying storage engine isn't based on RocksDB.
* We currently use RocksDB and this is already the internal format used by a
  RocksDB batch. We can completely avoid an additional copy and serialization
  step in this case (i.e. construct the batch, borrow out the contained
  representation to Raft until the command commits, and apply the batch).
  Followers can create a `WriteBatch` directly from this representation.
  Serialization is a key point in performance[1].
* Again for RocksDB, we might also be able to piggy-back on existing
  implementation to operate on the superposition of multiple `WriteBatch`es
  (which would be needed to remove write-write blocking).

The format should be relatively stable (likely only additions), but there might
be future migrations coming up as RocksDB changes their underlying assumptions.
We should consider checking in with them about this idea.

Using `WriteBatch` also means that the writes are completely opaque to the
stack (at least unless unserialized early, and loaded, and then accessed
through MVCC operations), so the followers essentially can't act based on
them. That should not be a problem (but could if we want the side effects
to use information from the write set in a performant way).

## Raft command application

This will be a new, straightforward facility which applies a `RaftCmd` against
the underlying `Engine` in a batch. The point of this whole change is that this
part of the stack is very straightforward and with no side effects.

`(*Replica).executeCmd` is likely a good place where most of this code would
live.

## Replay protection

Replay protection is naively an issue because applying a logicless batch of
writes is at odds with our current method, which largely relies on
write/timestamp conflicts.
However, as @bdarnell pointed out, as of recently (#5973) we (mostly) colocate
the Raft lease holder and the range lease holder on the same node and have more
options for dealing with this (i.e. making the application of a command
conditional on the Raft log position it was originally proposed at).

More concretely (via @bdarnell):

> The RaftCmd would also include a term and log index, to be populated with the
> lease holder's latest log information when it is proposed. The writes would only be
> applied if it committed at that index; they would be dropped (and reproposed)
> if they committed at any other index. This would only be a possibility when
> the lease holder is not the raft leader (and even then it's unlikely in
> stable lease situations). We may be able to be even stricter about colocating
> the two roles (in which case we could perhaps remove raft-level replays
> completely), but I'm wary of forcing too many raft elections.

### Test changes

These should be limited to those tests that exercise triggers and/or use
lower-level Raft internals inside of the `storage` package (plus some surprises
which are certain to occur with a change of this magnitude).

## Migration strategy

We implement the `Freeze`/`Unfreeze` Raft commands suggested by @bdarnell in
\#5985: everything proposed between `Freeze` and `Unfreeze` applies as a
non-retryable error (and in particular, does not change state). Assuming a
working implementation of this (which will need to be hashed out separately and
may require its own migration considerations), a migration entails the
following:

* **apply** `Freeze` on all Replicas
* stop all nodes (possibly through a special shutdown mode which terminates
  a node when all Replicas are in fact frozen)
* replace the binary
* start all nodes
* apply `Unfreeze` on all Replicas.

Since the replaced binary will not have to apply Raft log entries proposed by
the previous binary, all is well.

This work is nicely isolated from the remainder of this RFC, and can (should)
be tackled separately as soon as consensus is reached.

This migration strategy also has other upshots:

* it can migrate Raft changes while this RFC is being implemented
* it can be used for future stop-the-world upgrades even when a Raft log freeze
  isn't necessary(since it also introduces a "stop for upgrade" mode with user
  tooling, etc).

# Drawbacks

* The amount of data sent over the wire is expected to increase. In particular,
DeleteRange could be problematic.
It seems worthwhile to get a sense of expectations by means of a prototype.
* While the amount of code downstream of raft is drastically reduced, it is not
  zero, so we will still need online Raft migrations (#5985) eventually.

There were concerns that this change would make it harder/impossible to get
rid of write-write blocking in the future. However, that concern was based on
the possibility of Raft reordering, which we can eliminate at this point (see
[Replay protection](#replay-protection)), and the problem boils down to having
all intermediate states which would result from applying the prefixes of the
existing overlapping writes (including side effects) available to execute on
top of, which happens at a layer above the changes in this RFC.

# Implementation strategy

Tackle the following in parallel (and roughly in that order):

* flesh out and implement `{F,Unf}reeze`; these are immediately useful for
  all upcoming migrations.
* prototype and evaluate expected inflation of Raft proposal sizes
* separate the side effects of Raft applications from `*Replica` and clearly
  separate the parts which can be moved pre-proposal as well as the parameters
  required for the post-proposal part. This informs the actual design of the
  Raft "effects" in this proposal.
  Most of these should come up naturally when trying to remove the `(*Replica)`
  receiver on most of the `ReplicaCommands` (`RequestLease`, `Put`, ...) and
  manually going through the rest of the call stack up to `processRaftCommand`.
* implement protection against Raft replays/reordering, ideally such that
  the lease holder knows at proposal time the log index at which commands commit
  (unless they have to be reproposed).
* flesh out the WriteBatch encoding. Ideally, this includes a parser in Go (to
  avoid an obstruction to switching storage engines), but the critical part
  here for the purpose of this RFC is hooking up the `RocksDB`-specific
  implementation and guarding against upstream changes.
  We can trust RocksDB to be backwards-compatible in any changes they make, but
  we require bidirectional compatibility. We create a batch on the lease holder and
  ship it off to the follower to be applied, and the follower might be running
  either an older or a newer version of the code. If they add a new record
  type, we either need to be able to guarantee that the new record types won't
  be used in our case, or translate them back to something the old version can
  understand.
* introduce and implement the `Batch` interface. This could be done after the
  `WriteBatch` encoding is done or, if required to unblock the implementation
  of this RFC, start with a less performant version based on an auxiliary map
  and protobuf serialization.

The goal here should be to identify all the necessary refactors to keep them
out of the remaining "forklift" portion of the change.

## Is this a feasible change?

It's scary

* if you try to migrate it properly (but see above for an option which seems
  viable)
* if you try to forklift it all
* that with a change of this magnitude we could stumble upon surprises
  in late-ish stages which call the whole change into question
* that it requires a large concerted effort to be carried out, with lots of
  moving parts touched.
* that performance implications aren't 100% clear (need prototyping)

As I understand it, the change is very ambitious, but not as vast in scope as,
for example, the plans we have with distributed SQL (which, of course, are not
on such a tight deadline).

# Alternatives

Other than leaving the current system in place and pushing complexity onto the
migration story, none.

# Unresolved questions

None currently.

# Footnotes

[1]: via @petermattis:
  > There is another nice effect from using the WriteBatch: instead of
  marshaling and unmarshaling individual keys and values, we'll be passing around
  a blob. While investigating batch insert performance I added some
  instrumentation around RaftCommand marshaling and noticed:

  ```
  raft command marshal:   20002: 6.2ms
  raft command unmarshal: 20002: 29.1ms
  raft command marshal:   20002: 6.0ms
  raft command unmarshal: 20002: 12.4ms
  raft command marshal:   20002: 6.0ms
  raft command unmarshal: 20002: 42.6ms
  ```

  The first number (20002) is the number of requests in the BatchRequest and
  the second is the time it took to marshal/unmarshal. The marshaled data size
  is almost exactly 1MB.
