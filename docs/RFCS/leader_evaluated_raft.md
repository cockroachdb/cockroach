- Feature Name: leader_evaluated_raft
- Status: draft
- Start Date: 2016-04-19
- Authors: Tobias Schottdorf
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)


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
the batch, as computed (by the leader) earlier in `proposeRaftCommand`.

For simplicity, in this draft we will assume that the `raftCmd` struct will
be modified according to these requirements.

The "effects" contain

* any key-value writes: For example the outcome of a `Put`, `DeleteRange`, `CPut`, `BeginTransaction`, or `LeaderLease`.
  Some alternatives exist: writes could be encoded as `[]MVCCKeyValue`, or even
  as post-MVCC raw (i.e. opaque) key-value pairs. We'll assume the former for
  now.
* parameters for any "triggers" which must run with the *application* of the
  command. In particular, this is necessary on `EndTransaction` with a commit
  trigger, or when leadership changes (to apply the new lease).
  There are more of them, but this already shows that these triggers (which are
  downstream of Raft) will require various pieces of data to be passed around
  with them.

Note that for the "triggers" above, a lot of the work they currently do can
move pre-Raft as well. For example, for `LeaderLease` below we must change some
in-memory state on `*Replica` (lease proto and timestamp cache) plus call out
to Gossip. Still, the bulk of the code in `(*Replica).LeaderLease` (i.e. the
actual logic) will move pre-Raft, and the remaining trigger is conditional on
being on the leader (so that one could even remove it completely from the
"effects" and hack their execution into the leader role itself).

Thus, we arrive at the following (proto-backed) candidate struct:

```proto
# Pseudo-Go-Protobuf, all naming ad-hoc and TBD.

message RaftCmd {
  optional SomeMetadata # tbd (proposing replica, MVCCStats diff, ...)
  repeated oneof effect {
    # Applied in field and slice order.
    repeated Write writes = ...; # see MVCC section
    # Carries out the following:
    # * sanity checks
    # * compute stats for new left hand side (LHS)      [move pre-raft]
    # * copy some data to new range (abort cache)       [move pre-raft]
    # * make right-hand side `*Replica`
    # * copy tsCache (in-mem; only relevant on leader)  [leader-only]
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
    # * maybe add to GC queue                           [leader only]
    optional ChangeReplicas ...;
    # Carries out the following:
    # * maybe gossips system config                     [leader only]
    optional ModifiedSpan ...;
    # Carries out the following:
    # * sanity checks (in particular FindReplica)
    # * update in-memory lease
    # * on Raft(!) leader: maybe try to elect new lease holder
    # * on (new) lease holder: update tsCache low water mark
    #   (might be easier to do this unconditionally on all nodes)
    # * on (new) lease holder: maybe gossip system configs
    optional LeaderLease ...;
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

### Computing the write set

Before a leader proposes a command, it has the job of translating a
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
side effects (most prominently the commit and leader lease triggers). In the
process, it should be possible to remove these methods from `(*Replica)`
(though that is not a stated goal of this RFC).

### MVCC & Engine Layer

Operations in the MVCC layer (`MVCC{Get,Put,Scan,DeleteRange,...}`) currently
return some variation of `([]roachpb.KeyValue, []roachpb.Intent, error)` and
operate on the underlying `engine.Engine` (which in practice is a RocksDB
batch) with heavy use of iterators.

With the new code, we want mostly the same, but with two important changes:

* we do not actually intent to commit the batch (though we could do this on the
  proposing node to avoid overhead)
* we need to construct the repeated `Write` field, which (in some way or
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

type Writes []MVCCKeyValue // to be discussed; could also be raw KV ([][2]byte)

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

Naively, this can be achieved by wrapping a "regular" batch with an appropriate
in-memory map which is populated on writes. A better implementation may be
hard to get but even knowing the underlying `Engine` implementation
(i.e. currently almost exclusively RocksDB). Some refactoring should assure
that buffers used for writing MVCC values are assumed "owned" by the engine (so
that populating the internal map can freely hold on to the memory). It can be
fed back into the pool at a higher level (i.e. after having applied/discarded
the batch).

With this strategy, the MVCC changes are fairly locally scoped and not too
invasive.

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
the Raft leader and the leader lease holder on the same node and have more
options for dealing with this (i.e. making the application of a command
conditional on the Raft log position it was originally proposed at).

More concretely (via @bdarnell):

> The RaftCmd would also include a term and log index, to be populated with the
> leader's latest log information when it is proposed. The writes would only be
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

## Implementation strategy

Some lower-hanging fruit exist:

* the MVCC/Engine changes don't affect upgrades or existing functionality, so
they can be carried out freely.
* the change in replay protection mentioned above can also be developed/tested
separately. This seems wise since it's an absolute necessity.
* separating the side effects of Raft applications should also be doable; most
  of these should come up naturally when trying to remove the `(*Replica)`
  receiver on most of the `ReplicaCommands` (`LeaderLease`, `Put`, ...) and
  manually going through the rest of the call stack up to `processRaftCommand`.

The goal here should be to identify all the necessary refactors to keep them
out of the remaining "forklift" portion of the change.

This also includes the next section:

## Migration strategy

We implement the `Freeze`/`Unfreeze` Raft commands suggested by @bdarnell in
\#5985: everything proposed between `Freeze` and `Unfreeze` applies as a a
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
* The change may complicate getting rid of write/write blocking (which means a
  write having to wait for an overlapping prior write to be applied)

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

## Best batch implementation

This proposal currently suggests using an augmented RocksDB write batch as the
basis implementation of the new `Batch` interface.

@bdarnell suggested just computing the effects in-memory:

> We had a pure go batch implementation prior to 7d4e2fc. Would it be better to revive that than to wrap a batch with something that saves a second copy of all writes?

It's hard to say which one is better. The RocksDB write batch (at least in that
commit) was measurably faster (at least on the MVCC level) and does most of the
work in C++ (where it can manage memory efficiently). The Go option does more
work in Go, is slower and it's not clear whether memory-related performance
would overall increase (due to keeping a more complicated state in Go).

The proposed `Batch` interface would allow us to experiment with both, and to
switch to the better one (maybe even opportunistically depending on the cmd).

A third alternative is an implementation in which the `RocksDB` batch exposes
a read-only view of the mutations it contains, which would obviate any
additional state-keeping in Go. This is likely upstream work, but does seem
like an interesting venue (again, can be explored separately).
