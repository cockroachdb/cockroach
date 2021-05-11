# Range merges

**Last update:** April 10, 2019

**Original author:** Nikhil Benesch

This document serves as an end-to-end description of the implementation of range
merges. The target reader is someone who is reasonably familiar with core
but unfamiliar with either the "how" or the "why" of the range merge
implementation.

The most complete documentation, of course, is in the code, tests, and the
surrounding comments, but those pieces are necessarily split across several
files and packages. That scattered knowledge is centralized here, without
excessive detail that is likely to become stale.

## Table of Contents

* [Overview](#overview)
* [Implementation details](#implementation-details)
  * [Preconditions](#preconditions)
  * [Initiating a merge](#initiating-a-merge)
    * [AdminMerge race](#adminmerge-race)
  * [Merge transaction](#merge-transaction)
    * [Transfer of power](#transfer-of-power)
  * [Snapshots](#snapshots)
  * [Merge queue](#merge-queue)
* [Subtle complexities](#subtle-complexities)
  * [Range descriptor generation](#range-descriptor-generations)
  * [Misaligned replica sets](#misaligned-replica-sets)
  * [Replica GC](#replica-gc)
  * [Transaction record GC](#transaction-record-gc)
  * [Unanimity](#unanimity)
* [Safety recap](#safety-recap)
* [Appendix](#appendix)
  * [Key encoding oddities](#key-encoding-oddities)

## Overview

A range merge begins when two adjacent ranges are selected to be merged
together. For example, suppose our adjacent ranges are _P_ and _Q_, somewhere in
the middle of the keyspace:

```
--+-----+-----+--
  |  P  |  Q  |
--+-----+-----+--
```

We'll call _P_ the left-hand side (LHS) of the merge, and _Q_ the right-hand
side (RHS) of the merge. For reasons that will become clear later, we also refer
to _P_ as the subsuming range and _Q_ as the subsumed range.

The merge is coordinated by the LHS. The coordinator begins by verifying that a)
the two ranges are, in fact, adjacent, and b) that the replica sets of the two
ranges are aligned. Replica set alignment is a term that is currently only
relevant to merges; it means that the set of stores with replicas of the LHS
exactly matches the set of stores with replicas of the RHS. For example, this
replica set is aligned:

```
Store 1    Store 2     Store 3     Store 4
+-----+    +-----+     +-----+     +-----+
| P Q |    | P Q |     |     |     | P Q |
+-----+    +-----+     +-----+     +-----+
```

By requiring replica set alignment, the merge operation is reduced to a metadata
update, albeit a tricky one, as the stores that will have a copy of the merged
range _PQ_ already have all the constituent data, by virtue of having a copy of
both _P_ and _Q_ before the merge begins. Note that replicas of _P_ and _Q_ do
not need to be fully up-to-date before the merge begins; they'll be caught up as
necessary during the [transfer of power](#transfer-of-power).

After verifying that the merge is sensible, the coordinator transactionally
updates the implicated range descriptors, adjusting P's range descriptor so that
it extends to _Q_'s end and deleting _Q_'s range descriptor.

Then, the coordinator needs to [atomically move
responsibility](#transfer-of-power) for the data in the RHS to the LHS. This is
tricky, as the lease on the LHS may be held by a different store than the lease
on the RHS. The coordinator notifies the RHS that it is about to be subsumed and
it is prohibited from serving any additional read or write traffic. Only when
the coordinator has received an acknowledgement from _every_ replica of the RHS,
indicating that no traffic is possibly being served on the RHS, does the
coordinator commit the merge transaction.

Like with splits, the merge transaction is committed with a special "commit
trigger" that instructs the receiving store to update its in-memory bookkeeping
to match the updates to the range descriptors in the transaction. The moment the
merge transaction is considered committed, the merge is complete!

At the time of writing, merges are only initiated by the merge queue, which is
responsible both for locating ranges that are in need of a merge and aligning
their replica sets before initiating the merge.

The remaining sections cover each of these steps in more detail.

## Implementation details

### Preconditions

Not any two ranges can be merged. The first and most obvious criterion is that
the two ranges must be adjacent. Suppose a simplified cluster that has only
three ranges, _A_, _B_, and _C_:

```
+-----+-----+-----+
|  A  |  B  |  C  |
+-----+-----+-----+
```

Ranges _A_ and _B_ can be merged, as can ranges _B_ and _C_, but not ranges _A_
and _C_, as they are not adjacent. Note that adjacent ranges are equivalently
referred to as "neighbors", as in, range _B_ is range _A_'s right-hand neighbor.

The second criterion is that the replica sets must be aligned. To illustrate,
consider a four node cluster with the default 3x replication. The allocator has
attempted to balance ranges as evenly as possible:

```
Node  1    Node  2     Node  3     Node  4
+-----+    +-----+     +-----+     +-----+
| P Q |    |  P  |     |  Q  |     | P Q |
+-----+    +-----+     +-----+     +-----+
```

Notice how node 2 does not have a copy of _Q_, and node 3 does not have a copy
of _P_. These replica sets are considered "misaligned." Aligning them requires
rebalancing Q from node 3 to node 2, or rebalancing _P_ from node 2 to node 3:

```
Node  1    Node  2     Node  3     Node  4
+-----+    +-----+     +-----+     +-----+
| P Q |    | P Q |     |     |     | P Q |
+-----+    +-----+     +-----+     +-----+

Node  1    Node  2     Node  3     Node  4
+-----+    +-----+     +-----+     +-----+
| P Q |    |     |     | P Q |     | P Q |
+-----+    +-----+     +-----+     +-----+
```

We explored an alternative merge implementation that did not require aligned
replica sets, but found it to be unworkable. See the [misaligned replica sets
misstep](#misaligned-replica-sets) for details.

### Initiating a merge

A merge is initiated by sending a AdminMerge request to a range. Like other
admin commands, DistSender will automatically route the request to the
leaseholder of the range, but there is no guarantee that the store will retain
its lease while the admin command is executing.

Note that an AdminMerge request takes no arguments, as there is no choice in
what range will be merged. The recipient of the AdminMerge will always be the
LHS, subsuming range, and its right neighbor at the moment that the
AdminMerge command begins executing will always be the RHS, subsumed range.

It would have been reasonable to have instead used the RHS to coordinate the
merge. That is, the RHS would have been the subsuming range, and the LHS would
have been the subsumed range. Using the LHS to coordinate, however, yields a
nice symmetry with splits, where the range that coordinates a split becomes the
LHS of the split. Maintaining this symmetry means that a range's start key never
changes during its lifetime, while its end key may change arbitrarily in
response to splits and merges.

There is another reason to prefer using the LHS to coordinate involving an
oddity of key encoding and range bounds. It is trivial for a range to send a
request to its right neighbor, as it simply addresses the request to its end
key, but it is difficult to send a request to its left neighbor, as there is no
function to get the key that immediately precedes the range's start key. See the
[key encoding oddities](#key-encoding-oddities) section of the appendix for
details.

At the time of writing, only the [merge queue](#merge-queue) initiates merges,
and it does so by bypassing DistSender and invoking the AdminMerge command
directly on the local replica. At some point in the future, we may wish to
expose manual merges via SQL, at which point the SQL layer will need to send
proper AdminMerge requests through the KV API.

#### AdminMerge race

At present, AdminMerge requests are subject to a small race. It is possible for
the ranges implicated by an AdminMerge request to split or merge between when
the client decides to send an AdminMerge request and when the AdminMerge request
is processed.

For example, suppose the client decides that _P_ and _Q_ should be merged and
sends an AdminMerge request to _P_. It is possible that, before the AdminMerge
request is processed, _P_ splits into _P<sub>1</sub>_ and _P<sub>2</sub>_. The
AdminMerge request will thus result in _P<sub>1</sub>_ and _P<sub>2</sub>_
merging together, and not the desired _P_ and _Q_.

The race could have been avoided if the AdminMerge request required that the
descriptors for the implicated ranges were provided as arguments to the request.
Then the merge could be aborted if the merge transaction discovered that either
of the implicated ranges did not match the corresponding descriptor in the
AdminMerge request arguments, forming a sort of optimistic lock.

Fortunately, the race is rare in practice. If it proves to be a problem, the
scheme described above would be easy to implement while maintaining backwards
compatibility.

### Merge transaction

The merge transaction piggybacks on CockroachDB's serializability to provide
much of the necessary synchronization for the bookkeeping updates. For example,
merges cannot occur concurrently with any splits or replica changes on the
implicated ranges, because the merge transaction will naturally conflict with
those split transaction and change replicas transactions, as both transactions
will attempt to write updated range descriptors and conflict. No additional code
was needed to enforce this, as our standard transaction conflict detection
mechanisms kick in here (write intents, the timestamp cache, the span latch
manager, etc.).

Note that there was one surprising synchronization problem that was not
immediately handled by serializability. See [range descriptor
generations](#range-descriptor-generations) for details.

The standard KV operations that the merge transaction performs are:

  * Reading the LHS descriptor and RHS descriptor, and verifying that their
    replica sets are aligned.
  * Updating the local and meta copy of the LHS descriptor to reflect
    the widened end key.
  * Deleting the local and meta copy of the RHS descriptor.
  * Writing an entry to the `system.rangelog` table.

These operations are the essence of a merge, and in fact update all necessary
on-disk data! All the remaining complexity exists to update in-memory metadata
while the cluster is live.

Note that the merge transaction's KV operations are not fundamentally dependent
and so could theoretically be performed in any order. There are, however,
several implementation details that enforce some ordering constraints.

First, the merge transaction record needs to be located on the LHS.
Specifically, the transaction record needs to live on the subsuming range, as
the commit trigger that actually applies the merge to the replica's in-memory
state runs on the range where the transaction record lives. The transaction
record is created on the range that the transaction writes first; therefore, the
merge transaction is careful to update the local copy of the LHS descriptor as
its first operation, since the local copy of the LHS descriptor lives on the
LHS.

---
**UPDATE in v21.1:**

As of v21.1, the merge transaction uses a locking read on the LHS range's
transaction record as its first course of action. This eliminates thrashing and
livelock in the face of concurrent merge attempts, which we typically only see
in testing scenarios but which we'd like to handle effectively. It also has the
effect of locating the transaction record on the LHS earlier in the transaction.
Because of this, the merge transaction no longer needs to be quite as deliberate
about the order in which it updates descriptors later on because that order does
not impact the location of the transaction record.

---

Second, the merge transaction must ensure that, when it issues the delete
request to remove the local copy of the RHS descriptor, the resulting intent is
actually written to disk. (See the [transfer of power](#transfer-of-power)
subsection for why this is required.) Thanks to [transactional
pipelining][#26599], KV writes can return early, before their intents have
actually been laid down. The intents are not required to make it to disk until
the moment before the transaction commits. The merge transaction simply disables
pipelining to avoid this hazard.

As the last step before the commit, the merge transaction needs to freeze the
RHS, then wait for _every_ replica of the RHS to apply all outstanding commands.
This ensures that, when the merge commits, every LHS replica can blindly assume
that it has perfectly up-to-date data for the RHS. To quickly recap, this is
guaranteed because 1) the replica sets were aligned when the merge transaction
started, 2) rebalances that would misalign the replica sets will conflict with
the merge transaction, causing one of the transactions to abort, 3) the RHS is
frozen and cannot process any new commands, and 4) every replica of the RHS is
caught up on all commands. The process of freezing the RHS and waiting for every
replica to catch up is covered more thoroughly in the [transfer of
power](#transfer-of-power) subsection.

Finally, the merge transaction commits, attaching a special [merge commit
trigger] to the end transaction request. This trigger has three
responsibilities:

  1. It ensures the end transaction request knows which intents it can resolve
     locally. Intents that live on the RHS range would naively appear to belong
     to a different range than the one containing the transaction record (i.e.,
     the LHS), but if the merge is committing then the LHS is subsuming the RHS
     and thus the intents can be resolved locally.

     In fact, it's critical that these intents are considered local, because
     local intents are resolved synchronously while remote intents are resolved
     asynchronously. We need to maintain the invariant that, when a store boots
     up and discovers an intent on its local copy of a range descriptor, it can
     simply ignore the intent. Because we enforce that these intents are
     resolved synchronously with the commit of the merge transaction, we are
     guaranteed that, if we see an intent on a local range descriptor, this
     replica has not yet applied the `EndTransaction` request for the merge
     transaction, and it is therefore safe to load the replica. If the intent
     were instead resolved asynchronously, we could observe the state where the
     `EndTransaction` request for the merge had applied but the intent
     resolution had not applied, in which case we would attempt to load both the
     post-merge subsuming replica, and the subsumed replica, which would overlap
     and crash the node.

  2. It adjusts the LHS's MVCCStats to incorporate the subsumed range's
     MVCCstats.

  3. It copies necessary range-ID local data from the RHS and LHS, rekeying each
     key to use LHS's range ID. At the moment, the only necessary data is the
     [transaction abort span].

  4. It attaches a [merge payload to the replicated proposal result][pd-flag].
     When each replica of the LHS applies the command, it will notice the merge
     payload and adjust the store's in-memory state to match the changes to the
     on-disk state that were committed by the transaction. This entails
     atomically removing the RHS replica from the store and widening the LHS
     replica.

     This operation involves a delicate dance of acquiring store locks and locks
     for both replicas, in a certain order, at various points in the Raft
     command application flow. The details are too intricate to be worth
     describing here, especially considering that these tangled interactions
     between a store and its replicas are due for a refactor. The best thing to
     do, if you're interested in the details, is to trace through all references
     to `storagepb.ReplicatedEvalResult.Merge`.

[#26599]: https://github.com/cockroachdb/cockroach/pull/26599
[transaction abort span]: https://github.com/cockroachdb/cockroach/blob/82bcd948384e6a482cdd7c916c0aaca32367a7b0/pkg/storage/abortspan/abortspan.go
[merge commit trigger]: https://github.com/cockroachdb/cockroach/blob/82bcd948384e6a482cdd7c916c0aaca32367a7b0/pkg/storage/batcheval/cmd_end_transaction.go#L984-L994
[pd-flag]: https://github.com/cockroachdb/cockroach/blob/82bcd948384e6a482cdd7c916c0aaca32367a7b0/pkg/storage/batcheval/cmd_end_transaction.go#L1033-L1035

#### Transfer of power

The trickiest part of the merge transaction is making the LHS responsible for
the keyspace that was previously owned by the RHS. The transfer of power must be
performed atomically; otherwise, all manner of consistency violations can occur.
This is hopefully obvious, but here's a quick example to drive the point home.
Suppose _P_ and _Q_ simultaneously consider themselves responsible for the key
_q_. _Q_ could then allow a write to _q_ at time 1 at the same time that _P_
allowed a read of _q_ at time 2. Consistency requires that either the read see
the write, as the read is executing at a higher timestamp, or that the write is
bumped to time 3. But because _P_ and _Q_ have separate span latch managers, no
synchronization will occur, and the read might fail to see the write!

The transfer of power is complicated by the fact that there is no guarantee that
the leases on the LHS and the RHS will be aligned, nor is there any
straightforward way to provide such a guarantee. (Aligned leaseholders would
allow the merge transaction to use a simple in-memory lock to make the transfer
of power atomic.) The leaseholder of either range might fail at any moment, at
which point the lease can be acquired, after it expires, by any other live
member of the range.

Since leaseholder assignment is infeasible, the merge transaction implements
what is essentially a distributed lock.

The lock is initialized by sending a [Subsume][subsume-request] request to the
RHS. This is a single-purpose request that exists solely for use in the merge
transaction. It is unlikely to ever be useful in another situation, and (ab)uses
several implementation details to provide the necessary synchronization.

When the Subsume request returns, the RHS has made three important promises:

  1. There are no commands in flight.
  2. Any future requests will block until the merge transaction completes.
     If the merge transaction commits, the requests will be bounced with a
     RangeNotFound error. If the merge transaction aborts, the requests will
     be processed as normal.
  3. If the RHS loses its lease, the new leaseholder will adhere to promises
     1 and 2.

The Subsume request provides promise 1 by declaring that it reads and writes all
addressable keys in the range. This is a bald-faced lie, as the Subsume request
only reads one key and writes no keys, but it forces synchronization with all
latches in the span latch manager, as no other commands can possibly execute in
parallel with a command that claims to write all keys.

It provides promise 2 by flipping [a bit][merge-bit] on the replica that
indicates that a subsumption is in progress. When the bit is active, the
replica blocks processing of all requests.

Importantly, the bit needs to be cleared when the merge transaction completes,
so that the requests are not blocked forever. This is the responsibility of the
[merge watcher goroutine][watcher]. Determining whether a transaction has
committed or not is conceptually simple, but the details are brutally
complicated. See the [transaction record GC](#transaction-record-gc) section
for details.

Note that the Subsume request only runs on the leaseholder, and therefore the
merge bit is only set on the leaseholder and the watcher goroutine only runs on
the leaseholder. This is perfectly safe, as none of the follower replicas can
process requests.

Promise 3 is actually not provided by the Subsume request at all, but by a hook
in the lease acquisition code path. Whenever a replica acquires a lease, it
checks to see whether its local descriptor has a deletion intent. If it does, it
can infer that a subsumption is in progress, as nothing else leaves a deletion
intent on a range descriptor. In that case, the replica, instead of serving
traffic, flips the merge bit and launches its own merge watcher goroutine, just
as the Subsume command would have. This means there can actually be multiple
replicas of the RHS with the merge bit set and a merge watcher goroutine
running—assuming the old leaseholder did not crash but lost its lease for other
reasons—but this does not cause any problems.

[subsume-request]: https://github.com/cockroachdb/cockroach/blob/d6adf24cae788d7cd967feadae8e9c0388ce5273/pkg/storage/batcheval/cmd_subsume.go#L56-L86
[merge-bit]: https://github.com/cockroachdb/cockroach/blob/d6adf24cae788d7cd967feadae8e9c0388ce5273/pkg/storage/replica.go#L361-L364
[watcher]: https://github.com/cockroachdb/cockroach/blob/d6adf24cae788d7cd967feadae8e9c0388ce5273/pkg/storage/replica.go#L2817-L2926

### Snapshots

The LHS might be advanced over the command that commits a merge with a snapshot.
That means that all the complicated bookkeeping that normally takes place when a
replica processes a command with a non-nil `ReplicatedEvalResult.Merge` is
entirely skipped! Most problematically, the snapshot will need to widen the
receiving replica, but there will be a replica of the RHS in the way—remember,
this is guaranteed by replica set alignment. In fact, the snapshot could be
advancing over several merges, or a combination of several merges and splits, in
which case there will be several RHSes to subsume.

This turns out to be relatively straightforward to handle. If an initialized
replica receives a snapshot that widens it, it can infer that a merge occured,
and it simply subsumes all replicas that are overlapped by the snapshot in one
shot. This requires the same delicate synchronization dance, mentioned at the
end of the [merge transaction](#merge-transaction) section, to update bookeeping
information. After all, applying a widening snapshot is simply the bulk version
of applying a merge command directly. The details are too complicated to go into
here, but you can begin your own exploration by starting with this call to
[`Replica.maybeAcquireSnapshotMergeLock`][code-start] and tracing how the
returned `subsumedRepls` value is used.

[code-start]: https://github.com/cockroachdb/cockroach/blob/82bcd948384e6a482cdd7c916c0aaca32367a7b0/pkg/storage/replica.go#L4071-L4072

### Merge queue

The merge queue, like most of the other queues in the system, runs on every
store and periodically scans all replicas for which that store holds the lease.
For each replica, the merge queue evaluates whether it should be merged with
its right neighbor. Looking rightward is a natural fit for the reasons
described in the [key encoding oddities](#key-encoding-oddities) section of
the appendix.

In some ways, the merge queue has an easy job. For any given range _P_ and its
right neighbor _Q_, the merge queue synthesizes the hypothetical merged range
_PQ_ and asks whether the split queue would immediately split that merged range.
If the split queue would immediately split _PQ_, then obviously _P_ and _Q_
should not be merged; otherwise, the ranges _should_ be merged! This means that
any improvement to our split heuristics also improves our merge heuristics with
essentially no extra work. For example, load-based splitting hardly required any
changes to the merge queue.

Note that, to avoid thrashing, ranges at or above the minimum size threshold
(8MB) are never considered for a merge. The minimum size threshold is
configurable on a per-zone basis.

Unfortunately, constructing the hypothetical merged range _PQ_ requires
information about _Q_ that only _Q_'s leaseholder maintains, like the amount of
load that _Q_ is currently experiencing. The merge queue must send a RangeStats
RPC to collect this information from _Q_'s leaseholder, because there is no
guarantee that the current store is _Q_'s leaseholder—or that the current store
even has a replica of _Q_ at all.

To prevent unnecessary RPC chatter, the merge queue uses several heuristics to
void sending RangeStats requests when it seems like the merge is unlikely to be
permitted. For example, if it determines that _P_ and _Q_ store different
tables, the split between them is mandatory and it won't bother sending the
RangeStats requests. Similarly, if _P_ is above the minimum size threshold, it
doesn't bother asking about _Q_.

## Subtle complexities

### Range descriptor generations

There was one knotty race that was not immediately eliminated by transaction
serializability. Suppose we have our standard aligned replica set situation:

```
Store 1    Store 2     Store 3     Store 4
+-----+    +-----+     +-----+     +-----+
| P Q |    | P Q |     |     |     | P Q |
+-----+    +-----+     +-----+     +-----+
```

In an unfortunate twist of fate, a rebalance of P from store 2 to store 3
begins at the same time as a merge of P and Q begins. Let's quickly cover the
valid outcomes of this race.

  1. The rebalance commits before the merge. The merge must abort, as the
     replica sets of P and Q are no longer aligned.

  2. The merge commits before the rebalance starts. The rebalance should
     voluntarily abort, as the decision to rebalance P needs to be updated in
     light of the merge. It is not, however, a correctness problem if the
     rebalance commits; it simply results in rebalancing a larger range than
     may have been intended.

  3. The merge commits before the rebalance ends, but after the rebalance has
     sent a preemptive snapshot to store 3. The rebalance must abort, as
     otherwise the preemptive snapshot it sent to store 3 is a ticking time
     bomb.

     To see why, suppose the rebalance commits. Since the preemptive snapshot
     predates the commit of the merge transaction, the new replica on store 3
     will need to be streamed the Raft command that commits the merge
     transaction. But applying this merge command is disastrous, as store 3
     does not have a replica of Q to merge! This is a very subtle way in which
     replica set alignment can be subverted.

Guaranteeing the correct outcome in case 1 is easy. The merge transaction simply
checks for replica set alignment by transactionally reading the range descriptor
for _P_ and the range descriptor for _Q_ and verifying that they list the same
replicas. Serializability guarantees the rest.

Case 2 is similarly easy to handle. The rebalance transaction simply verifies
that the range descriptor used to make the rebalance decision matches the range
descriptor that it reads transactionally.

Case 3, however, has an extremely subtle pitfall. It seems like the solution for
case 2 should apply: simply abort the transaction if the range descriptor
changes between when the preemptive snapshot is sent and when the rebalance
transaction starts. But, as it turns out, this is not quite foolproof. What if,
between when the preemptive snapshot is sent and when the rebalance transaction
starts, _P_ and _Q_ merge together and then split at exactly the same key? The
range descriptor for _P_ will look entirely unchanged to the rebalance
transaction!

The solution was to add a generation counter to the range descriptor:

```protobuf
message RangeDescriptor {
    // ...

    // generation is incremented on every split and every merge, i.e., whenever
    // the end_key of this range changes. It is initialized to zero when the range
    // is first created.
    optional int64 generation = 6;
}
```

It is no longer possible for a range descriptor to be unchanged by a sequence of
splits and merges, as every split and merge will bump the generation counter.
Rebalances can thus detect if a merge commits between when the preemptive
snapshot is sent and when the transaction begins, and abort accordingly.

### Misaligned replica sets

An early implementation allowed merges between ranges with misaligned replica
sets. The intent was to simplify matters by avoiding replica rebalancing.

Consider again our example misaligned replica set:

```
Store 1    Store 2     Store 3     Store 4
+-----+    +-----+     +-----+     +-----+
| P Q |    |  P  |     |  Q  |     | P Q |
+-----+    +-----+     +-----+     +-----+

P: (s1, s2, s4)
Q: (s1, s3, s4)
```

Note that there are two perspectives shown here. The store boxes represent the
replicas that are *actually* present on that store, from the perspective of the
store itself. The descriptor tuples at the bottom represent the stores that are
considered to be members of the range, from the perspective of the most recently
committed range descriptor.

Now, to merge _P_ and _Q_ in this situation without aligning their replica sets,
store 2 needed to be provided a copy of store 3's data. To accomplish this, a
copy of _Q_'s data was stashed in the merge trigger, and _P_ would write this
data into its store when applying the merge trigger.

There was, sadly, a large and unforeseen problem with lagging replicas. Suppose
store 2 loses network connectivity a moment before ranges _P_ and _Q_ are
merged. Note that store 2 is not required for _P_ and _Q_ to merge, because only
a quorum is required on the LHS to commit a merge. Now the situation looks like
this:

```
Store 1    Store 2     Store 3     Store 4
+-----+    xxxxxxx     +-----+     +-----+
| PQ  |    |  P  |     |     |     | PQ  |
+-----+    xxxxxxx     +-----+     +-----+

PQ: (s1, s2, s4)
```

There is nothing stopping the newly merged _PQ_ range from immediately splitting
into _P_ and _Q'_. Note that _P_ is the same range as the original _P_ (i.e., it
has the same range ID) and so the replica on _P_ is still considered a member,
while _Q'_ is a new range, with a new ID, that is unrelated to _Q_:

```
Store 1    Store 2     Store 3     Store 4
+-----+    xxxxxxx     +-----+     +-----+
| P Q'|    |  P  |     |     |     | P Q'|
+-----+    xxxxxxx     +-----+     +-----+

P:  (s1, s2, s4)
Q': (s1, s2, s4)
```

When store 2 comes back online, it will start catching up on missed messages.
But notice how the meta ranges consider store 2 to be a member of _Q'_, because
it was a member of _P_ before the split. The leaseholder for _Q'_ will notice
that store 2's replica is out of date and send over a snapshot so that store 2
can initialize its replica... and all that might happen before store 2 manages
to apply the merge command for _PQ_. If so, applying the merge command for _PQ_
will explode, because the keyspace of the merged range _PQ_ intersects with the
keyspace of _Q'_!

By requiring aligned replica sets, we sidestep this problem. The RHS is, in
effect, a lock on the post-merge keyspace. Suppose we find ourselves in the
analogous situation with replica sets aligned:

```
Store 1    Store 2     Store 3     Store 4
+-----+    xxxxxxx     +-----+     +-----+
| P Q'|    | P Q |     |     |     | P Q'|
+-----+    xxxxxxx     +-----+     +-----+

P:  (s1, s2, s4)
Q': (s1, s2, s4)
```

Here, _PQ_ split into _P_ and _Q'_ immediately after merging, but notice how
store 2 has a replica of both _P_ and _Q_ because we required replica set
alignment during the merge. That replica of _Q_ prevents store 2 from
initializing a replica of _Q'_ until either store 2's replica of _P_ applies the
merge command (to _PQ_) and the split command (to _P_ and _Q'_), or store 2's
replica of _P_ is rebalanced away.

### Replica GC

Per the discussion in the last section, we use the replica of the RHS as a lock
on the keyspace extension. This means that we need to be careful not to GC this
replica too early.

It's easiest to see why this is a problem if we consider the case where one
replica is extremely slow in applying a merge:

```
Store 1    Store 2     Store 3     Store 4
+-----+    +-----+     +-----+     +-----+
| PQ  |    | PQ  |     |     |     | P Q |
+-----+    +-----+     +-----+     +-----+

PQ: (s1, s2, s4)
```

Here, _P_ and _Q_ have just merged. Store 4 hasn't yet processed the merge while
stores 1 and 2 have.

The replica GC queue is continually scanning for replicas that are no longer a
member of their range. What if the replica GC queue on store 4 scans its replica
of _Q_ at this very moment? It would notice that the _Q_ range has been merged
away and, conceivably, conclude that _Q_ could be garbage collected. This would
be disastrous, as when _P_ finally applied the merge trigger it would no longer
have a replica of _Q_ to subsume!

One potential solution would be for the replica GC queue to refuse to GC
replicas for ranges that have been merged away. But that could result in
replicas getting permanently stuck. Suppose that, before store 4 applies the
merge transaction, the _PQ_ range is rebalanced away to store 3:

```
Store 1    Store 2     Store 3     Store 4
+-----+    +-----+     +-----+     +-----+
| PQ  |    | PQ  |     | PQ  |     | P Q |
+-----+    +-----+     +-----+     +-----+

PQ: (s1, s2, s3)
```

Store 4's replica of _P_ will likely never hear about the merge, as it is no
longer a member of the range and therefore not receiving any additional Raft
messages from the leader, so it will never subsume _Q_. The replica GC queue
_must_ be capable of garbage collecting _Q_ in this case. Otherwise _Q_ will
be stuck on store 4 forever, permanently preventing the store from ever
acquiring a new replica that overlaps with that keyspace.

Solving this problem turns out to be quite tricky. What the replica GC queue
wants to know when it discovers that _Q_'s range has been subsumed is whether
the local replica of _Q_ might possibly still be subsumed by its local left
neighbor _P_. It can't just ask the local _P_ whether it's about to apply a
merge, since _P_ might be lagging behind, as it is here, and have no idea that a
merge is about to occur.

So the problem reduces to proving that _P_ cannot apply a merge trigger that
will subsume _Q_. The chosen approach is to fetch the current range descriptor
for _P_ from the meta index. If that descriptor exactly matches the local
descriptor, thanks to [range descriptor generations](#range-descriptor-generations),
we are assured that there are no merge triggers that _P_ has yet to apply, and
_Q_ can safely be GC'd.

Note that it is possible to form long chains of replicas that can only be GC'd
from left to right; the GC queue is not aware of these dependencies and
therefore processes such chains extremely inefficiently (i.e., by processing
replicas in an arbitrary order instead of the necessary order). These chains
turn out to be extremely rare in practice.

There is one additional subtlety here. Suppose we have two adjacent ranges, _O_
and _Q_. _O_ has just split into _O_ and _P_, but store 3 is lagging and has not
yet processed the split.

```
STATE 1

 Store 1      Store 2      Store 3     Store 4
+-------+    +-------+    +-------+   +-------+
| O P Q |    | O P Q |    | O   Q |   |       |
+-------+    +-------+    +-------+   +-------+

O: (s1, s2, s3)
P: (s1, s2, s3)
Q: (s1, s2, s3)
```

At this point, suppose the leader for the new range _P_ decides that store 3
will need a snapshot to catch up, and starts sending the snapshot over the
network. This will be important later. At the same time, _P_ and _Q_ merge while
store 3 is still lagging.

It may seem strange that this merge is permitted, but notice how the replica
sets are aligned according to the descriptors, even though store 3 does not
physically have a replica of _P_ yet. Here's the new state of the world:

```
STATE 2

 Store 1      Store 2      Store 3     Store 4
+-------+    +-------+    +-------+   +-------+
| O  PQ |    | O  PQ |    | O   Q |   |       |
+-------+    +-------+    +-------+   +-------+

O:  (s1, s2, s3)
PQ: (s1, s2, s3)
```

Finally, _O_ is rebalanced from store 3 to store 4 and garbage collected on
store 4:

```
STATE 3

 Store 1      Store 2      Store 3     Store 4
+-------+    +-------+    +-------+   +-------+
| O  PQ |    | O  PQ |    |     Q |   | O     |
+-------+    +-------+    +-------+   +-------+

O:  (s1, s2, s4)
PQ: (s1, s2, s3)
```

The replica GC queue might reasonably think that store 3's replica of _Q_ is out
of date, as _Q_ has no left neighbor that could subsume it. But, at any moment
in time, store 3 could finish receiving the snapshot for _P_ that was started
between state 1 and state 2. Crucially, this snapshot predates the merge, so it
will need to apply the merge trigger... and the replica for _Q_ had better be
present on the store!

This hazard is avoided by requiring that all replicas of the LHS are initialized
before a merge begins. This prevents a transition from state 1 to state 2, as
the merge of _P_ and _Q_ cannot occur until store 3 initializes its replica of
_P_. The AdminMerge command will wait a few seconds in the hope that store 3
catches up quickly; otherwise, it will refuse to launch the merge transaction.
It is therefore impossible to end up in a dangerous state, like state 3, and it
is thus safe for the replica GC queue to GC _Q_ if its left neighbor is
generationally up to date.

### Transaction record GC

The merge watcher goroutine needs to wait until the merge transaction completes,
and determine whether or not the transaction committed or aborted. This turns
out to be brutally complicated, thanks to the aggressive garbage collection of
transaction records.

The watcher goroutine begins by sending a PushTxn request to the merge
transaction. It can easily discover the necessary arguments for the PushTxn
request, that is, the ID and key of the current merge transaction record,
because they're recorded in the intent that the merge transaction has left on
the RHS's local copy of the descriptor.

If the PushTxn request reports that the merge transaction committed, we're
guaranteed that the merge transaction did, in fact, complete. That means that we
can mark the RHS replica as destroyed, bounce all requests back to DistSender
(where they'll get retried on the subsuming range), and clean up the watcher
goroutine. The RHS replica will be cleaned up either when the LHS replica
subsumes it or when the replica GC queue notices that it has been abandoned.

If the PushTxn request instead reports that the merge transaction aborted, we're
not guaranteed that the merge transaction actually aborted. The merge
transaction may have committed so quickly that its transaction record was
garbage collected before our PushTxn request arrived. The PushTxn incorrectly
interprets this state to mean that the transaction was aborted, when, in fact,
it was committed and GCed. To be fair, we're somewhat abusing the PushTxn
request here. Outside of merges, a PushTxn request is only sent when a pending
intent is discovered, and transaction records can't be GCed until all their
intents have been resolved.

So we need some way to determine whether a merge transaction was actually
aborted. What we do is look for the effects of the merge transaction in meta2.
If the merge aborted, we'll see our own range descriptor, with our range ID, in
meta2. By contrast, if the merge committed, we'll see a range descriptor for a
different range in meta2.

This complexity is extremely unfortunate, and turns what should be a simple
goroutine, spawned on the RHS leader for every merge transaction

```go
go func() {
    <-txn.Done() // wait for txn to complete
    if txn.Committed() {
        repl.MarkDestroyed("replica subsumed")
    }
    repl.UnblockRequests()
}
```

into [150 lines of hard to follow code][code].

[code]: https://github.com/cockroachdb/cockroach/blob/82bcd948384e6a482cdd7c916c0aaca32367a7b0/pkg/storage/replica.go#L2813-L2920

### Unanimity

The largest conceptual incongruity with the current merge implementation is the
fact that it requires unanimous consent from all replicas, instead of a majority
quorum, like everything else in Raft. Further confusing matters, only the RHS
needs unanimous consent; a merge can proceed with only majority consent from the
LHS. In fact, it's even a bit more subtle: while only a majority of the LHS
replicas need to vote on the merge command, all LHS and RHS replicas need to
confirm that they are initialized for the merge to start.

There is no theoretical reason that merges need unanimous consent, but the
complexity of the implementation quickly skyrockets without it. For example,
suppose you adjusted the transfer of power so that only a majority of replicas
on the RHS need to be fully up to date before the merge commits. Now, when
applying the merge trigger, the LHS needs to check to see if its copy of the RHS
is up to date; if it's not, the LHS needs to throw away its entire Raft state
and demand a snapshot from the leader because its copy of the RHS may never
catch up (its Raft group may have already been destroyed by this point). This is
both unsightly—our code is worse off every time we reach into Raft—and less
efficient than the existing implementation, as it requires sending a potentially
multi-megabyte snapshot if one replica of the RHS is just a little bit behind in
applying the latest commands.

It's possible that these problems could be mitigated while retaining the ability
to merge with a minority of replicas offline, but an obvious solution did not
present itself. On the bright side, having too many ranges is unlikely to cause
acute performance problems; that is, a situation where a merge is critical to
the health of a cluster is difficult to imagine. Unlike large ranges, which can
appear suddenly and require an immediate split or log truncation, merges are
only required when there are on the order of tens of thousands of excessively
small ranges, which takes a long time to build up.

## Safety recap

This section is a recap of the various mechanisms, which are described in
detail above, that work together to ensure that merges do not violate
consistency.

The first broad safety mechanism is replica set alignment, which is required so
that every store participating in the merge has a copy of both halves of the
data in the merged range. Replica sets are first optimistically aligned by the
merge queue. The replicas might drift apart, e.g., because the ranges in
question were also targeted for a rebalance by the replicate queue, so the
merge transaction verifies that the replica sets are still aligned from within
the transaction. If a concurrent split or rebalance were to occur on the
implicated ranges, transactional isolation kicks in and aborts one of the
transactions, so we know that the replica sets are still aligned at the moment
that the merge commits.

Crucially, we need to maintain alignment until the merge applies on all replicas
that were present at the time of the merge. This is enforced by refusing to
GC a replica of the RHS of a merge unless it can be proven that the store does
not have a replica of the LHS that predates the merge, _nor_ will it acquire
a replica of the LHS that predates the merge. Proving that it does not currently
have a replica of the LHS that predates the merge is fairly straightforward:
we simply prove that the local left neighbor's generation is the newest
generation, as indicated by the LHS's meta descriptor. Proving that the store
will _never_ acquire a replica of the LHS that predates the merge is harder—
there could be a snapshot in flight that the LHS is entirely unaware of. So
instead we require that replicas of the LHS in a merge are initialized on every
store before the merge can begin.

The second broad safety mechanism is the range freeze. This ensures that the
subsuming range and the subsumed range do not serve traffic at the same time,
which would lead to clear consistency violations. The mechanism works by tying
the freeze to the lifetime of the merge transaction; the merge will not commit
until all replicas of the RHS are verified to be frozen, and the replicas of the
RHS will not unfreeze unless the merge transaction is verified to be aborted.
Lease transfers are freeze-aware, so the freeze will persist even if the lease
moves around on the RHS during the merge or if the leaseholder restarts. The
implementation of the freeze ab(uses) the span latch manager, to flush out
in-flight commands on the RHS, an intent on the local range descriptor, to
ensure the freeze persists if the lease is transfered, and an RPC that
repeatedly polls the RHS to wait until it is fully caught up.

## Appendix

The appendix contains digressions that are not directly pertinent to range
merges, but are not covered in documentation elsewhere.

### Key encoding oddities

Lexicographic ordering of keys of unbounded length has the interesting property
that it is always possible to construct the key that immediately succeeds a
given key, but it is not always possible to construct the key that immediately
precedes a given key.

In the following diagrams `\xNN` represents a byte whose value in hexadecimal is
`NN`. The null byte is thus `\x00` and the maximal byte is thus `\xff`.

Now consider a sequence of keys that has no gaps:

```
a
a\x00
a\x00\x00
```

No gaps means that there are no possible keys that can sort between any of the
members of the sequence. For example, there is, simply, no key that sorts
between `a` and `a\x00`.

Because we can construct such a sequence, we must have next and previous
operations over the sequence, which, given a key, construct the immediately
following key and the immediately preceding key, respectively. We can see from
the diagram that the next operation appends a null byte (`\x00`), while the
previous operation strips off that null byte.

But what if we want to perform the previous operation on a key that does not end
in a null byte? For example, what is the key that immediately precedes `b`? It's
not `a`, because `a\x00` sorts between `a` and `b`. Similarly, it's not `a\xff`,
because `a\xff\xff` sorts between `a\xff` and `b`. This process continues
inductively until we conclude the key that immediately precedes `b` is
`a\xff\xff\xff...`, where there are an infinite number of trailing `\xff` bytes.

It is not possible to represent this key in CockroachDB without infinite space.
You could imagine designing the key encoding with an additional bit that means,
"pretend this key has an infinite number of trailing maximal bytes," but
CockroachDB does not have such a bit.

The upshot is that it is trivial to advance in the keyspace using purely
lexical operations, but it is impossible to reverse in the keyspace with purely
lexical operations.

This problem pervades the system. Given a range that spans from `StartKey`,
inclusive, to `EndKey`, exclusive, it is trivial to address a request to
following range, but *not* the preceding range. To route a request to a range,
we must construct a key that lives inside that range. Constructing such a key
for the following range is trivial, as the end key of a range is, by definition,
contained in the following range. But constructing such a key for the preceding
range would require constructing the key that immediately precedes `StartKey`,
which is not possible with CockroachDB's key encoding.
