# Range merges

**Last update:** January 3, 2018

**Original author:** Nikhil Benesch

This document serves as an end-to-end description of the implementation of range
merges. The target reader is someone who is reasonably familiar with core
but unfamiliar with either the "how" or the "why" of the range merge
implementation.

The most complete documentation, of course, is in the code, tests, and the
surrounding comments, but those pieces are necessarily split across several
files and packages. That scattered knowledge is centralized here, without
excessive detail that is likely to become stale.

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
range PQ already have all the constituent data, by virtue of having a copy of
both P and Q before the merge begins.

After verifying that the merge is sensible, the coordinator transactionally
updates the implicated range descriptors, adjusting P's range descriptor so that
it extends to Q's end and deleting Q's range descriptor.

Then, the coordinator needs to atomically move responsibility for the data in
the RHS to the LHS. This is tricky, as the lease on the LHS may be held by a
different store than the lease on the RHS, The coordinator notifies the RHS that
it is about to be subsumed and it is prohibited from serving any additional read
or write traffic. Only when the coordinator has received an acknowledgement from
_every_ replica of the RHS, indicating that no traffic is possibly being served
on the RHS, does the coordinator commit the merge transaction.

Like with splits, the merge transaction is committed with a special "commit
trigger" that instructs the receiving store to update its in-memory bookkeeping
to match the updates to the range descriptors in the transaction. The moment the
merge transaction is considered committed, the merge is complete!

The remaining sections cover each of these steps in more detail.

## Implementation details

### Preconditions

Not any two ranges can be merged. The first and most obvious criterion is that
the two ranges must be adjacent. Suppose a simplified cluster that has only
three ranges, A, B, and C:

```
+-----------+-----+
|  A  |  B  |  C  |
+-----------+-----+
```

Ranges A and B can be merged, as can ranges B and C, but not ranges A and C, as
they are not adjacent. Note that adjacent ranges are equivalently referred to
as "neighbors", as in, range B is range A's right-hand neighbor.

The second criterion is that the replica sets must be aligned. To illustrate,
consider a four node cluster with the default 3x replication. The allocator has
attempted to balance ranges as evenly as possible:

```
Node  1    Node  2     Node  3     Node  4
+-----+    +-----+     +-----+     +-----+
| P Q |    |  P  |     |  Q  |     | P Q |
+-----+    +-----+     +-----+     +-----+
```

Notice how node 2 does not have a copy of Q, and node 3 does not have a copy of
P. These replica sets are considered "misaligned." Aligning them requires
rebalancing Q from node 3 to node 2, or rebalancing P from node 2 to node 3:

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

Note that we explored an alternative merge implementation that did not require
aligned replica sets, but found it to be unworkable. See the
[misaligned replica sets misstep](#misaligned-replica-sets) for details.

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
have been the subsumed range. Using the LHS turned out to be easier, however,
due to an oddity of key encoding and range bounds. It is trivial for a range to
send a request to its right neighbor, as it simply addresses the request to its
end key, but it is difficult to send a request to its left neighbor, as there
is no function to get the key that immediately precedes the range's start key.
See the [Key encoding oddities](#key-encoding-oddities) section of the appendix
for details.

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

The merge transaction piggybacks on CockroachDB's strict serializability to
provide much of the necessary synchronization for the bookkeeping updates. For
example, merges cannot occur concurrently with any splits or replica changes on
the implicated ranges, because the merge transaction will naturally conflict
with those split transaction and change replicas transactions, as both
transactions will attempt to write updated range descriptors and conflict. No
additional code was needed to enforce this, as our standard transaction
conflict detection mechanisms kick in here (write intents, the timestamp cache,
the command queue, etc.).

Note that there was one surprising synchronization problem that was not
immediately handled by serializability. See [Range descriptor
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
Specifically, the transaction record needs to live on the subsuming range,
as the "internal commit trigger" that actually applies the merge to the
replica's in-memory state runs on the range where the transaction record lives.
The transaction record is created on the range that the transaction writes
first; therefore, the merge transaction is careful to update the local copy of
the LHS descriptor as its first operation, since the local copy of the LHS
descriptor lives on the LHS.

Second, the merge transaction must ensure that, when it issues the delete
request to remove the local copy of the RHS descriptor, the resulting intent is
actually written to disk. (See [Transfer of power](#transfer-of-power) for why
this is required.) Thanks to [transactional pipelining][#26599], KV writes can
return early, before their intents have actually been laid down. The intents are
not required to make it to disk until the moment before the transaction commits.
The merge transaction simply disables pipelining to avoid this hazard.

As the last step before the commit, the merge transaction needs to freeze the
RHS, then wait for _every_ replica of the RHS to apply all outstanding commands.
This ensures that, when the merge commits, every LHS replica can blindly assume
that it has perfectly up-to-date data for the RHS. To quickly recap, this is
guaranteed because 1) the replica sets were aligned when the merge transaction
started, 2) rebalances that would misalign the replica sets will conflict with
the merge transaction, causing one of the transactions to abort, 3) the RHS
is frozen and cannot process any new commands, and 4) every replica of the RHS
is caught up on all commands. The process of freezing the RHS and waiting for
every replica to catch up is covered more thoroughly in the next section,
[Transfer of power](#transfer-of-power).

Finally, the merge transaction commits with an "internal commit trigger" that
indicates a merge has completed. When applying a commit with a merge trigger,
the receiving replica runs additional code to update in-memory bookkeeping.
Specifically, it instructs the store to remove its right neighbor, the subsumed
replica, and widen its end key. (TODO: pronouns here are confused)

[#26599]: https://github.com/cockroachdb/cockroach/pull/26599

### Transfer of power

The trickiest part of a merge is atomically swapping responsibility for the
keyspace owned by the RHS to the LHS.

TODO(benesch): flesh out.

* The merge transaction locks the RHS. RHS promises not to serve any more
  reads and writes.
* Can't serve reads even beneath the merge txn timestamp due to clock skew.
  Same reason as lease transfers. (Serving at merge txn timestamp - max clock skew is probably safe for the same reason as lease transfers, but we don't do it
  because complicated).
* If the RHS loses its lease, the new leaseholder *also* promises not to serve
  any more reads and writes. How? The leaseholder always checks for a deletion
  intent on its range descriptor, indicating that a merge is in progress, when
  it first acquires the lease.
* All requests to the RHS block until the merge commits or aborts.
* The RHS spins up a watcher goroutine to observe the status of the merge txn.
  If the merge txn commits, the RHS marks itself as destroyed. This will bounce
  all pending requests back to the distsender, who will retry them on the LHS.
  If the merge txn aborts, the RHS processes the blocked requests.
* Determining when the merge txn is actually committed is really, really hard
  thanks to txn record GC. TODO: Add a subtle complexity section about this?

## Subtle complexities

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
considered to be members of the range, from the prespective of the most recently
committed range descriptor.

Now, to merge P and Q in this situation without aligning their replica sets,
store 2 needed to be provided a copy of store 3's data. To accomplish this, a
copy of Q's data was stashed in the merge trigger, and P would write this data
into its store when applying the merge trigger.

There was, sadly, a large and unforeseen problem with lagging replicas.
Suppose store 2 loses network connectivity a moment before ranges P and Q
are merged. Note that store 2 is not required for P and Q to merge, because
only a quorum is required on the LHS to commit a merge. Now the situation
looks like this:

```
Store 1    Store 2     Store 3     Store 4
+-----+    xxxxxxx     +-----+     +-----+
| PQ  |    |  P  |     |     |     | PQ  |
+-----+    xxxxxxx     +-----+     +-----+

PQ: (s1, s2, s4)
```

There is nothing stopping the newly merged PQ range from immediately splitting
into P and Q'. Note that P is the same range as the original P (i.e., it has
the same range ID) and so the replica on P is still considered a member, while
Q' is a new range, with a new ID, that is unrelated to Q:

```
Store 1    Store 2     Store 3     Store 4
+-----+    xxxxxxx     +-----+     +-----+
| P Q'|    |  P  |     |     |     | P Q'|
+-----+    xxxxxxx     +-----+     +-----+

P:  (s1, s2, s4)
Q': (s1, s2, s4)
```

When store 2 comes back online, it will start catching up on missed messages.
But notice how store 2 is considered to be a member of Q', because it was a
member of P before the split. The leaseholder for Q' will notice that store 2's
replica is out of date and send over a snapshot so that store 2 can initialize
its replica... and all that might happen before store 2 manages to apply the
merge command for PQ. If so, applying the merge command for PQ will explode,
because the keyspace of the merged range PQ intersects with the keyspace of Q'!

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

Here, PQ split into P and Q' immediately after merging, but notice how store 2
has a replica of both P and Q because we required replica set alignment during
the merge. That replica of Q prevents store 2 from initializing a replica of Q'
until either store 2's replica of P applies the merge command (to PQ) and the
split command (to P and Q'), or store 2's replica of P is rebalanced away.

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

Now, the replica GC queue is continually scanning for replicas that are no
longer a member of their range. What if the replica GC queue on store 4 scans
its replica of _Q_ at this very moment? It would notice that the _Q_ range has
been merged away and, conceivably, conclude that _Q_ could be garbage collected.
This would be disastrous, as when _P_ finally applied the merge trigger it would
no longer have a replica of _Q_ to subsume!

One potential solution would be for the replica GC queue to refuse to garbage
collect replicas for ranges that had been merged away. But that could result
in replicas getting permanently stuck. Suppose that, before store 4 applies
the merge transaction, the _PQ_ range is rebalanced away to store 3:

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
_must_ be capable of garbage collecting _Q_ in this case.

Solving this problem turns out to be quite tricky. What _Q_ wants to know is
whether it might possibly be subsumed by its left neighbor. _Q_ can't just ask
its local left neighbor (in this case, _P_) whether it's about to apply a merge,
as in this case _P_ is lagging and has no idea that a merge is about to occur.

Instead, the replica GC queue asks the question "is _Q_'s left neighbor
generationally up to date"? In other words, does the meta range descriptor
indicate that a split or merge has occurred on _P_ that is not reflected in the
local copy of _P_? If _P_ is generationally up to date, then we know there is
no merge that could possibly subsume _Q_, and it is therefore safe to GC _Q_.
Otherwise, we need to hold off on GC'ing _Q_. In this case, the situation is
resolved when _P_ a) applies a merge that subsumes _Q_, b) applies a series
of splits/merges that don't subsume _Q_ but make _P_ generationally up to date
and thus make _Q_ eligible for GC, or c) _P_ is itself GC'd, which also makes
_Q_ eligible for GC.

Note that it is possible to form long chains of replicas that can only be GC'd
from left to right; the GC queue is not aware of these dependencies and
therefore processes such chains extremely inefficiently. This turns out to be
extremely rare in practice.

TODO(benesch): TestStoreRangeMergeUninitializedLHSFollower and the uninitialized
replica problem.

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

  1. The rebalance commits before the merge commits. The merge must abort, as
     the replica sets of P and Q are no longer aligned.

  2. The merge commits before the rebalance starts. The rebalance should
     voluntarily abort, as the decision to rebalance P needs to be updated in
     light of the merge. It is not, however, a correctness problem if the
     rebalance commits; it simply results in rebalancing a larger range than
     may have been intended.

  3. The merge commits before the rebalance commits, but after the rebalance has
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
