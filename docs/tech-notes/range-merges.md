# Range merges

**Last update:** December 21, 2018

**Original author:** Nikhil Benesch

This document serves as an end-to-end description of the implementation of range
merges. The intended audience is someone who is reasonably familiar with core
but unfamiliar with either the "how" or the "why" of the range merge
implementation.

The most complete documentation, of course, is in the code, tests, and the
surrounding comments, but those pieces are necessarily split across several
files and packages. That scattered knowledge is centralized here, without
excessive detail that is likely to become stale.

## Overview

A range merge begins when two adjacent ranges are selected to be merged
together. For example, suppose our adjacent ranges are _Q_ and _R_, somewhere in
the middle of the keyspace:

```
--+-----+-----+--
  |  Q  |  R  |
--+-----+-----+--
```

We'll call _Q_ the left-hand side (LHS) of the merge, and _R_ the right-hand
side (RHS) of the merge. For reasons that will become clear later, we also refer
to _Q_ as the subsuming range and _R_ as the subsumed range.

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

By requiring replica set alignment, the merge operation is reduced to a
metadata update, albiet a tricky one, as the stores that will have a copy of
the merged range PQ already have all the constitudent data, by virtue of having
a copy of both P and Q before the merge begins.

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
trigger" that instructs the receiving store to update its in-memory bookkeping
to match the updates to the range descriptors in the transaction. The moment the
merge transaction is considered committed, the merge is complete!

The remaining sections cover each of these steps in more detail.

## Preconditions

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

## Initiating a merge

A merge is initiated by sending a AdminMerge request to a range. Like other
admin commands, DistSender will automatically route the request to the
leaseholder of the range, but there is no guarantee that the store will retain
its lease while the admin command is executing.

Note that an AdminMerge request takes no arguments, as there is no choice in
what range will be merged. The recipient of the AdminMerge will always be the
LHS, subsuming range, and its right neighbor at the moment that the AdminMerge
command begins executing will always be the RHS, subsumed range.

It would perhaps have been wise to require that the AdminMerge specify the
descriptor of the intented subsumption target. It is, at present, possible for
someone to send an admin merge to P, intending to merge P's right neighbor Q,
but Q can split in the meantime and so instead P merges with a different range.
Luckily, if this proves to be a problem, it is an easy extension to the API.

It would have been reasonable to have instead used the RHS to coordinate the
merge. That is, the RHS would have been the subsuming range, and the LHS would
have been the subsumed range. Using the LHS is slightly easier, however, due to
an oddity of key encoding and range bounds. It is trivial for a range to send a
request to its right neighbor, as it simply addresses the request to its end
key, but difficult to send a request to its left neighbor.

At the time of writing, only the merge queue initiates merges, and it does so
by bypassing DistSender and invoking the AdminMerge command directly on the
local replica. At some point in the future, we may wish to expose manual merges
via SQL, at which point the SQL layer will need to send proper AdminMerge
requests through the KV API.

## Merge transaction

The merge transaction piggybacks on CockroachDB's strict serializability to
provide much of the necessary synchronization for the bookkeeping updates. For
example, merges cannot occur concurrently with any splits or replica changes on
the implicated ranges, because the merge transaction will naturally conflict
with those split transaction and change replicas transactions, as both
transactions will attempt to write updated range descriptors and conflict. No
additional code was needed to enforce this, as our standard transaction
conflict detection mechanisms kick in here (write intents, the timestamp cache,
the command queue, etc.).

There was, however, one knotty edge case that was not immediately eliminated by
transaction serializability. Suppose we have our standard misaligned replica
set situation:

```
Store 1    Store 2     Store 3     Store 4
+-----+    +-----+     +-----+     +-----+
| P Q |    | P Q |     |     |     | P Q |
+-----+    +-----+     +-----+     +-----+
```

In an unfortunate twist of fate, a rebalance of P from store 2 to store 3
begins at the same time as a merge of P and Q begins. Let's quickly cover the
valid outcomes of this race.

  1. The rebalance commits first. The merge must abort, as the replica sets of
     P and Q are no longer aligned.

  2. The merge commits before the rebalance starts. The rebalance
     operation voluntarily aborts, as the decision to rebalance P needs to
     be updated for the newly extended range PQ.

  3. The merge commits first, but after the rebalance has started. The rebalance
     operation must abort, as it will be sending a preemptive snapshot for P
     to store 3.

  1. The merge commits and the rebalance fails.

The merge transaction uses internal commit triggers to...


## Transfer of power

The trickiest part of a merge is



## Missteps

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
+-----+    +xxxxx+     +-----+     +-----+
| PQ  |    X  P  X     |     |     | PQ  |
+-----+    +xxxxx+     +-----+     +-----+

PQ: (s1, s2, s4)
```

There is nothing stopping the newly merged PQ range from immediately splitting
into P and Q'. Note that P is the same range as the original P (i.e., it has
the same range ID) and so the replica on P is still considered a member, while
Q' is a new range, with a new ID, that is unrelated to Q:

```
Store 1    Store 2     Store 3     Store 4
+-----+    +xxxxx+     +-----+     +-----+
| P Q'|    X  P  X     |     |     | P Q'|
+-----+    +xxxxx+     +-----+     +-----+

P: (s1, s2, s4)
Q': (s1, s2, s4)
```

When store 2 comes back online, it will start catching up on missed messages.
But notice how store 2 is considered to be a member of Q', because it was a
member of P before the split. The leaseholder for Q' will send over a snapshot
to store 2 so that store 2 can initialize its replica... and all that might
happen before store 2 manages to apply the merge command for PQ. When store 2
came back online.
