- Feature Name: Range merges
- Status: completed
- Start Date: 2018-03-30
- Authors: Nikhil Benesch
- RFC PR: [#24394]
- Cockroach Issue: [#2433]

*Disclaimer: As expected, the implementation encountered serious hurdles with
the initial design proposed here. This document is preserved in its original
form for posterity. The resulting implementation, as well as a the challenges
that were encountered, are described in the [range merge tech note].*

[range merge tech note]: docs/tech-notes/range-merges.md

# Summary

A range merge combines two adjacent ranges into one. Ranges are merged
automatically to maintain target size and load, undoing the effects of automatic
size- or load-based splitting. Ranges can also be merged manually to undo the
effects of an `ALTER TABLE ... SPLIT AT` command.

# Motivation

Splitting a range in a CockroachDB cluster today is irreversible. Once a range
is created, it can never be destroyed, not even if it is emptied of all data.
This causes several problems, ordered from least to most important:

  1. **User confusion.** Creating a table introduces new ranges, but dropping a
     table does not remove its ranges. Adjusting the maximum range size
     downwards will split existing ranges where necessary, but adjusting it
     upwards will not merge existing ranges where possible. These incongruencies
     violate the principle of least surprise.

  2. **Bookkeeping overhead.** Every range requires a fixed amount of metadata:
     addressing information must be stored in meta2, and at least three stores
     must maintain replicas of the range. A pileup of unnecessary ranges causes
     observable slowdowns, as even quiescent ranges are periodically scanned by
     the queues and the Raft ticker.

  3. **Unnecessary caution.** Users must be careful with range-creating
     operations, like partitioning and `ALTER TABLE ... SPLIT AT`. The effects
     of a mistyped command can never be undone. Internally, the cluster must be
     careful not to automatically split ranges too aggressively.

So far, we've lived with the user confusion (1), and we've managed the
bookkeeping overhead (2) via range quiescence, which minimizes the overhead of
ranges that receive no traffic. The unnecessary caution (3) has proven to be a
harder pill to swallow. Not only is it a nuisance for our users not to have an
undo button, we're currently unwilling to introduce load-based splitting—an
important feature to unlock performance on skewed workloads—until we can clean
up after ourselves when the load disappears.

Queue-like workloads also suffer from performance problems without range merges,
as they leave behind an ever-growing tail of empty ranges. Removing an item from
the front of the queue in the obvious way (`SELECT * FROM queue ORDER BY id
LIMIT 1`) results in a scan over all these empty ranges.

# Guide-level explanation

Range merging will be largely invisible to users. Astute users will notice that
the "Number of Ranges" graph in the UI can now trend downwards as ranges are
merged; other users will, ideally, notice nothing but improved performance.

Power users who have run `ALTER TABLE ... SPLIT AT` may be interested in the new
`ALTER TABLE ... UNSPLIT AT` command. Specifically, the statement that forces a
split of table `t` at primary key `x`

```sql
ALTER TABLE t SPLIT AT VALUES (x)
```

can be undone by running:

```sql
ALTER TABLE t UNSPLIT AT VALUES (x)
```

`UNSPLIT AT` does not guarantee that the two ranges on either side of `x` will
merge; rather it lifts the restriction that they must be separate. For example,
if those ranges, when combined, would exceed size or load thresholds, the split
point will remain.

`UNSPLIT AT` returns an error if any of the listed split points were not created
by `SPLIT AT`.

# Reference-level explanation

## Detailed design

This is only a rough sketch of a design. It will be fleshed out via an iterative
prototype. Please excuse the stream of conciousness in the meantime.

### Merge queue

The first step is introducing a merge queue that decides when to merge. (We may
find that we actually want to repurpose the split queue.) The merge queue
periodically scans every range for which the store holds the lease to decide
whether it can be merged with its neighbor.

For simplicity, merges always look leftward. That is, a range _R_ will only ever
consider merging into the range _Q_ to its left. A merge of _R_ and its right
neighbor _S_ will take place only when _S_ considers merging to its left.

When considering range _R_, the merge queue first checks for reasons not to
merge _R_ into _Q_. If the merged range would require a split, as indicated by
`SystemConfig.NeedsSplit`, the merge is rejected. This handles the obvious
cases, i.e., when the split point is static, a table boundary, or a partition
boundary. It does not handle preserving split points created by `ALTER TABLE ...
SPLIT AT`.

To preserve these user-created split points, we need to store state. Every range
to the right of a split point created by an `ALTER TABLE ... SPLIT AT` command
will have a special range local key set that indicates it is not to be merged
into the range to its left. I'll hereafter refer to this key as the "sticky"
bit.

So the merge queue will also reject a merge of _R_ into _Q_ if _R_'s sticky bit
is set. The sticky bit can be cleared with an `ALTER TABLE ... MERGE AT`
command to allow the merge queue to proceed with its evaluation.

XXX: We might want to run a migration in 2.1 to backfill the sticky bit. It's
unfortunately impossible, I think, to back out which ranges were split due to a
manual `SPLIT AT` and which ranges were split due to exceeding a size threshold
that they no longer exceed. Maybe we take the conservative approach and give
every existing range the sticky bit?

If the merge queue discovers no reasons to reject the merge, it executes
AdminMerge(_Q_):

```protobuf
message RangeStats {
  int64 logical_bytes = 1;
  // Other information used by size-based splitting.

  float64 writes_per_second = 2;
  // Other information used by load-based splitting.
}

message AdminMergeRequest {
  Span header = 1;
  RangeStats rhs_stats = 2;
}
```

### Step one: merging small ranges

Merging ranges is simpler when we can write all of _R_'s data to _Q_ via a Raft
command. This requires empty or nearly-empty ranges; pushing a full 32MiB write
batch command through Raft is going to run into write amplification problems and
will barely clear the max command size.

So, to start, we'll limit merges to ranges that are small enough to avoid these
concerns. Here's how it works:

1. **Q's leaseholder runs AdminMerge(_Q_).**

   AdminMerge(_Q_) starts by ensuring that the hypothetical range _Q_ + _R_ will
   not be immediately split by the split queue. Ideally we'll just reuse the
   split queue's logic here, conjuring up a range to ask about by summing
   together _Q_ and _R_'s range statistics. (We have a local copy of _Q_ and
   its stats, and a recent copy of _R_'s stats were included in the AdminMerge
   request.)

   Then, AdminMerge(_Q_) launches a straightforward transaction.

   First the transaction updates _Q_'s local range descriptor to extend to _R_'s
   end key. This ensures that the transaction record is located on _Q_.

   Second, the transaction deletes _R_'s local range descriptor. (This leaves a
   deletion intent on the key.)

   Third, the transaction performs the equivalent updates to the meta addressing
   records.

   At some point while the transaction is open, _L<sub>Q</sub>_ executes
   GetSnapshotForMerge(_R_). This request returns a consistent snapshot of _R_;
   it also guarantees that, when it returns, _R_ has no pending commands and
   will not accept any pending commands until the transaction we have open
   either commits or aborts. See below for more details on how
   GetSnapshotForMerge works.

   Finally, the transaction commits, including the snapshot of _R_ in the
   merge trigger and sychronously resolving any local intents.

2. **Merge trigger evaluation**

   When the merge trigger is evaluated, _R_'s snapshot is incorporated into the
   command's write batch and the MVCCStats are updated accordingly.

3. **Merge trigger application**

   When the merge trigger applies, the local store checks to see whether it has
   a copy of both _Q_ and _R_. If it only has _Q_, it does nothing. If it has
   both _Q_ and _R_, it must somehow indicate in the store metadata that _R_ no
   longer owns its user data. (XXX: how?)

   XXX: There might be synchronization issues to work out here.

 4. **Replica garbage collection**

    Some time later, the replica garbage collection queue notices that _R_ has
    been subsumed by _Q_ and removes it from the store. In the usual case, _R_'s
    replicas will be listening for the merge transaction to commit and queue
    themselves for immediate garbage collection.

    XXX: There are synchronization issues to work out here.

### GetSnapshotForMerge

Above, we rely on capturing a consistent snapshot of _R_ and including it in the
merge trigger. If _R_ were to process writes between when we captured its
snapshot and when the merge transaction committed, those updates would be lost
after the merge. Similarly, if _R_ were to serve reads after the merge
committed, it might return stale data. We also want to avoid permanently
freezing _R_ because, if the merge transaction aborts, we want to resume
business as usual.

GetSnapshotForMerge, coupled with the IsMerging flag described below, is the
magic that holds _R_ still for exactly the right amount of time.

To the command queue, GetSnapshotForMerge appears as a read that touches all
keys. By the time the command is evaluated, we're guaranteed that there are no
other in-flight commands on the range. The command captures a snapshot of the
range at this point in time using the existing code paths for generating
preemptive/Raft snapshots.

Before evaluation completes, the command sets an IsMerging flag on _R_'s
in-memory state. Future commands are held in limbo until the flag is cleared.

XXX: There are synchronization issues to work out here.

### The IsMerging flag

The IsMerging flag must be cleared when the merge transaction completes. At that
point the replica must handle any pending commands that piled up while the merge
transaction was in progress.

If the transaction committed, the replica is no longer responsible for the data
in this range. It responds to any pending commands with a RangeKeyMismatchError.
Then it carefully removes itself from the store metadata and marks itself for
garbage collection.

XXX: There are synchronization issues to work out here.

If the merge aborted, the replica still responsible for the data in the range,
and it returns to processing commands as normal.

To determine when the merge transaction has completed without repeatedly polling
the transaction record, the replica enters the wait queue for the merge
transaction, which sends it a notification when the transaction completes.

We need to be careful that IsMerging state is not lost if the original
leaseholder loses its lease while the merge transaction is in progress. The new
leaseholder can derive whether IsMerging should be set by looking for an intent
on its range descriptor. If this intent refers to an in-progress transaction,
the new leaseholder sets the IsMerging flag on the replica. Note that this means
that after _every_ forced lease transfer the new leaseholder must check for an
intent on its local range descriptor. This check is expected to be a very
cheap read of a local key with a negligible effect on performance.

If _R_'s lease changes hands before the GetSnapshotForMerge request is sent, the
new leaseholder will set the IsMerging flag _before_ receiving the
GetSnapshotForMerge request. As long as the _R_ allows GetSnapshotForMerge
to proceed even when the IsMerging flag is set, this isn't a problem, but it is
somewhat odd.

One last complication arises if a node with a replica of _R_ reboots before the
merge transaction has committed. When initializing its replicas, it will see an
intent on _R_'s range descriptor. If the merge has committed, we want to be sure
that we resolve that intent as quickly as possible. We can use the pre-intent
version of the range descriptor to initialize the replica in the meantime,
though.

XXX: What happens if a range merges then immediately splits at exactly the same
key? The stores are likely to be very confused. In theory, since the second
split can't start its transaction until the merge's intents are all resolved,
and everything else happens synchronously in the commit triggers. This deserves
lots of tests. What if a node with a replica of _R_ goes down in the middle of
the merge transaction, comes up back up with an intent on its local range
descriptor, and then receives a snapshot of _R'_ (because _QR_ split at the same
key) while it's trying to resolve the intent on _R_'s descriptor?

### Assorted invariants

 * Meta ranges are never merged. Here be dragons.

 * If a merge trigger for _Q_ has been applied on a replica and that replica's
   store also has a replica of _R_, that replica of _R_ will never again touch
   its user data. That data now belongs to _Q_.

 * From the moment a snapshot of _R_ is generated until the merge transaction
   commits or aborts, _R_ will not process any commands.

 * _Q_'s local copy of its descriptor will never have an intent that refers to
   a committed or aborted transaction.

### Step two: general case

To support the general case of merging large ranges, we need to avoid sending
large write batches through Raft. The basic idea is to ensure that _R_ and _Q_'s
replica sets are aligned before and during the merge. In other words, every
replica of _Q_ must be located on a store that also has a replica of _R_, and
vice-versa. When the merge executes, only metadata needs to be updated; _R_'s
data will already be in the right place.

The merge queue will be taught to perform this colocation before sending an
AdminMerge request by coordinating with the replicate queue. In short, if
_L<sub>Q<sub>_ accepts a SuggestMergeRequest from _L<sub>R<sub>_ it expects for
_R_ to align its replica set with _Q_'s replica set, then transfer its lease to
_L<sub>Q</sub>.

During the execution of the merge transaction, _L<sub>Q<sub>_ and _L<sub>R<sub>_
need to prevent membership changes of either _Q_ or _R_. Because all membership
changes require updating the same range metadata that we'll be updating in the
merge transaction, we don't need to do anything special as long as we verify
that the replica sets are aligned within the merge transaction.

The behavior of GetSnapshotForMerge also changes slightly. Instead of returning
a consistent snapshot of _R_, it waits for all of _R_'s followers to apply all
prior commands. This will require an additional RPC to poll a follower's applied
status. We can't use the leader's knowledge of each follower's commit index, as
those commands may be sitting unapplied in the Raft log, and that Raft log will
be thrown away after the merge. Once GetSnapshotForMerge returns, it's safe to
commit the merge, as all replicas of _R_ are up-to-date and the presence of the
IsMerged flag will prevent _L<sub>R</sub>_ from processing any commands until
the merge completes.

XXX: We need more magic to ensure that membership changes are not processed
until after every replica has _applied_ the merge trigger. Waiting for commit is
not sufficient. This is a little scary. What if one of the replicas goes down
after the merge is committed but before it applies the merge trigger? Then we
need to very carefully upreplicate _QR_ with a snapshot from a replica that _is_
up-to-date. Or perhaps I'm missing something.

XXX: We also need to be sure not to send any snapshots that contain an unapplied
merge trigger. It sounds like this already happens.

## Miscellany

A few miscellaneous thoughts follow.

Nodes will only perform one merge at a time, which should limit any disruption
to the cluster when e.g. a table is dropped and many ranges become eligible for
merging at once.

Thrashing should be minimal, as _L<sub>Q</sub>_ will reject any
`SuggestMergeRequests` for _Q_ and _R_ received while attempting to merge _Q_
and _P_.

The merge queue will temporarily pause if the cluster is unbalanced to avoid
further unbalancing the cluster.

The replica GC queue will need to consider the case where looking up a replica's
range descriptor returns a "range no longer exists" error. This is no longer
indicative of programmer error but of a range that has been merged away.

With support for range merges, we'll be able to merge dropped tables down to a
single range, but we don't be able to merge a dropped table away completely
until we change SystemConfig.ComputeSplitKey to only split for tables that
exist. (At the moment, we introduce a split at every ID less than the maximum ID
seen.)

## Drawbacks

Range merges are complicated. They will introduce the three worst kind of bugs:
performance bugs, stability bugs, and correctness bugs.

On the bright side, range merges are a strict improvement for user experience.

## Rationale and Alternatives

What if we could further reduce the per-range overhead? Could we put off range
merges for another few releases if we made quiescent ranges sufficiently cheap?

For example, the Raft ticker could avoid scanning over quiescent ranges if we
stored quiescent ranges separately from active ranges.

This boils down to: will we ever make ranges cheap enough that we'll be
comfortable introducing load-based splitting? I suspect the answer is no.

## Unresolved questions

* The specifics of the synchronization necessary to achieve the design.
* Whatever else the implementation uncovers.

[#2433]: https://github.com/cockroachdb/cockroach/issues/2433
[#24394]: https://github.com/cockroachdb/cockroach/issues/24394
