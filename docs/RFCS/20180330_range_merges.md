- Feature Name: Range merges
- Status: draft
- Start Date: 2018-03-30
- Authors: Nikhil Benesch
- RFC PR:
- Cockroach Issue: (one or more # from the issue tracker)

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
     table does not remove its ranges. This violates the principle of least
     surprise.

  2. **Bookkeeping overhead.** Every range requires a fixed amount of metadata
     and must be periodically scanned by the queues (XXX is this true?).
     A pileup of unnecessary ranges causes observable slowdowns.

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

# Guide-level explanation

Range merging will be largely invisible to users. Astute users will notice that
the "Number of Ranges" graph in the UI can now trend downwards as ranges are
merged; other users will, ideally, notice nothing but improved performance.

Power users who have run `ALTER TABLE ... SPLIT AT` may be interested in the new
`ALTER TABLE ... MERGE AT` command. Specifically, the statement that forces a
split of table `t` at primary key `x`

```sql
ALTER TABLE t SPLIT AT VALUES (x)
```

can be undone by running:

```sql
ALTER TABLE t MERGE AT VALUES (x)
```

`MERGE AT` does not guarantee that the two ranges on either side of `x` are
actually merged; rather it lifts the restriction that they must be separate. For
example, if those ranges, when combined, would exceed size or load thresholds,
the split point will remain.

# Reference-level explanation

## Detailed design

This is only a rough sketch of a design. It will be fleshed out via an iterative
prototype. Please excuse the stream of conciousness in the meantime.

The first step is introducing a merge queue. (We may find that we actually want
to repurpose the split queue.) The merge queue periodically scans every range
for which the store holds the lease to decide whether it can be merged with
its neighbor.

For simplicity, merges always look leftward. That is, a range _R_ will only
ever consider merging into the range _Q_ to its left. A merge of _R_ and _S_
will take place only when _S_ considers merging to its left.

When considering range _R_, the merge queue first checks for reasons not to
merge _R_ into _Q_. If the merged range would require a split, as indicated by
`SystemConfig.NeedsSplit`, the merge is rejected. This handles the obvious
cases, i.e., when the split point is static, a table boundary, or a partition
boundary. It does not handle preserving split points created by `ALTER TABLE ...
SPLIT AT`.

To preserve these user-created split points, we need to store state. I
tentatively propose using a replicated range-ID local key. Every range to the
right of a split point created by an `ALTER TABLE ... SPLIT AT` command will
have a special replicated range-ID local key set that indicates it is not to be
merged into the range to its left. I'll hereafter refer to this key as the
"sticky" bit.

So the merge queue will also reject a merge of _R_ into _Q_ if _R_'s sticky bit
is set. The sticky bit can be cleared with an `ALTER TABLE ... MERGE AT`
command to allow the merge queue to proceed with its evaluation.

XXX: We might want to run a migration in 2.1 to backfill the sticky bit. It's
unfortunately impossible, I think, to back out which ranges were split due to a
manual `SPLIT AT` and which ranges were split due to exceeding a size threshold
that they no longer exceed. Maybe we take the conservative approach and give
every existing range the sticky bit?

If the merge queue discovers no reasons to reject the merge, it sends a
SuggestMergeRequest to range _Q_:

```protobuf
message RangeStats {
  int64 logical_bytes = 1;
  // Other information used by size-based splitting.

  float64 writes_per_second = 2;
  // Other information used by load-based splitting.
}

message SuggestMergeRequest {
  Span header = 1;
  RangeStats range_stats = 2;
}
```

Now _Q_'s leaseholder _L<sub>Q</sub>_ has perfect information. The merge
proceeds iff _L<sub>Q</sub>_ would _not_ split the hypothetical range _Q_ + _R_.
Ideally we'll just reuse the split queue's logic here, conjuring up a range
to ask about by summing together _Q_ and _R_'s range stats. _L<sub>Q</sub>_
responds immediately to _L<sub>Q</sub>_ with whether it will proceed with the merge.

If the merge is to proceed, _L<sub>Q</sub>_ places _Q_ into the replicate queue;
the replicate queue, seeing that _Q_ is slated for merging, transfers its lease
to _L<sub>R</sub>_, adding a replica first if necessary.

_L<sub>R</sub>_, seeing that its `SuggestMergeRequest` was successful, will poll
until it has received _Q_'s lease. (XXX: Is there a better way to do this than
polling?) Once it receives the lease, it coordinates with the replicate queue to
align the remainder of _Q_'s replicas with _R_'s. Once all replicas are aligned,
it executes an `AdminMerge` on _Q_ and the merge is complete.

If _L<sub>R</sub>_ does not receive _Q_'s lease in a reasonable amount of time,
it requeues _R_ for a later retry.

XXX:
  * Does `AdminMerge` properly handle in-flight traffic to _Q_ and _R_, or do we
    need to somehow "lock" them while the merge is in progress?

  * Is `AdminMerge` correct? Tobi suggests its correctness has likely rotted.

A few miscellaneous thoughts follow.

Nodes will only perform one merge at a time, which should limit any disruption
to the cluster when e.g. a table is dropped and many ranges become eligible for
merging at once.

Thrashing should be minimal, as _L<sub>Q</sub>_ will reject any
`SuggestMergeRequests` for _Q_ and _R_ received while attempting to merge _Q_
and _P_.

The merge queue will temporarily pause if the cluster is unbalanced to avoid
further unbalancing the cluster.

## Prototype ordering

I'm planning to start by implementing the merge queue with enough smarts to
avoid merging ranges that would be immediately split back apart by the split
queue. This makes for easy manual testing: create a table with some data,
run `ALTER TABLE ... SPLIT AT`, then watch the merge queue undo the effects of
the manual splits. If the data in the table isn't corrupted afterwards, we're
in good shape. If queries during the merge return correct data, we're in even
better shape.

## Drawbacks

Range merges are complicated. They will introduce the three worst kind of bugs:
performance bugs, stability bugs, and correctness bugs.

On the bright side, range merges are a strict improvement for user experience.

## Rationale and Alternatives

Several alternatives have been proposed:

* **Don't support the general case.**

  Perhaps it's easier if we start with a simpler problem.

  What if we only merged ranges in dropped tables? Correctness would be less
  important because we know that after the GC deadline, ranges in a dropped
  table cannot receive any external traffic.

  What if we only merged empty ranges? The allocator could be much dumber about
  deciding what ranges to merge.

  I don't yet have enough context to determine whether one of these special
  cases is meaningfully simpler than the general case. Iterative prototyping
  will prove useful here.

* **Further reduce the per-range overhead.**

  Can we put off range merges for another few releases if we make ranges
  sufficiently cheap?

  This boils down to: will we ever make ranges cheap enough that we'll be
  comfortable introducing load-based splitting? I suspect the answer is no.

## Unresolved questions

* Most of the implementation.