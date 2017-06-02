- Feature Name: Fast `DeleteRange` Command
- Status: draft
- Start Date: 2017-06-01
- Authors: Spencer Kimball, Daniel Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#2003](https://github.com/cockroachdb/cockroach/issues/2003) [#14120](https://github.com/cockroachdb/cockroach/issues/14120) [#14279](https://github.com/cockroachdb/cockroach/issues/14279) [#15723](https://github.com/cockroachdb/cockroach/issues/15723) [#16161](https://github.com/cockroachdb/cockroach/issues/16161)

# Summary

Efficient deletion of entire Ranges, including partial deletions which
cover only data written *after* a specified timestamp. This supports
fast, transactional `DROP TABLE`, `DROP INDEX`, and `TRUNCATE TABLE`
SQL statements, as well as point-in-time recovery.

# Motivation

The primary goal for an enhanced `DeleteRange` command is efficiency.
A secondary goal is to lay the foundation for building a point-in-time
recovery mechanism, capable of reversing mutations older than a
specified historical timestamp.

It is important to disambiguate the word "range" in this document. In
the context of the `DeleteRange` command, it refers to any range of
keys, which may contain 0 or more actual key-value pairs stored to the
underlying database. We use "Range" (with a capital "R") to instead
refer to a contiguous segment of the CockroachDB keyspace which is
replicated via an instance of the Raft consensus algorithm, and
accessed via the distributed KV layer via first the Node, then the
Store, and finally one of the Range's replicas (`storage.Replica`,
defined in `/pkg/storage/range.go`).

The current implementations of `DROP (TABLE|INDEX)` and `TRUNCATE
TABLE` work in much the same way, although the implementation of
`TRUNCATE TABLE` must accommodate all row deletions into a single
transaction and so will break for large tables. By contrast, `DROP`
uses many transactions, each with a limit on the total number of rows,
to keep each transaction under CockroachDB's max transaction size
threshold.

The reason that `DROP TABLE` is free to do its work using multiple
transactions is that it proceeds only *after* the schema change used
to drop the table has completed, unlinking the table ID from the
schema, and preventing further usage. Since no concurrent access of
the table is allowed, the contents can be deleted in any manner
convenient. A further small point about the existing implementation's
efficiency: when calling `DeleteRange` with a specified row limit, the
KV API distributed sender is not free to send to ranges in parallel,
causing it to serially process the ranges, regardless of how many nodes
are in the cluster (i.e. how much parallelism is available).

In both the case of `DROP` and `TRUNCATE`, if there are no interleaved
sub-tables, the code in `/pkg/sql/tablewriter.go:
tableDeleter.deleteAllRows` proceeds to delete the underlying data
without requiring a scan, using the `DeleteRange` KV command. This is
why we expect that improving the efficiency of the underlying KV
command will yield commensurate benefit to the SQL `DROP
(TABLE|INDEX)` and `TRUNCATE TABLE` commands, without requiring any
additional modifications to the SQL layer. One small but salubrious
exception is we'll remove the "chunking" that's currently done in the
case of `DROP TABLE`.

## Point-in-Time Recovery

As mentioned in the summary, the fast `DeleteRange` modifications
introduced in this RFC are meant to also enable a new enterprise-tier
feature, point-in-time recovery. Point-in-time recovery is similar to
queries at a historical timestamp ("time travel") via the `SELECT
... AS OF DATABASE TIME <X>` syntax. The difference is that
point-in-time recovery transactionally reverts a key range to the
specified historical time, assuming it's within the affected Range's
garbage collection TTL, permanently altering the data for all
observers and future mutations.

This feature is of particular interest to database operators and helps
address various and sundry use cases, such as "fat finger" operator
error, and allowing the wholesale rollback of errors introduced
into the database by the deployment of a buggy application.

# Detailed design

The current implementation of the `DeleteRange` KV command requires
that the keys in the range be read through the MVCC API, and then
appropriate deletion tombstones added on top of each pre-existing
"live" key. All keys are tracked in the transaction, to be sent with
the final `EndTransaction` command, at which point each deleted key
must additionally have its intent cleared. Note that this is the case
only for two phase distributed transactions, although this is the
bedrock expectation for a `DROP` or `TRUNCATE` of anything but a
trivially small table.

This fundamental mechanism will remain in place. The **key performance
optimization** is enabled *only* if the `DeleteRange` command covers
the entire key range of a Range. If it only covers `99.9%` of the
Range, it must revert to the "slow" approach of iterating over each
key in the range and adding individual tombstones.

The key performance optimization is to maintain a parallel set of
**full-Range tombstones**. These are either deletes -or- point-in-time
reverts of the *entire* Range. They are consulted on MVCC access and
merged with the underlying Range data.

## Full-Range Tombstones

The bulk of the fast `DeleteRange` optimization lies within the MVCC
layer (`/pkg/storage/engine/mvcc.go`). An additional MVCC Range-local
key called `FullRangeTombstone` contains values of type `Tombstone`:

```
type Tombstone struct {
  Timestamp    hlc.Timestamp // Range values before this timestamp considered deleted
  EndTimestamp hlc.Timestamp // Non-zero for deletion up to historical timestamp
}
```

`FullRangeTombstone` is a replicated *key-range-local* key so that it
can be addressed as part of a transaction, generate conflicts, and
accept intent resolutions. Note that the `EndTimestamp` for a
point-in-time recovery tombstone must be more recent than the Range's
`GCThreshold`.

## Reading With Full-Range Tombstones

The full spectrum of MVCC methods currently include `MVCCGetProto`,
`MVCCGet`, `MVCCPut`, `MVCCPutProto`, `MVCCConditionalPut`,
`MVCCBlindPut`, `MVCCDelete`, `MVCCIncrement`,
`MVCCBlindConditionalPut`, `MVCCInitPut`, `MVCCMerge`,
`MVCCDeleteRange`, `MVCCScan`, `MVCCReverseScan`, `MVCCIterate`,
`MVCCResolveWriteIntent`, `MVCCResolveWriteIntentRange`, and
`MVCCGarbageCollect`. Unfortunately, these are naked functions with no
Range-specific scope. We'll need to refactor these into a new
`rangeMVCC` object:

```
type rangeMVCC struct {
  // history is a sorted slice, cached from materialized versioned
  // values of the Range's FullRangeTombstone key.
  history []Tombstone // Sorted by Tombstone.Timestamp
}
```

The `rangeMVCC` functions will then be able to use the tombstone
history to merge access to the underlying key value data with any
deletion tombstones for the range. The underlying MVCC data is read
first and then augmented (i.e. nil'd out if deleted) based on the
relevant values in the tombstone history. For example, imagine a
tombstone history that looks as follows. Notice the tombstone history
contains both a full-Range deletion at `t=3` and a point-in-time
recovery at `t=9`, reverting back to `t=5`.

![Full-Range Tombstones](images/fast_delete_range-1.png?raw=true "Full-Range Tombstones")

There are also multiple versions of keys `a` through `f`, with
timestamps listed in the cells, starting from most recent (`t=10`)
to least recent (`t=1`).

- If `a` is read at `t=10`, the value at `t=10` will be returned.
- If `b` is read at `t=10`, the value at `t=5` will be returned.
- If `a` is read at `t=9`, `nil` will be returned. Note that in
  practice, this is more complicated than may be immediately obvious.

Reading `a` at `t=9` would normally yield the value `t=6`, but in
the process of merging that result with the point-in-time recovery
tombstone, the reader is forced to query again, this time starting at
the tombstone's `EndTimestamp`: `t=5`. This must in turn use the
correct full-Range tombstone for `t=5`, which is the deletion tombstone
starting at `t=3`. This is then applied to the value read at `t=3`,
causing a final value of `nil` to be returned. This process may
continue for an arbitrary number of re-reads, making point-in-time
recoveries potentially more expensive for reads, although the two
or more lookups are almost certainly in the same block cache entry
and in practice, this may not be noticeable.

## Mutations With Full-Range Tombstones

If there are full-Range tombstones present on a range, all mutations
must take the following steps to ensure correctness. *Note that these
same steps must also be taken before adding another tombstone*, not
just when writing data to the underlying Range.

- Consult the most recent `FullRangeTombstone` value; if an intent,
return `WriteIntentError`. If the mutation timestamp is older than
the most recent full-Range tombstone, return `WriteTooOldError`.
- [**For full-Range tombstones ONLY**]: if `MVCCStats.IntentCount` is
non-zero, revert to a traditional `DeleteRange` MVCC operation which
scans over each key in the range. This is necessary in order to find
the appropriate transaction(s) to push, and in order to lay down
intents in case this is a busy Range that could otherwise never become
sufficiently quiescent to accept a full-Range tombstone.
- [**For full-Range tombstones ONLY**]: if the `MVCCStats.LastUpdated`
timestamp is newer than the timestamp of the mutation, return
`WriteTooOldError`. This step avoids the necessity of scanning all
keys in the Range in order to ensure the full-Range tombstone is not
rewriting history.
- Perform mutation. Note that any mutations which involve reads will
require the read to consider past full-Range tombstones, as per usual.
- Forward `MVCCStats.LastUpdated` timestamp as appropriate.

Note that the consultation of the read timestamp cache is unchanged
when writing a new full-Range tombstone. The `DeleteRange` command
will have its start and end keys set to the bounds of the Range, which
will guarantee that writing the full-Range tombstone won't change
history for readers.

Blind puts should be disabled in the event there are any full-Range
tombstones, because they effectively count as a tombstone at every
possible key in the range.

## Range Garbage Collection

Because Range garbage collection scans through keys for GC'able keys
using the `MVCCIterate` command, it benefits from the work done at the
MVCC level to merge in the full-Range tombstones. However, it makes
sense to check first whether there is an intent on the
`FullRangeTombstone` value, and push that transaction directly if
so. This avoids the case where the iteration naively returns every
affected key as having the same transactional intent (as derived from
the full-Range tombstone).

There is an important optimization enabled for garbage collection in
the case of a Range whose `LastUpdated` timestamp has not been
modified since the most recent full-Range tombstone. In this case,
garbage collection of the underlying keys can be done using the
underlying storage engine's `DeleteRange` operation (RocksDB has this
capability). This case is not uncommon. In particular, this will be
the normal case for dropped tables, which will allow CockroachDB to
avoid any per-key manipulation over the entire lifecycle of `DROP
TABLE`. Implementing this will require a range-based extension to the
`GC` KV command.

## MVCC Statistics

In the section above on mutations with full-Range tombstones, the
`MVCCStats.LastUpdated` field is referenced. This field doesn't yet
exist and will need to be added. It's a simple, monotonically
increasing value which represents the high water mark for any writes
to the range. On merges, the greater of the two values is chosen.
On splits, the value is simply duplicated; it does not require a
scan to update exactly. This is a minimal change.

For full-Range tombstones which specify a zero `EndTimestamp`, all
data in the Range is being moved from "live" to "dead" bytes. The
`MVCCStats` object must be modified by setting `LiveBytes` and
`LiveCount` to zero.

For full-Range tombstones which specify a non-zero `EndTimestamp`
(i.e. point-in-time recovery), MVCC statistics cannot be recomputed
without a full scan through the underlying data. Here we propose a new
queue to lazily recompute MVCC stats for Ranges which have the
`ContainsEstimates` flag set to true (we currently only recompute
these in the event of splits and merges). The new queue is not an
immediate requirement -- in general, it seems acceptable to allow
temporarily out-of-date stats after a point-in-time recovery. If the
definition of "temporary" is stretched initially to mean "until the
next split, maybe never", that's an OK start. Expect the queue to be
added in a follow-on PR.

## Splits and Merges

Handling Range splits is straightforward: just duplicate the
full-Range deletion tombstone versions. It's easy to see how this is
correct: the full-Range tombstones covered the pre-split Range in full,
so they must also cover each post-split Range in full.

Merges are more difficult. If there are no full-Range tombstones,
merges may proceed as they currently do. However, if there are
full-Range tombstones on either half of the merge (or both halves and
they're not identical), then neither side's full-Range tombstones may
safely be applied to the other. For this reason, merges of ranges
which contain full-Range tombstones must be delayed until the
full-Range tombstones have been garbage collected.

Note that more could be done in the case of merges, especially where
long TTLs are concerned (e.g. a 7 year regulatory retention policy).
In these cases, if merge pressure is high, the full-Range tombstones
could themselves be merged into the underlying data, creating
individual tombstones. This will be left as a suggested TODO.

# Drawbacks

As always, complexity is a cause for concern. This RFC will introduce
a merging step at the MVCC layer to combine the full-Range deletion
tombstones with the underlying Range MVCC values.

# Alternatives

For `DROP TABLE`, an efficient alternative would be to effectively
unlink the dropped table and store its table ID in a `system.dropped`
table. That table would contain the drop time and a GC timestamp which
could be queried or updated (in order to control when the actual
garbage collection of the underlying data would take place). This
would allow instant drops, and would provide a clear mechanism to
undo the drop for the period of time up to the eventual GC.

However, this mechanism wouldn't account for the case of `TRUNCATE
TABLE` or point-in-time recovery. It also introduces a new workflow
(maybe not desirable from an operator-complexity standpoint) as well
as a new cluster-wide "moving part" to deal with scheduling garbage
collection.

# Unresolved questions

None that I'm aware of at Draft writing time. Ready to start
implementation.
