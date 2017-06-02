- Feature Name: `Revert` Command
- Status: draft
- Start Date: 2017-06-01
- Authors: Spencer Kimball, Daniel Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#2003](https://github.com/cockroachdb/cockroach/issues/2003) [#14120](https://github.com/cockroachdb/cockroach/issues/14120) [#14279](https://github.com/cockroachdb/cockroach/issues/14279) [#15723](https://github.com/cockroachdb/cockroach/issues/15723) [#16161](https://github.com/cockroachdb/cockroach/issues/16161)

# Summary

This RFC describes a new `Revert` KV operation for efficient,
transactional truncations and point-in-time recoveries of full
Ranges. This supports fast, transactional `DROP TABLE`, `DROP INDEX`,
and `TRUNCATE TABLE` SQL statements, as well as an as-yet-unspecified
point-in-time recovery capability.

# Background

It is important to disambiguate the word "range" in this document. In
the context of the `DeleteRange` command, it refers to a span of keys,
which may contain 0 or more actual key-value pairs stored to the
underlying database. We use "Range" (with a capital "R") to instead
refer to a contiguous segment of the CockroachDB keyspace which is
replicated via an instance of the Raft consensus algorithm, and
accessed via the distributed KV layer via first the Node, then the
Store, and finally one of the Range's replicas (`storage.Replica`,
defined in `/pkg/storage/range.go`).

# Motivation

The primary goal for the `Revert` command is more efficient table
drops and truncations. A secondary goal is to lay the foundation for
building a point-in-time recovery mechanism, capable of reversing
mutations newer than a specified historical timestamp.

The current implementations of `DROP (TABLE|INDEX)` and `TRUNCATE
TABLE` work in much the same way, either by scanning data and issuing
individual `Delete` commands, or by avoiding scanning entirely and
issuing `DeleteRange` commands to more efficiently delete larger spans
of keys. The "fast path", using `DeleteRange`, is available only if
there are no interleaved sub-tables. However, despite being faster
than scanning and issuing individual deletes, `DeleteRange` is still
much too slow in practice (and has gotten even slower with proposer
evaluated KV).

If the code in `/pkg/sql/tablewriter.go: tableDeleter.deleteAllRows`
proceeds to delete the underlying data using the `DeleteRange` KV
command, we expect that using a similar, but considerably more
efficient `Revert` KV command will yield commensurate benefit to the
SQL, without requiring any additional modifications to the SQL
layer. One small but salubrious exception is we'll remove the
"chunking" that's currently done in the case of `DROP TABLE`.

Another point worth mentioning is that users `DROP` or `TRUNCATE` a
table sometimes in order to free up space. The previous implementation
would end up writing more data to disk in the form of tombstones. This
RFC won't solve the underlying problem of wanting to free up space,
but it at least won't counter-intuitively lead to greater capacity
utilization.

**Side note**: The difference between `TRUNCATE TABLE` and `DROP
Table` is that `TRUNCATE TABLE` must accommodate all row deletions
into a single transaction and so will break for large tables. By
contrast, `DROP` uses many transactions, each with a limit on the
total number of rows, to keep each transaction under CockroachDB's max
transaction size threshold. `DROP TABLE` is free to do its work using
multiple transactions because it proceeds only *after* the schema
change used to drop the table has completed, unlinking the table ID
from the schema, and preventing further usage. Since no concurrent
access of the table is allowed, the contents can be deleted in any
manner convenient. A further small point about the existing
implementation's efficiency: when calling `DeleteRange` with a
specified row limit, the KV API distributed sender is not free to send
to Ranges in parallel, causing it to serially process the Ranges,
regardless of how many nodes are in the cluster (i.e. how much
parallelism is available).

## Point-in-Time Recovery

As mentioned in the summary, the `Revert` command introduced in this
RFC is meant to also enable a new enterprise-tier feature,
point-in-time recovery. Point-in-time recovery is similar to queries
at a historical timestamp ("time travel") via the `SELECT ... AS OF
SYSTEM TIME <X>` syntax. The difference is that point-in-time recovery
transactionally reverts a key span to the specified historical time,
assuming it's within the affected Range's garbage collection TTL,
permanently altering the data for all observers and future mutations.

This feature is of particular interest to database operators and helps
address various and sundry use cases, such as "fat finger" operator
error, and allowing the wholesale rollback of errors introduced
into the database by the deployment of a buggy application.

# Detailed design

The `DeleteRange` KV command requires that the keys in the span be
read through the MVCC API, and then appropriate deletion tombstones
added on top of each pre-existing "live" key. All keys are tracked in
the transaction, to be sent with the final `EndTransaction` command,
at which point each deleted key must additionally have its intent
cleared. Note that this is the case only for two phase distributed
transactions, although this is the bedrock expectation for a `DROP` or
`TRUNCATE` of anything but a trivially small table.

By contrast, the `Revert` KV command will operate only on the entire
key span of a Range. If it is directed to apply to only `99.9%` of the
Range, it will return a `IllegalRevertError`.

Note that the new fast path for `DROP` and `TRUNCATE` SQL commands that
will use `Revert` is active only in situations where there are:
- No interleaved sub-tables
- No active indexes on the table (only applies to `TRUNCATE`)
- No foreign keys referencing the table

The key performance optimization is to avoid the `DeleteRange`
command's per-key writes and instead utilize a parallel set of
**full-Range reversions**. These are either deletes -or- point-in-time
reverts of the *entire* Range span. They are consulted on MVCC access
and merged with the underlying Range data.

## Full-Range Reversions

The bulk of the fast `Revert` optimization lies within the MVCC
layer (`/pkg/storage/engine/mvcc.go`). An additional MVCC Range-local
key called `FullRangeReversions` contains values of type `Reversion`:

```
type Reversion struct {
  RevertFrom hlc.Timestamp // Delete values before this timestamp
  RevertTo   hlc.Timestamp // Don't delete Values before this timestamp; (zero for full deletion)
}
```

For example, for deletion, the full-Range reversion is:

```
Reversion {
  RevertFrom: now,
  RevertTo:   0,
}
```

For reverting to a previous time `t`, the full-Range reversion is:

```
Reversion {
  RevertFrom: now,
  RevertTo:   t,
}
```

To *revert the previous revert* at time `now`, the full-Range reversion is:
```
Reversion {
  RevertFrom: now,+1, // ",+1" is one logical tick after time t
  RevertTo:   now,-1, // ",-1" is one logical tick prior to time t
}
```

`FullRangeReversions` is a replicated *key-range-local* MVCC key so
that it can be addressed as part of a transaction, generate conflicts,
and accept intent resolutions. When reading, the value of the
`FullRangeReversions` is read at the transaction / command time, just
like the underlying KV data. Note that the `Timestamp` is set to the
transaction or non-transactional command's timestamp.

For a point-in-time recovery reversion, `RevertTo` must be more recent
than the Range's `GCThreshold`, or a new `RevertTooOldError` is
returned.

## Reading With Full-Range Reversions

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
  // values of the Range's FullRangeReversions key.
  history []Reversion // Sorted by MVCC timestamp
}
```

The `rangeMVCC` functions will then be able to use the reversion
history to merge access to the underlying key value data with any
reversions for the Range. The underlying MVCC data is read first and
then augmented (i.e. nil'd out if deleted) based on the relevant
values in the reversion history. For example, imagine a reversion
history that looks as follows. Notice the reversion history contains
both a full-Range reversion at `t=3` and a point-in-time recovery at
`t=9`, reverting back to `t=5`.

![Full-Range Reversions](images/revert-1.png?raw=true "Full-Range Reversions")

There are also multiple versions of keys `a` through `f`, with
timestamps listed in the cells, starting from most recent (`t=10`)
to least recent (`t=1`).

- If `a` is read at `t=10`, the value at `t=10` will be returned.
- If `b` is read at `t=10`, the value at `t=5` will be returned.
- If `a` is read at `t=9`, `nil` will be returned. Note that in
  practice, this is more complicated than may be immediately obvious.

Reading `a` at `t=9` would normally yield the value `t=6`, but in the
process of merging that result with the point-in-time recovery
reversion, the reader is forced to query again, this time starting at
the reversion's `RevertTo`: `t=5`. This must in turn use the correct
full-Range reversion for `t=5`, which is the reversion starting at
`t=3`. This is then applied to the value read at `t=3`, causing a
final value of `nil` to be returned. This process may continue for an
arbitrary number of re-reads, making point-in-time recoveries
potentially more expensive for reads, although the two or more lookups
are almost certainly in the same block cache entry and in practice,
this may not be noticeable.

Because full-Range reversions are written as part of transactions,
they will contain intents. Reads encountering an intent on the
full-Range reversion will return a `WriteIntentError`.

## Mutations With Full-Range Reversions

If there are full-Range reversions present on a Range, all mutations
must take the following steps to ensure correctness. *Note that these
same steps must also be taken before adding another reversion*, not
just when writing data to the underlying Range.

- Consult the most recent `FullRangeReversions` value; if an intent,
return `WriteIntentError`. Note that the same rules which apply to
writes of normal MVCC values apply to the full-Range reversion. In
particular, the value will be considered committed if read in the
same transaction, and will be replaced if overwritten by the same
transaction.
- Consult the most recent `FullRangeReversions` value; if the mutation
timestamp is older than the most recent full-Range reversion, return
`WriteTooOldError`.
- [**For full-Range reversions ONLY**]: if `MVCCStats.IntentCount` is
non-zero, revert to the equivalent of an MVCC `Scan` operation to
push txns for all intents in the span. Note this presumes that
full-Range reversion writes will occur on mostly-quiescent ranges; if
not, a busy range could stall progress.
- [**For full-Range reversions ONLY**]: if the `MVCCStats.LastUpdateNanos`
wall time is newer than the mutation timestamp's wall time, return
`WriteTooOldError`. This step avoids the necessity of scanning all
keys in the Range in order to ensure the full-Range reversion is not
rewriting history.
- Perform mutation. Note that any mutations which involve reads will
require the read to consider past full-Range reversions, as per usual.

Note that the consultation of the read timestamp cache is unchanged
when writing a new full-Range reversion. The `Revert` command
will have its start and end keys set to the bounds of the Range, which
will guarantee that writing the full-Range reversion won't change
history for readers.

Blind puts should be disabled in the event there are any full-Range
reversions, because they effectively count as a reversion at every
possible key in the Range.

## MVCC Garbage Collection

MVCC garbage collection will require changes in conjunction with the
addition of full-Range reversions. When the GC queue considers a
range, and there is an intent on the `FullRangeReversions` value, the
intent's transaction must be pushed before garbage collection can
proceed. This ensures that a transaction performing full-Range reverts
will not have Range `GCThresholds` advanced concurrently, which might
otherwise violate the prohibition that reverts cannot be made to a
time earlier than the `GCThreshold`.

In addition, the KV `GC` command must be augmented to accept the most
recent timestamp for the `FullRangeReversions` key, which is verified
(as still being the most recent) when the `GC` command is evaluated.

The versions of the `FullRangeReversions` key (as taken from the
snapshot that the GC queue uses to do its work) are passed to the
`GarbageCollector.Filter` method, which applies them to the slice of
versioned values to appropriately garbage collect versions older than
the threshold, but also in no way visible due to existing reversions.

**Algorithm:**

```
// Collect a slice of timestamps to garbage collect.
var gcTimestamps []hlc.Timestamp

// Versions are in order of most recent to least recent.
for i, v := range versions {
  // Never GC a version we can still reach via historical read.
  if v.Timestamp > gcThreshold {
    continue
  }
  // Keep the first version if not deleted and there are no reversions more recent.
  if i == 0 && !v.Deleted &&
    (len(reversions) == 0 || v.Timestamp > reversions[0].RevertFrom) {
    continue
  }

  // We don't GC versions which are made visible or "covered" by
  // either the most recent reversion (even if older than the GC
  // threshold) or any reversion more recent than the GC threshold.
  revertible := false
  for j, r := range reversions {
    if r.RevertFrom <= gcThreshold && j > 0 {
      break
    }
    covers := r.RevertFrom >= v.Timestamp && r.RevertTo < v.Timestamp
    reveals := r.RevertTo >= v.Timestamp && (i == 0 || r.RevertTo < versions[i-1].Timestamp)
    // If the reversion is more recent than the gc threshold, we must
    // preserve all versions which it covers or reveals.
    if r.RevertFrom > gcThreshold && (covers || reveals) {
      revertible = true
      break
    } else if j == 0 && reveals && !v.Deleted {
      // Otherwise, if the reversion is older than the gc threshold,
      // but is the most recent reversion, we only preserve the version
      // it reveals (if not deleted!).
      revertible = true
      break
    }
  }
  if !revertible {
    gcTimestamps = append(gcTimestamps, v.Timestamp)
  }
}
```

Currently, the `GC` command takes a slice of `GCKey` objects:

```
message GCKey {
  optional bytes key = 1 [(gogoproto.casttype) = "Key"];
  optional util.hlc.Timestamp timestamp = 2 [(gogoproto.nullable) = false];
}
```

The `GCKey` struct will need to be updated to make it clear that field
ID `2` is meant to mean "clear all values earlier than this
timestamp", as well as adding an additional optional slice of
"reverted" timestamped versions to delete:

```
message GCKey {
  optional bytes key = 1 [(gogoproto.casttype) = "Key"];
  // All versions with timestamp <= low_water_timestamp will be cleared.
  optional util.hlc.Timestamp low_water_timestamp = 2 [(gogoproto.nullable) = false];
  // Versions matching timestamps in the reverted_timestamps set will be cleared.
  repeated util.hlc.Timestamp reverted_timestamps = 3;
}
```

Versions of the `FullRangeReversions` key may be garbage collected as
soon as they are older than the GC threshold, but only after the Range
has been fully scanned and all batched KV `GC` commands successfully
processed at that GC threshold.

There is an important optimization enabled for garbage collection in
the case of a Range whose `MVCCStats.LastUpdateNanos` is newer than
the wall time of the most recent full-Range reversion. In this case,
garbage collection of the underlying keys can be done using the
underlying storage engine's `Revert` operation (RocksDB has this
capability). This case is not uncommon. In particular, this will be
the normal case for dropped tables, which will allow CockroachDB to
avoid any per-key manipulation over the entire lifecycle of `DROP
TABLE`. Implementing this will require a span-based extension to the
`GC` KV command.

## MVCC Statistics

In the section above on mutations with full-Range reversions,
`MVCCStats.LastUpdateNanos` is referenced. This provides a simple,
monotonically increasing value which represents the high water mark
for wall time for any writes to the Range, because every write to the
range updates the `MVCCStats` using the current wall time from the
node's `hlc.Clock`, which is itself monotonically increasing and
always updated to the highest timestamp seen from write batches. On
merges, the greater of the two values is already chosen. On splits,
the value is already duplicated.

For full-Range reversions which specify a zero `RevertTo`, all data in
the Range is being moved from "live" to "dead" bytes. The `MVCCStats`
object must be modified by setting `LiveBytes` and `LiveCount` to
zero.

For full-Range reversions which specify a non-zero `RevertFrom`
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
full-Range reversion versions. It's easy to see how this is correct:
the full-Range reversions covered the pre-split Range in full, so they
must also cover each post-split Range in full.

Merges are more difficult. If there are no full-Range reversions,
merges may proceed as they currently do. However, if there are
full-Range reversions on either half of the merge (or both halves and
they're not identical), then neither side's full-Range reversions may
safely be applied to the other. For this reason, merges of Ranges
which contain full-Range reversions must be delayed until the
full-Range reversions have been garbage collected.

Note that more could be done in the case of merges, especially where
long TTLs are concerned (e.g. a 7 year regulatory retention policy).
In these cases, if merge pressure is high, the full-Range reversions
could themselves be merged into the underlying data, creating
individual reversions. This will be left as a suggested TODO.

# Drawbacks

As always, complexity is a cause for concern. This RFC will introduce
a merging step at the MVCC layer to combine the full-Range reversions
with the underlying Range MVCC values.

# Alternatives

For `DROP TABLE`, an efficient alternative would be to effectively
unlink the dropped table and store its table ID in a `system.dropped`
table. That table would contain the drop time and a GC timestamp which
could be queried or updated (in order to control when the actual
garbage collection of the underlying data would take place). This
would allow instant drops, and would provide a clear mechanism to
undo the drop for the period of time up to the eventual GC.

For the case of `TRUNCATE TABLE`, work would need to be done at the
SQL layer to swap in a new ID. This would work well for the case of
blind puts, which would continue to be efficient even after the
truncate.

Switching IDs in the schema won't work for point-in-time recovery.

# Unresolved questions

- Span-based MVCC garbage collection will require some tricky
synchronization to ensure it doesn't race with any other writes. The
full implementation requires some thought and is currently outside
the scope of this RFC.
