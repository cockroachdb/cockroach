- Feature Name: `Revert` Command
- Status: draft
- Start Date: 2017-06-01
- Authors: Spencer Kimball, Daniel Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#2003](https://github.com/cockroachdb/cockroach/issues/2003) [#14120](https://github.com/cockroachdb/cockroach/issues/14120) [#14279](https://github.com/cockroachdb/cockroach/issues/14279) [#15723](https://github.com/cockroachdb/cockroach/issues/15723) [#16161](https://github.com/cockroachdb/cockroach/issues/16161)

# Summary

This RFC describes a new `Revert` KV operation for point-in-time
recovery of full Ranges.

# Notes

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

While high-frequency, incremental backups are required for responsible
production CockroachDB deployments, there remain important failure
scenarios (e.g. application errors or operator "fat fingers")
requiring a subtler mechanism for recovery. In particular, we require
a means to restore a complete table or database to a **consistent**
point in time preceding the introduction of data corruption. Point in
time (PIT) recovery traditionally is done by restoring to the latest
full backup, and replaying the database log up to and excluding the
introduction of data corruption. This is the means by which Oracle,
SQLServer, Postgres, and MySQL recommend performing PIT recovery.
MongoDB provides similar functionality via their Cloud Manager
product. Traditional mechanisms for PIT recovery are typically time
consuming and require the database to be offline.

Because CockroachDB is based on an MVCC model, the underlying
information for PIT recovery is available (modulo data garbage
collected past the configured time-to-live (TTL)), and is already
pre-distributed. Further, with the mechanism described in this RFC, we
can achieve efficient PIT recovery of whole tables or databases
**in-situ** -- that is, without requiring the database to be quiesced
in part or in whole, and restoring from backups.

# Detailed design

The `Revert` KV command will operate only on the entire key span of a
Range. If it is directed to apply to only `99.9%` of the Range, it
will return a `IllegalRevertError`.

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
  RevertTo hlc.Timestamp // Revert values written after this timestamp
}
```

For example, for deletion, the full-Range reversion is:

```
Reversion {
  RevertTo: 0,
}
```

For reverting to a previous time `t1` from time `t2`, the full-Range reversion is:

```
Reversion {
  RevertTo: t1,
}
```

To *revert the previous revert* at time `t2`, the full-Range reversion is:
```
Reversion {
  RevertTo: t2,-1, // ",-1" is one logical tick prior to time t2
}
```

`FullRangeReversions` is a replicated *key-range-local* MVCC key so
that it can be addressed as part of a transaction, generate conflicts,
and accept intent resolutions. When reading, the MVCC value of the
`FullRangeReversions` key is read using the command timestamp to index
into the available versions.

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
non-zero, return a new `CannotRevertActiveRange` error. Note this
presumes that full-Range reversion writes will occur on
mostly-quiescent ranges; if not, a busy range could prevent revert
from ever succeeding.
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
`MakeGarbageCollector` method, which applies them to the slice of
versioned values to appropriately garbage collect versions older than
the threshold, but also in no way visible due to existing reversions.

**Algorithm:**

```
- If there are any reversions more recent than the threshold, skip GC
- Find first version visible when reading at the GC threshold
- If visible version is deleted, delete all versions older than threshold
- Otherwise:
  - GC all versions older than threshold, but newer than visible version
  - GC all versions older than the visible version
```

Because the GC algorithm won't run until reversions are older than the
threshold, determining which versions to garbage collect is
straightforward. We simply follow the same procedure as is used to
read a KV version in the presence of reversions. This identifies which
historical version, if any, is "visible" when reading at the GC
threshold. All other versions older than the threshold are GC'd.

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

For full-Range reversions which specify a non-zero `RevertTo`
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
must also cover each post-split Range in full. Note that splits cannot
proceed if the `FullRangeReversions` key has an intent.

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

## SQL Syntax

The `REVERT` SQL syntax is modeled after `BACKUP`:

```
REVERT (TABLE | DATABASE) <(table pattern | database pattern) [, ...]> TO SYSTEM TIME <timestamp>
```

## Schema changes

A revert which takes a table back to a timestamp before a schema
change (or changes) were run could yield data which is no longer
compatible with the schema. For example, a schema change enforcing a
constraint on a column could conflict with a revert of the same data
to an earlier timestamp.

To solve this, Revert must ensure that the schema for a reverted table
is the same at the current timestamp (timestamp at which the Revert
was executed) as it was at the timestamp to which the table is being
reverted. If there were no schema changes in that interval, this is a
noop. Otherwise, the schema at the `RevertTo` timestamp must be
re-instituted at the current timestamp, as the last step of the Revert
transaction. *TODO: there are unexplored questions on feasibility
here*.

The first stage of a solution would be to disallow reverts to
timestamps earlier than the most recent schema change.

## Migration

This change will require a migration step because nodes running the
latest version which understands and uses the full-Range reversions
will not be able to mix with nodes which
don't. See
[#16977](https://github.com/cockroachdb/cockroach/issues/16977) for
a description of the migration process.

# Drawbacks

As always, complexity is a cause for concern. This RFC will introduce
a merging step at the MVCC layer to combine the full-Range reversions
with the underlying Range MVCC values.

The straightforward approach to garbage collection detailed in the
design will allow successive reversions to a Range to perpetually
delay GC, because reversions must be older than the configured GC
TTL.

Although a primary feature of the Revert mechanism is to provide
recovery in-situ, without having to restore the database from backups,
they are nevertheless unsuited for operation on active Ranges. When a
Revert is necessary, the expectation is that the operator will quiesce
applications writing to the database or tables being reverted. Also
worth mentioning here: while a large table or database is being
reverted, the constituent Ranges will be prevented from splitting or
merging.

# Alternatives

As mentioned in the motivation, the obvious alternative is to include
timestamped versions in the backups and allow a PIT recovery by
combining full and incremental backups with a change data capture
stream, rolling the database forward to a chosen timestamp.

# Unresolved questions

- Span-based MVCC garbage collection will require some tricky
synchronization to ensure it doesn't race with any other writes. The
full implementation requires some thought and is currently outside
the scope of this RFC.
