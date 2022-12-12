# MVCC Range Tombstones

Original author: Erik Grinaker (July 2022)

MVCC range tombstones provide a cheap means to delete a large amount of MVCC
data while still preserving the MVCC history. They are conceptually equivalent
to writing MVCC point tombstones across every MVCC point key in a key span, but
with a constant rather than linear write/storage cost. The data is marked as
deleted and no longer visible, but remains available for e.g. `AS OF SYSTEM
TIME` queries, incremental backups, CDC catchup scans, etc. until it is garbage
collected by the MVCC GC queue.

MVCC range tombstones are introduced in CockroachDB 22.2 behind the version gate
`MVCCRangeTombstones`, and disabled by default via the cluster setting
`storage.mvcc.range_tombstones.enabled`. KV callers must therefore check
`storage.CanUseMVCCRangeTombstones()` before attempting to write them. The read
path is always active (e.g. if the cluster setting is disabled again after
writing a range tombstone), and degrades gracefully if the Pebble database has
not yet been upgraded to support them (in the
`EnablePebbleFormatVersionRangeKeys` migration).

NB: MVCC range tombstones are not yet supported in transactions, since that
would require ranged write intents.

## Motivation

Previously, certain operations would rely on the non-MVCC methods `ClearRange`
and `RevertRange` to immediately delete large amounts of data. In particular,
cancellation of `IMPORT` and `RESTORE` used this to clean up partial data,
and schema GC used it to remove dropped tables/indexes after waiting out the
GC TTL. This had several problems, including:

* [Cluster-to-cluster replication](../RFCS/20201119_streaming_cluster_to_cluster.md)
  replicates the MVCC history to a target cluster. However, there was no record
  of these operations, and therefore they would not be replicated. Furthermore,
  even if we did keep a record of them as-is, different key spans are replicated
  independently, so cutover to the target cluster requires reverting all ranges
  to a consistent timestamp, but these operations are irreversible by nature.

* Incremental backups and CDC rely on the MVCC history to detect changes, but
  again, these operations left no trace. This required additional logic to avoid
  interacting with spans that might see such operations, as outlined below.
  In turn, this meant that e.g. a long-running import could not be incrementally
  backed up while it was running, making the next incremental backup after
  completion very expensive.

* These operations violate MVCC invariants, so they are not safe for concurrent
  use. SQL and other higher layers therefore have have to implement their own
  concurrency control, typically relying on taking the table offline via the
  table descriptor. These still have challenges with race conditions, e.g. due
  to replays and delayed application.

* Dropped tables/indexes were not immediately deleted, but only cleared after a
  job had waited out the GC TTL. The data was therefore still considered live
  from an MVCC point of view, leading to confusing UX where the cluster still
  reported this data as being in use. Furthermore, this means that SQL schema
  operations need their own GC infrastructure rather than using regular MVCC GC,
  and this can interact poorly with the serverless architecture where SQL pods
  may get shut down, preventing GC.

## Data Structure: MVCC Range Keys

MVCC range tombstones are implemented as generalized MVCC range keys, which
store an arbitrary value for an MVCC key span and timestamp. MVCC range
tombstones are signified by an empty value, similarly to MVCC point tombstones.
They are currently the only kind of MVCC range key, and this is enforced and
assumed throughout the code, but future uses of range keys might include e.g.
ranged intents, exclusive keyspan locks, and Pebble compaction hints for
more efficient MVCC garbage collection.

MVCC range keys are represented by `MVCCRangeKey`:

```go
type MVCCRangeKey struct {
	StartKey  roachpb.Key
	EndKey    roachpb.Key
	Timestamp hlc.Timestamp
}
```

A range key stores an encoded `MVCCValue`, similarly to `MVCCKey`. They can be
paired as an `MVCCRangeKeyValue`:

```go
type MVCCRangeKeyValue struct {
	RangeKey MVCCRangeKey
	Value    []byte // encoded MVCCValue
}
```

MVCC range keys are stored in Pebble as [Pebble range keys](https://github.com/cockroachdb/pebble/issues/1339),
which have nearly identical structure and semantics. Pebble implementation details
will not be covered here, but will be made available elsewhere.

Range keys and point keys exist separately, and can be accessed separately or
combined. A specific key position can have both a point key and/or multiple
range keys overlapping it. In principle (that is, in Pebble), it is possible for
a specific key/timestamp combination (e.g. `a@3`) to have both a point key value
and a range key value, since point keys and range keys exist separately.
However, the MVCC layer forbids this, in the same way that it forbids two MVCC
point key writes at the same timestamp.

Range keys exist between unversioned `roachpb.Key` bounds, and therefore overlap
_all_ point key versions between the bounds, regardless of timestamp. Consider
for example these point keys and range keys:

```
Time
5  a5  b5
4  [-----------)
3      b3  c3
2  [-----------)
1          c1  d1
   a   b   c   d   Key
```

Here, the MVCC point key versions `b@3` and `c@3` are deleted by the MVCC range
tombstone `[a-d)@4`, and `c@1` is deleted by `[a-d)@2`. `d@1` is not deleted at
all, because the range tombstones' end bound is exclusive. `a5`, `b5`, and so on
represent the value of the point key.

An MVCC iterator positioned at e.g. `c@3` would see _both_ `[a-d)@4` and
`[a-d)@2`, even though none of them have timestamp 3, because the range keys
exist between the bounds `[a-d)`, and `c` is within those bounds. The same is
true for `a@5`, even though it is above both MVCC range tombstones. It is up to
the iterator caller to interpret the range keys as appropriate relative to the
point key. It follows that all range keys overlapping a key will be pulled into
memory at once, but we assume that overlapping range keys will be few.

This is represented as a specialized compact data structure,
`MVCCRangeKeyStack`, where all range keys have the same bounds due to
fragmentation (described below):

```go
type MVCCRangeKeyStack struct {
	Bounds   roachpb.Span
	Versions MVCCRangeKeyVersions
}

type MVCCRangeKeyVersions []MVCCRangeKeyVersion

type MVCCRangeKeyVersion struct {
	Timestamp hlc.Timestamp
	Value     []byte // encoded MVCCValue
}
```

In the KV API, however, the relationship between point keys and range keys
doesn't really matter: `Get(c)` at timestamp >= 5 would simply return nothing,
while `Get(b)` would return `b5`. More on this later.

### Fragmentation

Range keys do not have a stable, discrete identity, and should be considered a
continuum: they may be partially removed or replaced, merged or fragmented by
other range keys, split and merged along with CRDB ranges, truncated by iterator
bounds, and so on. For example, a range key `[a-d)@1` may not remain as
`[a-d)@1` forever:

```
1  [-----------)   -> PutMVCCRangeKey([d-e)@1)   ->   1  [---------------)
   a   b   c   d                                         a   b   c   d   e

1  [-----------)   -> ClearMVCCRangeKey([b-c)@1) ->   1  [---)   [---)
   a   b   c   d                                         a   b   c   d

1  [-----------)   -> AdminSplit(b)              ->   1  [---|-------)
   a   b   c   d                                         a   b   c   d
```

Range keys are also fragmented by Pebble such that all overlapping range keys
between two keys form a stack of range key fragments at different timestamps.
For example, writing `[a-c)@1` and `[b-d)@2` will yield this fragment structure:

```
2                                                     2      [---|---)
1  [-------)       -> PutMVCCRangeKey([b-d)@2)   ->   1  [---|---)
   a   b   c   d                                         a   b   c   d
```

Thus, the two original range key writes resulted in four range keys stored in
Pebble: `[a-b)@1`, `[b-c)@2`, `[b-c)@1`, and `[c-d)@2`. Similarly, clearing
`[b-d)@2` would merge the remaining keys back into `[a-c)@1`.

This implies that all range keys exposed for a specific key position all have
the same key bounds, as shown in `MVCCRangeKeyStack`.

Fragmentation is beneficial because it makes all range key properties local,
which avoids incurring unnecessary access costs across SSTs and CRDB ranges when
reading a single key position. If we were to defragment them, we might have to
scan across the entire length of the range key to find its key bounds, which in
principle could span the entire keyspace. And stacking allows easy and efficient
access to all range keys overlapping a point key. This fragmentation is
deterministic on the current range key state, and does not depend on write
history.

## Writes

### `Writer` Primitives

The `Writer` interface has several primitives for mutating MVCC range keys.
These are thin wrappers around Pebble that primarily do validation and
encoding/decoding. In particular, they do not enforce any MVCC invariants such
as conflict checks and stats updates, similarly to other `Writer` methods.

* `PutMVCCRangeKey(MVCCRangeKey, MVCCValue)`:  Writes the given range key
  with the given value. Replaces any existing range key(s) within the keyspan
  at the same timestamp. Errors if the value is not a tombstone.

* `ClearMVCCRangeKey(MVCCRangeKey)`: Deletes the given range key
  with the given timestamp. Not required to exactly match a range key: can
  remove sections of range keys, or many smaller range keys within a span.
  Does not affect range keys at other timestamps (except fragmentation),
  nor point keys.

* `ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool)`: Deletes
  all range keys (i.e. at all timestamps) between the given key bounds using a
  single Pebble `RANGEKEYDEL` tombstone when `rangeKeys` is true. Can remove
  sections of range keys, or several range keys. Does not affect point keys.
  
* `PutRawMVCCRangeKey(MVCCRangeKey, []byte)`:  Like `PutMVCCRangeKey`, but
  takes an already-encoded `MVCCValue`. Can be used to avoid unnecessary
  decode/encode roundtrips when copying range keys, but should otherwise be
  avoided due to the lack of type safety.

* `PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte)`: Writes
  a raw range key directly to Pebble. Only for specialized low-level use.

* `ClearEngineRangeKey(start, end roachpb.Key, suffix []byte)`: Clears a raw
  range key directly in Pebble. Only for specialized low-level use.

Other `Writer` methods may also affect range keys, e.g. `ClearMVCCRange`, but
they rely on these primitives internally. See the interface documentation for
details.

### MVCC Writes

The primary MVCC API for writing MVCC range tombstones is
`MVCCDeleteRangeUsingTombstone()`, which deletes a key span at a given
timestamp. This simply calls through to `Writer.PutMVCCRangeKey()`, but also
enforces MVCC invariants by checking for conflicting intents and newer writes,
maintaining MVCC statistics, and recording the operation in the MVCC logical
operation log. This function cannot be used in a transaction, as we do not
support ranged write intents yet.

All other MVCC APIs also take MVCC range tombstones into account as appropriate.
For example, point key writes via `MVCCPut()` will check for conflicts with
newer MVCC range tombstones and take them into account for MVCC stats updates.
Refer to the MVCC API documentation for details.

### KV Writes

The primary KV API for writing MVCC range tombstones is the `DeleteRange` method
with the `UseRangeTombstone` option, which simply calls through to
`MVCCDeleteRangeUsingTombstone()`. Without this option, it instead iterates
across the keyspan and writes MVCC point tombstones above each key. This option
cannot be used in a transaction, as we do not support ranged write intents yet.

Other KV APIs, such as `ClearRange`, also take MVCC range tombstones into
account as appropriate. See the KV API documentation for details.

A notable and unfortunate implementation detail is that these KV mutations must
take out a latch that is slightly wider than the written span (i.e.
`StartKey.Prevish()` and `EndKey.Next()`) in order to look for any surrounding
MVCC range keys that might be affected by the mutation (e.g. by fragmenting
them) to correctly update MVCC stats. Two `UseRangeTombstone` range deletions
with abutting key spans will therefore be forced to serialize with each other.
However, these latches will not extend beyond the CRDB range bounds, since
latches are processed on a per-range basis.

## MVCC Reads

### `SimpleMVCCIterator` Primitives

The main primitives for accessing MVCC range keys are via `SimpleMVCCIterator`.
`IterOptions.KeyTypes` specifies which key types the iterator should surface:

* `IterKeyTypePointsOnly`: only MVCC point keys (default).
* `IterKeyTypePointsAndRanges`: combined MVCC point/range key iteration.
* `IterKeyTypeRangesOnly`: only MVCC range keys.

Recall from the data structure description above that MVCC range keys overlap
all MVCC point keys and versions between their bounds, regardless of timestamp.
During combined iteration, when positioned on a point key, the iterator will also
expose all range keys overlapping it.

The properties of point and range keys are accessed via:

* `HasPointAndRange()`: two booleans specifying whether the current position has
  a point and/or range key. At least one will be `true` for a valid iterator.
* `UnsafeKey()`: current key position (point key if present).
* `UnsafeValue()`: current point key value, if present.
* `RangeBounds()`: start and end bounds of range keys overlapping the current
  position, if any.
* `RangeKeys()`: all range keys at the current key position (i.e. at all
  timestamps), as `MVCCRangeKeyStack`.
* `RangeKeyChanged()`: returns `true` if the previous positioning operation
  caused `RangeKeys()` to change.

During iteration with `IterKeyTypePointsAndRanges`, range keys are emitted at
their start key and at every overlapping point key. Consider a modified
version of the example from earlier with fragmented range keys:

```
Time
5  a5  b5
4  [---|-------)
3      b3  c3
2      [-------)
1          c1  d1
   a   b   c   d   Key
```

Iterating across this span would emit this sequence:

| `UnsafeKey` | `HasPointAndRange` | `UnsafeValue` | `RangeKeys`          |
|-------------|--------------------|---------------|----------------------|
| `a`         | `false`, `true`    |               | `[a-b)@4`            |
| `a@5`       | `true`, `true`     | `a5`          | `[a-b)@4`            |
| `b`         | `false`, `true`    |               | `[b-d)@4`, `[b-d)@2` |
| `b@5`       | `true`, `true`     | `b5`          | `[b-d)@4`, `[b-d)@2` |
| `b@3`       | `true`, `true`     | `b3`          | `[b-d)@4`, `[b-d)@2` |
| `c@3`       | `true`, `true`     | `c3`          | `[b-d)@4`, `[b-d)@2` |
| `c@1`       | `true`, `true`     | `c1`          | `[b-d)@4`, `[b-d)@2` |
| `d@1`       | `true`, `false`    | `d1`          |                      |

Reverse iteration yields the above sequence in reverse. Notably, bare range
keys are still emitted at their start key (not end key), so they will be emitted
last in this example.

When using `SeekGE` within range key bounds, the iterator may land on the bare
range key first, unless seeking exactly to an existing point key version. For
example, given the above key layout, various seeks would yield:

| `SeekGE` | `UnsafeKey` | `HasPointAndRange` | `UnsafeValue` | `RangeKeys`          |
|----------|-------------|--------------------|---------------|----------------------|
| `a`      | `a`         | `false`, `true`    |               | `[a-b)@4`            |
| `a@6`    | `a@6`       | `false`, `true`    |               | `[a-b)@4`            |
| `a@5`    | `a@5`       | `true`, `true`     | `a5`          | `[a-b)@4`            |
| `a@4`    | `a@4`       | `false`, `true`    |               | `[a-b)@4`            |
| `a@3`    | `a@3`       | `false`, `true`    |               | `[a-b)@4`            |
| `c`      | `c`         | `false`, `true`    |               | `[b-d)@4`, `[b-d)@2` |
| `c@4`    | `c@4`       | `false`, `true`    |               | `[b-d)@4`, `[b-d)@2` |
| `c@3`    | `c@3`       | `true`, `true`     | `c3`          | `[b-d)@4`, `[b-d)@2` |
| `c@2`    | `c@2`       | `false`, `true`    |               | `[b-d)@4`, `[b-d)@2` |
| `d@5`    | `d@1`       | `true`, `false`    | `d5`          |                      |

The same is not true for `SeekLT`: when using it within range key bounds, it
will either stop at the first point key it encounters (while exposing the
overlapping range keys), or at the start bound of the range key.

| `SeekLT` | `UnsafeKey` | `HasPointAndRange` | `UnsafeValue` | `RangeKeys`          |
|----------|-------------|--------------------|---------------|----------------------|
| `a`      |             | `false`, `false`   |               |                      |
| `a@6`    | `a`         | `false`, `true`    |               | `[a-b)@4`            |
| `a@1`    | `a@5`       | `true`, `true`     | `a5`          | `[a-b)@4`            |
| `b@5`    | `b`         | `false`, `true`    |               | `[b-d)@4`, `[b-d)@2` |
| `c@3`    | `b@3`       | `true`, `true`     | `b3`          | `[b-d)@4`, `[b-d)@2` |
| `d@1`    | `c@1`       | `true`, `true`     | `c1`          | `[b-d)@4`, `[b-d)@2` |

Note that intents (with timestamp 0) encode to a bare `roachpb.Key`, so they
will be colocated with a range key start bound. For example, if there was an
intent on `a` in the above example, then both `SeekGE(a)` and forward iteration
would land on `a@0` and `[a-b)@4` simultaneously at `a`, instead of the bare
range key first.

Several additional `IterOptions` parameters also affect MVCC range keys:

* `LowerBound`, `UpperBound`: if an MVCC range key straddles the iterator bounds,
  it will be truncated to them. For example, if an iterator with bounds `[b-d)`
  encounters a range key `[a-f)@2`, it will be exposed as `[b-d)@2`.

* `MinTimestampHint`, `MaxTimestampHint`: SST block properties are recorded for
  MVCC range keys in the same way as for point keys. However, block property
  filters are not enabled for MVCC range keys due to complications with
  `MVCCIncrementalIterator` (differing views of range key fragmentation),
  so all range keys will currently be surfaced by time-bound iterators.

* `RangeKeyMaskingBelow`: given a timestamp, all MVCC range keys below that
  timestamp will hide any MVCC point key versions below them during Pebble
  iteration, which can improve performance. The typical use-case is for an MVCC
  operation reading at some timestamp `T` to set `RangeKeyMaskingBelow: T`,
  so that it will not see point keys covered by past MVCC range tombstones (<
  `T`,) but will see them if they're covered by a future MVCC range tombstone
  at > `T`. See method documentation for details.

Most MVCC iterators handle MVCC range tombstones, and the ones that don't will
panic with e.g. "not implemented" if any of the range key methods are called on
them.

Additionally, `EngineIterator` has corresponding functionality for processing
raw Pebble range keys, but these are primarily intended for internal, low-level
use, e.g. when building Raft snapshots. Most callers should use `MVCCIterator`.

### Gets, Scans, and Point Tombstone Synthesis

In the basic case, `MVCCGet` and `MVCCScan` will simply not return point keys
covered by MVCC range tombstones. Most callers can therefore safely ignore their
existence.

However, callers may request tombstones to be emitted via the `Tombstones`
option, e.g. for conflict checks and rangefeed value diffs. We do not want to
burden the rest of the codebase with having to handle MVCC range tombstones
explicitly, and therefore do not expose them directly. Instead, we synthesize
MVCC point tombstones. A scan emits synthetic point tombstones above existing
point keys, while a get emits synthetic point tombstones if a range tombstone
overlaps the key, regardless of whether there is an existing point key below it.

Consider this example:

```
Time
6  [---|----------)
5          c5
4      [----------)
3
2      [----------)
1              d1
   a   b   c   d  e    Key
```

An `MVCCScan` with `Tombstones` across this span at timestamp >= 6 would emit
synthetic MVCC point tombstones at `c@6` and `d@6`. However, a scan at timestamp
3 would only emit a synthetic tombstone at `d@2`, because the range tombstone is
below the point `c@5` and point tombstones are not synthesized below point keys.
Similarly, a scan across `[a-b)` at any timestamp would not emit anything.

Additionally, an `MVCCGet` will emit a synthetic point tombstone for the key
even if it has no existing point key below it, as these might be required for
conflict checks. For example, `Get(bar)` at timestamp >= 6 would return a
tombstone `bar@6` even though there is no real point key at `bar`. Similarly, a
`Get(c)` at timestamp 3 would return `c@2` even though the point key `c@5` is
above it. These synthetic tombstones would not be visible to an `MVCCScan`.

If callers need better visibility into range tombstones, they must use an
`MVCCIterator` that exposes them directly.

### KV APIs

Because most KV APIs (in particular, `Scan` and `Get`) do not expose tombstones,
most KV callers do not need to care about MVCC range tombstones at all --
similarly to `MVCCScan` and `MVCCGet`, the APIs simply won't emit keys covered
by MVCC range tombstones. Notable exceptions are `Export` and `RangeFeed`, which
are covered below.

## MVCC Statistics

MVCC range keys are currently tracked by the following MVCC statistics fields,
which are equivalent to the corresponding point key stats:

* `RangeKeyCount`: The number of MVCC range key fragment stacks, i.e. ignoring
  historical versions.
* `RangeKeyBytes`: The total encoded key size for all stacks, plus the encoded
  timestamp size for all versions.
* `RangeValCount`: The number of MVCC range key fragments, i.e. including
  historical versions.
* `RangeValBytes`: The total value size (including `MVCCValueHeader` metadata)
  of all range key versions.

Notably, because all MVCC range keys are currently assumed to be MVCC range
tombstones, they do not contribute to `LiveBytes` at all. They do, however,
contribute to `GCBytesAge` in the same way as point tombstones, and they do
cause covered point keys to be considered garbage for stats purposes as well.

Consider the following example:

```
Time
2      [---|-------|---|---)
1  [---|---)       [---)
   a   b   c   d   e   f   g
```

This would result in the following MVCC stats:

* `RangeKeyCount`: 5 (`[a-b)`, `[b-c)`, `[c-e)`, `[e-f)`, `[f-g)`).
* `RangeKeyBytes`: 83 (5 × 2 × 2 for encoded key bounds `a,b,b,c,c,e,e,f,f,g`
  of 2 bytes each including `\x00` sentinel byte, 7 × 9 for encoded timestamps
  `1,2,1,2,2,1,2` of 9 bytes each).
* `RangeValCount`: 7 (`[a-b)@1`, `[b-c)@2`, `[b-c)@1`, `[c-e)@2`, `[e-f)@2`,
  `[e-f)@1`, `[f-g)@2`).
* `RangeValBytes`: 0 (tombstones have an empty value, and here they don't have
  `MVCCValueHeader` metadata either).

Using fragmented range keys as the basis for MVCC stats makes them exactly
equivalent to point key stats. Consider the following data set, where `x`
denotes an MVCC point tombstone:

```
Time
3
2      x   x       [---|---)
1  x   x       [---|---)
   a   b   c   d   e   f   g   Key
```

Both for point keys and range keys, there are 3 keys of each type, 4 values of
each type, only the top-most key contributes the encoded key bytes but
historical versions also contribute encoded timestamp bytes, and so on. If we
introduce non-tombstone range keys then liveness will be determined in exactly
the same way as for point keys: if the top-most range key is not a tombstone
then that range key is considered live, and the previous range key becomes
garbage at its timestamp.

## MVCC Garbage Collection

During garbage collection, point keys and range keys are garbage collected
separately: the GC queue will first send `GCRequest`s with point key clears, and
then send separate `GCRequest`s with range key clears. The range key clears
will only clear individual range key stacks, and requires point keys below them
to have been cleared first.

The range key clears have to serialize with other range key writers overlapping
the GCed range key and its bounds. This is because the MVCC stats updates of the
writer and GC are not commutative if they cause any fragmentation or merging
around the range key. To avoid serializing with all writers, instead only
serializing with range key writers, GC takes out a write latch on the virtual
range-local key `LocalRangeMVCCRangeKeyGCLock`. All range key writers must take
out a read latch on this key, which properly serializes them with range key GC.

Additionally, if the GC queue detects that an entire range has been deleted by
an MVCC range tombstone, it uses a separate `GCRequest` with a fast path that
clears out the entire range using a `ClearRange` (i.e. a Pebble tombstone),
without having to do an MVCC stats scan across it. This commonly happens with
schema GC, e.g. when dropping an entire table/index.

## SSTs, Export, and Ingestion

### SST Tooling

As soon as the cluster is upgraded to 22.2, it can produce SSTs that contain
Pebble range keys and thus MVCC range tombstones.

`SSTWriter` implements the `Writer` interface, and supports the same range key
methods as the main Pebble engine, notably `PutMVCCRangeKey()`. Refer to the
`Writer` section above for details.

However, the old `SSTIterator` does not support range keys, as it is built on a
more primitive SST iterator infrastructure that e.g. does not support combined
point/range key iteration. `NewSSTIterator()` can be used instead to construct a
new `MVCCIterator` for SSTs using the same `pebbleIterator` implementation that
is used for Pebble itself, supporting the same options and functionality,
including MVCC range keys. Notably, this also supports iteration across several
multiplexed SSTs, replacing the current `multiIterator` with a much more capable
implementation that will be particularly useful for backup restoration.

### Exports

Unlike e.g. `Scan` and `Get`, `Export` requests do need to handle MVCC range
tombstones explicitly.

`MVCCExportToSST` will unconditionally include MVCC range tombstones in the
generated SSTs if they exist in the source data. These will be emitted together
with point keys, truncated to the same key span, and respect the same options
such as `ResumeSpan` and `StartTime`.

For example, if an export hits a size limit and returns an SST with keys in the
span `[b-d)`, and there was an MVCC range tombstone across `[a-f)@2`, then the
returned SST will include an MVCC range tombstone `[b-d)@2`. When the client
comes back to resume from `d`, the next SST will include the rest of the MVCC
range tombstone from `d` onwards.

NB: when using `SplitMidKey` to return a resume span in the middle of a point
key version series, MVCC range tombstones will overlap in these SSTs. For
example, if there is a range tombstone `[b-d)@5`, and iteration stops between
`c@3` and `c@1`, returning a resume span at `c@1`, then the previous SST will
contain `[b-c\0)@5` and the next SST will contain `[c-d)@5`. This ensures that
the range tombstones in each SST cover `c`, but the range tombstones in the
SSTs overlap at `[c-c\0)@5`. This will not present a problem with multiplexed
iteration using `NewSSTIterator()`, nor with `AddSSTable`.

### `AddSSTable` Ingestion

When upgraded to 22.2 and the `MVCCRangeTombstones` version gate is active,
`AddSSTable` will support ingesting SSTs containing MVCC range keys, with the
same options and behaviors.

## Rangefeed Emission

MVCC range tombstones will be emitted across rangefeeds as a new
`RangeFeedDeleteRange` message contained in `RangeFeedEvent.DeleteRange`.
The message key span will be truncated to the rangefeed subscription bounds as
necessary.

```protobuf
message RangeFeedDeleteRange {
  Span               span        = 1 [(gogoproto.nullable) = false];
  util.hlc.Timestamp timestamp   = 2 [(gogoproto.nullable) = false];
}
```

There will be no corresponding point key events for the keys deleted by the MVCC
range tombstone: it is up to the consumer to apply these to relevant point keys
as appropriate.

However, rangefeeds with diffs enabled will respect the MVCC range tombstone as
the previous (empty) key when new keys are written above them. For example,
if there is a range tombstone `[a-c)@2` above a point key `a@1`, then when a
new key `a@3` is written the event will show an empty previous value rather than
the value of `a@1`.

These messages will also be emitted during catchup scans, ordered at the start
key of the MVCC range tombstone. Initial scans (implemented on the client side)
do not emit tombstones at all, as they simply use a `Scan` request that ignores
keys covered by any kind of tombstone.

The rangefeed library has gained support for these events via a
`WithOnDeleteRange` callback option. If a `RangeFeedDeleteRange` message is
received without this option set, the rangefeed client will error, similarly to
the `WithOnSSTable` option.

CDC will not need to take these into account yet, because the two initial
use-cases (SQL schema GC and import cancellation) take the tables offline and
prevent CDC changefeeds across them (including later catchup scans). However,
future use-cases may cause these to be emitted across live tables, in which case
CDC will have to handle them.

Cluster replication, however, must consume these events by ingesting the MVCC
range tombstones into the target cluster. That was much of the reason for
building them in the first place.

## Related Reading

The initial internal design documents were in a state of constant flux, going
through multiple revisions and rewrites, and many details were also hashed out
during implementation. These documents are therefore mostly of historical value,
and serve as much to confuse as to enlighten. They are listed below for
completeness:

* [Non-MVCC Operation History Pre-RFC](https://docs.google.com/document/d/1wyOS6dSlmwMb1-QKj9oFNhwQIiq3z7R2qRFaOzJfzn4/edit)
* [Immutable MVCC History Pre-RFC](https://docs.google.com/document/d/179PYb9EYryBQ9-nX2OJBlDxbdrJ9ZTHiXZmm8-d-LaE/edit)
* [Efficient DeleteRange](https://docs.google.com/document/d/1ItxpitNwuaEnwv95RJORLCGuOczuS2y_GoM2ckJCnFs/edit#heading=h.x6oktstoeb9t)
* [Generalized Range Key-Value support in Pebble](https://docs.google.com/document/d/1nk921yVj7eltk-5Hv6wQ3n2-mPMH82aDkQIO9aOSRhY/edit)
* [db: support range-key => value](https://github.com/cockroachdb/pebble/issues/1339)
* [docs: draft design sketch for ranged keys](https://github.com/cockroachdb/pebble/pull/1341)
* [Meeting notes: cluster-to-cluster replication, incremental backups, and non-MVCC ops.](https://docs.google.com/document/d/1LM18uYixiTXqlyuvju_eXRSihWTDxKFAI7gn078jNf4/edit#heading=h.sf6aeh3pf4er)

Related RFCs:

* [Streaming Replication Between Clusters RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20201119_streaming_cluster_to_cluster.md)
* [RFC Draft - Import Rollbacks without MVCC Timestamps](https://docs.google.com/document/d/16TbkFznqbsu3mialSw6o1sxOEn1pKhhJ9HTxNdw0-WE/edit#heading=h.bpox0bmkz77i)