# Index backfills

This document explains why index backfills using historical reads and retry
transactions are always correct.

Index backfills work as follows:

1. Wait until index converges to write/delete state. This means that every
   write and delete to the primary table gets reflected in the index.
2. Fix an MVCC timestamp, `tScan`. This timestamp will be used to perform scans
   in the fast path, which are fast because historical scans don't need to
   acquire latches that could conflict with foreground traffic.
3. Backfill in chunks of size `n`, using DistSQL. The chunks should be distinct,
   but the chunk procedure works even if they're not. Each chunk performs steps
   3 to the end, occurring in parallel.
4. Do a KV scan over the span handed to the chunk by DistSQL, at timestamp
   `tScan`. Limit to the `n` chunk size. Create index entries for every found
   key.
5. Send `InitPut` commands for all of the index entries. These commands succeed
   when the index key's value is empty (and never written to, not even a
   tombstone) or when the key's value is identical to that of the index entry.
   The fast path is when all of the commands  succeed - then the chunk is done.
6. If any of the `InitPut` commands fail, revert to the slow path. Create a new
   transaction at `tNow`, in which the scan is re-run from scratch, index
   entries are rebuilt with data at `tNow`, and `InitPut`s are sent in the same
   transaction. These `InitPut` commands are set to *not fail* if a tombstone is
   encountered, to resolve conflicts generated during the `DELETE_ONLY` state.
   If this transaction fails due to an `InitPut` failure, that indicates an
   actual uniqueness violation or other error and the schema change is
   reversed.

## Why is this algorithm correct?

If `InitPut` with index values created with data from `tScan` succeeds (in
other words, sees either an empty entry or an entry with identical data no
matter what the MVCC timestamp), the index entry is always correct. To see,
let's look case by case.

We'll use the following table as an example throughout, where `k` is the
primary key and assuming we're building a unique index on `v`. Since we're
building a unique index, the key of the index will be `v` by itself. The value
of the index will be `k`.

Further, let's say the schema change flips into delete mode at time `tDelete`
and write/delete mode at time `tWrite`, where `tScan > tWrite > tDelete > 0`.
Recall `tScan` is a fixed timestamp chosen for the backfill's historical reads.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `1` | `a` | 0                    |               |               |                              |
| `2` | `b` | `tWrite+1`           | `b`           | `2`           | `tWrite+1`                   |
| `3` | `c` | 0                    | `c`           | `(tombstone)` | `tScan+1`                    |
| `3` | `d` | `tScan+1`            | `d`           | `3`           | `tScan+1`                    |
| `4` | `e` | 0                    |               |               |                              |
| `5` | `e` | `tScan+1`            | `e`           | `5`           | `tScan+1`                    |
| `6` | `f` | 0                    |               |               |                              |
| `6` | ` ` | `tScan+1`            | `f`           | `(tombstone)` | `tScan+1`                    |
| `7` | `g` | `0`                  |               |               |                              |
| `8` | `g` | `tScan+1`            | `g`           | `8`           | `tScan+1`                    |
| `9` | `h` | `0`                  |               |               |                              |
| `9` | ` ` | `tDelete+1`          | `h`           | `(tombstone)` | `tDelete+1`                  |
| `9` | `h` | `tDelete+2`          | `h`           | `(tombstone)` | `tDelete+1`                  |

## The `InitPut` succeeds

There are two scenarios where `InitPut` succeeds - there's no existing value or
the existing value is identical to the expected value.

### The `InitPut` sees an empty entry

In this case, the index entry found by `InitPut` has never been written to.
Since we know that all operations on the table at or after `tScan` cause updates
to the index, an absent index entry indicates that the primary table entry has
not been updated since `tScan` and is therefore safe to echo to the index.

For example, when the backfiller encounters the pair `1` `a`, the corresponding
index entry is empty, so it proceeds to write.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `1` | `a` | 0                    | `a`           | `1`           | `after tScan`                |

### The `InitPut` sees a non-empty entry with identical value bytes

In this case, the index entry found by `InitPut` has a value identical to the
key of the table. There are just two ways this could happen, both of which are
ok:

1. A table entry was written after `tWrite` but before `tScan`, leaving behind
   a corresponding index entry. This case is safe since we're just repeating
   previous work.
2. A backfiller dies before it checkpoints, leaving behind an index entry. When
   the backfiller restarts, it encounters its previously written entry. This
   case is safe since we're again just repeating previous work.

There's never a case where this would accidentally cause a uniqueness
constraint violation, since the index entry contains the (guaranteed unique)
primary key of the table in the value. A uniqueness constraint violation always
manifests as an unexpected value for a key.

For example, when the backfiller encounters the pair `2` `b`, the corresponding
index entry is `b` `2`, so the backfiller does nothing and moves on.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `2` | `b` | `tWrite+1`           | `b`           | `2`           | `tWrite+1`                   |

## The `InitPut` fails

In all other cases, the `InitPut` will fail. In these cases, we will retry the
operation transactionally with a fresh read to determine whether to return a
uniqueness constraint violation or to write a more up to date value.

### The `InitPut` fails due to a changed table value

In the case where a table entry's value changes, the backfill encounters a
tombstone and fails. When retried, the backfill will generate a new index entry
with the correct key and write it.

For example, when the pair `3` `c` is encountered, the `InitPut` sees a
tombstone and fails. When retried transactionally, the pair is determined to
actually be `3` `d`. The transactional `InitPut` then does nothing since the
entry was already in the index.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `3` | `c` | 0                    | `c`           | `(tombstone)` | `tScan+1`                    |
| `3` | `d` | `tScan+1`            | `d`           | `3`           | `tScan+1`                    |

### The `InitPut` fails due to a changed table key

In the case where a table entry's key changes, the backfill encounters a
mismatched value and fails. When retried, the backfill won't see the old key
and will do nothing. This is correct because the new entry will have already
been written by the table update, since it's occurred after `tWrite`.

For example, when the pair `4` `e` is encountered, the `InitPut` sees a
`5` when it expects a `4` and fails. When retried transactionally, there's no
longer an entry for `4`.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `4` | `e` | 0                    |               |               |                              |
| `5` | `e` | `tScan+1`            | `e`           | `5`           | `tScan+1`                    |

### The `InitPut` fails due to a deleted entry

In the case where a table entry was deleted, the backfill encounters a tombstone
and fails. When retried, the backfill won't see the deleted entry and will
therefore do nothing.

For example, when the pair `6` `f` is encountered, the `InitPut` sees a
tombstone and fails. When retried transactionally, there's no longer an entry
for `6`.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `6` | `f` | 0                    |               |               |                              |
| `6` | ` ` | `tScan+1`            | `f`           | `(tombstone)` | `tScan+1`                    |

### The `InitPut` fails due to a duplicate value

In the case where an entry was written that violates a uniquness constraint by
duplicating a value that had been written before `tWrite`, the backfill
encounters a mismatched value and fails. When retried, the entry is still there
and fails `InitPut` again, which fails the backfill.

For example, when the pair `7` `g` is encountered, the `InitPut` sees an `8`
when it expects a `7` and fails. When retried transactionally, the `InitPut`
still sees an `8` and fails again, correctly halting the backfill with a real
uniqueness constraint violation.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `7` | `g` | `0`                  |               |               |                              |
| `8` | `g` | `tScan+1`            | `g`           | `8`           | `tScan+1`                    |

### The `InitPut` fails due to a reinserted table value during delete only mode

In the case where a table entry is deleted and re-added when the cluster is
in the delete only state, the backfill encounters a tombstone value and fails.
When retried, the entry is still there but will not fail `InitPut` because
the transactional retries instruct `InitPut` to treat tombstone values the same
as absent values.

For example, when the pair `9` `h` is encountered, the `InitPut` sees a
tombstone when it expects a `9` and fails. When retried transactionally, the
`InitPut` still sees a tombstone and succeeds.

| `k` | `v` | `t` (MVCC timestamp) | `idx_k` (`v`) | `idx_v` (`k`) | `idx_t` (idx MVCC timestamp) |
|-----|-----|----------------------|---------------|---------------|------------------------------|
| `9` | `h` | `0`                  |               |               |                              |
| `9` | ` ` | `tDelete+1`          | `h`           | `(tombstone)` | `tDelete+1`                  |
| `9` | `h` | `tDelete+2`          | `h`           | `(tombstone)` | `tDelete+1`                  |
| `9` | `h` | `tScan+1             | `h`           | `9`           | `tScan+1`                    |

## Tests

The above scenarios are exercised in a unit test in
`pkg/sql/indexbackfiller_test.go`. If this tech note is updated, please ensure
that the updates are propagated to that test.

# Bulk-backfill via AddSSTable

Writing lots and lots of data put-by-put is expensive -- the write amplification
of putting them all in the Raft, the WAL, compacting, etc all adds up. While we
pay these costs on normal writes because we need the benefits those mechanisms
are providing (durability, txn isolation, etc), when doing big, bulk operations
using bulk-friendly operations can have significant benefits.

Specifically, AddSSTable is relatively cheaper way to ingest a large amount of
data compare to putting it via KV, even in batches. Given that an index backfill
potentially needs to ingest a large amount of data, this is an appealing option.

However, part of what makes these requests cheaper is the different semantics,
such as their lack of transactional semantics, so it is not quite a drop-in
replacement for the current batches of init-puts, and of course raises different
correctness and stability concerns.

## Bulk-backfill Overview

More specifically, we'll want to read much larger chunks at a time and build
SSTables of the produced entries.

There are two main questions / challenges to address with this approach though:
1) How we actually build and ingest the SSTs i.e. how many files we make and how
  much they overlap.
2) How we handle the difference in transitional / KV semantics between InitPuts
  (which have transactional isolation and return an error if they hit an
  existing row) and AddSSTable (which does not have transactional isolation or
  InitPut semantics).

## Challenge: Index entries produced from some chunk of the source table span many ranges.

As each chunk of the table is read and batches of index entries are produced,
those entries can be all over the key space of the index. Simply making the
whole batch a single SST will require many retires to correctly split since
AddSSTable can only add to a single range.

### Fix for SSTs that span ranges: Teach SST builder to split at known range boundaries

We're still need to handle an unexpected split and retry with rebuilt SSTs for
either side of the split, but we can use the local cache to at least _try_ to
avoid building an SST across a split, and instead flush before crossing the
split boundary.

## Challenge: SSTs of Index entries from separate chunks of the table overlap and ingest at higher levels

The batches of entries produced by separate chunks of the table overlap so
ingesting those SST will  mean placing them at higher levels -- since they have
to be placed at the highest non-overlapping level, they will likely end up in L0
relatively quickly. One of the major benefits of ingesting raw SSTs is avoiding
write amplification, part of which is from lower ingestion avoiding rewriting in
compactions.

## Challenge: too many overlapping ingestions can kill RocksDB

With many chunk readers producing lots of potentially small, overlapping files
e.g. one producer per node, each producer potentially reads a chunk produces has
at least 2 keys for every range that span the range, thus every range receives
num-nodes files that overlap at least all but one end up in L0. Every
overlapping ingestion can cause a memtable flush and its associated overhead,
plus the raw number of files in L0 can be an issue -- Rocks will back pressure
and eventually stop writes if we spam it too much.

### Potential fix for too many L0 files: Sort all produced entries across nodes before ingestion

IMPORT has exactly one writer responsible for making and adding the SSTs some
key range -- any producer that makes a key for that range must send it to that
writer rather than produce and apply its own SST. Currently this writer buffers
all the keys sent to it before iterating them in order to produce ordered,
non-overlapping SSTs to add. Combined with writing to new, unused key space,
this means IMPORTS ssts are ingested at L6 and see almost no compaction-derived
write application.

However, we currently believe this process -- the distributed sort and buffering
-- is expensive.

### Challenge: Dist-sort/buffering/non-overlapping writing is slow and requires lots of buffer space

We currently buffer the entire content before we start writing to ensure we can
sort and write completely non-overlapping SSTs, meaning we require significant
(i.e. 2x the data size) available free space in order to import.

The buffering and reading itself is also slow -- currently it is actually
writing each key to another RocksDB to buffer it, which obviously has some of
the same costs we initially tried to avoid be writing entire SSTs instead of
writing keys to a RocksDB.

#### Fix for costly dist-sort/buffering: tune the buffering and flush the writer

We could likely tune this sort and its buffering steps to reduce this cost.

One proposed fix for the cost of buffering and sorting is replacing the RocksDB
buffering with a simpler buffer-n-entries before flush to SST, then use
multi-iterator over SSTs for ordered reading.

Another proposal is to sort and ingest the buffered data when it reaches some
threshold, even if more is being produced --this would allow _some_ potential
for overlapping ingestion which might push some files higher, but would no
longer require unbounded buffering space to do the total sort required to avoid
overlapping files.

Any wins here would likely help IMPORT as well.

### Potential fix for L0 spam without a sort

We could also back pressure AddSSTable before regular writes to let the
compactor catch up. If we get the worst-case key distribution where we produce
many small files, then the compactor should be able to compact them relatively
quickly and the delay should be minimal. If we're producing lots of large files,
then we need to back off anyway to let the compactor catch up. We will see
higher write application, but we've to some degree just moved it from the
buffering stage down to the compactor (theory: rocks is pretty good at
compacting, and we're not great at buffering).

This approach can probably be used in IMPORT too if it works, eliminating the
dist-sort?


## Challenge: AddSSTable is non-transactional and does not have InitPut semantics

Currently the schema change advances an index to the DELETE_AND_WRITE state
before backfilling -- this means that CRUD ops will write index entries, and
these writes and the backfill writes are both transactional and thus protected
against the usual anomalies. If a query were to try to write a duplicate value,
one of either the index backfill write or the query write will succeed and the
other will (perhaps after being pushed/retried), get a conflict from its InitPut
and yield a uniqueness violation error.

AddSSTable however adds its batch of keys, which can each have any timestamp,
without consulting the timestamp cache, potentially rewriting history out from
under prior reads or shadowing existing keys at lower timestamps. This means the
backfiller could add a batch of entries including clobbering an entry written by
a query when one of the two should have instead failed.

### Rejected Fix: Teach AddSSTable to be transactional

We've looked at, and opted against, trying to teach AddSSTable to be more
transactional -- the point of it is to be cheaper than KV, and teaching it KV's
fancy tricks would likely start to defeat that purpose. For example, if we e.g.
teach it to check the timestamp cache (and optionally ensure the keys in the SST
match the command time), retrying a whole SST because of a conflicting is
expensive, and again, our goal is to make this cheaper.

### Rejected Fix: Teach AddSSTable to behave like InitPut w.r.t existing keys

Furthermore, we'd need more than just proper transactional semantics -- the
current index backfill relies on the InitPut semantics to correctly return
uniqueness conflict errors. Here again, we considered, and rejected, teaching
AddSST to support those semantics. Doing so would likely use a flag that added a
check the current iteration of the underlying range (for computing merged MVCC
stats) to check if any of the existing keys conflicted with keys in the SST --
and if so, return an error instead of ingesting it. However, as with
transactional isolation, it isn't clear that we actually want to reject the
whole SST. A rejected SST would be retried -- it might then find an actual
anomaly, or it might find that a query's point-write changed a stored column or
something. Retrying might have other challenges -- a) we want to make big SSTs
to minimize overhead, but that maximizes retry cost and also maximizes the
chance a given SST will require a retry -- with it eventually approaching a
certainty. It also isn't clear how we'd actually do the retry -- e.g. in a
dist-sort based approach, the chunk readers are divorced from the actual SST
that was rejected, so we'd need to figure that out. And, at a high level, the
simplicity of ingestion is what makes AddSSTable fast. The more we mess with it,
the more likely it could lose the attribute that makes it appealing in the first
place.

### Conflict and Anomaly Fix: Counting Entries

Overall it seems easier to simply cede perfect conflict detection during the
backfill and instead look for violations after the backfill has completed. We
think we can do this cheaply, by simply counting the entries in the backfilled
index --  violations potentially allowed by the lack of initput semantics or
transactional conflicts would manifest as writers for two different rows to the
same index entry (one of which should have failed), meaning that there would be
fewer index entries than rows. By comparing the table row count to the index
entry count we can determine if this happened (and potentially then do a more
expensive search for an actual violation to provide in the error message?).

Obviously just having the correct number of keys does not prove an index has the
right keys, or that they have the right values, but for the specific failure
mode of the anomalies and semantics changes we're introducing, it appears to
suffice and is hopefully inexpensive (easily parallelized, potentially able to
use a fast MVCC count or other pushed-down op).
