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
