# Gitload Oracle — Future Improvements

Identified gaps in the gitload oracle's verification and workload coverage.

Completed:
- Item A (blob SHA-256 content verification) — implemented in verify.go
- Item B (commit metadata verification) — verifyCommitMetadata() compares
  every field in commits table against getCommitMeta() from git
- Item C (commit graph verification) — verifyCommitGraph() compares
  commit_parents against git rev-list --parents bidirectionally
- Item D (total_blobs_bytes verification) — verifyRepoStats() now also
  checks total_blobs_bytes against SUM(size) FROM blobs
- Item E (merge diff handling) — validated as correct; increased test
  commits to 60 to exercise merge paths
- Item F (replay ordering) — fixed by building per-commit trees instead
  of a single cumulative tree; intermediate verification now works on
  branching histories
- Item I (secondary indexes) — three indexes added with FORCE_INDEX +
  EXCEPT ALL consistency verification
- Item J (roachtest transaction wrapping) — gitload_verify.go now wraps
  all checks in a single AS OF SYSTEM TIME follower_read_timestamp() txn
- Root commit diff bug — getDiffTree was missing --root flag, so root
  commit files were never stored in commit_diffs

## Verification Gaps

### ~~B. Verify commit metadata against git~~ (DONE)

Added `verifyCommitMetadata()` in verify.go that queries all commits
from the DB and calls `getCommitMeta()` for each, comparing every field
(tree_hash, author_name, author_email, author_date, committer_name,
committer_email, committer_date, message).

### ~~C. Verify commit graph (parents) against git~~ (DONE)

Added `verifyCommitGraph()` in verify.go that loads the git DAG via
`loadCommitDAG()`, queries all `commit_parents` rows from the DB, and
compares bidirectionally. Catches missing edges, extra edges, and
parent order mismatches.

### ~~D. Verify `total_blobs_bytes` in `verifyRepoStats`~~ (DONE)

Added `COALESCE(SUM(size), 0) FROM blobs` check to `verifyRepoStats()`.
Now verifies all three counters: total_commits, total_files_changed,
and total_blobs_bytes.

### ~~E. Fix merge commit diff handling~~ (VALIDATED)

Analysis: `getDiffTree` diffs against the first parent only, which was
originally thought to miss changes from the second parent. However,
`git diff-tree parent0 merge_commit` captures ALL differences between
parent0's tree and the merge tree, including files brought in via parent1.
The replay logic (clone parent0's tree + apply first-parent diffs) correctly
reconstructs the tree at merge commits. Test coverage was extended by
increasing commit count from 20 to 60 so that branches/merges are actually
generated, confirming correctness.

### ~~F. Replay ordering assumption~~ (DONE)

Fixed by building per-commit trees in `replay()`. Each commit's tree is
now constructed by cloning its first parent's tree and applying diffs,
rather than using a single cumulative tree. This correctly handles
branching histories and allows intermediate verification at any commit.

## Workload Gaps

### G. Add concurrent writers

Single worker, serial commit-by-commit ingestion. Many CRDB correctness
bugs manifest under concurrent multi-range transactions — write-write
conflicts, serialization anomalies, distributed deadlocks. This workload
doesn't exercise any of that.

**Fix:** Allow multiple workers ingesting different commit subsets
concurrently while respecting topological ordering constraints.

### ~~H. Add read-during-write verification~~ (DONE)

Added `--inline-verify` flag (interval in seconds, 0 = disabled). When
enabled, a background worker in `Ops()` runs all six oracle checks
concurrently with ingestion using `AS OF SYSTEM TIME
follower_read_timestamp()`. The commit graph check operates in partial
mode, validating only DB-side edges without requiring all git commits to
be present. Empty snapshots (during table truncation between cycles) are
skipped gracefully. `runInlineChecks()` in verify.go is the entry point.
Test coverage exercises empty, partial (20/60 commits), and full data.

### ~~I. Add secondary indexes~~ (DONE)

Added three secondary indexes (`idx_commit_diffs_file_path`,
`idx_commits_topo_order`, `idx_file_latest_last_commit`) in PostLoad.
Added `verifySecondaryIndexes()` in verify.go that uses `FORCE_INDEX`
hints and `EXCEPT ALL` to compare index-scan results against primary-scan
results, catching index corruption.

### ~~J. Wrap roachtest operation in a transaction~~ (DONE)

Wrapped all checks in `gitload_verify.go` in a single read-only
transaction with `SET TRANSACTION AS OF SYSTEM TIME
follower_read_timestamp()`. All queries now see a consistent snapshot,
eliminating false positives from concurrent writes. Also added
`total_blobs_bytes` verification to the roachtest operation.

### K. Add type diversity

All columns are STRING, INT, or BYTES. Doesn't exercise DECIMAL,
TIMESTAMP, JSONB, ARRAY, UUID, or other type-specific storage/encoding
paths.

**Fix:** Add columns with diverse types (e.g., store `author_date` as
TIMESTAMPTZ, add a JSONB column for structured metadata, use UUID for
some identifiers).

## Priority Order

1. ~~**A. Verify blob SHA-256 content**~~ (DONE)
2. ~~**F. Replay ordering / per-commit trees**~~ (DONE)
3. ~~**C. Verify commit graph**~~ (DONE)
4. ~~**E. Merge diff handling**~~ (VALIDATED — not a bug)
5. ~~**I. Add secondary indexes**~~ (DONE)
6. ~~**J. Wrap roachtest operation in a transaction**~~ (DONE)
7. ~~**B. Verify commit metadata**~~ (DONE)
8. ~~**D. Verify `total_blobs_bytes`**~~ (DONE)
9. ~~**H. Add read-during-write verification**~~ (DONE)
10. **G. Add concurrent writers**
11. **K. Add type diversity**
