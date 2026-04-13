# Git Oracle Workload (gitload)

**Author:** Chanderprabh Jain | **Date:** April 2026 | **Status:** Implemented

## What It Is

gitload ingests a git repository's commit history into CockroachDB and uses git as an independent oracle to verify the database stored everything correctly. Any mismatch between what the database returns and what git says means CockroachDB has a data integrity bug.

The workload continuously clears and re-ingests the same data in different topological orderings, exercising different lock acquisition patterns and FK validation sequences each cycle.

## Git Primer: The Concepts gitload Uses

A git repository is built from a small set of primitives. gitload maps each one to a relational table.

**Commits and the DAG.** Every commit has a unique hash, a pointer to its parent commit(s), and metadata (author, date, message). Linear commits have one parent; merge commits have two. Together they form a directed acyclic graph (DAG):

```
    A ---> B ---> C ---> F          main branch
                \         /
                 D ---> E           feature branch, merged at F
```

F is a merge commit (parents: C and E). Commits D and C have no ordering relative to each other -- a topological sort can place them in either order as long as parents come before children.

**Trees and blobs.** At every commit, git records a full snapshot of the file tree. Each file's content is stored as a **blob**, identified by a content hash. If two files have identical content (even across different commits), they share the same blob. The command `git ls-tree -r <commit>` returns the complete file listing at that commit -- this is the ground truth we compare the database against.

**Diffs.** `git diff-tree <parent> <commit>` lists what changed between two commits using four operation types:

- **A** (Add) -- a new file appeared
- **M** (Modify) -- an existing file's content changed
- **D** (Delete) -- a file was removed
- **R** (Rename) -- a file moved to a new path

These map directly to rows in the `commit_diffs` table and are replayed during verification.

**Why git is a good oracle.** Git is an independent, well-tested implementation. It is deterministic (same repo, same answers), content-addressable (every blob has a hash we can recompute), and structurally rich (the DAG, trees, and blobs give us multiple independent cross-checks). If git and CockroachDB disagree, the bug is in CockroachDB.

## How It Works

**Diagram 1 -- End-to-End Architecture:**

```
  +================+    git commands     +===============+
  |  Synthetic     |---(read commits)--->|  gitload      |
  |  Git Repo      |                     |  Worker       |
  |                |<--(read blobs)------|               |
  |  (generated    |                     |  Loop:        |       SQL
  |   once from    |                     |   clear       |----writes--->  CockroachDB
  |   a seed)      |                     |   topo sort   |               (6 tables)
  +======+=========+                     |   ingest      |
         |                               |   next cycle  |
         |                               +======+========+
         |                                      |
         |  ground truth        +===============+===============+
         +--------------------->|         Verifier              | <--read-only--
                                |  L1: counter consistency      |     query
                                |  L2: file snapshot vs git     |
                                |  L3: diff chain replay vs git |
                                |  Any mismatch = CRDB bug      |
                                +===============================+
```

### Schema

Six tables mirror git's data model:

| Table | Role |
|-------|------|
| **blobs** | Content-addressable file storage (SHA-256 PK). Deduplicated across commits. |
| **commits** | One row per commit with metadata and a `topo_order` column that changes each cycle. |
| **commit_parents** | DAG edges. Linear commits have one parent row; merges have two. |
| **commit_diffs** | Every file operation (A/M/D/R) per commit, with blob reference. |
| **file_latest** | Materialized current state of every file. Updated incrementally during ingestion. |
| **repo_stats** | Singleton counter row: total commits, total file changes, total blob bytes. |

FK constraints link `commit_parents`, `commit_diffs`, and `file_latest` back to `commits`, and `commit_diffs` to `blobs`.

### Ingestion

Each commit is ingested atomically in a single transaction. Git I/O (reading metadata, diffs, and blob content) happens **before** the transaction to minimize its duration. Inside the transaction:

1. INSERT blobs (ON CONFLICT DO NOTHING for dedup)
2. INSERT commit row
3. INSERT parent edges
4. Process diffs in priority order (deletes, then renames, then adds/modifies) to avoid path conflicts
5. For each diff: INSERT into commit_diffs and update file_latest (UPSERT for A/M, DELETE for D, DELETE+UPSERT for R)
6. UPDATE repo_stats counters

### Topological Sort Randomization

The commit DAG has many valid topological orderings. gitload uses Kahn's algorithm with a **seeded RNG** for tie-breaking: when multiple commits have no unprocessed parents, the RNG picks which to insert next.

Each cycle increments the seed, producing a different insertion order over the same data. This exercises different lock sequences, FK validation patterns, and contention profiles. A bug that only surfaces under a specific ordering will eventually be hit.

## Verification: Three Oracle Levels

All three checks run in a single read-only `AS OF SYSTEM TIME` transaction.

**Diagram 2 -- Verification Pyramid (weakest to strongest):**

```
                    +---------------------------+
                    |        Level 3            |
                    |      DIFF REPLAY          |  Rebuild file tree from
                    |   Reconstruct tree from   |  stored diffs, compare
                    |   stored diffs            |  against git at each commit
                +---+---------------------------+---+
                |           Level 2                 |
                |        FILE SNAPSHOT              |  Compare file_latest
                |   End-state against git ls-tree   |  against git ls-tree
            +---+-----------------------------------+---+
            |              Level 1                      |
            |        COUNTER CONSISTENCY                |  repo_stats counters
            |   Row counts match aggregate counters     |  vs COUNT(*)
            +-------------------------------------------+
```

### Level 1: Counter Consistency

Compares `repo_stats` counters against actual row counts:

```
repo_stats.total_commits       ==  COUNT(*) FROM commits
repo_stats.total_files_changed ==  COUNT(*) FROM commit_diffs
```

Catches: counter update bugs, lost commits, phantom rows.

### Level 2: File Snapshot

Compares `file_latest` against `git ls-tree` at the latest commit:

1. Get the latest commit by `topo_order`
2. Query `file_latest` from the database
3. Run `git ls-tree` for ground truth
4. Compare path sets bidirectionally (missing files, phantom files)
5. For each file, recompute SHA-256 from git and compare against stored `blob_sha256`

Catches: missing or extra files, corrupted blob content, wrong blob references.

### Level 3: Diff Chain Replay

The strongest check. Reconstructs the file tree from scratch using only stored diffs:

```
    Read all commits + diffs ordered by topo_order
    Maintain in-memory tree: map[path] -> blob_sha256

    For each commit:
        Apply diffs:  A/M -> set path    D -> delete path    R -> move path
        At sample points: compare tree vs git ls-tree at that commit

    Always verify the final commit.
```

Catches: incorrect diff operations, missing diffs, wrong operation types, ordering bugs. If any single diff was stored incorrectly, the accumulated tree diverges from git.

### Roachtest SQL-Only Checks

A separate `gitload_verify` operation validates referential integrity via SQL joins (no git repo needed): every FK reference in `commit_parents`, `commit_diffs`, and `file_latest` must point to a valid row in `commits` or `blobs`. Runs on any cluster with gitload data.
