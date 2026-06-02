# BACKUP

BACKUP captures the contents of a set of SQL objects (tables, databases,
the whole cluster, or a tenant) at a chosen MVCC timestamp into immutable
files in external storage. A backup consists of a metadata **manifest**
plus a set of **SST files** containing the data; together they constitute
a single **layer**. Layers can be stacked: an incremental backup writes a
new layer on top of an existing **chain**, capturing changes since the
chain's most recent end time. Backups live in a **collection** — a
destination directory holding one or more chains, usually maintained by
a schedule.

## Content of a backup

### Backup scope

A single table is the smallest restorable unit in CockroachDB. A
`BACKUP` statement targets one of three scopes:

- **A specific table or set of tables**, possibly through database
  wildcards. Captures only the listed descriptors and their span
  data.
- **A "cluster"** — `BACKUP INTO '…'` with no targets. Despite the
  name, this is not a snapshot of a running cluster: it's a backup
  of all tables in the system tenant plus a curated set of cluster
  configuration that a *user* expressed (zones, comments, role
  memberships, certain system tables). It does *not* include
  internal execution state like running jobs, the range log, or
  timeseries data — those are specific to a physical cluster and
  not meaningful in a restored one. User-created state in, internal
  execution state out.
- **A tenant** — `BACKUP TENANT …`, available from the system
  tenant. Captures the entire keyspace of one secondary tenant
  *opaquely*: every KV is included without semantic processing of
  what those KVs mean. This is deliberate. Operating opaquely lets
  a tenant be backed up and restored across versions, settings, and
  configurations of the host cluster, and keeps the host's backup
  logic free of tenant-version-specific knowledge. Tenant backups
  therefore capture *more* than cluster backups — every system
  table the tenant has, including `system.jobs` — and the result is
  enough to re-hydrate the tenant as a full SQL universe (see
  [restore.md § Tenant restore specifics](restore.md#tenant-restore-specifics)).

### What's captured

Within the chosen scope, a backup layer captures:

- **Descriptors** — tables, types, schemas, databases, functions, at
  the backup's `EndTime`. Descriptors are captured *with their
  in-flight schema-change state* (mutation queues, etc.); restoring
  them carries that state into the destination, where it is
  reconciled post-restore.
- **Span data** — the rows behind those descriptors, exported as SST
  files (see §"DistSQL flow & KV APIs"). An incremental layer
  *excludes* spans for tables that are OFFLINE during its read
  interval (e.g. an in-progress IMPORT or RESTORE); when the table
  later returns to PUBLIC, the next incremental re-introduces its
  full span from time zero so that any data written while it was
  OFFLINE is captured. This is why incremental span computation
  branches on table state, not just on changed-key timestamps.
- **Stats, zone configs, comments, role memberships** — pulled from
  the relevant system tables and included in the manifest or as side
  data.
- **System tables** — per `system_schema.go`, each system table is
  classified for how (or whether) it participates in a cluster /
  tenant backup. `system.jobs` is excluded from cluster backups; a
  restored cluster always starts with no live jobs from the source.

#### Exclude data from backup

A table can opt out of having its data backed up while still having
its descriptor captured:

```sql
ALTER TABLE t SET (exclude_data_from_backup = true);
```

The intended use case is high-churn ephemeral tables (queue tables,
large staging tables). The real cost being avoided isn't the backup
work itself but the PTS protection that backup needs while reading
the table: keeping its old versions alive long enough to be backed
up would block their prompt GC. Excluding the table from the backup
is a downstream consequence — the actual motivation is letting that
high-churn data GC promptly even while a long-running backup is in
flight.

The flag is stored on the `TableDescriptor` and propagated to KV via
SpanConfig. BACKUP honors it in two places:

- **Planning** (`backup_job.go`): excluded tables' spans are
  pre-marked completed so they never appear in the export plan.
- **KV server-side** (`batcheval/cmd_export.go`): an `ExportRequest`
  overlapping an excluded span returns empty.

The flag flows through SpanConfig (not just a planning-time filter)
because tenant backups operate opaquely on KV spans and don't
enumerate per-table flags themselves: each KV server checks its
local SpanConfig and elides exports for excluded ranges on its own.

PTS records also exclude these spans, so the GC of high-churn data
is not held up by an in-progress backup. RESTORE leaves the
descriptor in place but the table is empty.

Tables referenced by inbound foreign keys cannot be marked excluded
(this would otherwise yield FK violations on restore).

### Revision-history layers

`BACKUP … WITH revision_history` causes export to use
`MVCCFilter_All` instead of the default `MVCCFilter_Latest`. The
resulting SSTs contain the full MVCC history within the layer's time
interval — densely packed multi-version entries, not a single-version
snapshot. Such SSTs can't be linked directly into a fresh cluster's
LSM (see
[restore.md § Why fast/online still need above-raft for revision-history layers](restore.md#why-fastonline-still-need-above-raft-for-revision-history-layers));
fast/online restore falls back to above-raft ingest for these layers.

Revision-history chains also support `RESTORE … AS OF SYSTEM TIME` to
reconstruct state at any timestamp within the chain's coverage.

Revision-history backups have historically been more prone to bugs
than ordinary backups. They get less testing, and the widely-used
`Span` type representing key ranges is typically only the user-key
range without an MVCC suffix — which led to past bugs around mid-row
resume-span representation. Their SSTs are also incompatible with
fast/online restore. Future work may deprecate revision-history
backups in favor of point-in-time backups paired with a "revision
log" that restore replays to serve the same point-in-time-recovery
use case with lower RPO (today's revision-history backups are still
periodic and can't reliably run more often than every few minutes).

### Encryption

Backups can be encrypted with a passphrase or one or more KMS keys
(`WITH encryption_passphrase = …` or `WITH kms = …`). A per-chain
data key is generated once and stored in the `encryption-info` file
alongside the manifest, encrypted under each provided KMS or derived
from the passphrase. SST file contents and the manifest itself are
encrypted; the destination path layout is not.

Multiple KMS URIs may be supplied for redundancy or rotation: any one
suffices to decrypt the data key. The data key itself is not rotated
within a chain — re-encryption requires starting a new full backup.

KV-side export does not see the encryption key; the
`ExportRequest` evaluator explicitly rejects any encryption parameters
from clients. Encryption happens at the sink, after KV returns the
plaintext SST (§"Sink and on-disk layout").

### Protected timestamps & schedules

A running backup holds a **protected timestamp** (PTS) record covering
its target spans across the backup's read interval. PTS prevents MVCC
garbage collection from removing versions the backup still needs to
read. The record is released when the backup completes (or fails
terminally).

`CREATE SCHEDULE … FOR BACKUP` runs backups on a cron schedule.
Scheduled chains use **PTS chaining**: between scheduled incrementals,
the existing PTS record's timestamp is updated in place
(`pts.UpdateTimestamp` in `schedule_pts_chaining.go`) to cover the
next incremental's read interval, with no release-and-recreate gap.
A new full backup instead allocates a fresh PTS record and releases
the chain's prior PTS once the new full has begun reading.

The full chaining protocol — why scheduled chains hold a PTS in
addition to the job's, and how records hand off across incrementals
and successive fulls without ever leaving a gap — is documented in
the file-level comment in `schedule_pts_chaining.go`.

### Multi-region: locality-aware destinations & exec-locality

Two orthogonal multi-region knobs that compose freely:

- **Locality-aware partitioned backups** control *where* SSTs land.
  The destination URI list pairs URIs with locality patterns:
  `('s3://default/…', 's3://east/?COCKROACH_LOCALITY=region%3Dus-east1', …)`.
  Each leaseholder writes its SSTs to the URI matching its locality,
  falling back to the default. The manifest records the multi-URI
  layout so restore knows where each file lives.
- **Exec-locality constraints** (`EXECUTION LOCALITY = '…'`) control
  *which* SQL instances participate as backup processors. The planner
  restricts processor placement to instances matching the locality
  filter, independent of where data is written.

A backup can use neither, one, or both. A common pattern is to
constrain execution to a single region's nodes while still writing to
multi-region storage.

## Execution

### Flow at a glance

A BACKUP statement runs in two stages:

1. **Planning** (synchronous, within the SQL session): resolves
   destination URIs and encryption, evaluates authorization, resolves
   descriptor targets, and constructs the `BackupDetails` payload
   that will be persisted with the job.
2. **Job execution** (asynchronous, distributed across nodes):
   `Resume()` finalizes destination resolution, prepares the
   manifest, acquires PTS, plans per-node DistSQL processors, drives
   the distributed export, writes the final manifest, and releases
   PTS.

The deferred design exists so that a long-running backup can
reliably complete across client disconnects and node restarts:
state lives on the job, not in a SQL session, and a restart can
resume from the last checkpoint without losing minutes or hours of
work.

Within that split, the dividing line between planning and Resume
is: planning captures what the *user* asked for and persists it
once; Resume does everything that depends on the cluster's state at
execution time, and must be safe to re-enter after a restart.

- Planning runs once, synchronously in the SQL session. Its output
  is a serialized `BackupDetails` payload describing the user's
  request — targets, destination, encryption configuration,
  options. Errors here surface as statement errors.
- Resume runs repeatedly. A node restart, a leaseholder change, or
  an explicit `PAUSE` / `RESUME` re-enters it; every Resume step
  must therefore be idempotent or checkpoint-protected. Destination
  finalization (the chain may have grown since planning), processor
  placement (depends on the current topology), the actual export,
  and PTS management all happen here.

Keep this rule in mind when adding new work to either side: if it
depends on cluster state that can change between submission and
execution, or could need to re-run after a crash, it belongs in
Resume.

### Planning

`backup_planning.go` registers `backupPlanHook` with the SQL planner.
The hook:

- Evaluates SQL expressions (destination URIs, encryption parameters,
  options).
- Resolves descriptor targets (`backupresolver`).
- Performs privilege checks (`checkPrivilegesForBackup`).
- Builds the initial `jobspb.BackupDetails`: targets, destination
  URIs, encryption configuration, full-cluster flag, schedule linkage
  if any.
- Creates the job record and either runs it (default) or returns
  control to the session (`WITH DETACHED`).

Destination resolution against `LATEST` (and auto-naming a subdir for
a new full) is deferred to the job so that a backup can be re-planned
on resume against the same logical destination.

### Job coordinator

`backupResumer.Resume()` in `backup_job.go` walks these phases:

1. **Destination resolution** — `backupdest.ResolveDest` finalizes
   the URI list and the subdir (incrementals locate the chain root;
   fulls allocate a fresh path).
2. **Lock file** — `BACKUP-LOCK-<jobID>` is written to the
   destination at job start to prevent concurrent backups racing on
   the same path. The lock file is not removed on success; subsequent
   attempts scan existing lock files and reject planning if any
   belong to a still-running job.
3. **Manifest preparation** — `prepBackupMeta` reads prior layer
   manifests, computes the spans to export (filtering
   `exclude_data_from_backup` spans into `completedSpans` so they
   are not requested), and builds the initial `BackupManifest`.
4. **Protected timestamp** — `writePTSRecordOnBackup` writes the PTS
   record covering the read interval; from this point GC will not
   remove versions in the target spans before `EndTime`.
5. **Checkpoint** — the in-progress manifest is written to external
   storage so a resumed job can identify already-exported spans.
6. **Distributed export** — `backup()` plans per-node processor
   specs and drives the DistSQL flow with concurrent checkpoint
   and progress loops.
7. **Finalization** — `backupinfo.WriteBackupMetadata` writes the
   final manifest and side files; PTS is released; if scheduled, the
   schedule's PTS is rolled forward and a compaction may be queued
   (§"Compaction").

Resume-from-failure relies on the checkpoint manifest: on `Resume()`
after a crash, `getCompletedSpans` reads the checkpoint and filters
already-exported spans out of the export plan, so retries do not
redo finished work.

### DistSQL flow & KV APIs

`backup_processor_planning.go`'s `distBackupPlanSpecs` partitions the
spans to export across SQL instances. Span-to-instance assignment uses
`PartitionSpans`, which maps each range to its leaseholder's node so
that exports run locally where possible. `EXECUTION LOCALITY` is
applied here to restrict the candidate instance set.

Each node runs a `backupDataProcessor` (`backup_processor.go`). For
each assigned span it issues `kvpb.ExportRequest` against
`NonTransactionalSender` with admission control (`BulkNormalPri`,
`FROM_SQL`). The notable request fields:

- **`MVCCFilter`** — `_Latest` for ordinary backups, `_All` for
  revision history. Determined at planning time from the job's
  `RevisionHistory` flag.
- **`StartTime` / `RequestHeader`** — the time interval and key span
  for this request. For "introduced" spans in an incremental (spans
  not covered by prior layers), `StartTime` is empty so all
  historical versions are read.
- **`SplitMidKey: true`** — KV may chunk the response in the middle
  of a row's MVCC history to respect `TargetFileSize`. The processor
  resumes from `ResumeSpan` + `ResumeKeyTS` without skipping or
  duplicating any version.
- **`TargetFileSize`** — set from a cluster setting; controls
  server-side SST chunking.
- **`IncludeMVCCValueHeader`** — set so value headers survive
  round-trip.

`ExportRequest` always returns SSTs (there is no raw-KV mode for
backup). Encryption is *not* requested via the KV API; the returned
SSTs are plaintext from KV's perspective, and the sink encrypts them.

### Sink and on-disk layout

The processor hands each response to a `FileSSTSink`
(`backupsink/file_sst_sink.go`). The sink:

- Buffers KVs into in-memory SST writers.
- Flushes a file when it hits the target size threshold or a span
  completes.
- Applies prefix elision (below) and encryption.
- Uploads to the locality-appropriate external storage URI.
- Records the file's spans, entry counts, encryption header, and
  size in the per-file metadata that joins the manifest.

On disk, a backup layer is laid out as:

- `BACKUP_METADATA` — the modern slim manifest. The same
  `BackupManifest` protobuf as below, but with the large fields
  (file index, descriptors, descriptor changes) externalized into
  companion SST files referenced by the manifest. Restore reads this
  file first when present.
- `BACKUP_MANIFEST` — the legacy full manifest, retained for
  back-compat with older readers. Modern backups write both; restore
  falls back to it only when `BACKUP_METADATA` is absent.
- `BACKUP-STATISTICS` — table statistics for restored tables.
- `BACKUP_PARTITION_DESCRIPTOR_*` — one file per locality partition,
  when the backup is partitioned.
- `encryption-info` — the encrypted data-key wrapping, when the
  backup is encrypted.
- `data/` — the actual SST files exported by the processors.

During execution the manifest is written as a checkpoint variant
(`BACKUP_MANIFEST_CHECKPOINT`, `BACKUP-CHECKPOINT-*`) and replaced
by the final files on completion. A resumed job reads the checkpoint
to identify already-exported spans and skip them on retry.

The `BackupManifest` proto (defined in `backuppb/backup.proto`) is
versioned via `format_version` and `cluster_version` fields that
restore-time planning validates against the current cluster's binary
version. The file index records each file's key span and timestamp
range so restore can build span coverings without scanning the SSTs
themselves.

**Key prefix elision**. Rather than storing each key with its full
prefix (e.g. `/Tenant/5/Table/52/1/…` for rows of table 52 in
tenant 5), the sink strips a shared prefix from every key in an SST
it writes and records the elided prefix in the file's metadata. The
file then holds keys relative to that prefix only. This shrinks file contents
and lets restore re-prefix at link time without rewriting the file. See
[restore.md § Ingest — below-raft (fast/online)](restore.md#ingest--below-raft-fastonline)
for how restore consumes the elided prefix to produce a synthetic
prefix on the linked file.

### Backup collections

Backups are organized at the destination into **collections**. A
collection is a directory in external storage holding one or more
chains: each chain is a full backup plus zero or more incrementals
stacked on top. A new full within a collection starts a new chain;
a new incremental adds a layer to the most recent chain.

A `LATEST` pointer at the root of the collection names the most
recent full's subdirectory. RESTORE reads it to find what to
restore; BACKUP planning reads it to discover the chain an
incremental will extend. Picking the new incremental's `StartTime`
is then a chain walk: the prior layer's `EndTime` becomes the new
layer's `StartTime`, so an incremental captures exactly the changes
since the previous one.

Compaction (below) discovers its inputs the same way — it reads the
chain via the collection, picks a contiguous run of layers to merge,
and writes a single replacement layer.

`CREATE SCHEDULE … FOR BACKUP` is what produces this structure in
practice: the schedule runs configured incrementals against the
same collection between periodic fulls, generating the chain
structure the rest of the pipeline assumes.

### Compaction

`compaction_*.go` implements optional compaction of an incremental
chain: it merges several adjacent layers' SSTs into a single
replacement layer so the chain has fewer files for restore to read.
Compaction is triggered automatically after scheduled incrementals
when the chain length exceeds `backup.compaction.threshold`; it runs
as a separate job type with its own DistSQL flow
(`compaction_processor.go`). A completed compaction writes a new
manifest marked `IsCompacted = true` covering the merged time range.
When RESTORE walks the chain it sees the compacted manifest as a
single layer; the original layers' files may be deleted depending on
the schedule's retention configuration. Compaction is transparent to
RESTORE.

Beyond just "fewer files to read," compaction is load-bearing for
online restore in general, not only for revision-history chains.
Online restore registers each backup layer's files into the
destination LSM, and the LSM has only ~6 levels below L0; L0 itself
absorbs sublevels, but registering dozens of layers there would
push read amplification well into "unusable query performance"
territory. That caps the layer count online restore can accept far
below what schedules normally produce — BACKUP allows chains of
dozens of incrementals — so online restore rejects chains longer
than `onlineRestoreLayerLimit` at planning time
([restore.md § Mode compatibility checks](restore.md#mode-compatibility-checks)).
Compaction is what keeps a long-running schedule's chain short
enough for online restore to stay viable; the alternative — taking
more frequent fulls to cap chain length — is expensive enough that
operators have asked for higher layer limits over time.

Revision-history chains have an additional reason to compact: each
revision-history layer also forces fast/online restore to fall
back to above-raft for that layer's files, on top of counting
against the layer limit.
