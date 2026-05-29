# RESTORE

RESTORE reads one or more BACKUP layers from external storage and
re-creates SQL objects and their data in the current cluster.
CockroachDB supports three execution modes for the data-ingestion
portion of restore — **legacy** (above-raft), **fast** (below-raft,
with user traffic gated), and **online** (below-raft, with user
traffic enabled immediately) — that trade restore wall-clock time
against when the restored data becomes queryable.

## Mixed-version contract

Restore inherits the cross-version compatibility window from
upgrades: if a destination cluster can be upgraded from version X,
it can restore a backup taken at X. A backup taken at v25.4 can be
restored into v26.2 (whose minimum supported version is v25.4); a
backup taken at v25.2 cannot, because v25.2 is below v26.2's
minimum and the destination would have no upgrade path from it
either.

The window is inherited rather than independently designed. The
backup format itself
([backup.md § Sink and on-disk layout](backup.md#sink-and-on-disk-layout)
covers the manifest's `format_version` / `cluster_version` fields)
and most of what it contains — catalog descriptors in particular —
are already versioned and already expected to obey the upgrade
interop rules. Restore reads them through the same upgrade-time
paths the rest of the cluster uses; whatever invariants those paths
hold for live upgrades also hold for restore.

This window is unusual in CockroachDB. Most mixed-version reasoning
concerns RPCs between cluster nodes during a relatively short
upgrade transition. A backup written to object storage, by
contrast, may sit for months before being read by a different
cluster — the interop window must hold for that whole gap, not just
for the duration of an upgrade. Tenant backups carry this further
with less machinery: tenant data is captured opaquely
([backup.md § Backup scope](backup.md#backup-scope)), so the host
moves KVs across the gap without interpreting them, leaving any
version-specific logic inside the tenant.

The supported upgrade paths — and therefore the supported
backup-then-restore version pairs — are exercised by roachtests on
the upgrade matrix.

## What restore does

### Restorable units

A `RESTORE` statement targets one of:

- **A specific table or set of tables**, possibly through database
  wildcards. Re-creates the listed descriptors and their data in the
  current cluster, remapping descriptor IDs as needed.
- **A "cluster"** — re-creates the system tenant's SQL state into
  an empty destination cluster (the destination must have no user
  objects). See [backup.md § Backup scope](backup.md#backup-scope)
  for what "cluster" actually means.
- **A tenant** — `RESTORE TENANT …` re-hydrates a secondary tenant's
  entire keyspace from a tenant backup, opaquely. Unlike other
  restores, this carries the tenant's `system.jobs` table (see
  §"Tenant restore specifics").
- **System users** — `RESTORE SYSTEM USERS FROM …` re-creates only
  the role records and their memberships from a backup, leaving
  everything else untouched. Used to recover identity state without
  disturbing user data.

The restore unit must be compatible with what the backup was *taken
at*: a cluster backup can be used to restore individual tables, but
restoring a tenant requires a tenant backup.

### The three modes

The three modes differ in how SST data is placed into the destination
cluster's LSM and when user traffic can read it.

| Mode | KV write path | User traffic enabled |
|---|---|---|
| Legacy | Above-raft (`AddSSTable`) | After ingest completes |
| Fast (`WITH EXPERIMENTAL DEFERRED COPY`) | Below-raft (`LinkExternalSSTable`) | After download completes |
| Online (`WITH EXPERIMENTAL ONLINE`) | Below-raft (`LinkExternalSSTable`) | Immediately after link |

Above-raft and below-raft are defined in the
[README glossary](README.md#glossary). The three modes share the
same split-and-scatter and post-restore phases; they differ only in
how data lands in the LSM and when descriptors flip from `OFFLINE`
to `PUBLIC`.

Fast restore is faster than legacy because below-raft linking has
dramatically better hardware utilization than streaming SST bytes
through raft. Online restore goes a step further and enables user
reads immediately after link: when a read intersects a linked file,
Pebble fetches the needed SST blocks from external storage to
satisfy it, so reads pay external-storage latency until the file is
materialized locally by the background download (see §"Background
download (fast/online)").

### Mode compatibility checks

Planning rejects fast/online restore for several configurations:

- **Encryption** — encrypted backups cannot be restored
  online/fast. `LinkExternalSSTable` has no decryption hook, so
  encrypted external SSTs cannot be served on the fly. Use legacy
  restore instead.
- **Range keys** — backups containing MVCC range keys are not
  supported for online restore.
- **Too many layers** — chains longer than
  `onlineRestoreLayerLimit` are rejected at planning time. The
  destination LSM can only absorb so many registered file layers
  before read amplification gets unworkable; compaction
  ([backup.md § Compaction](backup.md#compaction)) is the mechanism
  that keeps long-running schedules under this cap.
- **`verify_backup_table_data`** — incompatible with online restore.
- **`userfile` storage** — `userfile://` URIs are resolved through a
  SQL session, so replicas can't open them during node startup. The
  link path needs every replica to be able to re-open the external
  file long after the restore job finishes (e.g. on a future node
  restart), so `userfile` URIs are rejected.

The full set of checks lives in `restore_planning.go` and
`checkManifestsForOnlineCompat` in `restore_online.go`.

### Why fast/online still need above-raft for revision-history layers

Linked SSTs are interpreted by Pebble as plain LSM data: at most one
version per key per file, optionally with all timestamps rewritten
on read to a single uniform restore-time value (the "synthetic
suffix"; fully described in §"Ingest — below-raft (fast/online)").
Revision-history layers
([backup.md § Revision-history layers](backup.md#revision-history-layers))
contain dense multi-version MVCC blocks that cannot be represented as
a plain LSM file with a synthetic suffix.

Both fast and online restore therefore fall back to the legacy
above-raft path *for those specific layers only*: revision-history
SSTs are read, rekeyed, and ingested via `AddSSTable`, while
non-history layers in the same restore use the link path. See
§"Above-raft fallback for revision-history layers" for how the two
paths are coordinated within a single restore.

### Partial-graph restore: what has to be reconciled

A RESTORE rarely restores a self-contained graph. Targets typically
have references to objects outside the restore set, and planning must
reconcile each kind of edge before the job can run:

- **Descriptor ID remapping** — backup IDs are reallocated in the
  destination. All in-payload references (FK columns, type
  references, view dependencies, sequence ownership, function
  references) must be rewritten to the new IDs.
- **Torn dependencies** — FKs pointing to a table outside the
  restore set, views referencing missing tables, sequences
  referenced by columns whose owning table isn't included. The
  `skip_missing_foreign_keys`, `skip_missing_sequences`,
  `skip_missing_views`, etc. options govern whether such edges are
  silently dropped, the dependent object is excluded, or planning
  errors out.
- **Type and UDF rewrites** — enum types and user-defined functions
  are rewritten to new IDs and validated against the destination's
  catalog.
- **Multi-region** — restoring into a cluster with a different
  region topology requires translating REGIONAL BY ROW partitions
  and database-level multi-region configuration to the destination's
  available regions.

`restore_planning.go` performs these reconciliations before any data
is touched; the resulting rewrites are persisted in `RestoreDetails`
and consumed by the ingest processors.

### What is and isn't carried over

This section applies to table / database / cluster restores. Tenant
restores have additional carry-over of jobs and other tenant-internal
state — see §"Tenant restore specifics".

- **Descriptors** — restored, with IDs rewritten. Descriptors are
  restored *including* in-flight schema-change state: a table that
  was mid-add-column at backup time has its mutation queue restored
  verbatim. The schema-change job that was driving that mutation is
  *not* restored, so the destination cluster surfaces and reconciles
  these in-flight mutations post-restore (typically by adopting the
  mutation as a new schema-change job).
- **Data** — restored, except for tables that opted out of backup
  via `exclude_data_from_backup`
  ([backup.md § Exclude data from backup](backup.md#exclude-data-from-backup)):
  their descriptors are restored but the tables come back empty.
- **System tables** — restored selectively per `system_schema.go`.
  Each system table is classified for how (or whether) it
  participates in a cluster or tenant restore. `system.jobs` is
  excluded for cluster restore: the destination cluster always
  starts with no running jobs from the source.
- **Stats, zone configs, comments, role memberships** — restored
  from manifest side data into the destination's corresponding
  system tables.
- **In-progress IMPORTs** — if the backup captured a table mid-IMPORT
  (the import had written some KVs before the backup ran), restore
  spawns a child `IMPORT ROLLBACK` job after ingest to unwind the
  partial-import data in the destination. The import job itself
  isn't restored, but the partial KV state it left in the table
  needs cleaning up — this is the mechanism.

### Tenant restore specifics

Tenant restore differs from table / database / cluster restore in
that it re-hydrates a full SQL universe: the destination tenant
should be indistinguishable from the source tenant at the backup's
`EndTime`. This requires carrying over `system.jobs` and other
tenant-internal state that other restores skip.

#### Detecting restoration in a job's `Resume()`

Every job's `Payload` stores a `CreationClusterID` populated by
`jobs.Registry` at creation. When a tenant is restored into a new
cluster, the restored job records carry the *source* cluster's ID.
Each job type compares `Payload().CreationClusterID` against the
current `LogicalClusterID()` in its `Resume()`; on mismatch, the job
terminates or refuses to act on stale external state. Examples:
changefeed jobs (`ensureClusterIDMatches` in
`changefeedccl/changefeed_stmt.go`), spanconfig jobs
(`spanconfigjob/job.go`), and BACKUP itself (`backup_job.go`).
There is no central "this is a restored job" flag — every job type
that has external dependencies is responsible for performing the
check.

#### Tenant-prefix shift

The restored tenant occupies a different tenant-ID prefix in the
destination cluster's keyspace. KV data is re-prefixed by the
`KeyRewriter` during ingest. For most jobs the shift is transparent.
Their state references descriptors; descriptors are read fresh on
`Resume()`; spans are produced through the destination's codec (e.g.
`PrimaryIndexSpan(execCfg.Codec)`), automatically picking up the
new prefix. Jobs that persist raw key spans in their progress —
uncommon in the current codebase and discouraged for new code — must
rewrite manually if they want to survive a tenant restore.

## Execution

### Flow at a glance

The job execution phases run in this order, with per-mode
divergences called out within each phase:

1. **Schema creation** — descriptors written in `OFFLINE` state so
   user queries cannot see partially-restored tables.
2. **Pre-data ingest** — system tables that affect ingest (zone
   configs, etc.) are restored first.
3. **Split & scatter** — the destination keyspace is pre-split into
   ranges aligned with the backup's data layout and scattered across
   nodes for parallel ingest.
4. **Data ingest** — per-mode: above-raft (legacy) or below-raft
   (fast/online), plus the above-raft fallback for revision-history
   layers running concurrently.
5. **Background download** (fast/online only) — materializes linked
   external SSTs into local storage.
6. **Traffic gate** — online publishes immediately after link; fast
   waits for download to complete before publishing.
7. **Post-restore** — FK validation, descriptor publish
   (`OFFLINE` → `PUBLIC`), stats restoration.

### Planning

`restore_planning.go`'s `restorePlanHook` handles `RESTORE`
statements. Planning:

- Resolves and reads the backup manifests in the chain.
- Resolves descriptor targets and applies `skip_missing_*` options.
- Computes ID rewrites and any descriptor mutations needed for the
  destination (see §"Partial-graph restore").
- Builds the **restore span covering** (`restore_span_covering.go`):
  the set of `RestoreSpanEntry` units that group backup files into
  chunks of work for processors, taking into account file count and
  target size limits.
- Picks the mode from the `WITH EXPERIMENTAL ONLINE` /
  `WITH EXPERIMENTAL DEFERRED COPY` options, stored in
  `RestoreDetails` as `ExperimentalOnline` / `ExperimentalCopy`.
- Computes `DownloadSpans` for below-raft modes: the destination
  spans whose linked data will need post-link download.
- Persists `RestoreDetails` with the job record.

### Job coordinator

`restoreResumer.Resume()` in `restore_job.go` dispatches between the
legacy path and the fast/online path (`restore_online.go`) based on
`details.OnlineImpl()`. The phases below run in the order shown;
mode-specific differences are called out within each.

#### Schema creation

`createImportingDescriptors` writes restored descriptors to the
catalog in `OFFLINE` state and reserves their new IDs.
`OFFLINE` makes them invisible to user queries while data is being
loaded. Pre-data system tables (zones in particular) are restored at
this point so their effects are in place before user data lands.

#### Split & scatter

Before any data ingest, the destination keyspace is pre-split into
ranges matching the backup's data layout and the ranges are
scattered across the cluster to spread the ingest load.

The implementation is the `GenerativeSplitAndScatterProcessor` — a
DistSQL stage running on all participating nodes that issues
`AdminSplit` and `AdminScatter` RPCs for chunks of the destination
span covering, and emits `RestoreSpanEntry` rows downstream to the
data processors. Splitting is "generative" in that span entries are
computed on the fly from the backup manifest rather than
materialized up-front.

`RestoreSpanEntry` rows are routed from this stage to the data
processors using range-based routing
(`restore_processor_planning.go`): each entry goes to the SQL
instance hosting the leaseholder of its destination span, so ingest
work runs local to the data it produces.

**Mid-row split safety**. Splits must not fall in the middle of a
SQL row (a row spans multiple KVs across column families and
secondary index entries; a split inside a row would break atomicity
of MVCC operations on it). The split processor calls
`keys.EnsureSafeSplitKey` on every candidate split key, which
advances the key forward to the next safe row boundary if necessary.

#### Ingest — above-raft (legacy)

`restore_processor_planning.go`'s `distRestore` plans
`RestoreDataProcessor` instances across nodes, taking
`RestoreSpanEntry` rows as input from the upstream split-scatter
stage. Each processor:

- Opens the SST files in the entry via `storage.ExternalSSTReader`,
  which transparently handles decryption via the job's encryption
  context.
- Iterates KVs, rewriting keys through `KeyRewriter` (descriptor
  IDs, and tenant prefix when applicable).
- Buffers rewritten KVs into a destination-side SST and ingests via
  `AddSSTable`.

#### Per-key elision during ingest

The `KeyRewriter` skips certain keys during ingest by returning
`(_, false, _)` from `RewriteKey`; the processor loop drops these.
Skipped keys are those whose source-cluster values would be actively
misleading in the destination:

- `keys.SqllivenessID` — SQL instance liveness heartbeats
  (per-instance, ephemeral).
- `keys.LeaseTableID` — distributed leases (would point at
  source-cluster instance IDs).
- `keys.SQLInstancesTableID` — SQL instance registry.
- In-progress IMPORT keys.

**Asymmetry worth knowing**. This filtering runs only on the
above-raft ingest path. The below-raft link path (next section) does
*not* filter per KV: the link RPC takes whole files as units and has
no equivalent of "skip this key." Stale ephemeral keys in linked
SSTs are therefore accepted into the LSM. They are shadowed by
fresh writes once the destination cluster's own liveness, lease, and
sqlinstances subsystems begin operating; both paths end up correct,
but by different mechanisms. Worth recognizing when debugging
unexpected post-restore state.

#### Ingest — below-raft (fast/online)

For fast and online modes, the same `RestoreDataProcessor` runs but
takes the `useLink` branch. Per restore span entry, it builds a
`kvpb.LinkExternalSSTableRequest` per file and dispatches via the
KV client. The request carries:

- The file's external-storage `Locator` + `Path` and size hints
  (`BackingFileSize`, `ApproximatePhysicalSize`).
- A **`SyntheticPrefix`** — bytes Pebble prepends to every key read
  from this file. Suppose the backup file held rows of table 52 in
  tenant 5 (the file stored its keys with `/Tenant/5/Table/52`
  elided; see
  [backup.md § Sink and on-disk layout](backup.md#sink-and-on-disk-layout)).
  Restoring it as table 73 in tenant 8, the synthetic prefix is
  `/Tenant/8/Table/73`; Pebble prepends it on read so the same
  on-disk bytes serve correctly in the destination's keyspace
  without anyone having to rewrite the file. This is the consumer of
  the backup-side prefix elision.
- **`UseSyntheticSuffix`** — makes linked KVs appear to have been
  written at restore time rather than at their original backup
  timestamps. When set, Pebble rewrites every key's MVCC timestamp
  suffix on read; the suffix bytes are computed server-side
  (`replica_proposal.go`) from the request's
  `RemoteRewriteTimestamp`.
- **`MVCCStats`** — range-level statistics for the linked file's
  contribution, pre-computed during planning.

**Relationship to Excise**. The destination ranges have just been
split out by the split-and-scatter phase and are typically empty,
so no separate `Excise` is needed before link. The link RPC also
supports an `MVCCHistoryMutation` side-effect for callers that need
it; RESTORE itself doesn't use that path. The evaluator lives at
`pkg/kv/kvserver/batcheval/cmd_link_external_sstable.go`.

**Encryption at link time**. `LinkExternalSSTableRequest` carries
no encryption field, and Pebble's `ExternalFile` representation has
no decryption hook. Encrypted external SSTs therefore cannot be read
in place by Pebble: queries against encrypted linked spans depend on
the background download (next section), which fetches files through
`storage.ExternalSSTReader` with the job's encryption context and
materializes them locally.

#### Background download (fast/online)

After link, the restore job spawns a separate download job
(`maybeWriteDownloadJob` in `restore_online.go`) that drives the
materialization of linked external SSTs into local storage. It
dispatches `serverpb.DownloadSpanRequest`s via the
`TenantStatusServer.DownloadSpan` API. Each request carries a batch
of spans to materialize and a `ViaBackingFileDownload` flag that
controls whether the receiving node copies the file or hard-links it
into its store directory (copy is required for backing files that
were not already local).

Progress is persisted on the download job via `TotalDownloadRequired`
and `ExternalBytesRemaining`. Failures retry with exponential
backoff (initial 100ms, capped at 5 minutes); the retry counter
resets when progress advances. Exhausted retries pause the job for
operator attention; `RESUME JOB` continues from where it left off,
since download progress persists.

The link phase is checkpointed via a span frontier: a pause mid-link
records which entries have been linked, and resume processes only
the un-linked entries. Download progress likewise persists across
pause/resume.

**Traffic gate — fast vs. online**:

- **Online**: the restore job publishes descriptors
  (`OFFLINE` → `PUBLIC`) immediately after link completes. User
  queries against restored spans are served on the fly from linked
  external files; downloads proceed in the background.
- **Fast**: the restore job waits on `waitForDownloadToComplete`
  before publishing descriptors. The destination is "off" until
  download finishes; once it does, the cluster sees fully-local
  data when descriptors flip to `PUBLIC`.

Both modes share the same link and download mechanisms; they differ
only in when the traffic gate releases.

#### Above-raft fallback for revision-history layers

When fast/online detects revision-history layers in the chain (see
§"Why fast/online still need above-raft for revision-history layers"),
those layers' files are routed through the legacy above-raft path:
same `RestoreDataProcessor`, same `KeyRewriter`, same `AddSSTable`
call. Below-raft link runs in parallel against the same destination
ranges for non-history layers in the same restore. The splits and
scatters from the earlier phase prepared range boundaries; both
ingest paths land within the same ranges.

#### Post-restore

After all ingestion completes (and, for fast mode, after the
download gate releases):

- **FK validation** runs on the restored tables in a post-data
  flow, ensuring no FK violations were introduced by the restore.
- **Descriptors publish**: `OFFLINE` → `PUBLIC`, in dependency
  order, making the restored objects visible to user queries.
- **Stats restoration**: backed-up table statistics are restored
  into `system.table_statistics`.

(Compacted chains restore the same way as uncompacted ones; see
[backup.md § Compaction](backup.md#compaction).)
