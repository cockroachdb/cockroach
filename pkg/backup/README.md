# pkg/backup

This package implements BACKUP and RESTORE: planning, job
coordination, distributed execution, on-disk format, and the three
RESTORE modes (legacy / fast / online). The architecture is covered
in depth in two reference documents:

- **[backup.md](backup.md)** — what BACKUP produces, what is and isn't
  captured, how planning and job execution work, the distributed export
  flow, on-disk layout, encryption, multi-region, and compaction.
- **[restore.md](restore.md)** — the three restore modes, what RESTORE
  does and doesn't carry over, partial-graph reconciliation, tenant
  restore specifics, and the mechanics of each ingestion path including
  the KV APIs involved.

The two documents cross-reference each other where the backup-side
format informs restore-side mechanics (notably prefix elision and
revision-history layers).

## About these docs

These docs describe the *intended* design of BACKUP and RESTORE —
not the current implementation, for which the code is always the
source of truth. They are written for humans building a mental
model of the subsystem and, more so, for AI agents asked to change
or debug it.

Two workflows shape what goes here:

- **Changing the subsystem.** Propose the change as a diff to these
  docs first, iterate with a reviewer on the intended behavior, then
  implement against the agreed design. The doc is the contract; the
  code follows.
- **Debugging.** Read these docs for how the subsystem was *supposed*
  to work, then compare to what the code does. A divergence is a
  finding — bug, stale doc, or intentional change that wasn't
  reflected back.

Contributions should describe principles, contracts, and the *why*
behind design choices. Enumerative detail about specific functions
and files belongs in the code: it rots if duplicated here.

## What BACKUP and RESTORE are for

The goal is to recover *user-created state* on a different cluster
from the one that created it, including when the original cluster
is gone.

"User-created state" means what a user put into the system: tables
and the rows they hold, schemas, types, roles, the cluster
configuration the user expressed. It is *not* cluster-internal
operational state — timeseries about CPU usage, the range log,
in-flight jobs, the source cluster's UUID — which is only
meaningful in the cluster that produced it.

Because the source cluster may no longer exist at restore time, two
properties are load-bearing:

- **Backups are self-describing.** Keys, schema metadata, encryption
  material, and version information all live in the backup files
  themselves. A restoring cluster talks only to object storage,
  never to the source.
- **Restore tolerates version drift.** A backup can be restored
  into a cluster of a different version, region topology, tenant
  model, or node count, within the same version-compatibility
  window as upgrades (see
  [restore.md § Mixed-version contract](restore.md#mixed-version-contract)).

## Package map

Files in this directory mostly follow a few naming conventions:

- `x_planning.go` — translates a user's SQL statement into a
  machine-readable, persisted, resumable job: target resolution,
  destination resolution, encryption setup, etc.
- `x_job.go` — entry point and coordination of that job's execution.
- `x_processor.go` — DistSQL processor definitions used during job
  execution.
- `x_processor_planning.go` — how processors are placed and work is
  distributed across the cluster for a given job.

A few specific files worth knowing about: `system_schema.go`
(per-system-table backup/restore policy), `show.go` (`SHOW BACKUP`),
`key_rewriter.go` (descriptor-ID and tenant-prefix rewriting plus
per-key skip filtering during restore ingest),
`generative_split_and_scatter_processor.go` (the split-and-scatter
stage that runs before any restore ingest).

Shared infrastructure lives in subpackages: `backupsink/`,
`backupencryption/`, `backupinfo/`, `backupdest/`, `backupresolver/`,
`backupbase/`, `backuputils/`.

## Glossary

- **Layer** — a single BACKUP run within an incremental chain
  (one full plus zero or more incrementals).
- **Chain** — a full backup and all incrementals stacked on top of it.
- **Manifest** — the metadata file (`BACKUP_MANIFEST`) describing a
  layer's contents: spans, descriptors, SST file index, etc.
- **Span covering** — restore-side algorithm that groups backup SST
  files into per-destination-range "restore span entries," each a
  unit of work for a restore processor.
- **External storage** — cloud/blob storage holding backup files
  (S3, GCS, Azure, nodelocal, etc.).
- **Above-raft** — writing data through the normal SQL → KV → raft
  path, so the SST bytes traverse the raft log (e.g. `AddSSTable`).
- **Below-raft** — registering a file in each replica's local
  storage engine through a replicated apply-time hook
  (e.g. `LinkExternalSSTable`). The backup SST's URL is replicated,
  not the SST's (restore-processed) contents.
- **Link** — register an external SST in Pebble's LSM without
  copying its contents; see "Below-raft" above.
- **Excise** — bulk-delete a range of keys from Pebble's LSM in a
  single metadata operation; faster than range tombstones for large
  clears.
- **PTS** — protected timestamp; prevents MVCC GC from collecting
  versions a backup still needs to read.
- **Locality-aware backup** — partitioned destination URIs; each
  leaseholder writes its SSTs to the URI matching its locality.
- **Exec locality** — `EXECUTION LOCALITY = '…'`; restricts which SQL
  instances participate as backup/restore processors.

## Debugging starting points

- **Job state**: `SHOW JOB <id>`, `SELECT * FROM crdb_internal.jobs
  WHERE job_type IN ('BACKUP','RESTORE','RESTORE-ONLINE')`,
  `crdb_internal.system_jobs` for the raw payload/progress.
- **Manifest and checkpoint files** in the destination URI:
  `BACKUP_MANIFEST`, `BACKUP_MANIFEST_CHECKPOINT`,
  `BACKUP-CHECKPOINT-*`, `encryption-info`.
- **Traces**: backup/restore jobs record traces accessible through the
  jobs UI or `crdb_internal.cluster_inflight_traces`.
- **Logs**: filter by job ID; the `backup`, `restore`, and
  `restore-online` log channels carry most of the interesting events.
- **Cluster settings**: `bulkio.backup.*`, `bulkio.restore.*`,
  `kv.bulk_io_write.*` govern most knobs that affect throughput and
  retry behavior.

