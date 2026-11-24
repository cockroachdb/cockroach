// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package backfill provides the core primitives for column and index
// backfill operations during online schema changes.
//
// # Overview
//
// When CockroachDB performs schema changes that add columns with default
// values or add new indexes, it must backfill existing data into the new
// schema elements while the table remains online for reads and writes. This
// package implements the low-level backfill logic used by both the legacy
// and declarative schema changers.
//
// The backfill process is always distributed: a DistSQL flow is planned with
// backfiller processors distributed across the cluster, each responsible for
// a subset of the table's key spans. The two main entry points are:
//
//   - ColumnBackfiller: Scans the primary index and writes default or
//     computed values for newly added columns, or NULLs for dropped columns.
//
//   - IndexBackfiller: Scans the source (usually primary) index and
//     generates index entries for newly added secondary or unique indexes.
//
// Both types offer InitForLocalUse (single-node, within a transaction) and
// InitForDistributedUse (multi-node, DistSQL processor) initialization
// paths.
//
// # Index Backfilling: MVCC-Compatible Backfill with Temporary Indexes
//
// Modern index creation in CockroachDB uses an MVCC-compatible backfill
// strategy built around temporary indexes. This approach is used by both
// the declarative schema changer and the legacy schema changer (for
// index creation). A much older non-MVCC code path exists in the legacy schema
// changer for in-flight schema changes that predate the temporary-index
// approach; that path writes directly to the final index using backdated
// timestamps and is not described further here.
//
// In the MVCC-compatible approach, the schema changer creates a
// temporary index that mirrors the final index's schema. Index creation
// then proceeds in two phases:
//
//  1. Backfill phase (BackfillIndex operation): The IndexBackfiller
//     scans the source index at a fixed read timestamp and writes
//     entries into the destination indexes. The new index is in a
//     BACKFILLING state where it does not receive concurrent writes.
//     Instead, a companion temporary index (in WRITE_ONLY state)
//     captures all concurrent DML (inserts, updates, deletes) that
//     occur during the backfill. Two ingestion paths exist for how the
//     scanned data reaches KV (see "Backfill Sinks" below):
//
//     - BulkAdder sink (default): Entries are ingested into KV
//     directly via AddSSTable.
//
//     - SST sink + distributed merge: Entries are written as sorted
//     SST files to nodelocal:// storage. After scanning completes,
//     a distributed merge flow (pkg/sql/bulkmerge) reads these
//     SSTs, merges them across nodes, and ingests the result into
//     KV. This avoids the write amplification that occurs when
//     BulkAdder ingests out-of-order SSTs whose key ranges overlap
//     in the destination index.
//
//  2. Merge phase (MergeIndex operation): After backfilling completes,
//     the new index transitions to MERGING state. The temporary
//     index's entries are merged into the final index using
//     IndexBackfillMerger. This step always runs regardless of which
//     ingestion path was used in phase 1, because concurrent DML
//     captured in the temporary index must be applied. The temporary
//     index is delete-preserving, meaning deletes are recorded as
//     tombstones rather than removed, so the merge can correctly
//     apply both inserts and deletes. Once the merge is complete, the
//     index advances to WRITE_ONLY for validation.
//
// IndexBackfillMerger runs as a DistSQL flow of processors that scan
// the temporary index in batches of keys, read the latest value for
// each key, and apply it to the final index using KV Put/Del
// operations. Each batch runs in its own transaction. Contention is
// handled by automatically reducing batch sizes when KV auto-retry
// limits are exceeded.
//
// The choice between BulkAdder and SST sink is controlled by the
// cluster setting bulkio.index_backfill.distributed_merge.mode and
// requires cluster version 26.2 or later. When the setting is
// "enabled", the system uses the SST sink only when the backfill
// data is unsorted (determined by IsBackfillDataSorted, which checks
// whether the destination index shares leading key columns with the
// source). The "force" mode (used by the "declarative" setting)
// always uses the SST sink regardless of sort order.
//
// # Backfill Sinks
//
// The IndexBackfiller supports two output sinks, selected at plan time:
//
//   - BulkAdder sink (default): Index entries are written to a BulkAdder
//     which ingests them into KV via SST ingestion (AddSSTable). This is
//     the traditional path used by both the legacy and declarative schema
//     changers.
//
//   - SST sink (distributed merge): When the distributed merge pipeline
//     is enabled, the IndexBackfiller writes SST files to nodelocal://
//     storage on the node running the processor. Each processor emits
//     SST metadata (URI, key range, row samples) as BulkMapProgress
//     messages. These manifests are collected by the job and used as
//     input to the distributed merge flow in pkg/sql/bulkmerge.
//
// The sink selection is configured via EnableDistributedMergeIndexBackfillSink,
// which sets UseDistributedMergeSink and DistributedMergeFilePrefix on the
// BackfillerSpec.
//
// # SST Manifest Tracking
//
// SSTManifestBuffer accumulates metadata about SST files produced during
// the map phase. Each manifest records the file URI, key span, file size,
// row sample (for split planning), and key count. The buffer supports
// append-and-snapshot semantics for checkpointing: the job periodically
// snapshots the buffer and persists it, allowing the backfill to resume
// from the last checkpoint after a pause or crash.
//
// # Vector Index Support
//
// The package includes specialized helpers for vector index backfilling
// and merging. VectorIndexHelper re-encodes vectors during the backfill
// phase, and VectorIndexMergeHelper handles the merge phase for vector
// indexes, which requires interaction with the vector index manager for
// fixup operations. Vector index merging uses much smaller batch sizes
// (default 3 vs 1000 for regular indexes) because fixup operations create
// significant contention with larger batches.
package backfill
