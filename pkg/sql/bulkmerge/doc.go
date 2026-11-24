// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package bulkmerge implements the distributed merge pipeline for index
// backfill and IMPORT operations. It provides a MapReduce-inspired
// DistSQL flow that distributes the ingestion of backfill SST files
// across all SQL instances in the cluster, replacing the default
// BulkAdder ingestion path with a more efficient multi-node merge.
//
// # Motivation
//
// The traditional BulkAdder ingestion path works well when the backfill
// data is sorted relative to the destination index, because SSTs from
// different PK ranges are non-overlapping and can be ingested directly.
// However, when the destination index key order differs from the source
// (e.g., creating an index on a column that is not a prefix of the primary
// key), the backfill produces SSTs whose key ranges overlap. Ingesting
// overlapping SSTs causes high write amplification in Pebble due to
// repeated LSM compactions (10-20x in practice).
//
// The distributed merge pipeline solves this by:
//  1. Having the map phase write SSTs to external storage instead of
//     directly to KV.
//  2. Running a distributed merge flow that reads, sorts, and merges
//     these SSTs into non-overlapping, range-aligned output.
//  3. Ingesting the merged result into KV in a single pass with minimal
//     write amplification.
//
// # Three-Phase Pipeline
//
// The pipeline operates in three logical phases: map, local merge, and
// final merge. The map phase is handled by the index backfiller (in
// pkg/sql/rowexec); this package implements the merge phases.
//
// Map Phase (external to this package):
//
//	The index backfiller scans the source index at a fixed read timestamp
//	and writes sorted SST files to nodelocal:// storage on each node. Each
//	SST's metadata (URI, key span, row samples, key count) is emitted as
//	a BulkMapProgress message and collected by the job as SST manifests.
//
// Local Merge (iteration 1):
//
//	Each node merges only its own local SSTs across the full key range,
//	producing consolidated output SSTs in nodelocal:// storage. This
//	reduces the number of files and avoids cross-node data transfer
//	during this phase. The output is N sets of SSTs (one per node).
//
// Final Merge (iteration 2):
//
//	All nodes' intermediate SSTs are merged together. The key space is
//	split into tasks based on row samples from the map phase for balanced
//	work distribution. Each task ingests its merged data directly into KV
//	using an SSTBatcher, bypassing external storage entirely.
//
// The number of merge iterations is always exactly 2. This is hardcoded
// in the index backfiller (see runDistributedMerge in
// pkg/sql/index_backfiller.go). Changing the iteration count would require
// updating the phased progress calculation.
//
// # DistSQL Flow Topology
//
// Each merge iteration runs as a DistSQL flow with three processor types
// arranged in a pipeline:
//
//	MergeLoopback (gateway)
//	    │
//	    │ range-routed by SQL instance ID
//	    ▼
//	BulkMerge processors (one per SQL instance)
//	    │
//	    │ pass-through
//	    ▼
//	MergeCoordinator (gateway)
//
// MergeLoopback:
//
//	Runs on the gateway node. It receives task assignments from the
//	MergeCoordinator via an in-process loopback channel and emits them
//	as rows to the BulkMerge processors. Each row contains a routing key
//	(SQL instance ID) and a task ID. The range router directs each row
//	to the appropriate BulkMerge processor.
//
// BulkMerge (bulkMergeProcessor):
//
//	Runs on every SQL instance. Receives task assignments, merges the
//	assigned key spans from the input SSTs, and emits results. Behavior
//	differs by iteration:
//
//	  - Local iteration: Opens only SSTs local to this node (filtering
//	    by instance ID in the URI). Merges them across the full key range
//	    and writes output to nodelocal:// external storage as new SSTs.
//
//	  - Final iteration: Opens all input SSTs (local and remote). Merges
//	    the assigned task span and ingests directly into KV via SSTBatcher.
//	    Remote SSTs are streamed over gRPC using the blob service; memory
//	    for in-flight RPC buffers is pre-reserved.
//
// MergeCoordinator:
//
//	Runs on the gateway node. Manages task assignment and completion:
//	  - On start, publishes initial tasks (one per worker) via the
//	    loopback channel.
//	  - As workers complete tasks, assigns the next available task to
//	    the same worker (work-stealing pattern).
//	  - Accumulates output SST metadata and emits the final result as
//	    a single protobuf row when all tasks are done.
//	  - Pushes progress metadata (completed task IDs) for progress
//	    tracking by the job.
//
// # Task Assignment
//
// Tasks are managed by a lightweight taskset scheduler
// (pkg/util/taskset). The MergeCoordinator claims tasks sequentially
// and assigns them to workers in a work-stealing fashion: when a worker
// finishes, it gets the next unclaimed task.
//
//   - Local merge: One task per node, each covering the full key span.
//     Every node merges its own local SSTs into consolidated output.
//
//   - Final merge: Tasks are split across schema spans using row samples
//     from the map phase (via bulksst.CombineFileInfo). This provides
//     balanced work distribution across nodes.
//
// # Uniqueness Enforcement
//
// When building unique indexes, the pipeline must detect duplicate keys.
// This is handled at two levels:
//
// Cross-SST duplicates (within the merge):
//
//	When EnforceUniqueness is true and multiple SSTs are present, the
//	merge processor uses suffixed iterators instead of a standard merged
//	iterator. Each SST's iterator appends a unique suffix (FNV-64 hash
//	of the SST path) to every key. This prevents Pebble's merge iterator
//	from shadowing duplicates. The merge processor then compares
//	consecutive base keys (with suffixes stripped): if two keys match but
//	have different values, a DuplicateKeyError is raised. Identical
//	duplicates (same key and value) are benign — they arise from
//	checkpoint-and-resume overlap and are silently skipped.
//
// Pre-existing KV conflicts (during ingest):
//
//	In the final iteration, the SSTBatcher is configured with
//	disallowShadowingBelow set to the write timestamp. This catches
//	conflicts with data already in KV (e.g., from concurrent writes)
//	and raises a KeyCollisionError.
//
// # SST Storage Layout
//
// All intermediate files for a job live under a single directory for
// easy cleanup:
//
//	nodelocal://{nodeID}/job/{jobID}/
//	├── map/                                       # Map phase output
//	│   ├── n{instanceID}-{walltime}-{logical}.sst # One SST per flush; the
//	│   │                                          # n{instanceID} prefix
//	│   │                                          # disambiguates writers
//	│   │                                          # when nodelocal:// URIs
//	│   │                                          # share a physical dir.
//	│   └── ...
//	├── merge/iter-1/                              # Local merge output
//	│   ├── n{instanceID}-{walltime}-{logical}.sst
//	│   └── ...
//	└── merge/iter-2/                              # (not used: final writes to KV)
//
// The /job/{jobID}/ prefix enables prefix-based cleanup on job
// completion, cancellation, or failure. Intermediate SSTs from the map
// phase can be deleted once the local merge consumes them, and local
// merge output can be deleted after the final merge. Cleanup is handled
// by the job (see cleanupRedoIterationSSTs and the backfiller cleanup
// logic).
//
// # Instance Availability
//
// Before starting a merge iteration, the pipeline checks that all SQL
// instances owning SST files are alive. If any are unavailable (e.g.,
// a node has been decommissioned), it retries with exponential backoff
// up to the configured timeout (bulkmerge.instance_unavailability.timeout,
// default 30 minutes). This prevents the merge from failing immediately
// due to transient instance unavailability.
//
// # Memory Management
//
// During the final iteration, each BulkMerge processor streams remote
// SSTs over gRPC. Each stream can buffer up to
// (flow_control_window * ChunkSize) bytes in transport buffers. The
// processor estimates the number of concurrently active streams using
// the bulkio.merge.rpc_inflight_fraction setting and reserves memory
// upfront from the memory monitor. If the reservation fails, the error
// message includes tuning recommendations.
//
// # Resume and Checkpoint
//
// The pipeline supports job pause/resume at multiple granularities:
//
//   - Map phase: SST manifests are checkpointed as they are produced.
//     On resume, only unprocessed source spans are re-scanned.
//
//   - Between iterations: After completing a merge iteration, the output
//     SST manifests and the current phase number are persisted. On resume,
//     the pipeline skips completed iterations and starts from the
//     checkpointed manifests.
//
//   - Within an iteration: If a merge iteration is interrupted, it
//     restarts from the beginning using the same input. Before restarting,
//     leftover output SSTs from the previous attempt are cleaned up.
//
// # Relationship to the Gateway Merge
//
// The distributed merge pipeline runs during the BackfillIndex phase
// and handles how the bulk backfill data is ingested into KV. It is
// an alternative to the BulkAdder ingestion path, not to the gateway
// merge (IndexBackfillMerger). The gateway merge always runs afterward
// during the MergeIndex phase to merge temporary index entries
// (concurrent DML captured during the backfill) into the final index,
// regardless of whether bulkmerge was used.
//
// The choice between BulkAdder and the distributed merge pipeline is
// made by the cluster setting
// bulkio.index_backfill.distributed_merge.mode:
//
//   - "disabled" (default): Use BulkAdder for ingestion.
//   - "enabled": Use distributed merge when the backfill data is
//     unsorted; fall back to BulkAdder when sorted (the sorted-data
//     optimization checks IsBackfillDataSorted).
//   - "legacy": Intended to enable only for the legacy schema
//     changer, but the legacy schema changer's distributed merge
//     execution was never implemented. The mode value is accepted
//     and stored in job details, but has no practical effect.
//   - "declarative": Only enable for the declarative schema changer;
//     forces distributed merge regardless of sort order.
//
// A version gate (V26_1) ensures the distributed merge processors
// exist on all nodes before the pipeline can be used. In practice,
// the feature is intended for use in v26.2 and later.
package bulkmerge
