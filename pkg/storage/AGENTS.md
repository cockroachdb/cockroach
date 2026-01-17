## At a glance

- **Responsibilities**: Engine-agnostic storage API over Pebble; MVCC encoding, iteration and range tombstones; intent interleaving; snapshots/SST ingestion; encryption-at-rest (EAR) registries; I/O categorization and disk health/metrics.
- **Primary entry points**:
  - `engine.go` — `Engine`, `Reader`, `Writer`, `MVCCIterator`/`EngineIterator`, `IterOptions`, metrics.
  - `pebble.go`/`pebble_iterator.go` — Pebble-backed engine and iterators.
  - `mvcc.go`, `mvccencoding/` — MVCC key/value types and encodings.
  - `intent_interleaving_iter.go`, `read_as_of_iterator.go`, `mvcc_incremental_iterator.go` — iterator variants.
  - `fs/` — filesystem env, disk health checks, read/write categories; `encryption_at_rest.go`.
  - `enginepb/` — protos: MVCC metadata/stats, file/key registries, store properties.
  - `sst_writer.go` — SST writer used by export/ingest/snapshots.
- **Boundaries**: Provides durable KV over Pebble to `pkg/kv/kvserver` (Raft, replication) and `pkg/kv/kvclient` (reads/writes). EAR integrates with CCL code.
- **Owners**: Storage
- **See also**: `pkg/kv/kvserver/AGENTS.md`, `pkg/kv/kvclient/kvcoord/AGENTS.md`
- **Critical invariants**:
  - MVCC point keys sort by user key and descending timestamp; intents (ts=0) encode at the user key prefix; range tombstones mask points below their timestamp.
  - MVCC iterators never enter the lock-table keyspace; intent interleaving merges lock-table (separated intents) with MVCC values consistently.
  - Iterators must set either `Prefix` or an `UpperBound`/`LowerBound` (perf and correctness). Some modes cannot span local↔global keys.
  - EAR registry reflects encrypted files; absence of a file in the registry implies plaintext.

- Iterator invariants (callers)
  - Always set bounds or `Prefix`; avoid unbounded iteration.
  - Handle `HasPointAndRange`; use `Next()` vs `NextKey()` appropriately.
  - Don’t cross local↔global with `MVCCKeyAndIntentsIterKind`.
  - Respect time-bound filters and range-key masking options.

Data flow (simplified)

```
Write (txn): KV → Writer.{PutMVCC,Merge,Clear*}
  → WAL append → memtable → flush/compaction
  ↳ intents stored in lock-table keyspace; provisional values in MVCC space

Read: Reader.NewMVCCIterator(opts)
  ↳ intentInterleavingIter merges intents+values (if requested)
  ↳ time-bound filters and range-key masking (when enabled)
```

## Deep dive
1) Architecture & control flow

- **Engine API (`engine.go`)**: `Engine` combines `Reader` + `Writer` plus engine ops (capacity, compaction, ingestion, snapshots). `Reader.NewMVCCIterator` and `NewEngineIterator` produce iterators scoped by `IterOptions` (bounds, key types, time hints, masking, read category). `Writer` covers MVCC/engine clears, puts, merges, range keys, and batch semantics.
- **Pebble integration**: `pebbleIterator` implements both MVCC and Engine iterators. Iterator stats (`IteratorStats`) expose external vs internal seeks/steps and block load metrics. `pebbleLogger_and_tracer.go` routes Pebble events to the STORAGE log and tracing.
- **Snapshots**: `Engine.NewSnapshot(spans...)` yields a consistent view; callers must keep iterator bounds within snapshot spans. SST-based export/ingest uses `sst_writer.go` and `Engine.IngestLocalFiles{,WithStats}`; `PreIngestDelay` backpressures ingestion based on L0 sublevels and pending compactions.

2) Data model & protos

- **Keys**:
  - `EngineKey` = user key + sentinel + optional version suffix (length-encoded). Suffix encodes either an MVCC timestamp (8/12/13 bytes) or a 17-byte lock-table suffix. See `engine_key.go`.
  - `MVCCKey` = `{Key, Timestamp}`. Intents have empty timestamp and live in lock-table; committed values have non-empty timestamp.
  - Range keys are MVCC range tombstones stored as Pebble range keys; they fragment and stack deterministically at different timestamps.
- **Encodings**: `mvccencoding/encode.go` encodes timestamps and composite MVCC keys to match Pebble comparisons. v3 MVCC encodings use `enginepb.MVCCValueHeader` while maintaining backward compatibility.
- **Registries (EAR)**: `enginepb/file_registry.proto` and `enginepb/key_registry.proto` persist file→encryption-settings and data/store keys. `fs/file_registry.go` maintains a records-based registry with rotation and elision of missing/plain files.

3) Concurrency & resource controls

- `Reader.ConsistentIterators()` indicates iterators from the same reader see the same engine state; `PinEngineStateForIterators` pins visibility. Indexed/unindexed batches determine whether a batch’s writes are visible to its iterators.
- `IterOptions.ReadCategory` maps to Pebble block QoS (`fs/category.go`) to separate latency-sensitive scans from background I/O.

4) Failure modes & recovery

- **Corruption detection**: `verifying_iterator.go` re-validates MVCC checksums during iteration; `EngineKey.Verify` verifies value checksums. Iterator invariants (`assert*Invariants`) detect protocol violations (e.g., MVCC iterator entering the lock table).
- **Disk slowness**: VFS health checks record slow syncs; `Engine.RegisterDiskSlowCallback` and `fs.MaxSyncDuration{,FatalOnExceeded}` provide detection and optional fataling.
- **Stalls/backpressure**: `PreIngestDelay` delays ingestion based on L0/compaction metrics.

5) Observability & triage

- `Engine.GetMetrics()` exposes Pebble metrics and aggregated iterator/batch stats, including disk write stats (`vfs.DiskWriteStatsAggregate`).
- Disk stats sampling and rolling traces: `disk/monitor.go` and `monitor_tracer.go` collect per-device stats; include in support bundles when diagnosing stalls.
- Iterator stats in traces/logs: examine `{Seek,Step}{Count}` deltas to spot MVCC version fan-out vs LSM garbage.

6) Interoperability

- Reads/writes originate in KV evaluation; replication and snapshots rely on export/ingest and snapshot primitives provided by the engine API.

7) Mixed-version / upgrades

- `Engine.MinVersion()` reflects store min-version; `fs/min_version.go` and `storageconfig/min_version.go` gate features (e.g., table formats, sized tombstones). EAR must be initialized before opening Pebble; disabling EAR requires no existing registry file.

8) Configuration & feature gates

- `storage.ingestion.value_blocks.enabled` (memory vs IO for huge SSTs).
- `pebble.pre_ingest_delay.enabled` and thresholds for L0/read-amp backpressure.
- Time filtering and masking: `IterOptions.Min/MaxTimestamp`, `RangeKeyMaskingBelow`.
- Disk health: `storage.max_sync_duration{,.fatal.enabled}`.

9) Edge cases & gotchas

- Iteration:
  - Seeking within a range key may land on a bare range key first; callers must handle `HasPointAndRange` and may need `Next()` to reach the point value.
  - `NextKey()` skips remaining versions of the current key but may not skip a colocated point key if starting from a bare range key.
  - `MVCCKeyAndIntentsIterKind` cannot span local→global keys; bounds constrain lifetime; prefix iteration disallows reverse/limits.
  - Time-bound iterators (TBI) guarantee no keys outside `[Min, Max]`. Range-key filtering is disabled in some paths due to fragmentation mismatches.
- Mutations: `SingleClearEngineKey` has strict invariants; don’t use for global keys that may be duplicated via ingestion/replay.

10) Examples

- Read as of T: wrap a `SimpleMVCCIterator` with `NewReadAsOfIterator(iter, T)` to surface only latest non-tombstone ≤ T, skipping tombstones and older masked versions.
- Incremental scan (start,end] over [A,Z): `NewMVCCIncrementalIterator` with `(StartTime, EndTime]`, optional intent policy; use `NextIgnoringTime` to continue physical iteration when you must advance past filtered versions.

11) References

- See also: `pkg/kv/kvserver/AGENTS.md`, `pkg/kv/kvclient/kvcoord/AGENTS.md`

12) Glossary

- **MVCC range key**: Range tombstone that masks point keys ≤ its timestamp across [start,end).
- **Separated intents**: Intents stored in lock-table keyspace; interleaved logically by `intentInterleavingIter`.
- **TBI**: Time-bound iterator for skipping SSTs/blocks outside `[Min, Max]`.
- **Range key masking**: Iterator option to hide point keys ≤ a threshold masked by range tombstones.
- **ReadCategory/WriteCategory**: I/O QoS tagging for reads/writes.
- **EAR**: Encryption-at-rest; file and key registries.

13) Search keys & synonyms

- Engine API, Pebble iterator stats, intentInterleavingIter, MVCCIncrementalIterator, ReadAsOfIterator, RangeKeyMaskingBelow, EncodeMVCCTimestampSuffix, EngineKey sentinel, RegisterOnDiskSlow, FileRegistry, IngestLocalFiles, PreIngestDelay.

Omitted sections: Background jobs & schedules
