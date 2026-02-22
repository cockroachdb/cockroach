## At a glance (keep ~200–300 words)
- **Component index**
  - Path: `pkg/kv/kvserver/`
  - Core entry points/types: `store.go`, `replica.go`, `replica_write.go`, `replica_read.go`, `raft.go`, `replicate_queue.go`, `split_queue.go`, `merge_queue.go`, `mvcc_gc_queue.go`, `lease_queue.go`, `raft_snapshot_queue.go`
  - Ownership: KV (range/replication subsystem). Boundaries: talks to `pkg/raft/` (consensus), `pkg/storage/` (Pebble/MVCC), upper KV APIs (`pkg/kv/`), and node liveness.
  - Owners: KV (kvserver)
  - See also: `pkg/kv/kvclient/kvcoord/AGENTS.md`, `pkg/kv/kvserver/concurrency/AGENTS.md`, `pkg/storage/AGENTS.md`, `pkg/settings/AGENTS.md`, `pkg/upgrade/AGENTS.md`
- **Responsibilities**
  - Manage per-range Raft state machines and leases. Apply replicated commands to MVCC storage. Move data via snapshots and replica changes. Maintain health via background queues (split/merge/GC/raft-log). Enforce flow control and admission.
- **Critical invariants**
  - Exactly one leaseholder per range at a time; only the leaseholder evaluates writes.
  - Raft linearizes proposals; replicas apply in log order; no write is acknowledged before durable quorum replication.
  - Snapshots install a consistent state at a specific Raft index and never regress applied state.
  - Log truncation never removes entries required by any replica to catch up (gated by followers' progress or snapshot index).
  - Splits/merges update range descriptors atomically with replicated split/merge triggers; ranges never overlap.
  - MVCC GC never deletes versions protected by protected timestamps.
  - Follower reads only at or below the closed timestamp proved by the leaseholder.
- **Request/data flow (write)**
```
Client → DistSender → leaseholder Replica → propose to Raft → replicated to quorum
   → commit index advances → apply on replicas (state machine) → ack client
```
- **Data movement (add/remove replica / rebalance)**
```
Allocator/replicate_queue decides → ChangeReplicas begins → send Raft snapshot to new
replica → apply snapshot → joint consensus (if needed) → commit membership → cleanup
```

- **Triage quickstart (hot/stuck range)**
  - Identify hot range from metrics or `range_log`; locate leaseholder.
  - Check `replicate_queue`/`raft_log_queue` activity and allocator decisions.
  - Confirm snapshots are progressing; no stuck learners; truncation not blocking catch-up.
  - Verify closed timestamps and `kvflowcontrol`/admission aren’t throttling proposals or replication.

## Deep dive
1) **Architecture & control flow**
   - Data is partitioned into keyspace-contiguous ranges. Each range is a Raft group with N replicas managed by `Replica` objects on a `Store`.
   - The leaseholder serves reads/writes; followers replicate and can serve follower reads ≤ closed timestamp.
   - Raft plumbing: `raft.go`, `replica_raft.go`, `store_raft.go`, `raft_transport.go`; application: `replica_application_state_machine.go`.
   - Background work via queues and scanners: `scanner.go` schedules `replicate_queue.go`, `split_queue.go`, `merge_queue.go`, `mvcc_gc_queue.go`, `raft_log_queue.go`, `lease_queue.go`, `raft_snapshot_queue.go`.

2) **Data model & protos**
   - RangeDescriptor (replicas, bounds), Lease (holder, epoch/expiration), RaftHardState/TruncatedState, ClosedTimestamp/Policy.
   - Protos and helpers: `kvserverpb/`, `rangelog/`, `stateloader/`, `kvstorage/`.

3) **Concurrency & resource controls**
   - Latches and spansets gate conflicting evaluations (`spanlatch/`, `spanset/`).
   - Write path quota/backpressure: `replica_proposal_quota.go`, `replica_backpressure.go`.
   - Admission/flow control: `kvadmission/`, `kvflowcontrol/`, and `flow_control_raft_transport.go` throttle proposal and replication traffic.
   - Quiescence reduces Raft traffic for idle ranges (`replica_raft_quiesce.go`).

4) **Failure modes & recovery**
   - Lease loss/transfer: `replica_range_lease.go`, `lease_queue.go`. Only a valid leaseholder serves writes; transfers are replicated and serialized.
   - Node failures: Raft continues with quorum; dead replicas are removed via allocator; liveness gating in `storeliveness/` and integration with node liveness.
   - Snapshots: sent when creating replicas, when a follower falls behind truncated log, or for relocation. Prepare/apply in `snapshot_apply_prepare.go`, `store_snapshot.go`; strategy in `kv_snapshot_strategy.go`.
   - Log truncation: `raft_log_truncator.go` and queue ensure logs are truncated only past followers' persisted/applied indices or a safe snapshot index. Never truncate past the min required by any replica.
   - Replica GC: removes destroyed/orphaned replicas (`replica_gc_queue.go`). Intent resolution and MVCC GC live in `gc/` and `mvcc_gc_queue.go`.
   - Joint config membership changes are used for safe replica set changes; learner replicas may participate before voting.

5) **Observability & triage**
   - Metrics: `metrics.go` (per-store/per-replica), Raft transport metrics (`raft_transport_metrics.go`).
   - Range log of structural events: `range_log.go` (splits/merges/replicas/leases) aids audits.
   - Debugging: inspect problematic ranges via range log, metrics, and per-range tracing (`replica_*_test.go` show patterns). Typical recipe: identify hot range → check leaseholder location → examine replicate/raft_log queues and allocator decisions → confirm snapshots progressing, not stuck.

6) **Interoperability**
   - Consensus: integrates with the Raft subsystem for replication, log application, and membership changes.
   - Storage: uses the storage engine API (Pebble-backed) exclusively for MVCC reads/writes and snapshots.
   - Closed timestamps: collaborates with the cluster closed-timestamp service to validate follower reads.
   - Protected timestamps: consumes the protection service to prevent GC of required historical data.

7) **Mixed-version / upgrades**
   - Feature gates in `pkg/clusterversion/` guard behaviors (e.g., membership change styles, snapshot formats) and are checked before enabling new paths. Replica changes prefer compatibility-safe sequences during upgrades.

8) **Configuration & feature gates**
   - Snapshot behavior and limits: see `snapshot_settings.go`.
   - Split/merge enablement and thresholds: `split/`, `merge_queue.go`.
   - Allocator and rebalancing knobs: `allocator/`, `store_rebalancer.go`, `rebalance_objective.go`.
   - Admission/flow control knobs: `kvadmission/`, `kvflowcontrol/`.

9) **Background jobs & schedules**
   - Scanners (`scanner.go`) drive queues: replicate (add/remove replicas, rebalancing), split (create subranges), merge (fold low-traffic ranges), MVCC GC, Raft log truncation, lease maintenance, and snapshot assistance.

10) **Edge cases & gotchas**
   - Snapshots: receiver installs a placeholder replica and reserves disk; application is atomic at a Raft index. If log truncation races, the sender ensures the snapshot index ≥ follower’s required catch-up point. Admission/flow control can throttle snapshots; watch for backpressure preventing progress.
   - Log truncation: never truncate beyond any follower’s persisted/applied index; coordinate with snapshots so that a follower can always recover via either remaining log or an available snapshot.
   - Split: a replicated split trigger installs Left/Right descriptors atomically. The lease moves to one side; both halves update meta records. Writes are rejected across boundaries during the split; intents on boundaries are carefully resolved (`split_trigger_helper.go`).
   - Merge: source is frozen; intents are drained; a replicated merge trigger updates descriptors atomically (`store_merge.go`, `merge_queue.go`). Merges can be blocked by active rangefeeds, protected timestamps, or pending learners.
   - Protected timestamps can block GC or merges until released; ensure backup/CDC jobs advance/release protections.
   - Closed timestamps: follower reads that exceed closed ts must route to the leaseholder; skewed closed ts can signal leaseholder lag.

11) **Examples**
   - Relocate a range: `replicate_queue.go` chooses a target using `allocator/`, adds learner → sends snapshot → promotes to voter → removes old voter.
   - Recover a stale follower: `raft_snapshot_queue.go` enqueues it; leader sends snapshot; follower applies and catches up; truncator later compacts the log.

12) **References**
   - docs: `docs/tech-notes/`
   - RFCs: `docs/RFCS/`

13) **Glossary**
   - Range: contiguous keyspan replicated via Raft.
   - Replica: a copy of a range on a store.
   - Leaseholder: replica authorized to serve writes and close timestamps.
   - Closed timestamp (CT): lower bound s.t. all timestamps ≤ CT are stable cluster-wide.
   - Protected timestamp (PTS): prevents GC of data ≤ PTS.
   - Snapshot: bulk state transfer to initialize/catch up a replica.
   - Log truncation: compaction of the Raft log up to a safe index.
   - Joint config: Raft membership change protocol with overlapping voter sets.
   - Learner: non-voting replica used for safe addition.
   - Allocator: chooses replica placements and rebalances.
   - Queue: background work scheduler per range.

14) **Search keys & synonyms**
   - kvserver, range, replica, lease, raft, snapshot, log truncation, split, merge, gc, replicate_queue, allocator, flow control, admission, closedts, protectedts, liveness
