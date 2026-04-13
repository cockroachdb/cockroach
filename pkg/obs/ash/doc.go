// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package ash implements Active Session History (ASH), a time-sampled
// observability framework that records what work is active in the
// system and what resources it is consuming or waiting on.
//
// # Motivation
//
// Users investigating performance issues need to answer the question:
// "what was running when performance changed?" ASH answers this by
// periodically sampling in-flight work and recording whether each
// unit of work is running on CPU or waiting on a specific subsystem.
// Aggregating these samples over time yields a picture of resource
// and wait-time accountability often described as "DB seconds."
//
// # Work State Registration
//
// Goroutines that perform work worth observing call SetWorkState to
// register their current activity. SetWorkState returns a cleanup
// function that must be deferred:
//
//	info := ash.WorkloadInfo{WorkloadID: fingerprintID, AppNameID: appNameID}
//	cleanup := ash.SetWorkState(tenantID, info, ash.WorkLock, "LockWait")
//	defer cleanup()
//
// Each call pushes a new WorkState onto a per-goroutine stack (stored
// in a global sync.Map keyed by goroutine ID). This stack allows
// nested instrumentation: a goroutine processing a SQL query can push
// a "ReplicaSend" state, and within that push a "LockWait" state. The
// cleanup function pops the current state and restores the previous
// one.
//
// WorkState objects are pooled via sync.Pool. When a state is popped
// it is placed on a retired list rather than immediately returned to
// the pool, because the sampler may be concurrently iterating over
// active states. The sampler drains the retired list after each
// iteration.
//
// Each WorkState records a WorkEvent (a specific label like
// "LockWait", "ReplicaSend", "DistSenderRemote") and a WorkEventType
// that categorizes the resource involved:
//
//   - CPU: active computation (e.g., optimizer planning, replica
//     evaluation).
//   - IO: storage I/O (e.g., batch evaluation in the storage layer).
//   - LOCK: lock or latch contention.
//   - NETWORK: waiting on remote RPCs.
//   - ADMISSION: queued in admission control.
//   - OTHER: other wait points (e.g., commit wait, backpressure).
//
// When ASH is disabled (obs.ash.enabled = false), SetWorkState is a
// no-op that returns a pre-allocated no-op function, so there is
// zero overhead on the hot path.
//
// # Instrumentation Guidance
//
// ASH should instrument goroutines that are tightly bound to a unit
// of work — actively computing, actively waiting on a specific
// resource, or actively performing I/O. It should NOT instrument
// orchestration goroutines that are idle while work happens elsewhere
// in the call graph.
//
// Good candidates for SetWorkState are goroutines that spend the
// overwhelming majority of their time in the instrumented region
// doing the thing the label describes:
//
//   - lock_table_waiter.go "LockWait": the goroutine is blocked
//     waiting to acquire a lock. It enters this state when it
//     discovers a conflicting lock and exits when the lock is
//     released or the wait is interrupted. The goroutine is doing
//     nothing else during this time.
//   - plan_opt.go "Optimize": the goroutine is actively running the
//     cost-based optimizer. This is CPU-bound work that dominates the
//     goroutine's time for the duration of the call.
//   - transport.go "DistSenderRemote": the goroutine is blocked on a
//     remote RPC. It enters the state immediately before the RPC and
//     exits immediately after the reply arrives.
//
// A poor candidate is connExecutor.execStmt
// (conn_executor_exec.go), even though it is the entry point for all
// SQL query execution. The connExecutor goroutine spends most of its
// lifetime idle, blocked in stmtBuf.CurCmd waiting for the next
// client command (conn_io.go). During the brief window when execStmt
// IS running, it acts as an orchestrator: dispatching to the
// optimizer, the execution engine, and KV — all of which already
// have their own fine-grained SetWorkState calls. Instrumenting at
// the execStmt level would either capture idle time (inflating
// "active" counts) or add a coarse "executing a statement" label
// that provides no diagnostic value beyond the existing
// instrumentation.
//
// As a rule of thumb: if the goroutine you are considering
// instrumenting spends significant time blocked on channels,
// condition variables, or select statements waiting for OTHER
// goroutines to do the real work, it is an orchestration goroutine
// and should not be instrumented. Instrument the goroutines doing
// the work instead.
//
// # Sampler
//
// The Sampler is a process-wide singleton that runs a background
// goroutine on a configurable tick interval (obs.ash.sample_interval,
// default 1s). On each tick it:
//
//  1. Calls rangeWorkStates to snapshot every registered goroutine's
//     current WorkState. rangeWorkStates also reclaims retired
//     WorkState objects back to the pool after iteration completes.
//  2. Converts each snapshot into an ASHSample and appends it to a
//     ring buffer.
//
// Because ASH only cares about currently active work, there is no need
// to copy data on the hot path (in contrast to tracing, where span
// data must be preserved when a span finishes). All data copying
// happens in the sampler goroutine, off the hot path.
//
// The sampler is initialized once per process via InitGlobalSampler,
// called during server startup after the node ID is known. In a
// shared-process multi-tenant environment, multiple SQL servers share
// the single sampler instance.
//
// To correlate samples with applications, the SQL layer hashes each
// session's application name to a uint64 ID (via GetOrStoreAppNameID)
// and stores the ID-to-string mapping in a process-wide cache. The ID
// is propagated through BatchRequest headers and DistSQL
// SetupFlowRequests, avoiding string copies on the hot path. During
// sampling, the sampler resolves app name IDs in two phases:
//
//  1. Local resolution: look up the ID in the process-wide cache.
//     This succeeds for work originating from the local node and for
//     DistSQL flows, which eagerly store their mapping on the remote
//     node during flow setup.
//  2. Remote resolution: for cache misses where the work state has a
//     non-zero GatewayNodeID different from the local node, the
//     sampler fetches mappings from the gateway via the
//     AppNameMappings RPC. RPCs are batched and deduplicated by
//     gateway node, with a 250ms timeout to avoid stalling the
//     sampling loop.
//
// # Storage and Access
//
// ASH samples are stored in a RingBuffer, a thread-safe circular
// buffer with a configurable capacity (obs.ash.buffer_size, default
// 1M samples, ~200MB at ~200 bytes per sample). When the buffer is
// full the oldest samples are overwritten. The buffer supports dynamic
// resizing via cluster setting changes.
//
// Samples are exposed through two crdb_internal virtual tables:
//
//   - crdb_internal.node_active_session_history: samples from the
//     local node's in-memory buffer.
//   - crdb_internal.cluster_active_session_history: samples from all
//     nodes, collected via RPC fan-out through the
//     ListActiveSessionHistory / ListLocalActiveSessionHistory status
//     server RPCs.
//
// # Workload Attribution
//
// Every ASH sample includes a WorkloadID that identifies the
// workload responsible for the sampled work: a statement fingerprint
// ID for SQL queries, a job ID for jobs, or a system task
// name for internal work. ASH instrumentation points throughout the
// KV stack read the WorkloadID from the BatchRequest header and pass
// it to SetWorkState. The challenge is getting the WorkloadID onto
// BatchRequest headers in the first place. Three propagation
// mechanisms are used, each suited to a different class of work.
//
// SQL statements use the kv.Txn as the carrier. At the start of
// each statement, the connExecutor computes the statement fingerprint
// ID and calls Txn.SetWorkloadInfo on the planner's transaction
// (conn_executor_exec.go). Txn.Send then auto-stamps the WorkloadID
// onto every BatchRequest header. The transaction is the right
// carrier because in an explicit transaction (BEGIN; stmt1; stmt2;
// COMMIT), the connExecutor's context spans multiple statements with
// different fingerprint IDs, whereas Txn.SetWorkloadInfo can be
// updated per-statement and immediately applies to all subsequent
// batches.
//
// Jobs use the Go context as the carrier. The jobs registry calls
// kv.ContextWithWorkloadInfo(ctx, jobID, WorkloadTypeJob) before
// invoking the job's Resume method (jobs/adopt.go). This context
// value is then picked up at two points:
//
//   - Transactional path: DB.NewTxn(ctx) extracts the workload info
//     from the context and calls SetWorkloadInfo on the new
//     transaction. Every transaction the job creates automatically
//     carries the job ID.
//   - Non-transactional path: CrossRangeTxnWrapperSender.Send
//     (kv/db.go) extracts the workload info from the context and
//     stamps it onto the batch header. This covers operations like
//     SSTBatcher AddSSTable requests (IMPORT, RESTORE) and backup
//     ExportRequests that bypass transactions.
//
// Context is the right carrier for jobs because a job creates many
// transactions and non-transactional operations over its lifetime,
// and the job ID is constant throughout.
//
// For DistSQL flows (whether spawned by SQL statements or jobs), the
// gateway includes the WorkloadID in SetupFlowRequest.EvalContext.
// On the remote node, the DistSQL server (distsql/server.go) uses
// this to: (1) stamp the leaf transaction via SetWorkloadInfo,
// (2) populate the flow's EvalContext for ASH sampling, and
// (3) inject kv.ContextWithWorkloadInfo so non-transactional KV
// operations and admission control pacers inherit the identity.
//
// System tasks are internal KV operations that do not originate from
// SQL or jobs — node liveness heartbeats, intent resolution, MVCC
// garbage collection, Raft log truncation, and so on. For tasks that
// create individual batches or transactions directly (e.g., node
// liveness heartbeats, MVCC GC), each call site stamps the workload
// ID explicitly on the batch header or transaction. For store-level
// queues with a well-defined processing entry point (e.g., replicate
// queue, split queue), the workload ID is stamped on the Go context
// via kv.ContextWithWorkloadInfo at the queue's process() method,
// propagating to all downstream transactions and batches. The set of
// system workload IDs is defined in obs/workloadid/workloadid.go.
//
// # Multi-Tenancy
//
// The sampler and app name cache are process-wide singletons, not
// per-tenant. Tenant isolation is enforced at the read path: each
// ASHSample records a TenantID, and the ListLocalActiveSessionHistory
// RPC filters samples so secondary tenants only see their own data.
//
// Shared-process (in-process) tenants: multiple SQL servers in the
// same process share the sampler and app name cache. The sampler
// observes work states from all tenants and can resolve app names
// for any of them, since all SQL servers populate the same cache.
// When a SQL statement executes on a remote node via DistSQL, the
// app name mapping is eagerly stored on the remote node during flow
// setup, so the remote sampler can resolve it without an RPC.
//
// Separate-process (out-of-process) tenants: each SQL pod runs its
// own process with its own sampler and app name cache. App names are
// always resolved locally on the SQL pod because the SQL execution
// path calls GetOrStoreAppNameID before any work states are
// registered. On KV nodes, BatchRequests from SQL pods carry the
// app name ID but have GatewayNodeID set to 0 (since the gateway is
// not a KV node). The sampler skips remote resolution for
// GatewayNodeID 0, so KV-side samples from SQL pod workloads will
// not have app names. This is acceptable because out-of-process
// tenants only observe samples from their own SQL pods, not from KV
// nodes.
//
// # Performance
//
// The on-hot-path cost is limited to SetWorkState and its cleanup
// function: a sync.Pool get/put and a sync.Map store/load. Benchmarks
// show ASH is allocation free in the steady state, and has no significant
// impact on latency or throughput.
//
// Plumbing workload identity also has allocation cost.
// context.WithValue allocates, so context-based propagation
// (kv.ContextWithWorkloadInfo) is used only at coarse-grained entry
// points — job adoption, queue processing — where the context is
// already being set up. For DistSQL flows, the allocation is
// acceptable because SetupFlow already performs many allocations.
// On the per-batch hot path, the workload ID is carried on the
// transaction or batch header instead. Workload identifiers in proto
// messages (BatchRequest headers, SetupFlowRequest) use integer
// fields rather than strings to avoid per-message string allocations
// on the send path.
//
// The off-hot-path cost is the sampler tick, which iterates the
// sync.Map of active work states. The sampling interval does not
// affect per-operation performance.
//
// # Cluster Settings
//
//   - obs.ash.enabled (bool, default false): enables ASH sampling.
//   - obs.ash.sample_interval (duration, default 1s): interval between
//     samples.
//   - obs.ash.buffer_size (int, default 1000000): maximum number of
//     samples retained in memory.
package ash
