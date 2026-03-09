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
// "what was running when performance changed?" ASH provides an indirect
// but statistically reliable view of where database time is spent by
// periodically sampling the state of in-flight work (SQL queries, jobs,
// internal tasks) and recording whether each unit of work is running on
// CPU or waiting on a specific subsystem such as I/O, locks, admission
// control, or network. Aggregating these samples over time yields a
// picture of resource and wait-time accountability often described as
// "DB seconds."
//
// # Architecture
//
// The package is structured around three core components: work state
// registration, sampling, and storage.
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
// When ASH is disabled (obs.ash.enabled = false), SetWorkState is a
// no-op that returns a pre-allocated no-op function, so there is
// zero overhead on the hot path.
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
// # App Name Resolution
//
// To correlate samples with applications, the SQL layer hashes each
// session's application name to a uint64 ID (via GetOrStoreAppNameID)
// and stores the ID-to-string mapping in a process-wide cache. The ID
// is propagated through BatchRequest headers and DistSQL
// SetupFlowRequests, avoiding string copies on the hot path.
//
// During sampling, the sampler resolves app name IDs in two phases:
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
// # Ring Buffer
//
// ASH samples are stored in a RingBuffer, a thread-safe circular
// buffer with a configurable capacity (obs.ash.buffer_size, default
// 1M samples, ~200MB at ~200 bytes per sample). When the buffer is
// full the oldest samples are overwritten. The buffer supports dynamic
// resizing via cluster setting changes.
//
// # SQL Interface
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
// # Work Events and Types
//
// Each sample records a WorkEvent (a specific label like "LockWait",
// "ReplicaSend", "DistSenderRemote") and a WorkEventType that
// categorizes the resource involved:
//
//   - CPU: active computation (e.g., optimizer planning, replica
//     evaluation).
//   - IO: storage I/O (e.g., batch evaluation in the storage layer).
//   - LOCK: lock or latch contention.
//   - NETWORK: waiting on remote RPCs.
//   - ADMISSION: queued in admission control.
//   - OTHER: other wait points (e.g., commit wait, backpressure).
//
// # Workload Attribution
//
// To attribute samples to workloads, the SQL layer plumbs a compact
// integer workload identifier (e.g., statement fingerprint ID) through
// BatchRequest headers and DistSQL SetupFlowRequests. This avoids
// string allocations on the hot path. The sampler converts these IDs
// to hex-encoded strings when writing samples, caching the conversion
// to avoid repeated allocations.
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
// show ASH adds a near-fixed ~600-700 bytes and ~17-21 allocations per
// operation with no statistically significant impact on latency or
// throughput.
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
