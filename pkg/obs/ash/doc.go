// Copyright 2025 The Cockroach Authors.
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
//	cleanup := ash.SetWorkState(tenantID, fingerprintID, ash.WORK_LOCK, "LockWait")
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
// When ASH is disabled (sql.ash.enabled = false), SetWorkState is a
// no-op that returns a pre-allocated no-op function, so there is
// zero overhead on the hot path.
//
// # Sampler
//
// The Sampler is a process-wide singleton that runs a background
// goroutine on a configurable tick interval (sql.ash.sample_interval,
// default 1s). On each tick it:
//
//  1. Calls RangeWorkStates to snapshot every registered goroutine's
//     current WorkState.
//  2. Reclaims retired WorkState objects back to the pool.
//  3. Converts each snapshot into an ASHSample and appends it to a
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
// # Ring Buffer
//
// ASH samples are stored in a RingBuffer, a thread-safe circular
// buffer with a configurable capacity (sql.ash.buffer_size, default
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
//   - sql.ash.enabled (bool, default false): enables ASH sampling.
//   - sql.ash.sample_interval (duration, default 1s): interval between
//     samples.
//   - sql.ash.buffer_size (int, default 1000000): maximum number of
//     samples retained in memory.
package ash
