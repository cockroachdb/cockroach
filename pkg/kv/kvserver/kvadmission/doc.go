// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvadmission is the integration layer between KV and admission
// control.
//
// # Overview
//
// This package provides admission control for all KV layer work. The primary
// entry point is Controller.AdmitKVWork, which is called before processing any
// BatchRequest. Depending on the request type and properties, work may pass
// through multiple admission control queues before execution.
//
// # Request Flow
//
// When a BatchRequest arrives, AdmitKVWork determines which admission control
// mechanism(s) to apply based on the request properties. A single request may
// pass through multiple queues in sequence:
//
//  1. Replication Admission Control (RACv2) - For replicated writes
//     Token-based flow control integrated with Raft replication.
//     Applies when kvflowcontrol is enabled and work is not bypassing admission.
//
//  2. Store Admission Queue - For leaseholder writes
//     Token-based IO admission control per store, monitoring Pebble LSM health.
//     Applies to writes when RACv2 is disabled or not admitted.
//
//  3. Elastic CPU Work Queue - For CPU-intensive background work
//     Token-based admission with cooperative scheduling for elastic work.
//     Applies to export requests and low-priority internal bulk operations.
//
//  4. KV Work Queue (slots) - For regular KV operations
//     Slot-based concurrency control, dynamically adjusted based on CPU load.
//     This is the default path for work not covered by the above mechanisms.
//
// # Request Property Mapping
//
// The workInfoForBatch function extracts admission control properties from
// incoming requests:
//
// ## Tenant ID
//
// The tenant ID determines fairness scheduling within work queues:
//
//   - For non-system tenants: Always uses the authenticated requestTenantID.
//   - For system tenant requests: Uses rangeTenantID (if set and non-admin)
//     to attribute work to the tenant owning the data. This enables proper
//     accounting when the system tenant operates on behalf of other tenants.
//     Controlled by kvadmission.use_range_tenant_id_for_non_admin.enabled.
//
// ## Priority
//
// Priority is extracted from BatchRequest.AdmissionHeader.Priority and uses
// the admissionpb.WorkPriority enum:
//
//   - HighPri (127): High-priority system work
//   - LockingUserHighPri (100): User high-priority transactions with locks
//   - UserHighPri (50): High-priority user queries
//   - LockingNormalPri (10): Normal-priority transactions with locks
//   - NormalPri (0): Default priority for most user work
//   - BulkNormalPri (-30): User-initiated bulk operations (backups, changefeeds)
//     and necessary background maintenance (index backfill, MVCC GC)
//   - UserLowPri (-50): Low-priority user work
//   - BulkLowPri (-100): Internal system maintenance (TTL deletion, schema
//     change cleanup, table metadata updates, SQL activity stats)
//   - LowPri (-128): Lowest priority background work
//
// Priority affects ordering within a tenant's work queue. Higher priority work
// can starve lower priority work within the same tenant.
//
// ## Priority to Admission Queue Mapping
//
// Priorities map to admission control concepts in two ways:
//
//  1. WorkClass: Determines Store IO token requirements (for writes)
//     - ElasticWorkClass (priority < NormalPri): Requires L0 tokens, elastic
//     L0 tokens, AND disk bandwidth tokens
//     - RegularWorkClass (priority >= NormalPri): Requires only L0 tokens
//
//  2. Admission Path: The sequence of admission queues work passes through
//     - Store IO: Token-based per-store IO admission (writes only currently)
//     - CPU: Elastic CPU work queue, uses CPU time tokens (e.g., 100ms grants)
//     and cooperative yielding when work exceeds its allotted time
//     - Slots: KV work queue, uses concurrency slots (e.g., max 8 concurrent ops)
//     that are held for the duration of the operation
//
// The mapping varies by request type and priority (defaults shown):
//
// ┌──────────────────┬────────┬───────────┬──────────────────────────────┐
// │ Priority         │ Value  │ WorkClass │ Admission Path               │
// ├──────────────────┼────────┼───────────┼──────────────────────────────┤
// │ Export requests³ │ -30    │ Elastic   │ IO(Elastic) → CPU(100ms)¹    │
// ├──────────────────┼────────┼───────────┼──────────────────────────────┤
// │ LowPri           │ -128   │ Elastic   │ IO(Elastic) → CPU(10ms)²     │
// ├──────────────────┼────────┼───────────┼──────────────────────────────┤
// │ BulkLowPri       │ -100   │ Elastic   │ IO(Elastic) → CPU(10ms)²     │
// ├──────────────────┼────────┼───────────┼──────────────────────────────┤
// │ UserLowPri       │ -50    │ Elastic   │ IO(Elastic) → Slots(1)       │
// ├──────────────────┼────────┼───────────┼──────────────────────────────┤
// │ BulkNormalPri    │ -30    │ Elastic   │ IO(Elastic) → Slots(1)       │
// ├──────────────────┼────────┼───────────┼──────────────────────────────┤
// │ NormalPri+       │ >= 0   │ Regular   │ IO(Regular) → Slots(1)       │
// └──────────────────┴────────┴───────────┴──────────────────────────────┘
//
// Notes:
//   - IO(Elastic) requires 3 token types: L0, elastic L0, and disk bandwidth
//     (see "Disk Bandwidth Tokens" section for details)
//   - IO(Regular) requires 1 token type: L0 only
//   - Store IO currently only applies to writes; read bandwidth limiting is planned
//   - CPU(Xms) grants X milliseconds of CPU time; work cooperatively yields when
//     it exceeds its grant
//   - Slots(1) requests 1 concurrency slot; typically ~8 total slots available,
//     dynamically adjusted based on CPU load
//
// Footnotes:
//
//	¹ kvadmission.export_request_elastic_control.enabled (default: true)
//	  When disabled, export requests become IO(Elastic) → Slots(1).
//
//	² kvadmission.elastic_control_bulk_low_priority.enabled (default: true)
//	  When disabled, BulkLowPri and LowPri become IO(Elastic) → Slots(1) instead
//	  of IO(Elastic) → CPU(10ms).
//
//	³ Export requests (used by BACKUP) are always assigned BulkNormalPri (-30)
//	  for admission control, regardless of the backup job's user priority.
//
// Additionally, kvadmission.elastic_cpu.duration_per_export_request (default: 100ms)
// and kvadmission.elastic_cpu.duration_per_low_pri_read (default: 10ms) control
// the CPU time tokens granted to elastic CPU work.
//
// Key observations:
//
//   - All work follows the IO → {CPU|Slots} pattern, with Store IO admission
//     currently applying only to writes (read bandwidth limiting planned).
//   - Export requests bypass normal priority-based routing to use IO → CPU.
//   - The boundary at NormalPri (0) separates ElasticWorkClass (can tolerate
//     delays) from RegularWorkClass (latency-sensitive).
//   - IO(Elastic) is more restrictive than IO(Regular): 3 token types vs 1.
//     The additional disk bandwidth token requirement provides backpressure
//     protection, limiting elastic writes to ~80% of provisioned disk bandwidth.
//   - Only BulkLowPri and LowPri end at the CPU queue (by default). UserLowPri
//     and BulkNormalPri end at Slots despite being ElasticWorkClass.
//
// ## Bypass Conditions
//
// Work may bypass admission control entirely under these conditions:
//
//   - Admin requests: ba.IsAdmin() returns true (e.g., splits, merges)
//   - Source is OTHER: AdmissionHeader.Source == OTHER
//   - LeaseInfo requests: Used as health probes by circuit breakers
//   - Legacy bulk-only mode: When KVBulkOnlyAdmissionControlEnabled is set,
//     work at NormalPri or above bypasses admission
//
// ## Create Time
//
// The admission create time (AdmissionHeader.CreateTime) tracks request
// queueing latency. If unset and work is not bypassing admission, it's set to
// the current timestamp. This enables admission control to prioritize older
// requests and track queueing delays.
//
// # Admission Control Mechanisms
//
// ## KV Work Queue (Slots)
//
// The default path for most KV operations uses slot-based admission:
//
//   - Slots represent concurrency limits (e.g., 8 concurrent KV operations)
//   - Dynamically adjusted by kvSlotAdjuster based on CPU load (runnable goroutines)
//   - Work blocks until a slot is available
//   - Slot is explicitly returned via AdmittedKVWorkDone
//   - Enables tracking of work completion for better admission decisions
//
// Used for: All KV work except writes (which use store queue) and elastic work.
//
// ## Store Admission Queue (Tokens)
//
// Per-store token-based IO admission control:
//
//   - Tokens represent estimated disk IO cost
//   - Dynamically adjusted based on Pebble LSM health metrics:
//   - L0 file count and compaction debt
//   - Write stalls and flush backlog
//   - Disk bandwidth utilization
//   - Tokens are consumed but not returned (unlike slots)
//   - Actual bytes written are reported for token adjustment
//
// Used for: Leaseholder writes when RACv2 is disabled or doesn't admit.
//
// ### Disk Bandwidth Tokens
//
// In addition to L0 tokens, Store Admission uses disk bandwidth tokens to limit
// elastic work based on the underlying storage's provisioned bandwidth:
//
//	Token Budget (per 15s interval):
//	  elastic_write_tokens = (provisioned_bandwidth × max_util × 15s) - observed_reads
//
// Where:
//   - provisioned_bandwidth: Storage capacity (e.g., 1 GB/s from disk specs)
//   - max_util: kvadmission.store.elastic_disk_bandwidth_max_util (default: 0.8)
//   - observed_reads: Actual read bytes from OS (e.g., Linux /proc/diskstats)
//
// Key properties:
//
//   - One token = one byte of LSM ingestion capacity (not raw disk bytes)
//   - Write amplification is modeled: 1MB write may consume 10MB of tokens
//   - Reads are observed (not admitted) and reduce the write budget
//   - Regular work requires only L0 tokens
//   - Elastic work requires L0 tokens AND disk bandwidth tokens
//   - The max_util cap (80%) is static - elastic work won't exceed this even
//     when regular work is idle, providing headroom for burst regular traffic
//
// Example calculation (15s interval, 1 GB/s disk, 80% max util, 2 GB reads):
//
//	Budget = (1 GB/s × 0.8 × 15s) - 2 GB = 12 GB - 2 GB = 10 GB for elastic writes
//
// ## Replication Admission Control (RACv2)
//
// Below-raft replication flow control:
//
//   - Token-based with per-stream flow tokens
//   - Integrated with Raft replication protocol
//   - Provides backpressure before proposals enter the Raft log
//   - Tokens are returned when followers apply and acknowledge entries
//
// Used for: Replicated writes when kvflowcontrol.enabled is true.
//
// ## Elastic CPU Work Queue (Tokens)
//
// Cooperative scheduling for elastic, CPU-intensive work:
//
//   - Token represents allotted CPU time (default 100ms for exports)
//   - Work checks ElasticCPUWorkHandle.OverLimit() in tight loops
//   - When over limit, work yields and re-admits with a new handle
//   - Total elastic CPU capacity adjusts based on scheduler latency
//   - Provides latency isolation between elastic and regular work
//
// Used for:
//   - Export requests (backups) when kvadmission.export_request_elastic_control.enabled
//   - Internal low-priority reads (TTL) at BulkLowPri when
//     kvadmission.elastic_control_bulk_low_priority.enabled
//
// # Admission Queue Ordering
//
// For writes (non-heartbeat), admission happens in this order:
//
//  1. RACv2 flow control (if enabled and not bypassing)
//  2. Store IO admission (if RACv2 disabled or didn't admit)
//  3. KV slots or Elastic CPU (depending on request type)
//
// For reads and other operations:
//
//  1. Elastic CPU admission (if export or low-priority bulk)
//  2. KV slots admission (otherwise)
//
// Each stage may block waiting for tokens/slots. Work bypassing admission
// still goes through the queues for accounting but doesn't block.
//
// # Example: Normal Read Request
//
// A BatchRequest with a Get operation at NormalPri:
//
//  1. Tenant ID: Extracted from requestTenantID (or rangeTenantID for system tenant)
//  2. Priority: NormalPri (0) from AdmissionHeader
//  3. Bypass: false (not an admin request)
//  4. Admission: kvAdmissionQ.Admit() - blocks until a slot is available
//  5. Execution: KV work executes
//  6. Completion: AdmittedKVWorkDone() returns the slot
//
// # Example: Write Request
//
// A BatchRequest with a Put operation at NormalPri:
//
//  1. Tenant ID: Extracted as above
//  2. Priority: NormalPri (0)
//  3. Bypass: false
//  4. RACv2 Admission: kvflowHandle.Admit() - waits for replication flow tokens
//  5. Store Admission: Skipped (RACv2 admitted)
//  6. KV Admission: kvAdmissionQ.Admit() - waits for a KV slot
//  7. Execution: KV work executes, Raft replication happens
//  8. Completion: AdmittedKVWorkDone() releases slot and IO tokens
//
// # Example: Export Request
//
// A backup export request at BulkNormalPri:
//
//  1. Tenant ID: Extracted as above
//  2. Priority: BulkNormalPri (-30)
//  3. Bypass: false
//  4. Elastic CPU Admission: ElasticCPUWorkQueue.Admit() - waits for 100ms of CPU time
//  5. Execution: Export loop checks OverLimit() periodically and yields when exceeded
//  6. Completion: AdmittedKVWorkDone() returns elastic CPU handle
//
// # Integration Points
//
// The Controller is initialized in server.go during node startup:
//
//   - kvAdmissionController receives the KVWork WorkQueue from GrantCoordinator
//   - elasticCPUGrantCoordinator is the separate elastic CPU coordinator
//   - storeGrantCoords provides per-store IO admission queues
//   - kvflowHandles tracks replication flow control handles per range
//
// Callers invoke AdmitKVWork before processing any BatchRequest and must call
// AdmittedKVWorkDone after completion (if a non-nil handle is returned).
package kvadmission
