// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// The admission package contains abstractions for admission control for
// CockroachDB nodes, both for single-tenant and multi-tenant (aka serverless)
// clusters. In the latter, both KV and SQL nodes are expected to use these
// abstractions.
//
// Admission control has the goal of
// - Limiting node overload, so that bad things don't happen due to starvation
//   of resources.
// - Providing performance isolation between low and high importance
//   activities, so that overload caused by the latter does not impact the
//   latency of the former. Additionally, for multi-tenant KV nodes, the
//   isolation should extend to inter-tenant performance isolation.
//   Isolation is strictly harder than limiting node overload, and the
//   abstractions here are likely to be average quality in doing so.
//
// At a high-level we are trying to shift queueing from system-provided
// resource allocation abstractions that we do not control, like the goroutine
// scheduler, to queueing in admission control, where we can reorder. This
// needs to be done while maintaining high utilization of the resource.
//
// Note that everything here operates at a single node level, and not at a
// cluster level. Cluster level admission control is insufficient for limiting
// node overload or to provide performance isolation in a distributed system
// with strong work affinity (which is true for a stateful system like
// CockroachDB, since rebalancing operates at time scales that can be higher
// than what we need). Cluster level admission control can complement node
// level admission control, in that it can prevent severe abuse, or provide
// cost controls to tenants.
//
// It is possible to also have intermediate mechanisms that gate admission of
// work on load signals of all the nodes in the raft group of the range. This
// could be especially useful for writes where non-leaseholder nodes could be
// suffering from cpu or disk IO overload. This is not considered in the
// following interfaces.
//
// TODO(sumeer): describe more of the design thinking documented in
// https://github.com/sumeerbhola/cockroach/blob/27ab4062ad1b036ab1e686a66a04723bd9f2b5a0/pkg/util/cpupool/cpu_pool.go
// either in a comment here or a separate RFC.
//

// Internal organization:
//
// The package is mostly structured as a set of interfaces that are meant to
// provide a general framework, and specific implementations that are
// initially quite simple in their heuristics but may become more
// sophisticated over time. The concrete abstractions:
// - Tokens and slots are the two ways admission is granted (see grantKind)
// - Categorization of kinds of work (see WorkKind), and a priority ordering
//   across WorkKinds that is used to reflect their shared need for underlying
//   resources.
// - The top-level GrantCoordinator which coordinates grants across these
//   WorkKinds. The WorkKinds handled by an instantiation of GrantCoordinator
//   will differ for single-tenant clusters, and multi-tenant clusters
//   consisting of (multi-tenant) KV nodes and (single-tenant) SQL nodes.
//
// The interfaces involved:
// -  requester: handles all requests for a particular WorkKind. Implemented by
//   WorkQueue. The requester implementation is responsible for controlling
//   the admission order within a WorkKind based on tenant fairness,
//   importance of work etc.
// - granter: the counterpart to requester which grants admission tokens or
//   slots. The implementations are slotGranter, tokenGranter, kvGranter. The
//   implementation of requester interacts with the granter interface.
// - granterWithLockedCalls: this is an extension of granter that is used
//   as part of the implementation of GrantCoordinator. This arrangement
//   is partly to centralize locking in the GrantCoordinator (except for
//   the lock in WorkQueue).
// - cpuOverloadIndicator: this serves as an optional additional gate on
//   granting, by providing an (ideally) instantaneous signal of cpu overload.
//   The kvSlotAdjuster is the concrete implementation, except for SQL
//   nodes, where this is implemented by sqlNodeCPUOverloadIndicator.
//   CPULoadListener is also implemented by these structs, to listen to
//   the latest CPU load information from the scheduler.
//
// Load observation and slot count or token burst adjustment: Currently the
// only dynamic adjustment is performed by kvSlotAdjuster for KVWork slots.
// This is because KVWork is expected to usually be CPU bound (due to good
// caching), and unlike SQLKVResponseWork and SQLSQLResponseWork (which are
// even more CPU bound), we have a completion indicator -- so we can expect to
// have a somewhat stable KVWork slot count even if the work sizes are
// extremely heterogeneous.
//
// Since there isn't token burst adjustment, the burst limits should be chosen
// to err on the side of fully saturating CPU, since we have the fallback of
// the cpuOverloadIndicator to stop granting even if tokens are available.
// If we figure out a way to dynamically tune the token burst count, or
// (even more ambitious) figure out a way to come up with a token rate, it
// should fit in the general framework that is setup here.
//

// Partial usage example (regular cluster):
//
// var metricRegistry *metric.Registry = ...
// coord, metrics := admission.NewGrantCoordinator(admission.Options{...})
// for i := range metrics {
//   registry.AddMetricStruct(metrics[i])
// }
// kvQueue := coord.GetWorkQueue(admission.KVWork)
// // Pass kvQueue to server.Node that implements roachpb.InternalServer.
// ...
// // Do similar things with the other WorkQueues.
//
// Usage of WorkQueue for KV:
// // Before starting some work
// if enabled, err := kvQueue.Admit(ctx, WorkInfo{TenantID: tid, ...}); err != nil {
//   return err
// }
// doWork()
// if enabled { kvQueue.AdmittedWorkDone(tid) }

package admission
