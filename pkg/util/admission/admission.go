// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// The admission package contains abstractions for admission control for
// CockroachDB nodes, both for single-tenant and multi-tenant (aka serverless)
// clusters. In the latter, both KV and SQL nodes are expected to use these
// abstractions.
//
// Admission control has the goal of
// - Limiting node overload, so that bad things don't happen due to starvation
//   of resources.
// - Providing performance isolation between low and high importance
//   activities, so that overload caused by the former does not impact the
//   latency of the latter. Additionally, for multi-tenant KV nodes, the
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

// TODO(sumeer): update with all the recent changes.

// Internal organization:
//
// The package is mostly structured as a set of interfaces that are meant to
// provide a general framework, and specific implementations that are
// initially quite simple in their heuristics but may become more
// sophisticated over time. The concrete abstractions:
// - Tokens and slots are the two ways admission is granted (see grantKind).
// - Categorization of kinds of work (see WorkKind), and a priority ordering
//   across WorkKinds that is used to reflect their shared need for underlying
//   resources.
// - The top-level GrantCoordinator which coordinates grants across these
//   WorkKinds. The WorkKinds handled by an instantiation of GrantCoordinator
//   will differ for single-tenant clusters, and multi-tenant clusters
//   consisting of (multi-tenant) KV nodes and (single-tenant) SQL nodes.
//
// The interfaces involved:
// - requester: handles all requests for a particular WorkKind. Implemented by
//   WorkQueue. The requester implementation is responsible for controlling
//   the admission order within a WorkKind based on tenant fairness,
//   importance of work etc.
// - granter: the counterpart to requester which grants admission tokens or
//   slots. The implementations are slotGranter, tokenGranter,
//   kvStoreTokenGranter. The implementation of requester interacts with the
//   granter interface.
// - granterWithLockedCalls: this is an extension of granter that is used
//   as part of the implementation of GrantCoordinator. This arrangement
//   is partly to centralize locking in the GrantCoordinator (except for
//   the lock in WorkQueue).
// - cpuOverloadIndicator: this serves as an optional additional gate on
//   granting, by providing an (ideally) instantaneous signal of cpu overload.
//   The kvSlotAdjuster is the concrete implementation, except for SQL
//   nodes, where this will be implemented by sqlNodeCPUOverloadIndicator.
//   CPULoadListener is also implemented by these structs, to listen to
//   the latest CPU load information from the scheduler.
//
// Load observation and slot count or token burst adjustment: Dynamic
// adjustment is performed by kvSlotAdjuster for KVWork slots. This is because
// KVWork is expected to usually be CPU bound (due to good caching), and
// unlike SQLKVResponseWork and SQLSQLResponseWork (which are even more CPU
// bound), we have a completion indicator -- so we can expect to have a
// somewhat stable KVWork slot count even if the work sizes are extremely
// heterogeneous.
//
// There isn't token burst adjustment (except for each store -- see below),
// and the burst limits should be chosen to err on the side of fully
// saturating CPU, since we have the fallback of the cpuOverloadIndicator to
// stop granting even if tokens are available. If we figure out a way to
// dynamically tune the token burst count, or (even more ambitious) figure out
// a way to come up with a token rate, it should fit in the general framework
// that is setup here.
//

// Partial usage example (regular cluster):
//
// var metricRegistry *metric.Registry = ...
// coord, metrics := admission.NewGrantCoordinator(admission.Options{...})
// for i := range metrics {
//   registry.AddMetricStruct(metrics[i])
// }
// kvQueue := coord.GetWorkQueue(admission.KVWork)
// // Pass kvQueue to server.Node that implements kvpb.InternalServer.
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

// Additionally, each store has a single StoreWorkQueue and GrantCoordinator
// for writes. See kvStoreTokenGranter and how its tokens are dynamically
// adjusted based on Pebble metrics.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/schedulerlatency"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// requester is an interface implemented by an object that orders admission
// work for a particular WorkKind. See WorkQueue for the implementation of
// requester.
type requester interface {
	// hasWaitingRequests returns whether there are any waiting/queued requests
	// of this WorkKind.
	hasWaitingRequests() bool
	// granted is called by a granter to grant admission to a single queued
	// request. It returns > 0 if the grant was accepted, else returns 0. A
	// grant may not be accepted if the grant raced with request cancellation
	// and there are now no waiting requests. The grantChainID is used when
	// calling continueGrantChain -- see the comment with that method below.
	// When accepted, the return value indicates the number of slots/tokens that
	// were used.
	// REQUIRES: count <= 1 for slots.
	granted(grantChainID grantChainID) int64
	close()
}

// granter is paired with a requester in that a requester for a particular
// WorkKind will interact with a granter. See admission.go for an overview of
// how this fits into the overall structure.
type granter interface {
	grantKind() grantKind
	// tryGet is used by a requester to get slots/tokens for a piece of work
	// that has encountered no waiting/queued work. This is the fast path that
	// avoids queueing in the requester.
	//
	// REQUIRES: count > 0. count == 1 for slots.
	tryGet(count int64) (granted bool)
	// returnGrant is called for:
	// - returning slots after use.
	// - returning either slots or tokens when the grant raced with the work
	//   being canceled, and the grantee did not end up doing any work.
	//
	// The last case occurs despite the return value on the requester.granted
	// method -- it is possible that the work was not canceled at the time when
	// requester.grant was called, and hence returned a count > 0, but later
	// when the goroutine doing the work noticed that it had been granted, there
	// is a possibility that that raced with cancellation.
	//
	// Do not use this for doing store IO-related token adjustments when work is
	// done -- that should be done via granterWithStoreReplicatedWorkAdmitted.storeWriteDone.
	//
	// REQUIRES: count > 0. count == 1 for slots.
	returnGrant(count int64)
	// tookWithoutPermission informs the granter that a slot or tokens were
	// taken unilaterally, without permission. This is useful:
	// - Slots: this is useful since KVWork is allowed to bypass admission
	//   control for high priority internal activities (e.g. node liveness) and
	//   for KVWork that generates other KVWork (like intent resolution of
	//   discovered intents). Not bypassing for the latter could result in
	//   single node or distributed deadlock, and since such work is typically
	//   not a major (on average) consumer of resources, we consider bypassing
	//   to be acceptable.
	// - Tokens: this is useful when the initial estimated tokens for a unit of
	//   work turned out to be an underestimate.
	//
	// Do not use this for doing store IO-related token adjustments when work is
	// done -- that should be done via granterWithStoreReplicatedWorkAdmitted.storeWriteDone.
	//
	// REQUIRES: count > 0. count == 1 for slots.
	tookWithoutPermission(count int64)
	// continueGrantChain is called by the requester at some point after grant
	// was called on the requester. The expectation is that this is called by
	// the grantee after its goroutine runs and notices that it has been granted
	// a slot/tokens. This provides a natural throttling that reduces grant
	// bursts by taking into immediate account the capability of the goroutine
	// scheduler to schedule such work.
	//
	// In an experiment, using such grant chains reduced burstiness of grants by
	// 5x and shifted ~2s of latency (at p99) from the scheduler into admission
	// control (which is desirable since the latter is where we can
	// differentiate between work).
	//
	// TODO(sumeer): the "grant chain" concept is subtle and under-documented.
	// It's easy to go through most of this package thinking it has something to
	// do with dependent requests (e.g. intent resolution chains on an end txn).
	// It would help for a top-level comment on grantChainID or continueGrantChain
	// to spell out what grant chains are, their purpose, and how they work with
	// an example.
	continueGrantChain(grantChainID grantChainID)
}

// granterWithLockedCalls is an encapsulation of typically one
// granter-requester pair, and for kvStoreTokenGranter of two
// granter-requester pairs (one for each workClass). It is used as an internal
// implementation detail of the GrantCoordinator. An implementer of
// granterWithLockedCalls responds to calls from its granter(s) by calling
// into the GrantCoordinator, which then calls the various *Locked() methods.
// The demuxHandle is meant to be opaque to the GrantCoordinator, and is used
// when this interface encapsulates multiple granter-requester pairs -- it is
// currently used only by kvStoreTokenGranter, where it is a workClass. The
// *Locked() methods are where the differences in slots and various kinds of
// tokens are handled.
type granterWithLockedCalls interface {
	// tryGetLocked is the real implementation of tryGet from the granter
	// interface. demuxHandle is an opaque handle that was passed into the
	// GrantCoordinator.
	tryGetLocked(count int64, demuxHandle int8) grantResult
	// returnGrantLocked is the real implementation of returnGrant from the
	// granter interface. demuxHandle is an opaque handle that was passed into
	// the GrantCoordinator.
	returnGrantLocked(count int64, demuxHandle int8)
	// tookWithoutPermissionLocked is the real implementation of
	// tookWithoutPermission from the granter interface. demuxHandle is an
	// opaque handle that was passed into the GrantCoordinator.
	tookWithoutPermissionLocked(count int64, demuxHandle int8)

	// The following methods are for direct use by GrantCoordinator.

	// requesterHasWaitingRequests returns whether some requester associated
	// with the granter has waiting requests.
	requesterHasWaitingRequests() bool
	// tryGrantLocked is used to attempt to grant to waiting requests.
	tryGrantLocked(grantChainID grantChainID) grantResult
}

// granterWithIOTokens is used to abstract kvStoreTokenGranter for testing.
// The interface is used by the entity that periodically looks at load and
// computes the tokens to grant (ioLoadListener).
type granterWithIOTokens interface {
	// setAvailableTokens bounds the available {io,elastic disk bandwidth} tokens
	// that can be granted to the value provided in the
	// {io,elasticDiskBandwidth}Tokens parameter. elasticDiskBandwidthTokens bounds
	// what can be granted to elastic work, and is based on disk bandwidth being a
	// bottleneck resource. These are not tight bounds when the callee has negative
	// available tokens, due to the use of granter.tookWithoutPermission, since in
	// that the case the callee increments that negative value with the value
	// provided by tokens. This method needs to be called periodically.
	// {io, elasticDiskBandwidth}TokensCapacity is the ceiling up to which we allow
	// elastic or disk bandwidth tokens to accumulate. The return value is the
	// number of used tokens in the interval since the prior call to this method
	// (and the tokens used by elastic work). Note that tokensUsed* can be
	// negative, though that will be rare, since it is possible for tokens to be
	// returned.
	setAvailableTokens(
		ioTokens int64, elasticIOTokens int64, elasticDiskWriteTokens int64, elasticDiskReadTokens int64,
		ioTokensCapacity int64, elasticIOTokenCapacity int64, elasticDiskWriteTokensCapacity int64,
		lastTick bool,
	) (tokensUsed int64, tokensUsedByElasticWork int64)
	// getDiskTokensUsedAndReset returns the disk bandwidth tokens used since the
	// last such call.
	getDiskTokensUsedAndReset() [admissionpb.NumStoreWorkTypes]diskTokens
	// setLinearModels supplies the models to use when storeWriteDone or
	// storeReplicatedWorkAdmittedLocked is called, to adjust token consumption.
	// Note that these models are not used for token adjustment at admission
	// time -- that is handled by StoreWorkQueue and is not in scope of this
	// granter. This asymmetry is due to the need to use all the functionality
	// of WorkQueue at admission time. See the long explanatory comment at the
	// beginning of store_token_estimation.go, regarding token estimation.
	setLinearModels(l0WriteLM, l0IngestLM, ingestLM, writeAmpLM tokensLinearModel)
}

// granterWithStoreReplicatedWorkAdmitted is used to abstract
// kvStoreTokenGranter for testing. The interface is used by StoreWorkQueue to
// pass on sizing information provided when the work is either done (for legacy,
// above-raft IO admission) or admitted (for below-raft, asynchronous admission
// control.
type granterWithStoreReplicatedWorkAdmitted interface {
	granter
	// storeWriteDone is used by legacy, above-raft IO admission control to
	// inform granters of when the write was actually done, post-admission. At
	// admit-time we did not have sizing info for these writes, so by
	// intercepting these writes at admit time we're able to make any
	// outstanding token adjustments in the granter. When adjusting tokens, if
	// we observe the granter was previously exhausted but is now no longer so,
	// this interface is allowed to admit other waiting requests.
	storeWriteDone(originalTokens int64, doneInfo StoreWorkDoneInfo) (additionalTokens int64)
	// storeReplicatedWorkAdmittedLocked is used by below-raft admission control
	// to inform granters of work being admitted in order for them to make any
	// outstanding token adjustments. It's invoked with the coord.mu held.
	// Unlike storeWriteDone, the token adjustments don't result in further
	// admission. There are two callsites to get here:
	// - From (*WorkQueue).granted, invoked in the GrantCoordinator loop. The
	//   caller itself is looping, so we don't grant further admission. If we
	//   did, our recursive callstack could be as large as the number of waiting
	//   requests.
	// - The fast path in (*WorkQueue).Admit, invoked by the work seeking
	//   admission that only wants to subtract tokens. Here again there's no
	//   need for further admission.
	storeReplicatedWorkAdmittedLocked(originalTokens int64, admittedInfo storeReplicatedWorkAdmittedInfo) (additionalTokens int64)
}

// cpuOverloadIndicator is meant to be an instantaneous indicator of cpu
// availability. Since actual scheduler stats are periodic, we prefer to use
// the KV slot availability, since it is instantaneous. The
// cpuOverloadIndicator is used to gate admission of work other than KVWork
// (KVWork only looks at slot availability). An instantaneous indicator limits
// over-admission and queueing in the scheduler, and thereby provider better
// isolation, especially in multi-tenant environments where tenants not
// responsible for a load spike expect to suffer no increase in latency.
//
// In multi-tenant settings, for single-tenant SQL nodes, which do not do KV
// work, we do not have an instantaneous indicator and instead use
// sqlNodeCPUOverloadIndicator.
type cpuOverloadIndicator interface {
	isOverloaded() bool
}

// CPULoadListener listens to the latest CPU load information. Currently we
// expect this to be called every 1ms, unless the cpu is extremely
// underloaded. If the samplePeriod is > 1ms, admission control enforcement
// for CPU is disabled.
type CPULoadListener interface {
	CPULoad(runnable int, procs int, samplePeriod time.Duration)
}

// storeRequester is used to abstract *StoreWorkQueue for testing.
type storeRequester interface {
	requesterClose
	getRequesters() [admissionpb.NumWorkClasses]requester
	getStoreAdmissionStats() storeAdmissionStats
	setStoreRequestEstimates(estimates storeRequestEstimates)
}

// elasticCPULimiter is used to set the CPU utilization limit for elastic work
// (defined as a % of available system CPU).
type elasticCPULimiter interface {
	getUtilizationLimit() float64
	setUtilizationLimit(limit float64)
	hasWaitingRequests() bool
	computeUtilizationMetric()
}

// SchedulerLatencyListener listens to the latest scheduler latency data. We
// expect this to be called every scheduler_latency.sample_period.
type SchedulerLatencyListener = schedulerlatency.LatencyObserver

// grantKind represents the two kind of ways we grant admission: using a slot
// or a token. The slot terminology is akin to a scheduler, where a scheduling
// slot must be free for a thread to run. But unlike a scheduler, we don't
// have visibility into the fact that work execution may be blocked on IO. So
// a slot can also be viewed as a limit on concurrency of ongoing work. The
// token terminology is inspired by token buckets. In this case the token is
// handed out for admission but it is not returned (unlike a slot). Unlike a
// token bucket, which shapes the rate, the current implementation (see
// tokenGranter) limits burstiness and does not do rate shaping -- this is
// because it is hard to predict what rate is appropriate given the difference
// in sizes of the work. This lack of rate shaping may change in the future
// and is not a limitation of the interfaces. Similarly, there is no rate
// shaping applied when granting slots and that may also change in the future.
// The main difference between a slot and a token is that a slot is used when
// we can know when the work is complete. Having this extra completion
// information can be advantageous in admission control decisions, so
// WorkKinds where this information is easily available use slots.
//
// StoreGrantCoordinators and its corresponding StoreWorkQueues are a hybrid
// -- they use tokens (as explained later). However, there is useful
// completion information such as how many tokens were actually used, which
// can differ from the up front information, and is utilized to adjust the
// available tokens.
type grantKind int8

const (
	slot grantKind = iota
	token
)

type grantResult int8

const (
	grantSuccess grantResult = iota
	// grantFailDueToSharedResource is returned when the granter is unable to
	// grant because a shared resource (CPU or memory) is overloaded. For grant
	// chains, this is a signal to terminate.
	grantFailDueToSharedResource
	// grantFailLocal is returned when the granter is unable to grant due to (a)
	// a local constraint -- insufficient tokens or slots, or (b) no work is
	// waiting.
	grantFailLocal
)

// grantChainID is the ID for a grant chain. See continueGrantChain for
// details.
type grantChainID uint64

// WorkKind represents various types of work that are subject to admission
// control.
type WorkKind int8

// The list of WorkKinds are ordered from lower level to higher level, and
// also serves as a hard-wired ordering from most important to least important
// (for details on how this ordering is enacted, see the GrantCoordinator
// code).
//
// KVWork, SQLKVResponseWork, SQLSQLResponseWork are the lower-level work
// units that are expected to be primarily CPU bound (with disk IO for KVWork,
// but cache hit rates are typically high), and expected to be where most of
// the CPU consumption happens. These are prioritized in the order
//
//	KVWork > SQLKVResponseWork > SQLSQLResponseWork
//
// The high prioritization of KVWork reduces the likelihood that non-SQL KV
// work will be starved. SQLKVResponseWork is prioritized over
// SQLSQLResponseWork since the former includes leaf DistSQL processing and we
// would like to release memory used up in RPC responses at lower layers of
// RPC tree. We expect that if SQLSQLResponseWork is delayed, it will
// eventually reduce new work being issued, which is a desirable form of
// natural backpressure.
//
// Furthermore, SQLStatementLeafStartWork and SQLStatementRootStartWork are
// prioritized lowest with
//
//	SQLStatementLeafStartWork > SQLStatementRootStartWork
//
// This follows the same idea of prioritizing lower layers above higher layers
// since it releases memory caught up in lower layers, and exerts natural
// backpressure on the higher layer.
//
// Consider the example of a less important long-running single statement OLAP
// query competing with more important small OLTP queries in a single node
// setting. Say the OLAP query starts first and uses up all the KVWork slots,
// and the OLTP queries queue up for the KVWork slots. As the OLAP query
// KVWork completes, it will queue up for SQLKVResponseWork, which will not
// start because the OLTP queries are using up all available KVWork slots. As
// this OLTP KVWork completes, their SQLKVResponseWork will queue up. The
// WorkQueue for SQLKVResponseWork, when granting tokens, will first admit
// those for the more important OLTP queries. This will prevent or slow down
// admission of further work by the OLAP query.
//
// In an ideal world with the only shared resource (across WorkKinds) being
// CPU, and control over the CPU scheduler, we could pool all work, regardless
// of WorkKind into a single queue, and would not need to rely on this
// indirect backpressure and hard-wired ordering. However, we do not have
// control over the CPU scheduler, so we cannot preempt work with widely
// different cpu consumption. Additionally, (non-preemptible) memory is also a
// shared resource, and we wouldn't want to have partially done KVWork not
// finish, due to preemption in the CPU scheduler, since it can be holding
// significant amounts of memory (e.g. in scans).
//
// The aforementioned prioritization also enables us to get instantaneous
// feedback on CPU resource overload. This instantaneous feedback for a grant
// chain (mentioned earlier) happens in two ways:
//   - the chain requires the grantee's goroutine to run.
//   - the cpuOverloadIndicator (see later), specifically the implementation
//     provided by kvSlotAdjuster, provides instantaneous feedback (which is
//     viable only because KVWork is the highest priority).
//
// Weaknesses of this strict prioritization across WorkKinds:
//   - Priority inversion: Lower importance KVWork, not derived from SQL, like
//     GC of MVCC versions, will happen before user-facing SQLKVResponseWork.
//     This is because the backpressure, described in the example above, does
//     not apply to work generated from within the KV layer.
//     TODO(sumeer): introduce a KVLowPriWork and put it last in this ordering,
//     to get over this limitation.
//   - Insufficient competition leading to poor isolation: Putting
//     SQLStatementLeafStartWork, SQLStatementRootStartWork in this list, within
//     the same GrantCoordinator, does provide node overload protection, but not
//     necessarily performance isolation when we have WorkKinds of different
//     importance. Consider the same OLAP example above: if the KVWork slots
//     being full due to the OLAP query prevents SQLStatementRootStartWork for
//     the OLTP queries, the competition is starved out before it has an
//     opportunity to submit any KVWork. Given that control over admitting
//     SQLStatement{Leaf,Root}StartWork is not primarily about CPU control (the
//     lower-level work items are where cpu is consumed), we could decouple
//     these two into a separate GrantCoordinator and only gate them with (high)
//     fixed slot counts that allow for enough competition, plus a memory
//     overload indicator.
//     TODO(sumeer): experiment with this approach.
//   - Continuing the previous bullet, low priority long-lived
//     {SQLStatementLeafStartWork, SQLStatementRootStartWork} could use up all
//     the slots, if there was no high priority work for some period of time,
//     and therefore starve admission of the high priority work when it does
//     appear. The typical solution to this is to put a max on the number of
//     slots low priority can use. This would be viable if we did not allow
//     arbitrary int8 values to be set for Priority.
const (
	// KVWork represents requests submitted to the KV layer, from the same node
	// or a different node. They may originate from the SQL layer or the KV
	// layer.
	KVWork WorkKind = iota
	// SQLKVResponseWork is response processing in SQL for a KV response from a
	// local or remote node. This can be either leaf or root DistSQL work, i.e.,
	// this is inter-layer and not necessarily inter-node.
	SQLKVResponseWork
	// SQLSQLResponseWork is response processing in SQL, for DistSQL RPC
	// responses. This is root work happening in response to leaf SQL work,
	// i.e., it is inter-node.
	SQLSQLResponseWork
	// SQLStatementLeafStartWork represents the start of leaf-level processing
	// for a SQL statement.
	SQLStatementLeafStartWork
	// SQLStatementRootStartWork represents the start of root-level processing
	// for a SQL statement.
	SQLStatementRootStartWork
	numWorkKinds
)

// SafeValue implements the redact.SafeValue interface.
func (WorkKind) SafeValue() {}

// String implements the fmt.Stringer interface.
func (wk WorkKind) String() string {
	switch wk {
	case KVWork:
		return "kv"
	case SQLKVResponseWork:
		return "sql-kv-response"
	case SQLSQLResponseWork:
		return "sql-sql-response"
	case SQLStatementLeafStartWork:
		return "sql-leaf-start"
	case SQLStatementRootStartWork:
		return "sql-root-start"
	default:
		panic(errors.AssertionFailedf("unknown WorkKind"))
	}
}

// QueueKind is used to track the specific WorkQueue an item of KVWork is in.
// The options are one of: "kv-regular-cpu-queue", "kv-elastic-cpu-queue",
// "kv-regular-store-queue", "kv-elastic-store-queue".
//
// It is left empty for SQL types of WorkKind.
type QueueKind string

// SafeValue implements the redact.SafeValue interface.
func (QueueKind) SafeValue() {}

// storeAdmissionStats are stats maintained by a storeRequester. The non-test
// implementation of storeRequester is StoreWorkQueue. StoreWorkQueue updates
// all of these when StoreWorkQueue.AdmittedWorkDone is called, so that these
// cumulative values are mutually consistent.
type storeAdmissionStats struct {
	// Total requests that called {Admitted,Bypassed}WorkDone, or in the case of
	// replicated writes, the requests that called Admit.
	workCount uint64
	// Sum of StoreWorkDoneInfo.WriteBytes.
	//
	// TODO(sumeer): writeAccountedBytes and ingestedAccountedBytes are not
	// actually comparable, since the former is uncompressed. We may need to fix
	// this inaccuracy if it turns out to be an issue.
	writeAccountedBytes uint64
	// Sum of StoreWorkDoneInfo.IngestedBytes.
	ingestedAccountedBytes uint64
	// statsToIgnore represents stats that we should exclude from token
	// consumption, and estimation of per-work-tokens. Currently, this is
	// limited to range snapshot ingestion. These are likely to usually land in
	// levels lower than L0, so may not fit the existing per-work-tokens model
	// well. Additionally, we do not want large range snapshots to consume a
	// huge number of tokens (see
	// https://github.com/cockroachdb/cockroach/pull/80914 for justification --
	// that PR is closer to the final solution, and this is a step in that
	// direction).
	statsToIgnore struct {
		// Stats for ingests.
		ingestStats pebble.IngestOperationStats
		// Stats for regular writes. These roughly correspond to what the writes
		// will turn into when written to a flushed sstable.
		writeBytes uint64
	}
	// aboveRaftStats is a subset of the top-level storeAdmissionStats that
	// represents admission that happened above-raft for which we deducted
	// tokens prior to proposal evaluation. Replication admission/flow control
	// happens below-raft, so those stats are *not* here. Only above-raft
	// request stats are used to estimate tokens to deduct prior to evaluation.
	// Since large write requests (often ingested) use the below-raft admission
	// path as part of replication admission control, not ignoring such requests
	// inflates estimates and results in under-admission. See
	// https://github.com/cockroachdb/cockroach/issues/113711.
	aboveRaftStats struct {
		workCount              uint64
		writeAccountedBytes    uint64
		ingestedAccountedBytes uint64
	}
	// aux represents additional information carried for informational purposes
	// (e.g. for logging).
	aux struct {
		// These bypassed numbers are already included in the corresponding
		// {workCount, writeAccountedBytes, ingestedAccountedBytes}. These are a
		// subset of the below-raft stats (those that were not subject to
		// replication admission control).
		bypassedCount                  uint64
		writeBypassedAccountedBytes    uint64
		ingestedBypassedAccountedBytes uint64
	}
}

// storeRequestEstimates are estimates that the storeRequester should use for
// its future requests.
type storeRequestEstimates struct {
	// writeTokens is the tokens to request at admission time. Must be > 0.
	writeTokens int64
}

// PacerFactory is used to construct a new admission.Pacer.
type PacerFactory interface {
	NewPacer(unit time.Duration, wi WorkInfo) *Pacer
}

var _ PacerFactory = &ElasticCPUGrantCoordinator{}
