// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// requester is an interface implemented by an object that orders admission
// work for a particular WorkKind. See AdmissionQueue for the implementation
// of requester.
type requester interface {
	// hasWaitingRequests returns whether there are any waiting/queued requests
	// of this WorkKind.
	hasWaitingRequests() bool
	// granted returns true if the grant was accepted, else returns false. A
	// grant may not be accepted if the grant raced with request cancellation
	// and there are now no waiting requests.
	granted() bool
}

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
type grantKind int8

const (
	slot grantKind = iota
	token
)

// granter is paired with a requester in that a requester for a particular
// WorkKind will interact with a granter. See doc.go for an overview of how
// this fits into the overall structure.
type granter interface {
	grantKind() grantKind
	// tryGet is used by a requester to get a slot/token for a piece of work
	// that has encountered no waiting/queued work. This is the fast path that
	// avoids queueing in the requester.
	tryGet() bool
	// returnGrant is called for returning slots after use, and used for
	// returning either slots or tokens when the grant raced with the work being
	// cancelled, and the grantee did not end up doing any work. The latter case
	// occurs despite the bool return value on the requester.granted method --
	// it is possible that the work was not cancelled at the time when
	// requester.grant was called, and hence returned true, but later when the
	// goroutine doing the work noticed that it had been granted, there is a
	// possibility that that raced with cancellation.
	returnGrant()
	// tookWithoutPermission informs the granter that a slot/token was taken
	// unilaterally, without permission. Currently we only implement this for
	// slots, since only KVWork is allowed to bypass admission control for high
	// priority internal activities (e.g. node liveness) and for KVWork that
	// generates other KVWork (like intent resolution of discovered intents).
	// Not bypassing for the latter could result in single node or distributed
	// deadlock, and since such work is typically not a major (on average)
	// consumer of resources, we consider bypassing to be acceptable.
	tookWithoutPermission()
	// continueGrantChain is called by the requester at some point after grant
	// was called on the requester. The expectation is that this is called by
	// the grantee after its goroutine runs and notices that it has been granted
	// a slot/token. This provides a natural throttling that reduces grant
	// bursts by taking into immediate account the capability of the goroutine
	// scheduler to schedule such work.
	//
	// In an experiment, using such grant chains reduced burstiness of grants by
	// 5x and shifted ~2s of latency (at p99) from the scheduler into admission
	// control (which is desirable since the latter is where we can
	// differentiate between work).
	// TODO(sumeer): the above experiments used grant chains only for
	// non-KVWork. It is possible that grant chains could under-utilize CPU. Run
	// more experiments.
	continueGrantChain()
}

// WorkKind represents various types of work that are subject to admission
// control.
type WorkKind int8

// The list of WorkKinds are ordered from lower level to higher level,
// and also serves as a hard-wired ordering from most important to least important.
//
// KVWork, SQLKVResponseWork, SQLSQLResponseWork are the lower-level work
// units that are expected to be primarily CPU bound (with disk IO for KVWork,
// but cache hit rates are typically high), and expected to be where most of
// the CPU consumption happens. These are prioritized in the order
//   KVWork > SQLKVResponseWork > SQLSQLResponseWork
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
//   SQLStatementLeafStartWork > SQLStatementRootStartWork
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
// AdmissionQueue for SQLKVResponseWork, when granting tokens, will first
// admit those for the more important OLTP queries. This will prevent or slow
// down admission of further work by the OLAP query.
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
// - the chain requires the grantee's goroutine to run.
// - the cpuOverloadIndicator (see later), specifically the implementation
//   provided by kvSlotAdjuster, provides instantaneous feedback (which is
//   viable only because KVWork is the highest priority).
//
// Weaknesses of this strict prioritization across WorkKinds:
// - Priority inversion: Lower importance KVWork, not derived from SQL, like
//   GC of MVCC versions, will happen before user-facing SQLKVResponseWork.
//   This is because the backpressure, described in the example above, does
//   not apply to work generated from within the KV layer.
//   TODO(sumeer): introduce a KVLowPriWork and put it last in this ordering,
//   to get over this limitation.
// - Insufficient competition leading to poor isolation: Putting
//   SQLStatementLeafStartWork, SQLStatementRootStartWork in this list, within
//   the same GrantCoordinator, does provide node overload protection, but not
//   necessarily performance isolation when we have WorkKinds of different
//   importance. Consider the same OLAP example above: if the KVWork slots
//   being full due to the OLAP query prevents SQLStatementRootStartWork for
//   the OLTP queries, the competition is starved out before it has an
//   opportunity to submit any KVWork. Given that control over admitting
//   SQLStatement{Leaf,Root}StartWork is not primarily about CPU control (the
//   lower-level work items are where cpu is consumed), we could decouple
//   these two into a separate GrantCoordinator and only gate them with (high)
//   fixed slot counts that allow for enough competition, plus a memory
//   overload indicator.
//   TODO(sumeer): experiment with this approach.
// - Continuing the previous bullet, low priority long-lived
//   {SQLStatementLeafStartWork, SQLStatementRootStartWork} could use up all
//   the slots, if there was no high priority work for some period of time,
//   and therefore starve admission of the high priority work when it does
//   appear. The typical solution to this is to put a max on the number of
//   slots low priority can use. This would be viable if we did not allow
//   arbitrary int8 values to be set for WorkPriority.

const (
	// KVWork represents requests submitted to the KV layer, from the same node
	// or a different node. They may originate from the SQL layer or the KV
	// layer.
	KVWork WorkKind = iota
	// SQLKVResponseWork is response processing in SQL for a KV response from a
	// local or remote node. This can be either leaf or root DistSQL work, i.e.,
	// this is inter-layer and not necessarily inter-node.
	SQLKVResponseWork
	// SQLSQLResponseWork is response processing in SQL, for SQL RPC responses.
	// This is root work happening in response to leaf SQL work, i.e., it is
	// inter-node.
	SQLSQLResponseWork
	// SQLStatementLeafStartWork represents the start of leaf-level processing
	// for a SQL statement.
	SQLStatementLeafStartWork
	// SQLStatementRootStartWork represents the start of root-level processing
	// for a SQL statement.
	SQLStatementRootStartWork
	numWorkKinds
)

func workKindString(workKind WorkKind) redact.RedactableString {
	switch workKind {
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

type grantResult int8

const (
	grantSuccess grantResult = iota
	// grantFailDueToSharedResource is returned when the granter is unable to
	// grant because a shared resource (CPU or memory) is overloaded. For grant
	// chains, this is a signal to terminate.
	grantFailDueToSharedResource
	// grantFailLocal is returned when the granter is unable to grant due to a
	// local constraint -- insufficient tokens or slots.
	grantFailLocal
)

// granterWithLockedCalls is an extension of the granter and requester
// interfaces that is used as an internal implementation detail of the
// GrantCoordinator.
type granterWithLockedCalls interface {
	granter
	requester
	// tryGetLocked is the real implementation of tryGet in the granter interface.
	// Additionally, it is also used when continuing a grant chain.
	tryGetLocked() grantResult
	// returnGrantLocked is the real implementation of returnGrant.
	returnGrantLocked()
	// tookWithoutPermissionLocked is the real implementation of
	// tookWithoutPermission.
	tookWithoutPermissionLocked()
}

// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord    *GrantCoordinator
	workKind WorkKind
	requester
	usedSlots  int
	totalSlots int

	// Optional. Nil for a slotGranter used for KVWork since the slots for that
	// slotGranter are directly adjusted by the kvSlotAdjuster (using the
	// kvSlotAdjuster here would provide a redundant identical signal).
	cpuOverload cpuOverloadIndicator
	// TODO(sumeer): Add an optional overload indicator for memory, that will be
	// relevant for SQLStatementLeafStartWork and SQLStatementRootStartWork.

	usedSlotsMetric *metric.Gauge
}

var _ granterWithLockedCalls = &slotGranter{}

func (sg *slotGranter) grantKind() grantKind {
	return slot
}

func (sg *slotGranter) tryGet() bool {
	return sg.coord.tryGet(sg.workKind)
}

func (sg *slotGranter) tryGetLocked() grantResult {
	if sg.cpuOverload != nil && sg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if sg.usedSlots < sg.totalSlots {
		sg.usedSlots++
		sg.usedSlotsMetric.Update(int64(sg.usedSlots))
		return grantSuccess
	}
	if sg.workKind == KVWork {
		return grantFailDueToSharedResource
	}
	return grantFailLocal
}

func (sg *slotGranter) returnGrant() {
	sg.coord.returnGrant(sg.workKind)
}

func (sg *slotGranter) returnGrantLocked() {
	sg.usedSlots--
	if sg.usedSlots < 0 {
		panic(errors.AssertionFailedf("used slots is negative %d", sg.usedSlots))
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

func (sg *slotGranter) tookWithoutPermission() {
	sg.coord.tookWithoutPermission(sg.workKind)
}

func (sg *slotGranter) tookWithoutPermissionLocked() {
	sg.usedSlots++
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

func (sg *slotGranter) continueGrantChain() {
	sg.coord.continueGrantChain(sg.workKind)
}

// tokenGranter implements granterWithLockedCalls.
type tokenGranter struct {
	coord    *GrantCoordinator
	workKind WorkKind
	requester
	availableBurstTokens int
	maxBurstTokens       int
	// Optional. Practically, both uses of tokenGranter, for SQLKVResponseWork
	// and SQLSQLResponseWork have a non-nil value. We don't expect to use
	// memory overload indicators here since memory accounting and disk spilling
	// is what should be tasked with preventing OOMs, and we want to finish
	// processing this lower-level work.
	cpuOverload cpuOverloadIndicator
}

var _ granterWithLockedCalls = &tokenGranter{}

func (tg *tokenGranter) refillBurstTokens() {
	tg.availableBurstTokens = tg.maxBurstTokens
}

func (tg *tokenGranter) grantKind() grantKind {
	return token
}

func (tg *tokenGranter) tryGet() bool {
	return tg.coord.tryGet(tg.workKind)
}

func (tg *tokenGranter) tryGetLocked() grantResult {
	if tg.cpuOverload != nil && tg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if tg.availableBurstTokens > 0 {
		tg.availableBurstTokens--
		return grantSuccess
	}
	return grantFailLocal
}

func (tg *tokenGranter) returnGrant() {
	tg.coord.returnGrant(tg.workKind)
}

func (tg *tokenGranter) returnGrantLocked() {
	tg.availableBurstTokens++
	if tg.availableBurstTokens > tg.maxBurstTokens {
		tg.availableBurstTokens = tg.maxBurstTokens
	}
}

func (tg *tokenGranter) tookWithoutPermission() {
	panic(errors.AssertionFailedf("unimplemented"))
}

func (tg *tokenGranter) tookWithoutPermissionLocked() {
	panic(errors.AssertionFailedf("unimplemented"))
}

func (tg *tokenGranter) continueGrantChain() {
	tg.coord.continueGrantChain(tg.workKind)
}

// GrantCoordinator is the top-level object that coordinates grants.
// See detailed comment at the top of the file.
type GrantCoordinator struct {
	// mu is ordered before any mutex acquired in a requester implementation.
	mu syncutil.Mutex
	// NB: Some granters can be nil.
	granters [numWorkKinds]granterWithLockedCalls
	// The AdmissionQueues behaving as requesters in each
	// granterWithLockedCalls. This is kept separately only to service
	// GetAdmissionQueue calls.
	queues               [numWorkKinds]*AdmissionQueue
	cpuOverloadIndicator cpuOverloadIndicator
	cpuLoadListener      CPULoadListener
	grantChainActive     bool
	// forceTerminateGrantChain implies we should start another since the
	// previous one did not die a natural death.
	forceTerminateGrantChain bool
	// Index into granters.
	grantChainIndex int
}

var _ CPULoadListener = &GrantCoordinator{}

type Options struct {
	MinCPUSlots                    int
	MaxCPUSlots                    int
	SQLKVResponseBurstTokens       int
	SQLSQLResponseBurstTokens      int
	SQLStatementLeafStartWorkSlots int
	SQLStatementRootStartWorkSlots int
}

// NewGrantCoordinator constructs a GrantCoordinator and AdmissionQueues for a
// regular cluster node. Caller is responsible for hooking this up to receive
// calls to CPULoad.
func NewGrantCoordinator(opts Options) (*GrantCoordinator, []metric.Struct) {
	metrics := MakeGranterMetrics()
	metricStructs := append([]metric.Struct(nil), metrics)
	kvSlotAdjuster := &kvSlotAdjuster{
		minCPUSlots:      opts.MinCPUSlots,
		maxCPUSlots:      opts.MaxCPUSlots,
		totalSlotsMetric: metrics.KVTotalSlots,
	}
	coord := &GrantCoordinator{
		cpuOverloadIndicator: kvSlotAdjuster,
		cpuLoadListener:      kvSlotAdjuster,
	}

	sg := &slotGranter{
		coord:           coord,
		workKind:        KVWork,
		totalSlots:      opts.MinCPUSlots,
		usedSlotsMetric: metrics.KVUsedSlots,
	}
	kvSlotAdjuster.granter = sg
	coord.queues[KVWork] = MakeAdmissionQueue(
		KVWork, sg, false /* usesTokens */, true /* tiedToRange */)
	sg.requester = coord.queues[KVWork]
	coord.granters[KVWork] = sg

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	coord.queues[SQLKVResponseWork] = MakeAdmissionQueue(
		SQLKVResponseWork, tg, true /* usesTokens */, false /* tiedToRange */)
	tg.requester = coord.queues[SQLKVResponseWork]
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	coord.queues[SQLSQLResponseWork] = MakeAdmissionQueue(
		SQLSQLResponseWork, tg, true /* usesTokens */, false /* tiedToRange */)
	tg.requester = coord.queues[SQLSQLResponseWork]
	coord.granters[SQLSQLResponseWork] = tg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	coord.queues[SQLStatementLeafStartWork] = MakeAdmissionQueue(
		SQLStatementLeafStartWork, sg, false /* usesTokens */, false /* tiedToRange */)
	sg.requester = coord.queues[SQLStatementLeafStartWork]
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	coord.queues[SQLStatementRootStartWork] = MakeAdmissionQueue(
		SQLStatementRootStartWork, sg, false /* usesTokens */, false /* tiedToRange */)
	sg.requester = coord.queues[SQLStatementRootStartWork]
	coord.granters[SQLStatementRootStartWork] = sg

	return coord, appendMetricStructs(metricStructs, coord)
}

// NewGrantCoordinatorServerlessKV constructs a GrantCoordinator and
// AdmissionQueues for a serverless KV node. Caller is responsible for hooking
// this up to receive calls to CPULoad.
func NewGrantCoordinatorServerlessKV(opts Options) (*GrantCoordinator, []metric.Struct) {
	metrics := MakeGranterMetrics()
	metricStructs := append([]metric.Struct(nil), metrics)
	kvSlotAdjuster := &kvSlotAdjuster{
		minCPUSlots:      opts.MinCPUSlots,
		maxCPUSlots:      opts.MaxCPUSlots,
		totalSlotsMetric: metrics.KVTotalSlots,
	}
	coord := &GrantCoordinator{
		cpuOverloadIndicator: kvSlotAdjuster,
		cpuLoadListener:      kvSlotAdjuster,
	}

	sg := &slotGranter{
		coord:           coord,
		workKind:        KVWork,
		totalSlots:      opts.MinCPUSlots,
		usedSlotsMetric: metrics.KVUsedSlots,
	}
	kvSlotAdjuster.granter = sg
	coord.queues[KVWork] = MakeAdmissionQueue(
		KVWork, sg, false /* usesTokens */, true /* tiedToRange */)
	sg.requester = coord.queues[KVWork]
	coord.granters[KVWork] = sg

	return coord, appendMetricStructs(metricStructs, coord)
}

// NewGrantCoordinatorServerlessSQL constructs a GrantCoordinator and
// AdmissionQueues for a serverless SQL node. Caller is responsible for
// hooking this up to receive calls to CPULoad.
func NewGrantCoordinatorServerlessSQL(opts Options) (*GrantCoordinator, []metric.Struct) {
	metrics := MakeGranterMetrics()
	metricStructs := append([]metric.Struct(nil), metrics)
	sqlNodeCPU := &sqlNodeCPUOverloadIndicator{}
	coord := &GrantCoordinator{
		cpuOverloadIndicator: sqlNodeCPU,
		cpuLoadListener:      sqlNodeCPU,
	}

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          sqlNodeCPU,
	}
	coord.queues[SQLKVResponseWork] = MakeAdmissionQueue(
		SQLKVResponseWork, tg, true /* usesTokens */, false /* tiedToRange */)
	tg.requester = coord.queues[SQLKVResponseWork]
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          sqlNodeCPU,
	}
	coord.queues[SQLSQLResponseWork] = MakeAdmissionQueue(
		SQLSQLResponseWork, tg, true /* usesTokens */, false /* tiedToRange */)
	tg.requester = coord.queues[SQLSQLResponseWork]
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	coord.queues[SQLStatementLeafStartWork] = MakeAdmissionQueue(
		SQLStatementLeafStartWork, sg, false /* usesTokens */, false /* tiedToRange */)
	sg.requester = coord.queues[SQLStatementLeafStartWork]
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	coord.queues[SQLStatementRootStartWork] = MakeAdmissionQueue(
		SQLStatementRootStartWork, sg, false /* usesTokens */, false /* tiedToRange */)
	sg.requester = coord.queues[SQLStatementRootStartWork]
	coord.granters[SQLStatementRootStartWork] = sg

	return coord, appendMetricStructs(metricStructs, coord)
}

func appendMetricStructs(ms []metric.Struct, coord *GrantCoordinator) []metric.Struct {
	for i := range coord.queues {
		if coord.queues[i] != nil {
			ms = append(ms, coord.queues[i].metrics)
		}
	}
	return ms
}

func (coord *GrantCoordinator) GetAdmissionQueue(workKind WorkKind) *AdmissionQueue {
	return coord.queues[workKind]
}

// CPULoad implements CPULoadListener and is called every 1ms. The same
// frequency is used for refilling the burst tokens since synchronizing the
// two means that the refilled burst can take into account the latest
// schedulers stats (indirectly, via the implementation of
// cpuOverloadIndicator).
// TODO(sumeer): after experimentation, possibly generalize the 1ms ticks used
// for CPULoad.
func (coord *GrantCoordinator) CPULoad(runnable int, procs int) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.cpuLoadListener.CPULoad(runnable, procs)
	coord.granters[SQLKVResponseWork].(*tokenGranter).refillBurstTokens()
	coord.granters[SQLSQLResponseWork].(*tokenGranter).refillBurstTokens()
	if coord.grantChainActive {
		coord.forceTerminateGrantChain = true
		return
	}
	coord.tryGrant()
}

func (coord *GrantCoordinator) tryGet(workKind WorkKind) bool {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	// It is possible that a grant chain is active, and has not yet made its way
	// to this workKind. So it may be more reasonable to queue. But we have some
	// concerns about incurring the delay of multiple goroutine context switches
	// so we ignore this case.
	res := coord.granters[workKind].tryGetLocked()
	switch res {
	case grantSuccess:
		// Grant chain may be active, but it did not get in the way of this grant,
		// and the effect of this grant in terms of overload will be felt by the
		// grant chain.
		return true
	case grantFailDueToSharedResource:
		// This could be a transient overload, that may not be noticed by the
		// grant chain. We don't want it to continue granting to lower priority
		// WorkKinds, while a higher priority one is waiting, so we terminate it.
		if coord.grantChainActive && coord.grantChainIndex >= int(workKind) {
			coord.forceTerminateGrantChain = true
		}
		return false
	case grantFailLocal:
		return false
	default:
		panic(errors.AssertionFailedf("unknown case"))
	}
}

func (coord *GrantCoordinator) returnGrant(workKind WorkKind) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.granters[workKind].returnGrantLocked()
	if coord.grantChainActive {
		if coord.grantChainIndex > int(workKind) && coord.granters[workKind].hasWaitingRequests() {
			// There are waiting requests that will not be served by the grant chain.
			// Better to terminate it and start afresh.
			coord.forceTerminateGrantChain = true
		}
		// Else either the grant chain will get to this workKind, or there are no waiting requests.
		return
	}
	coord.tryGrant()
}

func (coord *GrantCoordinator) tookWithoutPermission(workKind WorkKind) {
	coord.granters[workKind].tookWithoutPermissionLocked()
}

func (coord *GrantCoordinator) continueGrantChain(workKind WorkKind) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if coord.forceTerminateGrantChain {
		coord.forceTerminateGrantChain = false
		coord.grantChainActive = false
	}
	coord.tryGrant()
}

func (coord *GrantCoordinator) tryGrant() {
	if !coord.grantChainActive {
		coord.grantChainIndex = 0
	}
	coord.grantChainActive = false
	for i := coord.grantChainIndex; i < len(coord.granters); i++ {
		localDone := false
		for coord.granters[i].hasWaitingRequests() && !localDone {
			res := coord.granters[i].tryGetLocked()
			switch res {
			case grantSuccess:
				if !coord.granters[i].granted() {
					coord.granters[i].returnGrantLocked()
				} else {
					coord.grantChainActive = true
					return
				}
			case grantFailDueToSharedResource:
				return
			case grantFailLocal:
				localDone = true
			}
		}
	}
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
// In serverless settings, for SQL nodes, which do not do KV work, we do not
// have an instantaneous indicator and instead use
// sqlNodeCPUOverloadIndicator.
type cpuOverloadIndicator interface {
	isOverloaded() bool
}

// CPULoadListener listens to the latest CPU load information. Currently we
// expect this to be called every 1ms.
// TODO(sumeer): experiment with more smoothing.
type CPULoadListener interface {
	CPULoad(runnable int, procs int)
}

// kvSlotAdjuster is an implementer of CPULoadListener and
// cpuOverloadIndicator.
type kvSlotAdjuster struct {
	// This is the slotGranter used for KVWork. In non-serverless settings, it
	// is the only one we adjust using the periodic cpu overload signal. We
	// don't adjust slots for SQLStatementLeafStartWork and
	// SQLStatementRootStartWork using the periodic cpu overload signal since:
	// - these are potentially long-lived work items and not CPU bound
	// - we don't know how to coordinate adjustment of those slots and the KV
	//   slots.
	granter     *slotGranter
	minCPUSlots int
	maxCPUSlots int

	totalSlotsMetric *metric.Gauge

	// TODO(sumeer): also compute slots for disk IO and use min(totalCPUSlots,
	// totalIOSlots) to configure granter. In this setting the
	// cpuOverloadIndicator.isOverloaded implementation will compare usedSlots
	// >= totalCPUSlots. That is, if totalIOSlots < totalCPUSlots, which means
	// KV is constrained by IO, the isOverloaded signal will be false, and will
	// therefore not constrain admission of non-KVWork. If this lack of
	// constraint on non-KVWork overloads the CPU, totalCPUSlots will be
	// decreased, and eventually totalCPUSlots could become <= totalIOSlots and
	// the isOverloaded signal will become true.
}

var _ cpuOverloadIndicator = &kvSlotAdjuster{}
var _ CPULoadListener = &kvSlotAdjuster{}

func (kvsa *kvSlotAdjuster) CPULoad(runnable int, procs int) {
	// Simple heuristic, which worked ok in experiments. More sophisticated ones
	// could be devised.
	// TODO(sumeer): add some configurability after more experimentation.
	if runnable >= procs {
		// Overload.
		// If using some slots, and the used slots is less than the total slots,
		// and total slots hasn't bottomed out at the min, decrease the total
		// slots. If currently using more than the total slots, it suggests that
		// the previous slot reduction has not taken effect yet, so we hold off on
		// further decreasing.
		if kvsa.granter.usedSlots > 0 && kvsa.granter.totalSlots > kvsa.minCPUSlots &&
			kvsa.granter.usedSlots <= kvsa.granter.totalSlots {
			kvsa.granter.totalSlots--
		}
	} else if float64(runnable) <= float64(procs/2) {
		// Underload.
		// Used all its slots and can increase further, so additive increase.
		if kvsa.granter.usedSlots >= kvsa.granter.totalSlots &&
			kvsa.granter.totalSlots < kvsa.maxCPUSlots {
			kvsa.granter.totalSlots++
		}
	}
	kvsa.totalSlotsMetric.Update(int64(kvsa.granter.totalSlots))
}

func (kvsa *kvSlotAdjuster) isOverloaded() bool {
	return kvsa.granter.usedSlots >= kvsa.granter.totalSlots
}

// sqlNodeCPUOverloadIndicator is the implementation of cpuOverloadIndicator
// for a SQL node. This has to rely on the periodic load information from the
// cpu scheduler and will therefore be tuned towards indicating overload at
// higher overload points (otherwise we could fluctuate into underloaded
// territory due to restricting admission, and not be work conserving). Such
// tuning towards more overload, and therefore more queueing inside the
// scheduler, is somewhat acceptable since a SQL node is not multi-tenant.
//
// TODO(sumeer): implement.
type sqlNodeCPUOverloadIndicator struct {
}

var _ cpuOverloadIndicator = &sqlNodeCPUOverloadIndicator{}
var _ CPULoadListener = &sqlNodeCPUOverloadIndicator{}

func (sn *sqlNodeCPUOverloadIndicator) CPULoad(runnable int, procs int) {
}

func (sn *sqlNodeCPUOverloadIndicator) isOverloaded() bool {
	return false
}

var (
	totalSlots = metric.Metadata{
		Name:        "admission.granter.total_slots.kv",
		Help:        "Total slots for kv work",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSlots = metric.Metadata{
		Name:        "admission.granter.used_slots.",
		Help:        "Used slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
)

type GranterMetrics struct {
	KVTotalSlots          *metric.Gauge
	KVUsedSlots           *metric.Gauge
	SQLLeafStartUsedSlots *metric.Gauge
	SQLRootStartUsedSlots *metric.Gauge
}

func (GranterMetrics) MetricStruct() {}

func MakeGranterMetrics() GranterMetrics {
	m := GranterMetrics{
		KVTotalSlots: metric.NewGauge(totalSlots),
		KVUsedSlots:  metric.NewGauge(addName(string(workKindString(KVWork)), usedSlots)),
		SQLLeafStartUsedSlots: metric.NewGauge(
			addName(string(workKindString(SQLStatementLeafStartWork)), usedSlots)),
		SQLRootStartUsedSlots: metric.NewGauge(
			addName(string(workKindString(SQLStatementRootStartWork)), usedSlots)),
	}
	return m
}

// TODO(sumeer): experiment with approaches to adjust slots for
// SQLStatementLeafStartWork and SQLStatementRootStartWork for SQL nodes in
// serverless settings. Note that for these WorkKinds we are currently setting
// very high slot counts since we rely on other signals like memory and
// cpuOverloadIndicator to gate admission. One could debate whether we should
// be using rate limiting instead of counting slots for such work. The only
// reason the above code uses the term "slot" for these is that we have a
// completion indicator, and when we do have such an indicator it can be
// beneficial to be able to keep track of how many ongoing work items we have.
