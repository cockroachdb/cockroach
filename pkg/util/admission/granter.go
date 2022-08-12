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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
)

// KVSlotAdjusterOverloadThreshold sets a goroutine runnable threshold at
// which the CPU will be considered overloaded, when running in a node that
// executes KV operations.
var KVSlotAdjusterOverloadThreshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"admission.kv_slot_adjuster.overload_threshold",
	"when the number of runnable goroutines per CPU is greater than this threshold, the "+
		"slot adjuster considers the cpu to be overloaded",
	32, settings.PositiveInt)

// EnabledSoftSlotGranting can be set to false to disable soft slot granting.
var EnabledSoftSlotGranting = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"admission.soft_slot_granting.enabled",
	"soft slot granting is disabled if this setting is set to false",
	true,
)

// grantChainID is the ID for a grant chain. See continueGrantChain for
// details.
type grantChainID uint64

// noGrantChain is a sentinel value representing that the grant is not
// responsible for continuing a grant chain. It is only used internally in
// this file -- requester implementations do not need to concern themselves
// with this value.
var noGrantChain grantChainID = 0

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

type requesterClose interface {
	close()
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

// granter is paired with a requester in that a requester for a particular
// WorkKind will interact with a granter. See doc.go for an overview of how
// this fits into the overall structure.
type granter interface {
	grantKind() grantKind
	// tryGet is used by a requester to get slots/tokens for a piece of work
	// that has encountered no waiting/queued work. This is the fast path that
	// avoids queueing in the requester.
	//
	// REQUIRES: count > 0. count == 1 for slots.
	tryGet(count int64) bool
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
	// done -- that should be done via granterWithStoreWriteDone.storeWriteDone.
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
	// done -- that should be done via granterWithStoreWriteDone.storeWriteDone.
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
//   arbitrary int8 values to be set for Priority.

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
	// grantFailLocal is returned when the granter is unable to grant due to (a)
	// a local constraint -- insufficient tokens or slots, or (b) no work is
	// waiting.
	grantFailLocal
)

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

// For the cpu-bound slot case we have background activities (like Pebble
// compactions) that would like to utilize additional slots if available (e.g.
// to do concurrent compression of ssblocks). These activities do not want to
// wait for a slot, since they can proceed without the slot at their usual
// slower pace (e.g. without doing concurrent compression). They also are
// sensitive to small overheads in their tight loops, and cannot afford the
// overhead of interacting with admission control at a fine granularity (like
// asking for a slot when compressing each ssblock). A coarse granularity
// interaction causes a delay in returning slots to admission control, and we
// don't want that delay to cause admission delay for normal work. Hence, we
// model slots granted to background activities as "soft-slots". Think of
// regular used slots as "hard-slots", in that we assume that the holder of
// the slot is still "using" it, while a soft-slot is "squishy" and in some
// cases we can pretend that it is not being used. Say we are allowed
// to allocate up to M slots. In this scheme, when allocating a soft-slot
// one must conform to usedSoftSlots+usedSlots <= M, and when allocating
// a regular (hard) slot one must conform to usedSlots <= M.
//
// That is, soft-slots allow for over-commitment until the soft-slots are
// returned, which may mean some additional queueing in the goroutine
// scheduler.
//
// We have another wrinkle in that we do not want to maintain a single M. For
// these optional background activities we desire to do them only when the
// load is low enough. This is because at high load, all work suffers from
// additional queueing in the goroutine scheduler. So we want to make sure
// regular work does not suffer such goroutine scheduler queueing because we
// granted too many soft-slots and caused CPU utilization to be high. So we
// maintain two kinds of M, totalHighLoadSlots and totalModerateLoadSlots.
// totalHighLoadSlots are estimated so as to allow CPU utilization to be high,
// while totalModerateLoadSlots are trying to keep queuing in the goroutine
// scheduler to a lower level. So the revised equations for allocation are:
// - Allocating a soft-slot: usedSoftSlots+usedSlots <= totalModerateLoadSlots
// - Allocating a regular slot: usedSlots <= totalHighLoadSlots
//
// NB: we may in the future add other kinds of background activities that do
// not have a lag in interacting with admission control, but want to schedule
// them only under moderate load. Those activities will be counted in
// usedSlots but when granting a slot to such an activity, the equation will
// be usedSoftSlots+usedSlots <= totalModerateLoadSlots.
//
// That is, let us not confuse that moderate load slot allocation is only for
// soft-slots. Soft-slots are introduced only for squishiness.
//
// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord                  *GrantCoordinator
	workKind               WorkKind
	requester              requester
	usedSlots              int
	usedSoftSlots          int
	totalHighLoadSlots     int
	totalModerateLoadSlots int
	skipSlotEnforcement    bool

	// Optional. Nil for a slotGranter used for KVWork since the slots for that
	// slotGranter are directly adjusted by the kvSlotAdjuster (using the
	// kvSlotAdjuster here would provide a redundant identical signal).
	cpuOverload cpuOverloadIndicator

	usedSlotsMetric     *metric.Gauge
	usedSoftSlotsMetric *metric.Gauge
}

var _ granterWithLockedCalls = &slotGranter{}
var _ granter = &slotGranter{}

// grantKind implements granter.
func (sg *slotGranter) grantKind() grantKind {
	return slot
}

// tryGet implements granter.
func (sg *slotGranter) tryGet(count int64) bool {
	return sg.coord.tryGet(sg.workKind, count, 0 /*arbitrary*/)
}

// tryGetLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGetLocked(count int64, _ int8) grantResult {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.cpuOverload != nil && sg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if sg.usedSlots < sg.totalHighLoadSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
		sg.usedSlotsMetric.Update(int64(sg.usedSlots))
		return grantSuccess
	}
	if sg.workKind == KVWork {
		return grantFailDueToSharedResource
	}
	return grantFailLocal
}

// returnGrant implements granter.
func (sg *slotGranter) returnGrant(count int64) {
	sg.coord.returnGrant(sg.workKind, count, 0 /*arbitrary*/)
}

func (sg *slotGranter) tryGetSoftSlots(count int) int {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	spareModerateLoadSlots := sg.totalModerateLoadSlots - sg.usedSoftSlots - sg.usedSlots
	if spareModerateLoadSlots <= 0 {
		return 0
	}
	allocatedSlots := count
	if allocatedSlots > spareModerateLoadSlots {
		allocatedSlots = spareModerateLoadSlots
	}
	sg.usedSoftSlots += allocatedSlots
	sg.usedSoftSlotsMetric.Update(int64(sg.usedSoftSlots))
	return allocatedSlots
}

func (sg *slotGranter) returnSoftSlots(count int) {
	sg.coord.mu.Lock()
	defer sg.coord.mu.Unlock()
	sg.usedSoftSlots -= count
	sg.usedSoftSlotsMetric.Update(int64(sg.usedSoftSlots))
	if sg.usedSoftSlots < 0 {
		panic("used soft slots is negative")
	}
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) returnGrantLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	sg.usedSlots--
	if sg.usedSlots < 0 {
		panic(errors.AssertionFailedf("used slots is negative %d", sg.usedSlots))
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

// tookWithoutPermission implements granter.
func (sg *slotGranter) tookWithoutPermission(count int64) {
	sg.coord.tookWithoutPermission(sg.workKind, count, 0 /*arbitrary*/)
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (sg *slotGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	sg.usedSlots++
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

// continueGrantChain implements granter.
func (sg *slotGranter) continueGrantChain(grantChainID grantChainID) {
	sg.coord.continueGrantChain(sg.workKind, grantChainID)
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (sg *slotGranter) requesterHasWaitingRequests() bool {
	return sg.requester.hasWaitingRequests()
}

// tryGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := sg.tryGetLocked(1, 0 /*arbitrary*/)
	if res == grantSuccess {
		slots := sg.requester.granted(grantChainID)
		if slots == 0 {
			// Did not accept grant.
			sg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if slots != 1 {
			panic(errors.AssertionFailedf("unexpected count %d", slots))
		}
	}
	return res
}

// tokenGranter implements granterWithLockedCalls.
type tokenGranter struct {
	coord                *GrantCoordinator
	workKind             WorkKind
	requester            requester
	availableBurstTokens int64
	maxBurstTokens       int64
	skipTokenEnforcement bool
	// Optional. Practically, both uses of tokenGranter, for SQLKVResponseWork
	// and SQLSQLResponseWork have a non-nil value. We don't expect to use
	// memory overload indicators here since memory accounting and disk spilling
	// is what should be tasked with preventing OOMs, and we want to finish
	// processing this lower-level work.
	cpuOverload cpuOverloadIndicator
}

var _ granterWithLockedCalls = &tokenGranter{}
var _ granter = &tokenGranter{}

func (tg *tokenGranter) refillBurstTokens(skipTokenEnforcement bool) {
	tg.availableBurstTokens = tg.maxBurstTokens
	tg.skipTokenEnforcement = skipTokenEnforcement
}

// grantKind implements granter.
func (tg *tokenGranter) grantKind() grantKind {
	return token
}

// tryGet implements granter.
func (tg *tokenGranter) tryGet(count int64) bool {
	return tg.coord.tryGet(tg.workKind, count, 0 /*arbitrary*/)
}

// tryGetLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tryGetLocked(count int64, _ int8) grantResult {
	if tg.cpuOverload != nil && tg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if tg.availableBurstTokens > 0 || tg.skipTokenEnforcement {
		tg.availableBurstTokens -= count
		return grantSuccess
	}
	return grantFailLocal
}

// returnGrant implements granter.
func (tg *tokenGranter) returnGrant(count int64) {
	tg.coord.returnGrant(tg.workKind, count, 0 /*arbitrary*/)
}

// returnGrantLocked implements granterWithLockedCalls.
func (tg *tokenGranter) returnGrantLocked(count int64, _ int8) {
	tg.availableBurstTokens += count
	if tg.availableBurstTokens > tg.maxBurstTokens {
		tg.availableBurstTokens = tg.maxBurstTokens
	}
}

// tookWithoutPermission implements granter.
func (tg *tokenGranter) tookWithoutPermission(count int64) {
	tg.coord.tookWithoutPermission(tg.workKind, count, 0 /*arbitrary*/)
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	tg.availableBurstTokens -= count
}

// continueGrantChain implements granter.
func (tg *tokenGranter) continueGrantChain(grantChainID grantChainID) {
	tg.coord.continueGrantChain(tg.workKind, grantChainID)
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (tg *tokenGranter) requesterHasWaitingRequests() bool {
	return tg.requester.hasWaitingRequests()
}

// tryGrantLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := tg.tryGetLocked(1, 0 /*arbitrary*/)
	if res == grantSuccess {
		tokens := tg.requester.granted(grantChainID)
		if tokens == 0 {
			// Did not accept grant.
			tg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if tokens > 1 {
			tg.tookWithoutPermissionLocked(tokens-1, 0 /*arbitrary*/)
		}
	}
	return res
}

type workClass int8

const (
	// regularWorkClass is for work corresponding to workloads that are
	// throughput and latency sensitive.
	regularWorkClass workClass = iota
	// elasticWorkClass is for work corresponding to workloads that can handle
	// reduced throughput, possibly by taking longer to finish a workload. It is
	// not latency sensitive.
	elasticWorkClass
	numWorkClasses
)

// kvStoreTokenGranter implements granterWithLockedCalls. It is used for
// grants to KVWork to a store, that is limited by IO tokens. It encapsulates
// two granter-requester pairs, for the two workClasses. The granter in these
// pairs is implemented by kvStoreTokenChildGranter, and the requester by
// WorkQueue. We have separate WorkQueues for these work classes so that we
// don't have a situation where tenant1's elastic work is queued ahead of
// tenant2's regular work (due to inter-tenant fairness) and blocks the latter
// from getting tokens, because elastic tokens are exhausted (and tokens for
// regular work are not exhausted).
//
// The kvStoreTokenChildGranters delegate the actual interaction to
// their "parent", kvStoreTokenGranter. For elasticWorkClass, multiple kinds
// of tokens need to be acquired, (a) the usual IO tokens (based on
// compactions out of L0 and flushes into L0) and (b) elastic disk bandwidth
// tokens, which are based on disk bandwidth as a constrained resource, and
// apply to all the elastic incoming bytes into the LSM.
type kvStoreTokenGranter struct {
	coord            *GrantCoordinator
	regularRequester requester
	elasticRequester requester
	// There is no rate limiting in granting these tokens. That is, they are all
	// burst tokens.
	availableIOTokens int64
	// startingIOTokens is the number of tokens set by
	// setAvailableIOTokensLocked. It is used to compute the tokens used, by
	// computing startingIOTokens-availableIOTokens.
	startingIOTokens                int64
	ioTokensExhaustedDurationMetric *metric.Counter
	exhaustedStart                  time.Time

	// Disk bandwidth tokens.
	elasticDiskBWTokensAvailable int64
	diskBWTokensUsed             [numWorkClasses]int64

	// Estimation models.
	l0WriteLM, l0IngestLM, ingestLM tokensLinearModel
}

var _ granterWithLockedCalls = &kvStoreTokenGranter{}
var _ granterWithIOTokens = &kvStoreTokenGranter{}

// kvStoreTokenChildGranter handles a particular workClass. Its methods
// pass-through to the parent after adding the workClass as a parameter.
type kvStoreTokenChildGranter struct {
	workClass workClass
	parent    *kvStoreTokenGranter
}

var _ granterWithStoreWriteDone = &kvStoreTokenChildGranter{}
var _ granter = &kvStoreTokenChildGranter{}

// grantKind implements granter.
func (cg *kvStoreTokenChildGranter) grantKind() grantKind {
	return token
}

// tryGet implements granter.
func (cg *kvStoreTokenChildGranter) tryGet(count int64) bool {
	return cg.parent.tryGet(cg.workClass, count)
}

// returnGrant implements granter.
func (cg *kvStoreTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(cg.workClass, count)
}

// tookWithoutPermission implements granter.
func (cg *kvStoreTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(cg.workClass, count)
}

// continueGrantChain implements granter.
func (cg *kvStoreTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	// Ignore since grant chains are not used for store tokens.
}

// storeWriteDone implements granterWithStoreWriteDone.
func (cg *kvStoreTokenChildGranter) storeWriteDone(
	originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	return cg.parent.storeWriteDone(cg.workClass, originalTokens, doneInfo)
}

func (sg *kvStoreTokenGranter) tryGet(workClass workClass, count int64) bool {
	return sg.coord.tryGet(KVWork, count, int8(workClass))
}

// tryGetLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tryGetLocked(count int64, demuxHandle int8) grantResult {
	wc := workClass(demuxHandle)
	// NB: ideally if regularRequester.hasWaitingRequests() returns true and
	// wc==elasticWorkClass we should reject this request, since it means that
	// more important regular work is waiting. However, we rely on the
	// assumption that elasticWorkClass, once throttled, will have a non-empty
	// queue, and since the only case where tryGetLocked is called for
	// elasticWorkClass is when the queue is empty, this case should be rare
	// (and not cause a performance isolation failure).
	switch wc {
	case regularWorkClass:
		if sg.availableIOTokens > 0 {
			sg.subtractTokens(count, false)
			sg.diskBWTokensUsed[wc] += count
			return grantSuccess
		}
	case elasticWorkClass:
		if sg.elasticDiskBWTokensAvailable > 0 && sg.availableIOTokens > 0 {
			sg.elasticDiskBWTokensAvailable -= count
			sg.subtractTokens(count, false)
			sg.diskBWTokensUsed[wc] += count
			return grantSuccess
		}
	}
	return grantFailLocal
}

func (sg *kvStoreTokenGranter) returnGrant(workClass workClass, count int64) {
	sg.coord.returnGrant(KVWork, count, int8(workClass))
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) returnGrantLocked(count int64, demuxHandle int8) {
	wc := workClass(demuxHandle)
	// Return count tokens to the "IO tokens".
	sg.subtractTokens(-count, false)
	if wc == elasticWorkClass {
		// Return count tokens to the elastic disk bandwidth tokens.
		sg.elasticDiskBWTokensAvailable += count
	}
	sg.diskBWTokensUsed[wc] -= count
}

func (sg *kvStoreTokenGranter) tookWithoutPermission(workClass workClass, count int64) {
	sg.coord.tookWithoutPermission(KVWork, count, int8(workClass))
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tookWithoutPermissionLocked(count int64, demuxHandle int8) {
	wc := workClass(demuxHandle)
	sg.subtractTokens(count, false)
	if wc == elasticWorkClass {
		sg.elasticDiskBWTokensAvailable -= count
	}
	sg.diskBWTokensUsed[wc] += count
}

// subtractTokens is a helper function that subtracts count tokens (count can
// be negative, in which case this is really an addition).
func (sg *kvStoreTokenGranter) subtractTokens(count int64, forceTickMetric bool) {
	avail := sg.availableIOTokens
	sg.availableIOTokens -= count
	if count > 0 && avail > 0 && sg.availableIOTokens <= 0 {
		// Transition from > 0 to <= 0.
		sg.exhaustedStart = timeutil.Now()
	} else if count < 0 && avail <= 0 && (sg.availableIOTokens > 0 || forceTickMetric) {
		// Transition from <= 0 to > 0, or forced to tick the metric. The latter
		// ensures that if the available tokens stay <= 0, we don't show a sudden
		// change in the metric after minutes of exhaustion (we had observed such
		// behavior prior to this change).
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
		sg.ioTokensExhaustedDurationMetric.Inc(exhaustedMicros)
		if sg.availableIOTokens <= 0 {
			sg.exhaustedStart = now
		}
	}
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) requesterHasWaitingRequests() bool {
	return sg.regularRequester.hasWaitingRequests() || sg.elasticRequester.hasWaitingRequests()
}

// tryGrantLocked implements granterWithLockedCalls.
func (sg *kvStoreTokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	// First try granting to regular requester.
	for wc := range sg.diskBWTokensUsed {
		req := sg.regularRequester
		if workClass(wc) == elasticWorkClass {
			req = sg.elasticRequester
		}
		if req.hasWaitingRequests() {
			res := sg.tryGetLocked(1, int8(wc))
			if res == grantSuccess {
				tookTokenCount := req.granted(grantChainID)
				if tookTokenCount == 0 {
					// Did not accept grant.
					sg.returnGrantLocked(1, int8(wc))
					// Continue with the loop since this requester does not have waiting
					// requests. If the loop terminates we will correctly return
					// grantFailLocal.
				} else {
					// May have taken more.
					if tookTokenCount > 1 {
						sg.tookWithoutPermissionLocked(tookTokenCount-1, int8(wc))
					}
					return grantSuccess
				}
			} else {
				// Was not able to get token. Do not continue with looping to grant to
				// less important work (though it would be harmless since won't be
				// able to get a token for that either).
				return res
			}
		}
	}
	return grantFailLocal
}

// setAvailableIOTokensLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAvailableIOTokensLocked(tokens int64) (tokensUsed int64) {
	tokensUsed = sg.startingIOTokens - sg.availableIOTokens
	// It is possible for availableIOTokens to be negative because of
	// tookWithoutPermission or because tryGet will satisfy requests until
	// availableIOTokens become <= 0. We want to remember this previous
	// over-allocation.
	sg.subtractTokens(-tokens, true)
	if sg.availableIOTokens > tokens {
		// Clamp to tokens.
		sg.availableIOTokens = tokens
	}
	sg.startingIOTokens = tokens
	return tokensUsed
}

// setAvailableElasticDiskBandwidthTokensLocked implements
// granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAvailableElasticDiskBandwidthTokensLocked(tokens int64) {
	sg.elasticDiskBWTokensAvailable += tokens
	if sg.elasticDiskBWTokensAvailable > tokens {
		sg.elasticDiskBWTokensAvailable = tokens
	}
}

// getDiskTokensUsedAndResetLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) getDiskTokensUsedAndResetLocked() [numWorkClasses]int64 {
	result := sg.diskBWTokensUsed
	for i := range sg.diskBWTokensUsed {
		sg.diskBWTokensUsed[i] = 0
	}
	return result
}

// setAdmittedModelsLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAdmittedDoneModelsLocked(
	l0WriteLM tokensLinearModel, l0IngestLM tokensLinearModel, ingestLM tokensLinearModel,
) {
	sg.l0WriteLM = l0WriteLM
	sg.l0IngestLM = l0IngestLM
	sg.ingestLM = ingestLM
}

// storeWriteDone implements granterWithStoreWriteDone.
func (sg *kvStoreTokenGranter) storeWriteDone(
	wc workClass, originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	// Normally, we follow the structure of a foo() method calling into a foo()
	// method on the GrantCoordinator, which then calls fooLocked() on the
	// kvStoreTokenGranter. For example, returnGrant follows this structure.
	// This allows the GrantCoordinator to do two things (a) acquire the mu
	// before calling into kvStoreTokenGranter, (b) do side-effects, like
	// terminating grant chains and doing more grants after the call into the
	// fooLocked() method.
	// For storeWriteDone we don't bother with this structure involving the
	// GrantCoordinator (which has served us well across various methods and
	// various granter implementations), since the decision on when the
	// GrantCoordinator should call tryGrant is more complicated. And since this
	// storeWriteDone is unique to the kvStoreTokenGranter (and not implemented
	// by other granters) this approach seems acceptable.

	// Reminder: coord.mu protects the state in the kvStoreTokenGranter.
	sg.coord.mu.Lock()
	exhaustedFunc := func() bool {
		return sg.availableIOTokens <= 0 ||
			(wc == elasticWorkClass && sg.elasticDiskBWTokensAvailable <= 0)
	}
	wasExhausted := exhaustedFunc()
	actualL0WriteTokens := sg.l0WriteLM.applyLinearModel(doneInfo.WriteBytes)
	actualL0IngestTokens := sg.l0IngestLM.applyLinearModel(doneInfo.IngestedBytes)
	actualL0Tokens := actualL0WriteTokens + actualL0IngestTokens
	additionalL0TokensNeeded := actualL0Tokens - originalTokens
	sg.subtractTokens(additionalL0TokensNeeded, false)
	actualIngestTokens := sg.ingestLM.applyLinearModel(doneInfo.IngestedBytes)
	additionalDiskBWTokensNeeded := (actualL0WriteTokens + actualIngestTokens) - originalTokens
	if wc == elasticWorkClass {
		sg.elasticDiskBWTokensAvailable -= additionalDiskBWTokensNeeded
	}
	sg.diskBWTokensUsed[wc] += additionalDiskBWTokensNeeded
	if additionalL0TokensNeeded < 0 || additionalDiskBWTokensNeeded < 0 {
		isExhausted := exhaustedFunc()
		if wasExhausted && !isExhausted {
			sg.coord.tryGrant()
		}
	}
	sg.coord.mu.Unlock()
	// For multi-tenant fairness accounting, we choose to ignore disk bandwidth
	// tokens. Ideally, we'd have multiple resource dimensions for the fairness
	// decisions, but we don't necessarily need something more sophisticated
	// like "Dominant Resource Fairness".
	return additionalL0TokensNeeded
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

// kvSlotAdjuster is an implementer of CPULoadListener and
// cpuOverloadIndicator.
type kvSlotAdjuster struct {
	settings *cluster.Settings
	// This is the slotGranter used for KVWork. In single-tenant settings, it
	// is the only one we adjust using the periodic cpu overload signal. We
	// don't adjust slots for SQLStatementLeafStartWork and
	// SQLStatementRootStartWork using the periodic cpu overload signal since:
	// - these are potentially long-lived work items and not CPU bound
	// - we don't know how to coordinate adjustment of those slots and the KV
	//   slots.
	granter     *slotGranter
	minCPUSlots int
	maxCPUSlots int
	// moderateSlotsClamp is the most recent value which may have been used to
	// clamp down on slotGranter.totalModerateLoadSlots. Justification for
	// clamping down on totalModerateLoadSlots is given where the moderateSlotsClamp
	// value is written to.
	moderateSlotsClamp int
	// moderateSlotsClampOverride is used during testing to override the value of the
	// moderateSlotsClamp. Its purpose is to make it easier to write tests. A default
	// value of 0 implies no override.
	moderateSlotsClampOverride int
	// runnableEWMA is a weighted average of the most recent runnable goroutine counts.
	// runnableEWMA is used to tune the slotGranter.totalModerateLoadSlots.
	runnableEWMA float64
	// runnableAlphaOverride is used to override the value of runnable alpha during testing.
	// A 0 value indicates that there is no override.
	runnableAlphaOverride float64

	totalSlotsMetric         *metric.Gauge
	totalModerateSlotsMetric *metric.Gauge
}

var _ cpuOverloadIndicator = &kvSlotAdjuster{}
var _ CPULoadListener = &kvSlotAdjuster{}

func (kvsa *kvSlotAdjuster) CPULoad(runnable int, procs int, samplePeriod time.Duration) {
	threshold := int(KVSlotAdjusterOverloadThreshold.Get(&kvsa.settings.SV))

	// 0.009 gives weight to at least a few hundred samples at a 1ms sampling rate.
	alpha := 0.009 * float64(samplePeriod/time.Millisecond)
	if alpha > 0.5 {
		alpha = 0.5
	} else if alpha < 0.001 {
		alpha = 0.001
	}
	if kvsa.runnableAlphaOverride > 0 {
		alpha = kvsa.runnableAlphaOverride
	}
	kvsa.runnableEWMA = kvsa.runnableEWMA*(1-alpha) + float64(runnable)*alpha

	// Simple heuristic, which worked ok in experiments. More sophisticated ones
	// could be devised.
	usedSlots := kvsa.granter.usedSlots + kvsa.granter.usedSoftSlots
	tryDecreaseSlots := func(total int) int {
		// Overload.
		// If using some slots, and the used slots is less than the total slots,
		// and total slots hasn't bottomed out at the min, decrease the total
		// slots. If currently using more than the total slots, it suggests that
		// the previous slot reduction has not taken effect yet, so we hold off on
		// further decreasing.
		// TODO(sumeer): despite the additive decrease and high multiplier value,
		// the metric showed some drops from 40 slots to 1 slot on a kv50 overload
		// workload. It was not accompanied by a drop in runnable count per proc,
		// so it is suggests that the drop in slots should not be causing cpu
		// under-utilization, but one cannot be sure. Experiment with a smoothed
		// signal or other ways to prevent a fast drop.
		if usedSlots > 0 && total > kvsa.minCPUSlots && usedSlots <= total {
			total--
		}
		return total
	}
	tryIncreaseSlots := func(total int) int {
		// Underload.
		// Used all its slots and can increase further, so additive increase. We
		// also handle the case where the used slots are a bit less than total
		// slots, since callers for soft slots don't block.
		if usedSlots >= total && total < kvsa.maxCPUSlots {
			// NB: If the workload is IO bound, the slot count here will keep
			// incrementing until these slots are no longer the bottleneck for
			// admission. So it is not unreasonable to see this slot count go into
			// the 1000s. If the workload switches to being CPU bound, we can
			// decrease by 1000 slots every second (because the CPULoad ticks are at
			// 1ms intervals, and we do additive decrease).
			total++
		}
		return total
	}

	if runnable >= threshold*procs {
		// Very overloaded.
		kvsa.granter.totalHighLoadSlots = tryDecreaseSlots(kvsa.granter.totalHighLoadSlots)
		kvsa.granter.totalModerateLoadSlots = tryDecreaseSlots(kvsa.granter.totalModerateLoadSlots)
	} else if float64(runnable) <= float64((threshold*procs)/4) {
		// Very underloaded.
		kvsa.granter.totalHighLoadSlots = tryIncreaseSlots(kvsa.granter.totalHighLoadSlots)
		kvsa.granter.totalModerateLoadSlots = tryIncreaseSlots(kvsa.granter.totalModerateLoadSlots)
	} else if float64(runnable) <= float64((threshold*procs)/2) {
		// Moderately underloaded -- can afford to increase regular slots.
		kvsa.granter.totalHighLoadSlots = tryIncreaseSlots(kvsa.granter.totalHighLoadSlots)
	} else if runnable >= 3*threshold*procs/4 {
		// Moderately overloaded -- should decrease moderate load slots.
		//
		// NB: decreasing moderate load slots may not halt the runnable growth
		// since the regular traffic may be high and can use up to the high load
		// slots. When usedSlots>totalModerateLoadSlots, we won't actually
		// decrease totalModerateLoadSlots (see the logic in tryDecreaseSlots).
		// However, that doesn't mean that totalModerateLoadSlots is accurate.
		// This inaccuracy is fine since we have chosen to be in a high load
		// regime, since all the work we are doing is non-optional regular work
		// (not background work).
		//
		// Where this will help is when what is pushing us over moderate load is
		// optional background work, so by decreasing totalModerateLoadSlots we will
		// contain the load due to that work.
		kvsa.granter.totalModerateLoadSlots = tryDecreaseSlots(kvsa.granter.totalModerateLoadSlots)
	}
	// Consider the following cases, when we started this method with
	// totalHighLoadSlots==totalModerateLoadSlots.
	// - underload such that we are able to increase totalModerateLoadSlots: in
	//   this case we will also be able to increase totalHighLoadSlots (since
	//   the used and total comparisons gating the increase in tryIncreaseSlots
	//   will also be true for totalHighLoadSlots).
	// - overload such that we are able to decrease totalHighLoadSlots: in this
	//   case the logic in tryDecreaseSlots will also be able to decrease
	//   totalModerateLoadSlots.
	// So the natural behavior of the slot adjustment itself guarantees
	// totalHighLoadSlots >= totalModerateLoadSlots. But as a defensive measure
	// we clamp totalModerateLoadSlots to not exceed totalHighLoadSlots.
	if kvsa.granter.totalHighLoadSlots < kvsa.granter.totalModerateLoadSlots {
		kvsa.granter.totalModerateLoadSlots = kvsa.granter.totalHighLoadSlots
	}

	// During a kv50 workload, we noticed soft slots grants succeeding despite
	// high cpu utilization, and high runnable goroutine counts.
	//
	// Consider the following log lines from the kv50 experiment:
	// [runnable count 372 threshold*procs 256]
	// [totalHighLoadSlots 254 totalModerateLoadSlots 164 usedSlots 0 usedSoftSlots 1]
	//
	// Note that even though the runnable count is high, of the (254, 164),
	// (totalHighLoad, totalModerateLoad) slots respectively, only 1 slot is
	// being used. The slot mechanism behaves in a bi-modal manner in nodes that
	// do both KV and SQL processing. While there is backlogged KV work, the slot
	// usage is high, and blocks all SQL work, but eventually all callers have done
	// their KV processing and are queued up for SQL work. The latter causes bursts
	// of grants (because it uses tokens), gated only by the grant-chain mechanism,
	// during which runnable count is high but used (KV) slots are low. This is exactly
	// the case where we have low slot usage, but high CPU utilization.
	//
	// We can afford to be more conservative in calculating totalModerateLoadSlots
	// since we don't care about saturating CPU for the less important work that is
	// controlled by these slots. So we could use a slow reacting and conservative
	// signal to decide on the value of totalModerateLoadSlots.
	//
	// To account for the increased CPU utilization and runnable counts when the used
	// slots are low, we clamp down on the totalModerateSlots value by keeping track
	// of a historical runnable goroutine average.
	kvsa.moderateSlotsClamp = int(float64(threshold*procs)/2 - kvsa.runnableEWMA)
	if kvsa.moderateSlotsClampOverride != 0 {
		kvsa.moderateSlotsClamp = kvsa.moderateSlotsClampOverride
	}
	if kvsa.granter.totalModerateLoadSlots > kvsa.moderateSlotsClamp {
		kvsa.granter.totalModerateLoadSlots = kvsa.moderateSlotsClamp
	}
	if kvsa.granter.totalModerateLoadSlots < 0 {
		kvsa.granter.totalModerateLoadSlots = 0
	}

	kvsa.totalSlotsMetric.Update(int64(kvsa.granter.totalHighLoadSlots))
	kvsa.totalModerateSlotsMetric.Update(int64(kvsa.granter.totalModerateLoadSlots))
}

func (kvsa *kvSlotAdjuster) isOverloaded() bool {
	return kvsa.granter.usedSlots >= kvsa.granter.totalHighLoadSlots && !kvsa.granter.skipSlotEnforcement
}

// sqlNodeCPUOverloadIndicator is the implementation of cpuOverloadIndicator
// for a single-tenant SQL node in a multi-tenant cluster. This has to rely on
// the periodic load information from the cpu scheduler and will therefore be
// tuned towards indicating overload at higher overload points (otherwise we
// could fluctuate into underloaded territory due to restricting admission,
// and not be work conserving). Such tuning towards more overload, and
// therefore more queueing inside the scheduler, is somewhat acceptable since
// a SQL node is not multi-tenant.
//
// TODO(sumeer): implement.
type sqlNodeCPUOverloadIndicator struct {
}

// PebbleMetricsProvider provides the pebble.Metrics for all stores.
type PebbleMetricsProvider interface {
	GetPebbleMetrics() []StoreMetrics
}

// IOThresholdConsumer is informed about updated IOThresholds.
type IOThresholdConsumer interface {
	UpdateIOThreshold(roachpb.StoreID, *admissionpb.IOThreshold)
}

// StoreMetrics are the metrics for a store.
type StoreMetrics struct {
	StoreID int32
	*pebble.Metrics
	WriteStallCount int64
	*pebble.InternalIntervalMetrics
	// Optional.
	DiskStats DiskStats
}

// DiskStats provide low-level stats about the disk resources used for a
// store. We assume that the disk is not shared across multiple stores.
// However, transient and moderate usage that is not due to the store is
// tolerable, since the diskBandwidthLimiter is only using this to compute
// elastic tokens and is designed to deal with significant attribution
// uncertainty.
//
// DiskStats are not always populated. A ProvisionedBandwidth of 0 represents
// that the stats should be ignored.
type DiskStats struct {
	// BytesRead is the cumulative bytes read.
	BytesRead uint64
	// BytesWritten is the cumulative bytes written.
	BytesWritten uint64
	// ProvisionedBandwidth is the total provisioned bandwidth in bytes/s.
	ProvisionedBandwidth int64
}

// granterWithIOTokens is used to abstract kvStoreTokenGranter for testing.
// The interface is used by the entity that periodically looks at load and
// computes the tokens to grant (ioLoadListener).
type granterWithIOTokens interface {
	// setAvailableIOTokensLocked bounds the available tokens that can be
	// granted to the value provided in the tokens parameter. This is not a
	// tight bound when the callee has negative available tokens, due to the use
	// of granter.tookWithoutPermission, since in that the case the callee
	// increments that negative value with the value provided by tokens. This
	// method needs to be called periodically. The return value is the number of
	// used tokens in the interval since the prior call to this method. Note
	// that tokensUsed can be negative, though that will be rare, since it is
	// possible for tokens to be returned.
	setAvailableIOTokensLocked(tokens int64) (tokensUsed int64)
	// setAvailableElasticDiskBandwidthTokensLocked bounds the available tokens
	// that can be granted to elastic work. These tokens are based on disk
	// bandwidth being a bottleneck resource.
	setAvailableElasticDiskBandwidthTokensLocked(tokens int64)
	// getDiskTokensUsedAndResetLocked returns the disk bandwidth tokens used
	// since the last such call.
	getDiskTokensUsedAndResetLocked() [numWorkClasses]int64
	// setAdmittedDoneModelsLocked supplies the models to use when
	// storeWriteDone is called, to adjust token consumption. Note that these
	// models are not used for token adjustment at admission time -- that is
	// handled by StoreWorkQueue and is not in scope of this granter. This
	// asymmetry is due to the need to use all the functionality of WorkQueue at
	// admission time. See the long explanatory comment at the beginning of
	// store_token_estimation.go, regarding token estimation.
	setAdmittedDoneModelsLocked(l0WriteLM tokensLinearModel, l0IngestLM tokensLinearModel,
		ingestLM tokensLinearModel)
}

// granterWithStoreWriteDone is used to abstract kvStoreTokenGranter for
// testing. The interface is used by StoreWorkQueue to pass on sizing
// information provided when the work was completed.
type granterWithStoreWriteDone interface {
	granter
	storeWriteDone(originalTokens int64, doneInfo StoreWorkDoneInfo) (additionalTokens int64)
}

// storeAdmissionStats are stats maintained by a storeRequester. The non-test
// implementation of storeRequester is StoreWorkQueue. StoreWorkQueue updates
// all of these when StoreWorkQueue.AdmittedWorkDone is called, so that these
// cumulative values are mutually consistent.
type storeAdmissionStats struct {
	// Total requests that called AdmittedWorkDone or BypassedWorkDone.
	admittedCount uint64
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
		pebble.IngestOperationStats
	}
	// aux represents additional information carried for informational purposes
	// (e.g. for logging).
	aux struct {
		// These bypassed numbers are already included in the corresponding
		// {admittedCount, writeAccountedBytes, ingestedAccountedBytes}.
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

// storeRequester is used to abstract *StoreWorkQueue for testing.
type storeRequester interface {
	requesterClose
	getRequesters() [numWorkClasses]requester
	getStoreAdmissionStats() storeAdmissionStats
	setStoreRequestEstimates(estimates storeRequestEstimates)
}

var _ cpuOverloadIndicator = &sqlNodeCPUOverloadIndicator{}
var _ CPULoadListener = &sqlNodeCPUOverloadIndicator{}

func (sn *sqlNodeCPUOverloadIndicator) CPULoad(
	runnable int, procs int, samplePeriod time.Duration,
) {
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
	totalModerateSlots = metric.Metadata{
		Name:        "admission.granter.total_moderate_slots.kv",
		Help:        "Total moderate load slots for low priority work",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSlots = metric.Metadata{
		// Note: we append a WorkKind string to this name.
		Name:        "admission.granter.used_slots.",
		Help:        "Used slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSoftSlots = metric.Metadata{
		Name:        "admission.granter.used_soft_slots.kv",
		Help:        "Used soft slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensExhaustedDuration = metric.Metadata{
		Name:        "admission.granter.io_tokens_exhausted_duration.kv",
		Help:        "Total duration when IO tokens were exhausted, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
)

// TODO(irfansharif): we are lacking metrics for IO tokens and load, including
// metrics from helper classes used by ioLoadListener, like the code in
// disk_bandwidth.go and store_token_estimation.go. Additionally, what we have
// below is per node, while we want such metrics per store. We should add
// these metrics via StoreGrantCoordinators.SetPebbleMetricsProvider, which is
// used to construct the per-store GrantCoordinator. These metrics should be
// embedded in kvserver.StoreMetrics. We should also separate the metrics
// related to cpu slots from the IO metrics.

// TODO(sumeer): experiment with approaches to adjust slots for
// SQLStatementLeafStartWork and SQLStatementRootStartWork for SQL nodes. Note
// that for these WorkKinds we are currently setting very high slot counts
// since we rely on other signals like memory and cpuOverloadIndicator to gate
// admission. One could debate whether we should be using rate limiting
// instead of counting slots for such work. The only reason the above code
// uses the term "slot" for these is that we have a completion indicator, and
// when we do have such an indicator it can be beneficial to be able to keep
// track of how many ongoing work items we have.

// SoftSlotGranter grants soft slots without queueing. See the comment with
// kvGranter.
type SoftSlotGranter struct {
	kvGranter *slotGranter
}

// MakeSoftSlotGranter constructs a SoftSlotGranter given a GrantCoordinator
// that is responsible for KV and lower layers.
func MakeSoftSlotGranter(gc *GrantCoordinator) (*SoftSlotGranter, error) {
	kvGranter, ok := gc.granters[KVWork].(*slotGranter)
	if !ok {
		return nil, errors.Errorf("GrantCoordinator does not support soft slots")
	}
	return &SoftSlotGranter{
		kvGranter: kvGranter,
	}, nil
}

// TryGetSlots attempts to acquire count slots and returns what was acquired
// (possibly 0).
func (ssg *SoftSlotGranter) TryGetSlots(count int) int {
	if !EnabledSoftSlotGranting.Get(&ssg.kvGranter.coord.settings.SV) {
		return 0
	}
	return ssg.kvGranter.tryGetSoftSlots(count)
}

// ReturnSlots returns count slots (count must be >= 0).
func (ssg *SoftSlotGranter) ReturnSlots(count int) {
	ssg.kvGranter.returnSoftSlots(count)
}
