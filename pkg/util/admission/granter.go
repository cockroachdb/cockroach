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
	"context"
	"math"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
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

// L0FileCountOverloadThreshold sets a file count threshold that signals an
// overloaded store.
var L0FileCountOverloadThreshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"admission.l0_file_count_overload_threshold",
	"when the L0 file count exceeds this theshold, the store is considered overloaded",
	l0FileCountOverloadThreshold, settings.PositiveInt)

// L0SubLevelCountOverloadThreshold sets a sub-level count threshold that
// signals an overloaded store.
var L0SubLevelCountOverloadThreshold = settings.RegisterIntSetting(
	settings.TenantWritable,
	"admission.l0_sub_level_count_overload_threshold",
	"when the L0 sub-level count exceeds this threshold, the store is considered overloaded",
	l0SubLevelCountOverloadThreshold, settings.PositiveInt)

// EnabledSoftSlotGranting can be set to false to disable soft slot granting.
var EnabledSoftSlotGranting = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"admission.soft_slot_granting.enabled",
	"soft slot granting is disabled if this setting is set to false",
	true,
)

// MinFlushUtilizationFraction is a lower-bound on the dynamically adjusted
// flush utilization target fraction that attempts to reduce write stalls. Set
// it to a high fraction (>>1, e.g. 10), to effectively disable flush based
// tokens.
//
// The target fraction is used to multiply the (measured) peak flush rate, to
// compute the flush tokens. For example, if the dynamic target fraction (for
// which this setting provides a lower bound) is currently 0.75, then
// 0.75*peak-flush-rate will be used to set the flush tokens. The lower bound
// of 0.5 should not need to be tuned, and should not be tuned without
// consultation with a domain expert. If the storage.write-stall-nanos
// indicates significant write stalls, and the granter logs show that the
// dynamic target fraction has already reached the lower bound, one can
// consider lowering it slightly and then observe the effect.
var MinFlushUtilizationFraction = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.min_flush_util_fraction",
	"when computing flush tokens, this fraction is a lower bound on the dynamically "+
		"adjusted flush utilization target fraction that attempts to reduce write stalls. Set "+
		"it to a high fraction (>>1, e.g. 10), to disable flush based tokens. The dynamic "+
		"target fraction is used to multiply the (measured) peak flush rate, to compute the flush "+
		"tokens. If the storage.write-stall-nanos indicates significant write stalls, and the granter "+
		"logs show that the dynamic target fraction has already reached the lower bound, one can "+
		"consider lowering it slightly (after consultation with domain experts)", 0.5,
	settings.PositiveFloat)

// DiskBandwidthTokensForElasticEnabled controls whether the disk bandwidth
// resource is considered as a possible bottleneck resource. When it becomes a
// bottleneck, tokens for elastic work are limited based on available disk
// bandwidth. The default is true since actually considering disk bandwidth as
// a bottleneck resource requires additional configuration (outside the
// admission package) to calculate the provisioned bandwidth.
var DiskBandwidthTokensForElasticEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"admission.disk_bandwidth_tokens.elastic.enabled",
	"when true, and provisioned bandwidth for the disk corresponding to a store is configured, "+
		"tokens for elastic work will be limited if disk bandwidth becomes a bottleneck",
	true).WithPublic()

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

// GrantCoordinator is the top-level object that coordinates grants across
// different WorkKinds (for more context see the comment in doc.go, and the
// comment where WorkKind is declared). Typically there will one
// GrantCoordinator in a node for CPU intensive work, and for nodes that also
// have the KV layer, one GrantCoordinator per store (these are managed by
// StoreGrantCoordinators) for KVWork that uses that store. See the
// NewGrantCoordinators and NewGrantCoordinatorSQL functions.
type GrantCoordinator struct {
	ambientCtx log.AmbientContext

	settings                *cluster.Settings
	lastCPULoadSamplePeriod time.Duration

	// mu is ordered before any mutex acquired in a requester implementation.
	// TODO(sumeer): move everything covered by mu into a nested struct.
	mu syncutil.Mutex
	// NB: Some granters can be nil.
	granters [numWorkKinds]granterWithLockedCalls
	// The WorkQueues behaving as requesters in each granterWithLockedCalls.
	// This is kept separately only to service GetWorkQueue calls and to call
	// close().
	queues [numWorkKinds]requesterClose
	// The cpu fields can be nil, and the IO field can be nil, since a
	// GrantCoordinator typically handles one of these two resources.
	cpuOverloadIndicator cpuOverloadIndicator
	cpuLoadListener      CPULoadListener
	ioLoadListener       *ioLoadListener

	// The latest value of GOMAXPROCS, received via CPULoad. Only initialized if
	// the cpu resource is being handled by this GrantCoordinator.
	numProcs int

	// See the comment at continueGrantChain that explains how a grant chain
	// functions and the motivation. When !useGrantChains, grant chains are
	// disabled.
	useGrantChains bool

	// The admission control code needs high sampling frequency of the cpu load,
	// and turns off admission control enforcement when the sampling frequency
	// is too low. For testing queueing behavior, we do not want the enforcement
	// to be turned off in a non-deterministic manner so add a testing flag to
	// disable that feature.
	testingDisableSkipEnforcement bool

	// grantChainActive indicates whether a grant chain is active. If active,
	// grantChainID is the ID of that chain. If !active, grantChainID is the ID
	// of the next chain that will become active. IDs are assigned by
	// incrementing grantChainID. If !useGrantChains, grantChainActive is never
	// true.
	grantChainActive bool
	grantChainID     grantChainID
	// Index into granters, which represents the current WorkKind at which the
	// grant chain is operating. Only relevant when grantChainActive is true.
	grantChainIndex WorkKind
	// See the comment at delayForGrantChainTermination for motivation.
	grantChainStartTime time.Time
}

var _ CPULoadListener = &GrantCoordinator{}

// Options for constructing GrantCoordinators.
type Options struct {
	MinCPUSlots int
	MaxCPUSlots int
	// RunnableAlphaOverride is used to override the alpha value used to
	// compute the ewma of the runnable goroutine counts. It is only used
	// during testing. A 0 value indicates that there is no override.
	RunnableAlphaOverride          float64
	SQLKVResponseBurstTokens       int64
	SQLSQLResponseBurstTokens      int64
	SQLStatementLeafStartWorkSlots int
	SQLStatementRootStartWorkSlots int
	TestingDisableSkipEnforcement  bool
	Settings                       *cluster.Settings
	// Only non-nil for tests.
	makeRequesterFunc      makeRequesterFunc
	makeStoreRequesterFunc makeStoreRequesterFunc
}

var _ base.ModuleTestingKnobs = &Options{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*Options) ModuleTestingKnobs() {}

// DefaultOptions are the default settings for various admission control knobs.
var DefaultOptions = Options{
	MinCPUSlots:                    1,
	MaxCPUSlots:                    100000, /* TODO(sumeer): add cluster setting */
	SQLKVResponseBurstTokens:       100000, /* TODO(sumeer): add cluster setting */
	SQLSQLResponseBurstTokens:      100000, /* TODO(sumeer): add cluster setting */
	SQLStatementLeafStartWorkSlots: 100,    /* arbitrary, and unused */
	SQLStatementRootStartWorkSlots: 100,    /* arbitrary, and unused */
}

// Override applies values from "override" to the receiver that differ from Go
// defaults.
func (o *Options) Override(override *Options) {
	if override.MinCPUSlots != 0 {
		o.MinCPUSlots = override.MinCPUSlots
	}
	if override.MaxCPUSlots != 0 {
		o.MaxCPUSlots = override.MaxCPUSlots
	}
	if override.SQLKVResponseBurstTokens != 0 {
		o.SQLKVResponseBurstTokens = override.SQLKVResponseBurstTokens
	}
	if override.SQLSQLResponseBurstTokens != 0 {
		o.SQLSQLResponseBurstTokens = override.SQLSQLResponseBurstTokens
	}
	if override.SQLStatementLeafStartWorkSlots != 0 {
		o.SQLStatementLeafStartWorkSlots = override.SQLStatementLeafStartWorkSlots
	}
	if override.SQLStatementRootStartWorkSlots != 0 {
		o.SQLStatementRootStartWorkSlots = override.SQLStatementRootStartWorkSlots
	}
	if override.TestingDisableSkipEnforcement {
		o.TestingDisableSkipEnforcement = true
	}
}

type makeRequesterFunc func(
	_ log.AmbientContext, workKind WorkKind, granter granter, settings *cluster.Settings,
	opts workQueueOptions) requester

type makeStoreRequesterFunc func(
	_ log.AmbientContext, granters [numWorkClasses]granterWithStoreWriteDone,
	settings *cluster.Settings, opts workQueueOptions) storeRequester

// NewGrantCoordinators constructs GrantCoordinators and WorkQueues for a
// regular cluster node. Caller is responsible for hooking up
// GrantCoordinators.Regular to receive calls to CPULoad, and to set a
// PebbleMetricsProvider on GrantCoordinators.Stores. Every request must pass
// through GrantCoordinators.Regular, while only subsets of requests pass
// through each store's GrantCoordinator. We arrange these such that requests
// (that need to) first pass through a store's GrantCoordinator and then
// through the regular one. This ensures that we are not using slots in the
// latter on requests that are blocked elsewhere for admission. Additionally,
// we don't want the CPU scheduler signal that is implicitly used in grant
// chains to delay admission through the per store GrantCoordinators since
// they are not trying to control CPU usage, so we turn off grant chaining in
// those coordinators.
func NewGrantCoordinators(
	ambientCtx log.AmbientContext, opts Options,
) (GrantCoordinators, []metric.Struct) {
	makeRequester := makeWorkQueue
	if opts.makeRequesterFunc != nil {
		makeRequester = opts.makeRequesterFunc
	}
	st := opts.Settings

	metrics := makeGranterMetrics()
	metricStructs := append([]metric.Struct(nil), metrics)
	kvSlotAdjuster := &kvSlotAdjuster{
		settings:                 st,
		minCPUSlots:              opts.MinCPUSlots,
		maxCPUSlots:              opts.MaxCPUSlots,
		totalSlotsMetric:         metrics.KVTotalSlots,
		totalModerateSlotsMetric: metrics.KVTotalModerateSlots,
		moderateSlotsClamp:       opts.MaxCPUSlots,
		runnableAlphaOverride:    opts.RunnableAlphaOverride,
	}
	coord := &GrantCoordinator{
		ambientCtx:                    ambientCtx,
		settings:                      st,
		cpuOverloadIndicator:          kvSlotAdjuster,
		cpuLoadListener:               kvSlotAdjuster,
		useGrantChains:                true,
		testingDisableSkipEnforcement: opts.TestingDisableSkipEnforcement,
		numProcs:                      1,
		grantChainID:                  1,
	}

	kvg := &slotGranter{
		coord:                  coord,
		workKind:               KVWork,
		totalHighLoadSlots:     opts.MinCPUSlots,
		totalModerateLoadSlots: opts.MinCPUSlots,
		usedSlotsMetric:        metrics.KVUsedSlots,
		usedSoftSlotsMetric:    metrics.KVUsedSoftSlots,
	}

	kvSlotAdjuster.granter = kvg
	req := makeRequester(ambientCtx, KVWork, kvg, st, makeWorkQueueOptions(KVWork))
	coord.queues[KVWork] = req
	kvg.requester = req
	coord.granters[KVWork] = kvg

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	req = makeRequester(
		ambientCtx, SQLKVResponseWork, tg, st, makeWorkQueueOptions(SQLKVResponseWork))
	coord.queues[SQLKVResponseWork] = req
	tg.requester = req
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	req = makeRequester(ambientCtx,
		SQLSQLResponseWork, tg, st, makeWorkQueueOptions(SQLSQLResponseWork))
	coord.queues[SQLSQLResponseWork] = req
	tg.requester = req
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:              coord,
		workKind:           SQLStatementLeafStartWork,
		totalHighLoadSlots: opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:        kvSlotAdjuster,
		usedSlotsMetric:    metrics.SQLLeafStartUsedSlots,
	}
	req = makeRequester(ambientCtx,
		SQLStatementLeafStartWork, sg, st, makeWorkQueueOptions(SQLStatementLeafStartWork))
	coord.queues[SQLStatementLeafStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:              coord,
		workKind:           SQLStatementRootStartWork,
		totalHighLoadSlots: opts.SQLStatementRootStartWorkSlots,
		cpuOverload:        kvSlotAdjuster,
		usedSlotsMetric:    metrics.SQLRootStartUsedSlots,
	}
	req = makeRequester(ambientCtx,
		SQLStatementRootStartWork, sg, st, makeWorkQueueOptions(SQLStatementRootStartWork))
	coord.queues[SQLStatementRootStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementRootStartWork] = sg

	metricStructs = appendMetricStructsForQueues(metricStructs, coord)

	// TODO(sumeerbhola): these metrics are shared across all stores and all
	// priorities across stores (even the coarser workClasses, which are a
	// mapping from priority, share the same metrics). Fix this by adding
	// labeled Prometheus metrics.
	storeWorkQueueMetrics := makeWorkQueueMetrics(string(workKindString(KVWork)) + "-stores")
	metricStructs = append(metricStructs, storeWorkQueueMetrics)
	makeStoreRequester := makeStoreWorkQueue
	if opts.makeStoreRequesterFunc != nil {
		makeStoreRequester = opts.makeStoreRequesterFunc
	}
	storeCoordinators := &StoreGrantCoordinators{
		settings:                    st,
		makeStoreRequesterFunc:      makeStoreRequester,
		kvIOTokensExhaustedDuration: metrics.KVIOTokensExhaustedDuration,
		workQueueMetrics:            storeWorkQueueMetrics,
	}

	return GrantCoordinators{Stores: storeCoordinators, Regular: coord}, metricStructs
}

// NewGrantCoordinatorSQL constructs a GrantCoordinator and WorkQueues for a
// single-tenant SQL node in a multi-tenant cluster. Caller is responsible for
// hooking this up to receive calls to CPULoad.
func NewGrantCoordinatorSQL(
	ambientCtx log.AmbientContext, opts Options,
) (*GrantCoordinator, []metric.Struct) {
	makeRequester := makeWorkQueue
	if opts.makeRequesterFunc != nil {
		makeRequester = opts.makeRequesterFunc
	}
	st := opts.Settings

	metrics := makeGranterMetrics()
	metricStructs := append([]metric.Struct(nil), metrics)
	sqlNodeCPU := &sqlNodeCPUOverloadIndicator{}
	coord := &GrantCoordinator{
		ambientCtx:           ambientCtx,
		settings:             st,
		cpuOverloadIndicator: sqlNodeCPU,
		cpuLoadListener:      sqlNodeCPU,
		useGrantChains:       true,
		numProcs:             1,
		grantChainID:         1,
	}

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          sqlNodeCPU,
	}
	req := makeRequester(ambientCtx,
		SQLKVResponseWork, tg, st, makeWorkQueueOptions(SQLKVResponseWork))
	coord.queues[SQLKVResponseWork] = req
	tg.requester = req
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          sqlNodeCPU,
	}
	req = makeRequester(ambientCtx,
		SQLSQLResponseWork, tg, st, makeWorkQueueOptions(SQLSQLResponseWork))
	coord.queues[SQLSQLResponseWork] = req
	tg.requester = req
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:              coord,
		workKind:           SQLStatementLeafStartWork,
		totalHighLoadSlots: opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:        sqlNodeCPU,
		usedSlotsMetric:    metrics.SQLLeafStartUsedSlots,
	}
	req = makeRequester(ambientCtx,
		SQLStatementLeafStartWork, sg, st, makeWorkQueueOptions(SQLStatementLeafStartWork))
	coord.queues[SQLStatementLeafStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:              coord,
		workKind:           SQLStatementRootStartWork,
		totalHighLoadSlots: opts.SQLStatementRootStartWorkSlots,
		cpuOverload:        sqlNodeCPU,
		usedSlotsMetric:    metrics.SQLRootStartUsedSlots,
	}
	req = makeRequester(ambientCtx,
		SQLStatementRootStartWork, sg, st, makeWorkQueueOptions(SQLStatementRootStartWork))
	coord.queues[SQLStatementRootStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementRootStartWork] = sg

	return coord, appendMetricStructsForQueues(metricStructs, coord)
}

func appendMetricStructsForQueues(ms []metric.Struct, coord *GrantCoordinator) []metric.Struct {
	for i := range coord.queues {
		if coord.queues[i] != nil {
			q, ok := coord.queues[i].(*WorkQueue)
			if ok {
				ms = append(ms, q.metrics)
			}
		}
	}
	return ms
}

// pebbleMetricsTick is called every adjustmentInterval seconds and passes
// through to the ioLoadListener, so that it can adjust the plan for future IO
// token allocations.
func (coord *GrantCoordinator) pebbleMetricsTick(ctx context.Context, m StoreMetrics) {
	coord.ioLoadListener.pebbleMetricsTick(ctx, m)
}

// allocateIOTokensTick tells the ioLoadListener to allocate tokens.
func (coord *GrantCoordinator) allocateIOTokensTick() {
	coord.ioLoadListener.allocateTokensTick()
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if !coord.grantChainActive {
		coord.tryGrant()
	}
	// Else, let the grant chain finish. NB: we turn off grant chains on the
	// GrantCoordinators used for IO, so the if-condition is always true.
}

// testingTryGrant is only for unit tests, since they sometimes cut out
// support classes like the ioLoadListener.
func (coord *GrantCoordinator) testingTryGrant() {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if !coord.grantChainActive {
		coord.tryGrant()
	}
}

// GetWorkQueue returns the WorkQueue for a particular WorkKind. Can be nil if
// the NewGrantCoordinator* function does not construct a WorkQueue for that
// work.
// Implementation detail: don't use this method when the GrantCoordinator is
// created by the StoreGrantCoordinators since those have a StoreWorkQueues.
// The TryGetQueueForStore is the external facing method in that case since
// the individual GrantCoordinators are hidden.
func (coord *GrantCoordinator) GetWorkQueue(workKind WorkKind) *WorkQueue {
	return coord.queues[workKind].(*WorkQueue)
}

// CPULoad implements CPULoadListener and is called periodically (see
// CPULoadListener for details). The same frequency is used for refilling the
// burst tokens since synchronizing the two means that the refilled burst can
// take into account the latest schedulers stats (indirectly, via the
// implementation of cpuOverloadIndicator).
func (coord *GrantCoordinator) CPULoad(runnable int, procs int, samplePeriod time.Duration) {
	ctx := coord.ambientCtx.AnnotateCtx(context.Background())

	if coord.lastCPULoadSamplePeriod != 0 && coord.lastCPULoadSamplePeriod != samplePeriod &&
		KVAdmissionControlEnabled.Get(&coord.settings.SV) {
		log.Infof(ctx, "CPULoad switching to period %s", samplePeriod.String())
	}
	coord.lastCPULoadSamplePeriod = samplePeriod

	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.numProcs = procs
	coord.cpuLoadListener.CPULoad(runnable, procs, samplePeriod)

	// Slot adjustment and token refilling requires 1ms periods to work well. If
	// the CPULoad ticks are less frequent, there is no guarantee that the
	// tokens or slots will be sufficient to service requests. This is
	// particularly the case for slots where we dynamically adjust them, and
	// high contention can suddenly result in high slot utilization even while
	// cpu utilization stays low. We don't want to artificially bottleneck
	// request processing when we are in this slow CPULoad ticks regime since we
	// can't adjust slots or refill tokens fast enough. So we explicitly tell
	// the granters to not do token or slot enforcement.
	skipEnforcement := samplePeriod > time.Millisecond
	coord.granters[SQLKVResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	coord.granters[SQLSQLResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	if coord.granters[KVWork] != nil {
		if !coord.testingDisableSkipEnforcement {
			kvg := coord.granters[KVWork].(*slotGranter)
			kvg.skipSlotEnforcement = skipEnforcement
		}
	}
	if coord.grantChainActive && !coord.tryTerminateGrantChain() {
		return
	}
	coord.tryGrant()
}

// tryGet is called by granter.tryGet with the WorkKind.
func (coord *GrantCoordinator) tryGet(workKind WorkKind, count int64, demuxHandle int8) bool {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	// It is possible that a grant chain is active, and has not yet made its way
	// to this workKind. So it may be more reasonable to queue. But we have some
	// concerns about incurring the delay of multiple goroutine context switches
	// so we ignore this case.
	res := coord.granters[workKind].tryGetLocked(count, demuxHandle)
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
		if coord.grantChainActive && coord.grantChainIndex >= workKind {
			coord.tryTerminateGrantChain()
		}
		return false
	case grantFailLocal:
		return false
	default:
		panic(errors.AssertionFailedf("unknown grantResult"))
	}
}

// returnGrant is called by granter.returnGrant with the WorkKind.
func (coord *GrantCoordinator) returnGrant(workKind WorkKind, count int64, demuxHandle int8) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.granters[workKind].returnGrantLocked(count, demuxHandle)
	if coord.grantChainActive {
		if coord.grantChainIndex > workKind &&
			coord.granters[workKind].requesterHasWaitingRequests() {
			// There are waiting requests that will not be served by the grant chain.
			// Better to terminate it and start afresh.
			if !coord.tryTerminateGrantChain() {
				return
			}
		} else {
			// Else either the grant chain will get to this workKind, or there are no waiting requests.
			return
		}
	}
	coord.tryGrant()
}

// tookWithoutPermission is called by granter.tookWithoutPermission with the
// WorkKind.
func (coord *GrantCoordinator) tookWithoutPermission(
	workKind WorkKind, count int64, demuxHandle int8,
) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.granters[workKind].tookWithoutPermissionLocked(count, demuxHandle)
}

// continueGrantChain is called by granter.continueGrantChain with the
// WorkKind. Never called if !coord.useGrantChains.
func (coord *GrantCoordinator) continueGrantChain(workKind WorkKind, grantChainID grantChainID) {
	if grantChainID == noGrantChain {
		return
	}
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if coord.grantChainID != grantChainID {
		// Someone terminated grantChainID by incrementing coord.grantChainID.
		return
	}
	coord.tryGrant()
}

// delayForGrantChainTermination causes a delay in terminating a grant chain.
// Terminating a grant chain immediately typically causes a new one to start
// immediately that can burst up to its maximum initial grant burst. Which
// means frequent terminations followed by new starts impose little control
// over the rate at which tokens are granted (slots are better controlled
// since we know when the work finishes). This causes huge spikes in the
// runnable goroutine count, observed at 1ms granularity. This spike causes
// the kvSlotAdjuster to ratchet down the totalSlots for KV work all the way
// down to 1, which later causes the runnable gorouting count to crash down
// to a value close to 0, leading to under-utilization.
//
// TODO(sumeer): design admission behavior metrics that can be used to
// understand the behavior in detail and to quantify improvements when changing
// heuristics. One metric would be mean and variance of the runnable count,
// computed using the 1ms samples, and exported/logged every 60s.
var delayForGrantChainTermination = 100 * time.Millisecond

// tryTerminateGrantChain attempts to terminate the current grant chain, and
// returns true iff it is terminated, in which case a new one can be
// immediately started.
// REQUIRES: coord.grantChainActive==true
func (coord *GrantCoordinator) tryTerminateGrantChain() bool {
	now := timeutil.Now()
	if delayForGrantChainTermination > 0 &&
		now.Sub(coord.grantChainStartTime) < delayForGrantChainTermination {
		return false
	}
	// Incrementing the ID will cause the existing grant chain to die out when
	// the grantee calls continueGrantChain.
	coord.grantChainID++
	coord.grantChainActive = false
	coord.grantChainStartTime = time.Time{}
	return true
}

// tryGrant tries to either continue an existing grant chain, or if no grant
// chain is active, tries to start a new grant chain when grant chaining is
// enabled, or grants as much as it can when grant chaining is disabled.
func (coord *GrantCoordinator) tryGrant() {
	startingChain := false
	if !coord.grantChainActive {
		// NB: always set to true when !coord.useGrantChains, and we won't
		// actually use this to start a grant chain (see below).
		startingChain = true
		coord.grantChainIndex = 0
	}
	// Assume that we will not be able to start a new grant chain, or that the
	// existing one will die out. The code below will set it to true if neither
	// is true.
	coord.grantChainActive = false
	grantBurstCount := 0
	// Grant in a burst proportional to numProcs, to generate a runnable for
	// each.
	grantBurstLimit := coord.numProcs
	// Additionally, increase the burst size proportional to a fourth of the
	// overload threshold. We experimentally observed that this resulted in
	// better CPU utilization. We don't use the full overload threshold since we
	// don't want to over grant for non-KV work since that causes the KV slots
	// to (unfairly) start decreasing, since we lose control over how many
	// goroutines are runnable.
	multiplier := int(KVSlotAdjusterOverloadThreshold.Get(&coord.settings.SV) / 4)
	if multiplier == 0 {
		multiplier = 1
	}
	grantBurstLimit *= multiplier
	// Only the case of a grant chain being active returns from within the
	// OuterLoop.
OuterLoop:
	for ; coord.grantChainIndex < numWorkKinds; coord.grantChainIndex++ {
		localDone := false
		granter := coord.granters[coord.grantChainIndex]
		if granter == nil {
			// A GrantCoordinator can be limited to certain WorkKinds, and the
			// remaining will be nil.
			continue
		}
		for granter.requesterHasWaitingRequests() && !localDone {
			chainID := noGrantChain
			if grantBurstCount+1 == grantBurstLimit && coord.useGrantChains {
				chainID = coord.grantChainID
			}
			res := granter.tryGrantLocked(chainID)
			switch res {
			case grantSuccess:
				grantBurstCount++
				if grantBurstCount == grantBurstLimit && coord.useGrantChains {
					coord.grantChainActive = true
					if startingChain {
						coord.grantChainStartTime = timeutil.Now()
					}
					return
				}
			case grantFailDueToSharedResource:
				break OuterLoop
			case grantFailLocal:
				localDone = true
			default:
				panic(errors.AssertionFailedf("unknown grantResult"))
			}
		}
	}
	// INVARIANT: !grantChainActive. The chain either did not start or the
	// existing one died. If the existing one died, we increment grantChainID
	// since it represents the ID to be used for the next chain. Note that
	// startingChain is always true when !useGrantChains, so this if-block is
	// not executed.
	if !startingChain {
		coord.grantChainID++
	}
}

// Close implements the stop.Closer interface.
func (coord *GrantCoordinator) Close() {
	for i := range coord.queues {
		if coord.queues[i] != nil {
			coord.queues[i].close()
		}
	}
}

func (coord *GrantCoordinator) String() string {
	return redact.StringWithoutMarkers(coord)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (coord *GrantCoordinator) SafeFormat(s redact.SafePrinter, verb rune) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	s.Printf("(chain: id: %d active: %t index: %d)",
		coord.grantChainID, coord.grantChainActive, coord.grantChainIndex,
	)

	spaceStr := redact.RedactableString(" ")
	newlineStr := redact.RedactableString("\n")
	curSep := spaceStr
	for i := range coord.granters {
		kind := WorkKind(i)
		switch kind {
		case KVWork:
			switch g := coord.granters[i].(type) {
			case *slotGranter:
				kvsa := coord.cpuLoadListener.(*kvSlotAdjuster)
				s.Printf(
					"%s%s: used: %d, high(moderate)-total: %d(%d) moderate-clamp: %d", curSep, workKindString(kind),
					g.usedSlots, g.totalHighLoadSlots, g.totalModerateLoadSlots, kvsa.moderateSlotsClamp)
				if g.usedSoftSlots > 0 {
					s.Printf(" used-soft: %d", g.usedSoftSlots)
				}
			case *kvStoreTokenGranter:
				s.Printf(" io-avail: %d, elastic-disk-bw-tokens-avail: %d", g.availableIOTokens,
					g.elasticDiskBWTokensAvailable)
			}
		case SQLStatementLeafStartWork, SQLStatementRootStartWork:
			if coord.granters[i] != nil {
				g := coord.granters[i].(*slotGranter)
				s.Printf("%s%s: used: %d, total: %d", curSep, workKindString(kind), g.usedSlots, g.totalHighLoadSlots)
			}
		case SQLKVResponseWork, SQLSQLResponseWork:
			if coord.granters[i] != nil {
				g := coord.granters[i].(*tokenGranter)
				s.Printf("%s%s: avail: %d", curSep, workKindString(kind), g.availableBurstTokens)
				if kind == SQLKVResponseWork {
					curSep = newlineStr
				} else {
					curSep = spaceStr
				}
			}
		}
	}
}

// StoreGrantCoordinators is a container for GrantCoordinators for each store,
// that is used for KV work admission that takes into account store health.
// Currently it is intended only for writes to stores.
type StoreGrantCoordinators struct {
	ambientCtx log.AmbientContext

	settings                    *cluster.Settings
	makeStoreRequesterFunc      makeStoreRequesterFunc
	kvIOTokensExhaustedDuration *metric.Counter
	// These metrics are shared by WorkQueues across stores.
	workQueueMetrics WorkQueueMetrics

	gcMap syncutil.IntMap // map[int64(StoreID)]*GrantCoordinator
	// numStores is used to track the number of stores which have been added
	// to the gcMap. This is used because the IntMap doesn't expose a size
	// api.
	numStores             int
	pebbleMetricsProvider PebbleMetricsProvider
	closeCh               chan struct{}

	disableTickerForTesting bool
}

// SetPebbleMetricsProvider sets a PebbleMetricsProvider and causes the load
// on the various storage engines to be used for admission control.
func (sgc *StoreGrantCoordinators) SetPebbleMetricsProvider(
	startupCtx context.Context, pmp PebbleMetricsProvider, iotc IOThresholdConsumer,
) {
	if sgc.pebbleMetricsProvider != nil {
		panic(errors.AssertionFailedf("SetPebbleMetricsProvider called more than once"))
	}
	sgc.pebbleMetricsProvider = pmp
	sgc.closeCh = make(chan struct{})
	metrics := sgc.pebbleMetricsProvider.GetPebbleMetrics()
	for _, m := range metrics {
		gc := sgc.initGrantCoordinator(m.StoreID)
		// Defensive call to LoadAndStore even though Store ought to be sufficient
		// since SetPebbleMetricsProvider can only be called once. This code
		// guards against duplication of stores returned by GetPebbleMetrics.
		_, loaded := sgc.gcMap.LoadOrStore(int64(m.StoreID), unsafe.Pointer(gc))
		if !loaded {
			sgc.numStores++
		}
		gc.pebbleMetricsTick(startupCtx, m)
		gc.allocateIOTokensTick()
	}
	if sgc.disableTickerForTesting {
		return
	}
	// Attach tracer and log tags.
	ctx := sgc.ambientCtx.AnnotateCtx(context.Background())

	go func() {
		var ticks int64
		ticker := time.NewTicker(ioTokenTickDuration)
		done := false
		for !done {
			select {
			case <-ticker.C:
				ticks++
				if ticks%ticksInAdjustmentInterval == 0 {
					metrics := sgc.pebbleMetricsProvider.GetPebbleMetrics()
					if len(metrics) != sgc.numStores {
						log.Warningf(ctx,
							"expected %d store metrics and found %d metrics", sgc.numStores, len(metrics))
					}
					for _, m := range metrics {
						if unsafeGc, ok := sgc.gcMap.Load(int64(m.StoreID)); ok {
							gc := (*GrantCoordinator)(unsafeGc)
							gc.pebbleMetricsTick(ctx, m)
							iotc.UpdateIOThreshold(roachpb.StoreID(m.StoreID), gc.ioLoadListener.ioThreshold)
						} else {
							log.Warningf(ctx,
								"seeing metrics for unknown storeID %d", m.StoreID)
						}
					}
				}
				sgc.gcMap.Range(func(_ int64, unsafeGc unsafe.Pointer) bool {
					gc := (*GrantCoordinator)(unsafeGc)
					gc.allocateIOTokensTick()
					// true indicates that iteration should continue after the
					// current entry has been processed.
					return true
				})
			case <-sgc.closeCh:
				done = true
			}
		}
		ticker.Stop()
	}()
}

// Experimental observations:
// - Sub-level count of ~40 caused a node heartbeat latency p90, p99 of 2.5s,
//   4s. With a setting that limits sub-level count to 10, before the system
//   is considered overloaded, and adjustmentInterval = 60, we see the actual
//   sub-level count ranging from 5-30, with p90, p99 node heartbeat latency
//   showing a similar wide range, with 1s, 2s being the middle of the range
//   respectively.
// - With tpcc, we sometimes see a sub-level count > 10 with only 100 files in
//   L0. We don't want to restrict tokens in this case since the store is able
//   to recover on its own. One possibility would be to require both the
//   thresholds to be exceeded before we consider the store overloaded. But
//   then we run the risk of having 100+ sub-levels when we hit a file count
//   of 1000. Instead we use a sub-level overload threshold of 20.
//
// We've set these overload thresholds in a way that allows the system to
// absorb short durations (say a few minutes) of heavy write load.
const l0FileCountOverloadThreshold = 1000
const l0SubLevelCountOverloadThreshold = 20

func (sgc *StoreGrantCoordinators) initGrantCoordinator(storeID int32) *GrantCoordinator {
	coord := &GrantCoordinator{
		settings:       sgc.settings,
		useGrantChains: false,
		numProcs:       1,
	}

	kvg := &kvStoreTokenGranter{
		coord: coord,
		// Setting tokens to unlimited is defensive. We expect that
		// pebbleMetricsTick and allocateIOTokensTick will get called during
		// initialization, which will also set these to unlimited.
		availableIOTokens:               unlimitedTokens / ticksInAdjustmentInterval,
		startingIOTokens:                unlimitedTokens / ticksInAdjustmentInterval,
		ioTokensExhaustedDurationMetric: sgc.kvIOTokensExhaustedDuration,
		elasticDiskBWTokensAvailable:    unlimitedTokens / ticksInAdjustmentInterval,
	}
	opts := makeWorkQueueOptions(KVWork)
	// This is IO work, so override the usesTokens value.
	opts.usesTokens = true
	// Share the WorkQueue metrics across all stores.
	// TODO(sumeer): add per-store WorkQueue state for debug.zip and db console.
	opts.metrics = &sgc.workQueueMetrics
	granters := [numWorkClasses]granterWithStoreWriteDone{
		&kvStoreTokenChildGranter{
			workClass: regularWorkClass,
			parent:    kvg,
		},
		&kvStoreTokenChildGranter{
			workClass: elasticWorkClass,
			parent:    kvg,
		},
	}

	storeReq := sgc.makeStoreRequesterFunc(sgc.ambientCtx, granters, sgc.settings, opts)
	coord.queues[KVWork] = storeReq
	requesters := storeReq.getRequesters()
	kvg.regularRequester = requesters[regularWorkClass]
	kvg.elasticRequester = requesters[elasticWorkClass]
	coord.granters[KVWork] = kvg
	coord.ioLoadListener = &ioLoadListener{
		storeID:               storeID,
		settings:              sgc.settings,
		kvRequester:           storeReq,
		perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
		diskBandwidthLimiter:  makeDiskBandwidthLimiter(),
	}
	coord.ioLoadListener.mu.Mutex = &coord.mu
	coord.ioLoadListener.mu.kvGranter = kvg
	return coord
}

// TryGetQueueForStore returns a WorkQueue for the given storeID, or nil if
// the storeID is not known.
func (sgc *StoreGrantCoordinators) TryGetQueueForStore(storeID int32) *StoreWorkQueue {
	if unsafeGranter, ok := sgc.gcMap.Load(int64(storeID)); ok {
		granter := (*GrantCoordinator)(unsafeGranter)
		return granter.queues[KVWork].(*StoreWorkQueue)
	}
	return nil
}

func (sgc *StoreGrantCoordinators) close() {
	// closeCh can be nil in tests that never called SetPebbleMetricsProvider.
	if sgc.closeCh != nil {
		close(sgc.closeCh)
	}

	sgc.gcMap.Range(func(_ int64, unsafeGc unsafe.Pointer) bool {
		gc := (*GrantCoordinator)(unsafeGc)
		gc.Close()
		// true indicates that iteration should continue after the
		// current entry has been processed.
		return true
	})
}

// GrantCoordinators holds a regular GrantCoordinator for all work, and a
// StoreGrantCoordinators that allows for per-store GrantCoordinators for
// KVWork that involves writes.
type GrantCoordinators struct {
	Stores  *StoreGrantCoordinators
	Regular *GrantCoordinator
}

// Close implements the stop.Closer interface.
func (gcs GrantCoordinators) Close() {
	gcs.Stores.close()
	gcs.Regular.Close()
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

type ioLoadListenerState struct {
	// Cumulative.
	cumL0AddedBytes uint64
	// Gauge.
	curL0Bytes int64
	// Cumulative.
	cumWriteStallCount int64
	diskBW             struct {
		// Cumulative
		bytesRead        uint64
		bytesWritten     uint64
		incomingLSMBytes uint64
	}

	// Exponentially smoothed per interval values.

	smoothedIntL0CompactedBytes int64 // bytes leaving L0
	// Smoothed history of byte tokens calculated based on compactions out of L0.
	smoothedCompactionByteTokens float64

	// Smoothed history of flush tokens calculated based on memtable flushes,
	// before multiplying by target fraction.
	smoothedNumFlushTokens float64
	// The target fraction to be used for the effective flush tokens. It is in
	// the interval
	// [MinFlushUtilizationFraction,maxFlushUtilTargetFraction].
	flushUtilTargetFraction float64

	// totalNumByteTokens represents the tokens to give out until the next call to
	// adjustTokens. They are parceled out in small intervals. byteTokensAllocated
	// represents what has been given out.
	totalNumByteTokens  int64
	byteTokensAllocated int64
	// Used tokens can be negative if some tokens taken in one interval were
	// returned in another, but that will be extremely rare.
	byteTokensUsed int64

	// elasticDiskBWTokens represents the tokens to give out until the next call
	// to adjustTokens. They are parceled out in small intervals.
	// elasticDiskTokensAllocated represents what has been given out.
	elasticDiskBWTokens          int64
	elasticDiskBWTokensAllocated int64
}

// ioLoadListener adjusts tokens in kvStoreTokenGranter for IO, specifically due to
// overload caused by writes. IO uses tokens and not slots since work
// completion is not an indicator that the "resource usage" has ceased -- it
// just means that the write has been applied to the WAL. Most of the work is
// in flushing to sstables and the following compactions, which happens later.
//
//
// Token units are in bytes and used to protect a number of virtual or
// physical resource bottlenecks:
// - Compactions out of L0: compactions out of L0 can fall behind and cause
//   too many sub-levels or files in L0.
// - Flushes into L0: flushes of memtables to L0 can fall behind and cause
//   write stalls due to too many memtables.
// - Disk bandwidth: there is typically an aggregate read+write provisioned
//   bandwidth, and if it is fully utilized, IO operations can start queueing
//   and encounter high latency.
//
// For simplicity, after ioLoadListener computes the tokens due to compaction
// or flush bottlenecks, it computes the minimum and passes that value to
// granterWithIOTokens.setAvailableIOTokensLocked. That is, instead of working
// with multiple token dimensions, these two token dimensions get collapsed
// into one for enforcement purposes. This also helps simplify the integration
// with WorkQueue which is dealing with a single dimension. The consumption of
// these tokens is based on how many bytes an admitted work adds to L0.
//
// The disk bandwidth constraint is used to compute a token count for elastic
// work (see disk_bandwidth.go for the reasons why this is limited to elastic
// work). Additionally, these tokens are meant be consumed for all incoming
// bytes into the LSM, and not just those written to L0 e.g. ingested bytes
// into L3 should also consume such tokens. Note that we call these disk
// bandwidth tokens, but that is a misnomer -- these are tokens for incoming
// bytes into the LSM, motivated by disk bandwidth as a bottleneck resource,
// and not consumed for every write to the disk (e.g. by compactions). Since
// these tokens are consumed by all incoming bytes into the LSM, and not just
// those into L0, it suggests explicitly modeling this as a separate
// dimension. However, since modeling as a separate dimension everywhere would
// lead to code complexity, we adopt the following compromise:
//
// - Like the other token dimensions, ioLoadListener computes a different
//   elastic token count (using diskBandwidthLimiter), and a different model
//   for consumption (via
//   storePerWorkTokenEstimator.atDoneDiskBWTokensLinearModel).
//
// - granterWithIOTokens, implemented by kvStoreTokenGranter, which enforces
//   the token count, also treats this as a separate dimension.
//
// - WorkQueue works with a single dimension, so the tokens consumed at
//   admission time are based on L0-bytes estimate. However, when
//   StoreWorkQueue informs kvStoreTokenGranter of work completion (by calling
//   storeWriteDone), the tokens are adjusted differently for the
//   flush/compaction L0 tokens and for the "disk bandwidth" tokens.
type ioLoadListener struct {
	storeID     int32
	settings    *cluster.Settings
	kvRequester storeRequester
	mu          struct {
		// Used when changing state in kvGranter. This is a pointer since it is
		// the same as GrantCoordinator.mu.
		*syncutil.Mutex
		kvGranter granterWithIOTokens
	}

	// Stats used to compute interval stats.
	statsInitialized bool
	adjustTokensResult
	perWorkTokenEstimator storePerWorkTokenEstimator
	diskBandwidthLimiter  diskBandwidthLimiter
}

const unlimitedTokens = math.MaxInt64

// Token changes are made at a coarse time granularity of 15s since
// compactions can take ~10s to complete. The totalNumByteTokens to give out over
// the 15s interval are given out in a smoothed manner, at 250ms intervals.
// This has similarities with the following kinds of token buckets:
// - Zero replenishment rate and a burst value that is changed every 15s. We
//   explicitly don't want a huge burst every 15s.
// - A replenishment rate equal to totalNumByteTokens/60, with a burst capped at
//   totalNumByteTokens/60. The only difference with the code here is that if
//   totalNumByteTokens is small, the integer rounding effects are compensated for.
//
// In an experiment with extreme overload using KV0 with block size 64KB,
// and 4096 clients, we observed the following states of L0 at 1min
// intervals (r-amp is the L0 sub-level count), in the absence of any
// admission control:
//
// __level_____count____size___score______in__ingest(sz_cnt)____move(sz_cnt)___write(sz_cnt)____read___r-amp___w-amp›
//       0        96   158 M    2.09   315 M     0 B       0     0 B       0   305 M     178     0 B       3     1.0›
//       0      1026   1.7 G    3.15   4.7 G     0 B       0     0 B       0   4.7 G   2.8 K     0 B      24     1.0›
//       0      1865   3.0 G    2.86   9.1 G     0 B       0     0 B       0   9.1 G   5.5 K     0 B      38     1.0›
//       0      3225   4.9 G    3.46    13 G     0 B       0     0 B       0    13 G   8.3 K     0 B      59     1.0›
//       0      4720   7.0 G    3.46    17 G     0 B       0     0 B       0    17 G    11 K     0 B      85     1.0›
//       0      6120   9.0 G    4.13    21 G     0 B       0     0 B       0    21 G    14 K     0 B     109     1.0›
//
//
// Note the fast growth in sub-level count. Production issues typically have
// slower growth towards an unhealthy state (remember that similar stats in
// the logs of a regular CockroachDB node are at 10min intervals, and not at
// 1min).
//
// In the above experiment, L0 compaction durations at 200+ sub-levels were
// usually sane, with most L0 compactions < 10s, and with a bandwidth of
// ~80MB/s. There were some 1-2GB compactions that took ~20s. The
// expectation is that with the throttling done by admission control here,
// we should almost never see multi-minute compactions. Which makes it
// reasonable to simply use metrics that are updated when compactions
// complete (as opposed to also tracking progress in bytes of on-going
// compactions).
//
// The 250ms interval to hand out the computed tokens is due to the needs of
// flush tokens. For compaction tokens, a 1s interval is fine-grained enough.
//
// If flushing a memtable take 100ms, then 10 memtables can be sustainably
// flushed in 1s. If we dole out flush tokens in 1s intervals, then there are
// enough tokens to create 10 memtables at the very start of a 1s interval,
// which will cause a write stall. Intuitively, the faster it is to flush a
// memtable, the smaller the interval for doling out these tokens. We have
// observed flushes taking ~0.5s, so we picked a 250ms interval for doling out
// these tokens. We could use a value smaller than 250ms, but we've observed
// CPU utilization issues at lower intervals (see the discussion in
// runnable.go).
const adjustmentInterval = 15
const ticksInAdjustmentInterval = 60
const ioTokenTickDuration = 250 * time.Millisecond

func cumLSMWriteAndIngestedBytes(
	m *pebble.Metrics,
) (writeAndIngestedBytes uint64, ingestedBytes uint64) {
	for i := range m.Levels {
		writeAndIngestedBytes += m.Levels[i].BytesIngested + m.Levels[i].BytesFlushed
		ingestedBytes += m.Levels[i].BytesIngested
	}
	return writeAndIngestedBytes, ingestedBytes
}

// pebbleMetricsTicks is called every adjustmentInterval seconds, and decides
// the token allocations until the next call.
func (io *ioLoadListener) pebbleMetricsTick(ctx context.Context, metrics StoreMetrics) {
	ctx = logtags.AddTag(ctx, "s", io.storeID)
	m := metrics.Metrics
	if !io.statsInitialized {
		io.statsInitialized = true
		sas := io.kvRequester.getStoreAdmissionStats()
		cumLSMIncomingBytes, cumLSMIngestedBytes := cumLSMWriteAndIngestedBytes(metrics.Metrics)
		io.perWorkTokenEstimator.updateEstimates(metrics.Levels[0], cumLSMIngestedBytes, sas)
		io.adjustTokensResult = adjustTokensResult{
			ioLoadListenerState: ioLoadListenerState{
				cumL0AddedBytes:    m.Levels[0].BytesFlushed + m.Levels[0].BytesIngested,
				curL0Bytes:         m.Levels[0].Size,
				cumWriteStallCount: metrics.WriteStallCount,
				// No initial limit, i.e, the first interval is unlimited.
				totalNumByteTokens:  unlimitedTokens,
				elasticDiskBWTokens: unlimitedTokens,
			},
			aux: adjustTokensAuxComputations{},
			ioThreshold: &admissionpb.IOThreshold{
				L0NumSubLevels:          int64(m.Levels[0].Sublevels),
				L0NumSubLevelsThreshold: math.MaxInt64,
				L0NumFiles:              m.Levels[0].NumFiles,
				L0NumFilesThreshold:     math.MaxInt64,
			},
		}
		io.diskBW.bytesRead = metrics.DiskStats.BytesRead
		io.diskBW.bytesWritten = metrics.DiskStats.BytesWritten
		io.diskBW.incomingLSMBytes = cumLSMIncomingBytes
		io.copyAuxEtcFromPerWorkEstimator()
		return
	}
	io.adjustTokens(ctx, metrics)
}

// allocateTokensTick gives out 1/ticksInAdjustmentInterval of the
// various tokens every 250ms.
func (io *ioLoadListener) allocateTokensTick() {
	allocateFunc := func(total int64, allocated int64) (toAllocate int64) {
		// unlimitedTokens==MaxInt64, so avoid overflow in the rounding up
		// calculation.
		if total >= unlimitedTokens-(ticksInAdjustmentInterval-1) {
			toAllocate = total / ticksInAdjustmentInterval
		} else {
			// Round up so that we don't accumulate tokens to give in a burst on the
			// last tick.
			toAllocate = (total + ticksInAdjustmentInterval - 1) / ticksInAdjustmentInterval
			if toAllocate < 0 {
				panic(errors.AssertionFailedf("toAllocate is negative %d", toAllocate))
			}
			if toAllocate+allocated > total {
				toAllocate = total - allocated
			}
		}
		return toAllocate
	}
	// INVARIANT: toAllocate* >= 0.
	toAllocateByteTokens := allocateFunc(io.totalNumByteTokens, io.byteTokensAllocated)
	if toAllocateByteTokens < 0 {
		panic(errors.AssertionFailedf("toAllocateByteTokens is negative %d", toAllocateByteTokens))
	}
	toAllocateElasticDiskBWTokens :=
		allocateFunc(io.elasticDiskBWTokens, io.elasticDiskBWTokensAllocated)
	if toAllocateElasticDiskBWTokens < 0 {
		panic(errors.AssertionFailedf("toAllocateElasticDiskBWTokens is negative %d",
			toAllocateElasticDiskBWTokens))
	}
	// INVARIANT: toAllocate >= 0.
	io.mu.Lock()
	defer io.mu.Unlock()
	io.byteTokensAllocated += toAllocateByteTokens
	if io.byteTokensAllocated < 0 {
		panic(errors.AssertionFailedf("tokens allocated is negative %d", io.byteTokensAllocated))
	}
	io.byteTokensUsed += io.mu.kvGranter.setAvailableIOTokensLocked(toAllocateByteTokens)
	io.elasticDiskBWTokensAllocated += toAllocateElasticDiskBWTokens
	io.mu.kvGranter.setAvailableElasticDiskBandwidthTokensLocked(toAllocateElasticDiskBWTokens)
}

func computeIntervalDiskLoadInfo(
	prevCumBytesRead uint64, prevCumBytesWritten uint64, diskStats DiskStats,
) intervalDiskLoadInfo {
	return intervalDiskLoadInfo{
		readBandwidth:        int64((diskStats.BytesRead - prevCumBytesRead) / adjustmentInterval),
		writeBandwidth:       int64((diskStats.BytesWritten - prevCumBytesWritten) / adjustmentInterval),
		provisionedBandwidth: diskStats.ProvisionedBandwidth,
	}
}

// adjustTokens computes a new value of totalNumByteTokens (and resets
// tokensAllocated). The new value, when overloaded, is based on comparing how
// many bytes are being moved out of L0 via compactions with the average
// number of bytes being added to L0 per KV work. We want the former to be
// (significantly) larger so that L0 returns to a healthy state. The byte
// token computation also takes into account the flush throughput, since an
// inability to flush fast enough can result in write stalls due to high
// memtable counts, which we want to avoid as it can cause latency hiccups of
// 100+ms for all write traffic.
func (io *ioLoadListener) adjustTokens(ctx context.Context, metrics StoreMetrics) {
	sas := io.kvRequester.getStoreAdmissionStats()
	res := io.adjustTokensInner(ctx, io.ioLoadListenerState,
		metrics.Levels[0], metrics.WriteStallCount, metrics.InternalIntervalMetrics,
		L0FileCountOverloadThreshold.Get(&io.settings.SV),
		L0SubLevelCountOverloadThreshold.Get(&io.settings.SV),
		MinFlushUtilizationFraction.Get(&io.settings.SV),
	)
	io.adjustTokensResult = res
	cumLSMIncomingBytes, cumLSMIngestedBytes := cumLSMWriteAndIngestedBytes(metrics.Metrics)
	{
		// Disk Bandwidth tokens.
		io.aux.diskBW.intervalDiskLoadInfo = computeIntervalDiskLoadInfo(
			io.diskBW.bytesRead, io.diskBW.bytesWritten, metrics.DiskStats)
		io.mu.Lock()
		diskTokensUsed := io.mu.kvGranter.getDiskTokensUsedAndResetLocked()
		io.mu.Unlock()
		io.aux.diskBW.intervalLSMInfo = intervalLSMInfo{
			incomingBytes:     int64(cumLSMIncomingBytes) - int64(io.diskBW.incomingLSMBytes),
			regularTokensUsed: diskTokensUsed[regularWorkClass],
			elasticTokensUsed: diskTokensUsed[elasticWorkClass],
		}
		if metrics.DiskStats.ProvisionedBandwidth > 0 {
			io.elasticDiskBWTokens = io.diskBandwidthLimiter.computeElasticTokens(ctx,
				io.aux.diskBW.intervalDiskLoadInfo, io.aux.diskBW.intervalLSMInfo)
			io.elasticDiskBWTokensAllocated = 0
		}
		if metrics.DiskStats.ProvisionedBandwidth == 0 ||
			!DiskBandwidthTokensForElasticEnabled.Get(&io.settings.SV) {
			io.elasticDiskBWTokens = unlimitedTokens
		}
		io.diskBW.bytesRead = metrics.DiskStats.BytesRead
		io.diskBW.bytesWritten = metrics.DiskStats.BytesWritten
		io.diskBW.incomingLSMBytes = cumLSMIncomingBytes
	}
	io.perWorkTokenEstimator.updateEstimates(metrics.Levels[0], cumLSMIngestedBytes, sas)
	io.copyAuxEtcFromPerWorkEstimator()
	requestEstimates := io.perWorkTokenEstimator.getStoreRequestEstimatesAtAdmission()
	io.kvRequester.setStoreRequestEstimates(requestEstimates)
	l0WriteLM, l0IngestLM, ingestLM := io.perWorkTokenEstimator.getModelsAtAdmittedDone()
	io.mu.Lock()
	io.mu.kvGranter.setAdmittedDoneModelsLocked(l0WriteLM, l0IngestLM, ingestLM)
	io.mu.Unlock()
	if _, overloaded := io.ioThreshold.Score(); overloaded || io.aux.doLogFlush ||
		io.elasticDiskBWTokens != unlimitedTokens {
		log.Infof(ctx, "IO overload: %s", io.adjustTokensResult)
	}
}

// copyAuxEtcFromPerWorkEstimator copies the auxiliary and other numerical
// state from io.perWorkTokenEstimator. This is helpful in keeping all the
// numerical state for understanding the behavior of ioLoadListener and its
// helpers in one place for simplicity of logging.
func (io *ioLoadListener) copyAuxEtcFromPerWorkEstimator() {
	// Copy the aux so that the printing story is simplified.
	io.adjustTokensResult.aux.perWorkTokensAux = io.perWorkTokenEstimator.aux
	requestEstimates := io.perWorkTokenEstimator.getStoreRequestEstimatesAtAdmission()
	io.adjustTokensResult.requestEstimates = requestEstimates
	l0WriteLM, l0IngestLM, ingestLM := io.perWorkTokenEstimator.getModelsAtAdmittedDone()
	io.adjustTokensResult.l0WriteLM = l0WriteLM
	io.adjustTokensResult.l0IngestLM = l0IngestLM
	io.adjustTokensResult.ingestLM = ingestLM
}

type tokenKind int8

const (
	compactionTokenKind tokenKind = iota
	flushTokenKind
)

// adjustTokensAuxComputations encapsulates auxiliary numerical state for
// ioLoadListener that is helpful for understanding its behavior.
type adjustTokensAuxComputations struct {
	intL0AddedBytes     int64
	intL0CompactedBytes int64

	intFlushTokens      float64
	intFlushUtilization float64
	intWriteStalls      int64

	prevTokensUsed int64
	tokenKind      tokenKind

	perWorkTokensAux perWorkTokensAux
	doLogFlush       bool

	diskBW struct {
		intervalDiskLoadInfo intervalDiskLoadInfo
		intervalLSMInfo      intervalLSMInfo
	}
}

// adjustTokensInner is used for computing tokens based on compaction and
// flush bottlenecks.
func (*ioLoadListener) adjustTokensInner(
	ctx context.Context,
	prev ioLoadListenerState,
	l0Metrics pebble.LevelMetrics,
	cumWriteStallCount int64,
	im *pebble.InternalIntervalMetrics,
	threshNumFiles, threshNumSublevels int64,
	minFlushUtilTargetFraction float64,
) adjustTokensResult {
	ioThreshold := &admissionpb.IOThreshold{
		L0NumFiles:              l0Metrics.NumFiles,
		L0NumFilesThreshold:     threshNumFiles,
		L0NumSubLevels:          int64(l0Metrics.Sublevels),
		L0NumSubLevelsThreshold: threshNumSublevels,
	}

	curL0Bytes := l0Metrics.Size
	cumL0AddedBytes := l0Metrics.BytesFlushed + l0Metrics.BytesIngested
	// L0 growth over the last interval.
	intL0AddedBytes := int64(cumL0AddedBytes) - int64(prev.cumL0AddedBytes)
	if intL0AddedBytes < 0 {
		// intL0AddedBytes is a simple delta computation over individually cumulative
		// stats, so should not be negative.
		log.Warningf(ctx, "intL0AddedBytes %d is negative", intL0AddedBytes)
		intL0AddedBytes = 0
	}
	// intL0CompactedBytes are due to finished compactions.
	intL0CompactedBytes := prev.curL0Bytes + intL0AddedBytes - curL0Bytes
	if intL0CompactedBytes < 0 {
		// Ignore potential inconsistencies across cumulative stats and current L0
		// bytes (gauge).
		intL0CompactedBytes = 0
	}
	const alpha = 0.5
	// Compaction scheduling can be uneven in prioritizing L0 for compactions,
	// so smooth out what is being removed by compactions.
	smoothedIntL0CompactedBytes := int64(alpha*float64(intL0CompactedBytes) + (1-alpha)*float64(prev.smoothedIntL0CompactedBytes))

	// Flush tokens:
	//
	// Write stalls happen when flushing of memtables is a bottleneck.
	//
	// Computing Flush Tokens:
	// Flush can go from not being the bottleneck in one 15s interval
	// (adjustmentInterval) to being the bottleneck in the next 15s interval
	// (e.g. when L0 falls below the unhealthy threshold and compaction tokens
	// become unlimited). So the flush token limit has to react quickly (cannot
	// afford to wait for multiple 15s intervals). We've observed that if we
	// normalize the flush rate based on flush loop utilization (the PeakRate
	// computation below), and use that to compute flush tokens, the token
	// counts are quite stable. Here are two examples, showing this steady token
	// count computed using PeakRate of the flush ThroughputMetric, despite
	// changes in flush loop utilization (the util number below).
	//
	// Example 1: Case where IO bandwidth was not a bottleneck
	// flush: tokens: 2312382401, util: 0.90
	// flush: tokens: 2345477107, util: 0.31
	// flush: tokens: 2317829891, util: 0.29
	// flush: tokens: 2428387843, util: 0.17
	//
	// Example 2: Case where IO bandwidth became a bottleneck (and mean fsync
	// latency was fluctuating between 1ms and 4ms in the low util to high util
	// cases).
	//
	// flush: tokens: 1406132615, util: 1.00
	// flush: tokens: 1356476227, util: 0.64
	// flush: tokens: 1374880806, util: 0.24
	// flush: tokens: 1328578534, util: 0.96
	//
	// Hence, using PeakRate as a basis for computing flush tokens seems sound.
	// The other important question is what fraction of PeakRate avoids write
	// stalls. It is likely less than 100% since while a flush is ongoing,
	// memtables can accumulate and cause a stall. For example, we have observed
	// write stalls at 80% of PeakRate. The fraction depends on configuration
	// parameters like MemTableStopWritesThreshold (defaults to 4 in
	// CockroachDB), and environmental and workload factors like how long a
	// flush takes to flush a single 64MB memtable. Instead of trying to measure
	// and adjust for these, we use a simple multiplier,
	// flushUtilTargetFraction. By default, flushUtilTargetFraction ranges
	// between 0.5 and 1.5. The lower bound is configurable via
	// admission.min_flush_util_percent and if configured above the upper bound,
	// the upper bound will be ignored and the target fraction will not be
	// dynamically adjusted. The dynamic adjustment logic uses an additive step
	// size of flushUtilTargetFractionIncrement (0.025), with the following
	// logic:
	// - Reduce the fraction if there is a write-stall. The reduction may use a
	//   small multiple of flushUtilTargetFractionIncrement. This is so that
	//   this probing spends more time below the threshold where write stalls
	//   occur.
	// - Increase fraction if no write-stall and flush tokens were almost all
	//   used.
	//
	// This probing unfortunately cannot eliminate write stalls altogether.
	// Future improvements could use more history to settle on a good
	// flushUtilTargetFraction for longer, or use some measure of how close we
	// are to a write-stall to stop the increase.
	//
	// Ingestion and flush tokens:
	//
	// Ingested sstables do not utilize any flush capacity. Consider 2 cases:
	// - sstable ingested into L0: there was either data overlap with L0, or
	//   file boundary overlap with L0-L6. To be conservative, lets assume there
	//   was data overlap, and that this data overlap extended into the memtable
	//   at the time of ingestion. Memtable(s) would have been force flushed to
	//   handle such overlap. The cost of flushing a memtable is based on how
	//   much of the allocated memtable capacity is used, so an early flush
	//   seems harmless. However, write stalls are based on allocated memtable
	//   capacity, so there is a potential negative interaction of these forced
	//   flushes since they cause additional memtable capacity allocation.
	// - sstable ingested into L1-L6: there was no data overlap with L0, which
	//   implies that there was no reason to flush memtables.
	//
	// Since there is some interaction noted in bullet 1, and because it
	// simplifies the admission control token behavior, we use flush tokens in
	// an identical manner as compaction tokens -- to be consumed by all data
	// flowing into L0. Some of this conservative choice will be compensated for
	// by flushUtilTargetFraction (when the mix of ingestion and actual flushes
	// are stable). Another thing to note is that compactions out of L0 are
	// typically the more persistent bottleneck than flushes for the following
	// reason:
	// There is a dedicated flush thread. With a maximum compaction concurrency
	// of C, we have up to C threads dedicated to handling the write-amp of W
	// (caused by rewriting the same data). So C/(W-1) threads on average are
	// reading the original data (that will be rewritten W-1 times). Since L0
	// can have multiple overlapping files, and intra-L0 compactions are usually
	// avoided, we can assume (at best) that the original data (in L0) is being
	// read only when compacting to levels lower than L0. That is, C/(W-1)
	// threads are reading from L0 to compact to levels lower than L0. Since W
	// can be 20+ and C defaults to 3 (we plan to dynamically adjust C but one
	// can expect C to be <= 10), C/(W-1) < 1. So the main reason we are
	// considering flush tokens is transient flush bottlenecks, and workloads
	// where W is small.

	// Compute flush utilization for this interval. A very low flush utilization
	// will cause flush tokens to be unlimited.
	intFlushUtilization := float64(0)
	if im.Flush.WriteThroughput.WorkDuration > 0 {
		intFlushUtilization = float64(im.Flush.WriteThroughput.WorkDuration) /
			float64(im.Flush.WriteThroughput.WorkDuration+im.Flush.WriteThroughput.IdleDuration)
	}
	// Compute flush tokens for this interval that would cause 100% utilization.
	intFlushTokens := float64(im.Flush.WriteThroughput.PeakRate()) * adjustmentInterval
	intWriteStalls := cumWriteStallCount - prev.cumWriteStallCount

	// Ensure flushUtilTargetFraction is in the configured bounds. This also
	// does lazy initialization.
	const maxFlushUtilTargetFraction = 1.5
	flushUtilTargetFraction := prev.flushUtilTargetFraction
	if flushUtilTargetFraction == 0 {
		// Initialization: use the maximum configured fraction.
		flushUtilTargetFraction = minFlushUtilTargetFraction
		if flushUtilTargetFraction < maxFlushUtilTargetFraction {
			flushUtilTargetFraction = maxFlushUtilTargetFraction
		}
	} else if flushUtilTargetFraction < minFlushUtilTargetFraction {
		// The min can be changed in a running system, so we bump up to conform to
		// the min.
		flushUtilTargetFraction = minFlushUtilTargetFraction
	}
	numFlushTokens := int64(unlimitedTokens)
	// doLogFlush becomes true if something interesting is done here.
	doLogFlush := false
	smoothedNumFlushTokens := prev.smoothedNumFlushTokens
	const flushUtilIgnoreThreshold = 0.05
	if intFlushUtilization > flushUtilIgnoreThreshold {
		if smoothedNumFlushTokens == 0 {
			// Initialization.
			smoothedNumFlushTokens = intFlushTokens
		} else {
			smoothedNumFlushTokens = alpha*intFlushTokens + (1-alpha)*prev.smoothedNumFlushTokens
		}
		const flushUtilTargetFractionIncrement = 0.025
		// Have we used, over the last (15s) cycle, more than 90% of the tokens we
		// would give out for the next cycle? If yes, highTokenUsage is true.
		highTokenUsage :=
			float64(prev.byteTokensUsed) >= 0.9*smoothedNumFlushTokens*flushUtilTargetFraction
		if intWriteStalls > 0 {
			// Try decrease since there were write-stalls.
			numDecreaseSteps := 1
			// These constants of 5, 3, 2, 2 were found to work reasonably well,
			// without causing large decreases. We need better benchmarking to tune
			// such constants.
			if intWriteStalls >= 5 {
				numDecreaseSteps = 3
			} else if intWriteStalls >= 2 {
				numDecreaseSteps = 2
			}
			for i := 0; i < numDecreaseSteps; i++ {
				if flushUtilTargetFraction >= minFlushUtilTargetFraction+flushUtilTargetFractionIncrement {
					flushUtilTargetFraction -= flushUtilTargetFractionIncrement
					doLogFlush = true
				} else {
					break
				}
			}
		} else if flushUtilTargetFraction < maxFlushUtilTargetFraction-flushUtilTargetFractionIncrement &&
			intWriteStalls == 0 && highTokenUsage {
			// No write-stalls, and token usage was high, so give out more tokens.
			flushUtilTargetFraction += flushUtilTargetFractionIncrement
			doLogFlush = true
		}
		if highTokenUsage {
			doLogFlush = true
		}
		flushTokensFloat := flushUtilTargetFraction * smoothedNumFlushTokens
		if flushTokensFloat < float64(math.MaxInt64) {
			numFlushTokens = int64(flushTokensFloat)
		}
		// Else avoid overflow by using the previously set unlimitedTokens. This
		// should not really happen.
	}
	// Else intFlushUtilization is too low. We don't want to make token
	// determination based on a very low utilization, so we hand out unlimited
	// tokens. Note that flush utilization has been observed to fluctuate from
	// 0.16 to 0.9 in a single interval, when compaction tokens are not limited,
	// hence we have set flushUtilIgnoreThreshold to a very low value. If we've
	// erred towards it being too low, we run the risk of computing incorrect
	// tokens. If we've erred towards being too high, we run the risk of giving
	// out unlimitedTokens and causing write stalls.

	// We constrain admission based on compactions, if the store is over the L0
	// threshold.
	var totalNumByteTokens int64
	var smoothedCompactionByteTokens float64

	_, overloaded := ioThreshold.Score()
	if overloaded {
		// Don't admit more byte work than we can remove via compactions. totalNumByteTokens
		// tracks our goal for admission.
		// Scale down since we want to get under the thresholds over time. This
		// scaling could be adjusted based on how much above the threshold we are,
		// but for now we just use a constant.
		fTotalNumByteTokens := float64(smoothedIntL0CompactedBytes / 2.0)
		// Smooth it. This may seem peculiar since we are already using
		// smoothedIntL0CompactedBytes, but the else clause below uses a different
		// computation so we also want the history of smoothedTotalNumByteTokens.
		smoothedCompactionByteTokens = alpha*fTotalNumByteTokens + (1-alpha)*prev.smoothedCompactionByteTokens
		if float64(math.MaxInt64) < smoothedCompactionByteTokens {
			// Avoid overflow. This should not really happen.
			totalNumByteTokens = math.MaxInt64
		} else {
			totalNumByteTokens = int64(smoothedCompactionByteTokens)
		}
	} else {
		// Under the threshold. Maintain a smoothedTotalNumByteTokens based on what was
		// removed, so that when we go over the threshold we have some history.
		// This is also useful when we temporarily dip below the threshold --
		// we've seen extreme situations with alternating 15s intervals of above
		// and below the threshold.
		numTokens := intL0CompactedBytes
		smoothedCompactionByteTokens = alpha*float64(numTokens) + (1-alpha)*prev.smoothedCompactionByteTokens
		totalNumByteTokens = unlimitedTokens
	}
	// Use the minimum of the token count calculated using compactions and
	// flushes.
	tokenKind := compactionTokenKind
	if totalNumByteTokens > numFlushTokens {
		totalNumByteTokens = numFlushTokens
		tokenKind = flushTokenKind
	}
	// Install the latest cumulative stats.
	return adjustTokensResult{
		ioLoadListenerState: ioLoadListenerState{
			cumL0AddedBytes:              cumL0AddedBytes,
			curL0Bytes:                   curL0Bytes,
			cumWriteStallCount:           cumWriteStallCount,
			smoothedIntL0CompactedBytes:  smoothedIntL0CompactedBytes,
			smoothedCompactionByteTokens: smoothedCompactionByteTokens,
			smoothedNumFlushTokens:       smoothedNumFlushTokens,
			flushUtilTargetFraction:      flushUtilTargetFraction,
			totalNumByteTokens:           totalNumByteTokens,
			byteTokensAllocated:          0,
			byteTokensUsed:               0,
		},
		aux: adjustTokensAuxComputations{
			intL0AddedBytes:     intL0AddedBytes,
			intL0CompactedBytes: intL0CompactedBytes,
			intFlushTokens:      intFlushTokens,
			intFlushUtilization: intFlushUtilization,
			intWriteStalls:      intWriteStalls,
			prevTokensUsed:      prev.byteTokensUsed,
			tokenKind:           tokenKind,
			doLogFlush:          doLogFlush,
		},
		ioThreshold: ioThreshold,
	}
}

// adjustTokensResult encapsulates all the numerical state of ioLoadListener.
type adjustTokensResult struct {
	ioLoadListenerState
	requestEstimates storeRequestEstimates
	l0WriteLM        tokensLinearModel
	l0IngestLM       tokensLinearModel
	ingestLM         tokensLinearModel
	aux              adjustTokensAuxComputations
	ioThreshold      *admissionpb.IOThreshold // never nil
}

func max(i, j int64) int64 {
	if i < j {
		return j
	}
	return i
}

func (res adjustTokensResult) SafeFormat(p redact.SafePrinter, _ rune) {
	ib := humanizeutil.IBytes
	// NB: "≈" indicates smoothed quantities.
	p.Printf("compaction score %v (%d ssts, %d sub-levels), ", res.ioThreshold, res.ioThreshold.L0NumFiles, res.ioThreshold.L0NumSubLevels)
	p.Printf("L0 growth %s (write %s ingest %s ignored %s): ", ib(res.aux.intL0AddedBytes),
		ib(res.aux.perWorkTokensAux.intL0WriteBytes), ib(res.aux.perWorkTokensAux.intL0IngestedBytes),
		ib(res.aux.perWorkTokensAux.intL0IgnoredIngestedBytes))
	// Writes to L0 that we expected because requests told admission control.
	// This is the "easy path", from an estimation perspective, if all regular
	// writes accurately tell us what they write, and all ingests tell us what
	// they ingest and all of ingests into L0.
	p.Printf("requests %d (%d bypassed) with ", res.aux.perWorkTokensAux.intWorkCount,
		res.aux.perWorkTokensAux.intBypassedWorkCount)
	p.Printf("%s acc-write (%s bypassed) + ",
		ib(res.aux.perWorkTokensAux.intL0WriteAccountedBytes),
		ib(res.aux.perWorkTokensAux.intL0WriteBypassedAccountedBytes))
	// Ingestion bytes that we expected because requests told admission control.
	p.Printf("%s acc-ingest (%s bypassed) + ",
		ib(res.aux.perWorkTokensAux.intIngestedAccountedBytes),
		ib(res.aux.perWorkTokensAux.intIngestedBypassedAccountedBytes))
	// The models we are fitting to compute tokens based on the reported size of
	// the write and ingest.
	p.Printf("write-model %.2fx+%s (smoothed %.2fx+%s) + ",
		res.aux.perWorkTokensAux.intL0WriteLinearModel.multiplier,
		ib(res.aux.perWorkTokensAux.intL0WriteLinearModel.constant),
		res.l0WriteLM.multiplier, ib(res.l0WriteLM.constant))
	p.Printf("ingested-model %.2fx+%s (smoothed %.2fx+%s) + ",
		res.aux.perWorkTokensAux.intL0IngestedLinearModel.multiplier,
		ib(res.aux.perWorkTokensAux.intL0IngestedLinearModel.constant),
		res.l0IngestLM.multiplier, ib(res.l0IngestLM.constant))
	// The tokens used per request at admission time, when no size information
	// is known.
	p.Printf("at-admission-tokens %s, ", ib(res.requestEstimates.writeTokens))
	// How much got compacted out of L0 recently.
	p.Printf("compacted %s [≈%s], ", ib(res.aux.intL0CompactedBytes), ib(res.smoothedIntL0CompactedBytes))
	// The tokens computed for flush, based on observed flush throughput and
	// utilization.
	p.Printf("flushed %s [≈%s]; ", ib(int64(res.aux.intFlushTokens)),
		ib(int64(res.smoothedNumFlushTokens)))
	p.Printf("admitting ")
	if n := res.ioLoadListenerState.totalNumByteTokens; n < unlimitedTokens {
		p.Printf("%s (rate %s/s)", ib(n), ib(n/adjustmentInterval))
		switch res.aux.tokenKind {
		case compactionTokenKind:
			p.Printf(" due to L0 growth")
		case flushTokenKind:
			p.Printf(" due to memtable flush (multiplier %.3f)", res.flushUtilTargetFraction)
		}
		p.Printf(" (used %s)", ib(res.aux.prevTokensUsed))
	} else {
		p.SafeString("all")
	}
	if res.elasticDiskBWTokens != unlimitedTokens {
		p.Printf("; elastic tokens %s (used %s, regular used %s): "+
			"write model %.2fx+%s ingest model %.2fx+%s, ",
			ib(res.elasticDiskBWTokens), ib(res.aux.diskBW.intervalLSMInfo.elasticTokensUsed),
			ib(res.aux.diskBW.intervalLSMInfo.regularTokensUsed),
			res.l0WriteLM.multiplier, ib(res.l0WriteLM.constant),
			res.ingestLM.multiplier, ib(res.ingestLM.constant))
		p.Printf("disk bw read %s write %s provisioned %s",
			ib(res.aux.diskBW.intervalDiskLoadInfo.readBandwidth),
			ib(res.aux.diskBW.intervalDiskLoadInfo.writeBandwidth),
			ib(res.aux.diskBW.intervalDiskLoadInfo.provisionedBandwidth))
	}
}

func (res adjustTokensResult) String() string {
	return redact.StringWithoutMarkers(res)
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

// GranterMetrics are metrics associated with a GrantCoordinator.
type GranterMetrics struct {
	KVTotalSlots                *metric.Gauge
	KVUsedSlots                 *metric.Gauge
	KVTotalModerateSlots        *metric.Gauge
	KVUsedSoftSlots             *metric.Gauge
	KVIOTokensExhaustedDuration *metric.Counter
	SQLLeafStartUsedSlots       *metric.Gauge
	SQLRootStartUsedSlots       *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (GranterMetrics) MetricStruct() {}

func makeGranterMetrics() GranterMetrics {
	m := GranterMetrics{
		KVTotalSlots:                metric.NewGauge(totalSlots),
		KVUsedSlots:                 metric.NewGauge(addName(string(workKindString(KVWork)), usedSlots)),
		KVTotalModerateSlots:        metric.NewGauge(totalModerateSlots),
		KVUsedSoftSlots:             metric.NewGauge(usedSoftSlots),
		KVIOTokensExhaustedDuration: metric.NewCounter(kvIOTokensExhaustedDuration),
		SQLLeafStartUsedSlots: metric.NewGauge(
			addName(string(workKindString(SQLStatementLeafStartWork)), usedSlots)),
		SQLRootStartUsedSlots: metric.NewGauge(
			addName(string(workKindString(SQLStatementRootStartWork)), usedSlots)),
	}
	return m
}

// Prevent the linter from emitting unused warnings.
var _ = NewGrantCoordinatorSQL

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
