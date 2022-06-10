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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

// MinFlushUtilizationPercentage is a lower-bound on the dynamically adjusted
// flush utilization target that attempts to reduce write stalls. Set it to a
// high percentage, like 1000, to effectively disable flush based tokens.
var MinFlushUtilizationPercentage = settings.RegisterIntSetting(
	settings.SystemOnly,
	"admission.min_flush_util_percent",
	"when computing flush tokens, this percentage is used as a lower bound on the dynamic"+
		"multiplier of the peak flush rate", 50, settings.PositiveInt)

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
	// DO NOT USE for adjustments to IO-related tokens when the work is
	// completed.
	//
	// REQUIRES: count > 0. count == 1 for slots.
	returnGrant(count int64)
	// tookWithoutPermission informs the granter that a slot or tokens were
	// taken unilaterally, without permission. This is used for KVWork is
	// allowed to bypass admission control for high priority internal activities
	// (e.g. node liveness) and for KVWork that generates other KVWork (like
	// intent resolution of discovered intents). Not bypassing for the latter
	// could result in single node or distributed deadlock, and since such work
	// is typically not a major (on average) consumer of resources, we consider
	// bypassing to be acceptable.
	//
	// DO NOT USE for adjustments to IO-related tokens when the work is
	// completed.
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
	// grantFailLocal is returned when the granter is unable to grant due to a
	// (a) local constraint -- insufficient tokens or slots, or (b) no work is
	// waiting.
	grantFailLocal
)

// granterWithLockedCalls is an encapsulation of typically one
// granter-requester pair, and for kvStoreTokenGranter of two
// granter-requester pairs. It is used as an internal implementation detail of
// the GrantCoordinator. An implementer of granterWithLockedCalls responds to
// calls from its granter(s) by calling into the GrantCoordinator, which then
// calls the various *Locked() methods. The demuxHandle is meant to be opaque
// to the GrantCoordinator, and is used when this interface encapsulates
// multiple granter-requester pairs. The *Locked() methods are where the
// differences in slots and various kinds of tokens are handled.
type granterWithLockedCalls interface {
	// tryGetLocked is the real implementation of tryGet from the granter
	// interface.
	tryGetLocked(count int64, demuxHandle int8) grantResult
	// returnGrantLocked is the real implementation of returnGrant from the
	// granter interface.
	returnGrantLocked(count int64, demuxHandle int8)
	// tookWithoutPermissionLocked is the real implementation of
	// tookWithoutPermission from the granter interface.
	tookWithoutPermissionLocked(count int64, demuxHandle int8)

	// The following methods are for direct use by GrantCoordinator.

	// requesterHasWaitingRequests returns whether some requester associated
	// with the granter has waiting requests.
	requesterHasWaitingRequests() bool
	// tryGrant is used to attempt to grant to waiting requests.
	tryGrantLocked(grantChainID grantChainID) grantResult
}

// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord               *GrantCoordinator
	workKind            WorkKind
	requester           requester
	usedSlots           int
	totalSlots          int
	skipSlotEnforcement bool

	// Optional. Nil for a slotGranter used for KVWork since the slots for that
	// slotGranter are directly adjusted by the kvSlotAdjuster (using the
	// kvSlotAdjuster here would provide a redundant identical signal).
	cpuOverload cpuOverloadIndicator

	usedSlotsMetric *metric.Gauge
}

var _ granterWithLockedCalls = &slotGranter{}
var _ granter = &slotGranter{}

func (sg *slotGranter) grantKind() grantKind {
	return slot
}

func (sg *slotGranter) tryGet(count int64) bool {
	return sg.coord.tryGet(sg.workKind, count, 0)
}

func (sg *slotGranter) tryGetLocked(count int64, _ int8) grantResult {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.cpuOverload != nil && sg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
		sg.usedSlotsMetric.Update(int64(sg.usedSlots))
		return grantSuccess
	}
	if sg.workKind == KVWork {
		return grantFailDueToSharedResource
	}
	return grantFailLocal
}

func (sg *slotGranter) returnGrant(count int64) {
	sg.coord.returnGrant(sg.workKind, count, 0)
}

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

func (sg *slotGranter) tookWithoutPermission(count int64) {
	sg.coord.tookWithoutPermission(sg.workKind, count, 0)
}

func (sg *slotGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	sg.usedSlots++
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

func (sg *slotGranter) continueGrantChain(grantChainID grantChainID) {
	sg.coord.continueGrantChain(sg.workKind, grantChainID)
}

func (sg *slotGranter) requesterHasWaitingRequests() bool {
	return sg.requester.hasWaitingRequests()
}

func (sg *slotGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := sg.tryGetLocked(1, 0 /*arbitrary*/)
	if res == grantSuccess {
		slots := sg.requester.granted(grantChainID)
		if slots == 0 {
			// Did not accept grant
			sg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if slots != 1 {
			panic(errors.AssertionFailedf("unexpected count", slots))
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

func (tg *tokenGranter) grantKind() grantKind {
	return token
}

func (tg *tokenGranter) tryGet(count int64) bool {
	return tg.coord.tryGet(tg.workKind, count, 0)
}

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

func (tg *tokenGranter) returnGrant(count int64) {
	tg.coord.returnGrant(tg.workKind, count, 0)
}

func (tg *tokenGranter) returnGrantLocked(count int64, _ int8) {
	tg.availableBurstTokens += count
	if tg.availableBurstTokens > tg.maxBurstTokens {
		tg.availableBurstTokens = tg.maxBurstTokens
	}
}

func (tg *tokenGranter) tookWithoutPermission(count int64) {
	tg.coord.tookWithoutPermission(tg.workKind, count, 0)
}

func (tg *tokenGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	tg.availableBurstTokens -= count
}

func (tg *tokenGranter) continueGrantChain(grantChainID grantChainID) {
	tg.coord.continueGrantChain(tg.workKind, grantChainID)
}

func (tg *tokenGranter) requesterHasWaitingRequests() bool {
	return tg.requester.hasWaitingRequests()
}

func (tg *tokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := tg.tryGetLocked(1, 0 /*arbitrary*/)
	if res == grantSuccess {
		tokens := tg.requester.granted(grantChainID)
		if tokens == 0 {
			// Did not accept grant
			tg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if tokens > 1 {
			tg.tookWithoutPermissionLocked(tokens-1, 0 /*arbitrary*/)
		}
	}
	return res
}

// kvStoreTokenGranter implements granterWithLockedCalls. It is used for
// grants to KVWork to a store, that is limited by IO tokens.
type kvStoreTokenGranter struct {
	coord            *GrantCoordinator
	regularRequester requester
	elasticRequester requester

	// Tokens for L0.
	// TODO(sumeer): add L0 to the names.
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
	elasticDiskTokensAvailable int64
	diskTokensUsed             [numWorkClasses]int64

	atAdmittedDoneByteAddition   int64
	atAdmittedDoneL0ByteAddition int64
}

var _ granterWithLockedCalls = &kvStoreTokenGranter{}
var _ granterWithIOTokens = &kvStoreTokenGranter{}

type workClass int8

const (
	regularWorkClass workClass = iota
	elasticWorkClass
	numWorkClasses
)

type kvStoreTokenChildGranter struct {
	workClass workClass
	parent    *kvStoreTokenGranter
}

type storeWriteDoneState struct {
	tokens                       int64
	atAdmittedDoneAccountedBytes int64
	atAdmittedDoneL0Bytes        int64
}

type granterWithStoreWriteDone interface {
	granter
	storeWriteDone(doneState storeWriteDoneState)
}

var _ granterWithStoreWriteDone = &kvStoreTokenChildGranter{}

func (cg *kvStoreTokenChildGranter) grantKind() grantKind {
	return token
}

func (cg *kvStoreTokenChildGranter) tryGet(count int64) bool {
	return cg.parent.tryGet(cg.workClass, count)
}

func (cg *kvStoreTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(cg.workClass, count)
}

func (cg *kvStoreTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(cg.workClass, count)
}

func (cg *kvStoreTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	panic("grant chains are not used for store tokens")
}

func (cg *kvStoreTokenChildGranter) storeWriteDone(doneState storeWriteDoneState) {
	cg.parent.storeWriteDone(cg.workClass, doneState)
}

func (sg *kvStoreTokenGranter) storeWriteDone(workClass workClass, doneState storeWriteDoneState) {
	// We don't bother with the *Locked dance through the GrantCoordinator here
	// since the grant coordinator doesn't know when to call the tryGrant since
	// it does not know whether tokens have become available. There is probably
	// scope to improve the code abstractions in this file since they are
	// getting strained, but we should wait till we have more completeness.
	sg.coord.mu.Lock()
	exhaustedFunc := func() bool {
		return sg.availableIOTokens <= 0 ||
			(workClass == elasticWorkClass && sg.elasticDiskTokensAvailable <= 0)
	}
	wasExhausted := exhaustedFunc()
	ioTokensAtAdmittedDone := doneState.atAdmittedDoneL0Bytes + sg.atAdmittedDoneL0ByteAddition
	additionalIOTokensToGet := doneState.tokens - ioTokensAtAdmittedDone
	sg.subtractTokens(additionalIOTokensToGet, false)
	diskTokensAtAdmittedDone :=
		doneState.atAdmittedDoneAccountedBytes + sg.atAdmittedDoneByteAddition
	additionalDiskTokensToGet := doneState.tokens - diskTokensAtAdmittedDone
	if workClass == elasticWorkClass {
		sg.elasticDiskTokensAvailable -= additionalDiskTokensToGet
	}
	sg.diskTokensUsed[workClass] += additionalDiskTokensToGet
	isExhausted := exhaustedFunc()
	if wasExhausted && !isExhausted {
		sg.coord.tryGrant()
	}
	sg.coord.mu.Unlock()
}

func (sg *kvStoreTokenGranter) tryGet(workClass workClass, count int64) bool {
	return sg.coord.tryGet(KVWork, count, int8(workClass))
}

func (sg *kvStoreTokenGranter) tryGetLocked(count int64, demuxHandle int8) grantResult {
	wc := workClass(demuxHandle)
	switch wc {
	case regularWorkClass:
		if sg.availableIOTokens > 0 {
			sg.subtractTokens(count, false)
			sg.diskTokensUsed[wc] += count
			return grantSuccess
		}
	case elasticWorkClass:
		if sg.elasticDiskTokensAvailable > 0 && sg.availableIOTokens > 0 {
			sg.elasticDiskTokensAvailable -= count
			sg.subtractTokens(count, false)
			sg.diskTokensUsed[wc] += count
			return grantSuccess
		}
	}
	return grantFailLocal
}

func (sg *kvStoreTokenGranter) returnGrant(workClass workClass, count int64) {
	sg.coord.returnGrant(KVWork, count, int8(workClass))
}

func (sg *kvStoreTokenGranter) returnGrantLocked(count int64, demuxHandle int8) {
	wc := workClass(demuxHandle)
	sg.subtractTokens(-count, false)
	if wc == elasticWorkClass {
		sg.elasticDiskTokensAvailable += count
	}
	sg.diskTokensUsed[wc] -= count
}

func (sg *kvStoreTokenGranter) tookWithoutPermission(workClass workClass, count int64) {
	sg.coord.tookWithoutPermission(KVWork, count, int8(workClass))
}

func (sg *kvStoreTokenGranter) tookWithoutPermissionLocked(count int64, demuxHandle int8) {
	wc := workClass(demuxHandle)
	sg.subtractTokens(count, false)
	if wc == elasticWorkClass {
		sg.elasticDiskTokensAvailable -= count
	}
	sg.diskTokensUsed[wc] += count
}

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

func (sg *kvStoreTokenGranter) requesterHasWaitingRequests() bool {
	return sg.regularRequester.hasWaitingRequests() || sg.elasticRequester.hasWaitingRequests()
}

func (sg *kvStoreTokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	// First try granting to regular requester.
	for _, wc := range []workClass{regularWorkClass, elasticWorkClass} {
		req := sg.regularRequester
		if wc == elasticWorkClass {
			req = sg.elasticRequester
		}
		if req.hasWaitingRequests() {
			if sg.tryGetLocked(1, int8(wc)) == grantSuccess {
				tookTokenCount := req.granted(grantChainID)
				if tookTokenCount == 0 {
					// Did not accept grant.
					sg.returnGrantLocked(1, int8(wc))
				} else {
					// May have taken more.
					if tookTokenCount > 1 {
						sg.tookWithoutPermissionLocked(tookTokenCount-1, int8(wc))
					}
					return grantSuccess
				}
			}
		}
	}
	return grantFailLocal
}

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

func (sg *kvStoreTokenGranter) setAvailableElasticDiskTokensLocked(tokens int64) {
	sg.elasticDiskTokensAvailable += tokens
	if sg.elasticDiskTokensAvailable > tokens {
		sg.elasticDiskTokensAvailable = tokens
	}
}

func (sg *kvStoreTokenGranter) getDiskTokensUsedAndResetLocked() [numWorkClasses]int64 {
	result := sg.diskTokensUsed
	for i := workClass(0); i < numWorkClasses; i++ {
		sg.diskTokensUsed[i] = 0
	}
	return result
}

func (sg *kvStoreTokenGranter) setAdmittedDoneByteAdditionLocked(bytes int64, l0Bytes int64) {
	sg.atAdmittedDoneByteAddition = bytes
	sg.atAdmittedDoneL0ByteAddition = l0Bytes
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
	MinCPUSlots                    int
	MaxCPUSlots                    int
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
	settings *cluster.Settings,
	opts workQueueOptions) storeRequester

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
		settings:         st,
		minCPUSlots:      opts.MinCPUSlots,
		maxCPUSlots:      opts.MaxCPUSlots,
		totalSlotsMetric: metrics.KVTotalSlots,
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
		coord:           coord,
		workKind:        KVWork,
		totalSlots:      opts.MinCPUSlots,
		usedSlotsMetric: metrics.KVUsedSlots,
	}
	kvSlotAdjuster.granter = kvg
	requester := makeRequester(ambientCtx, KVWork, kvg, st, makeWorkQueueOptions(KVWork))
	coord.queues[KVWork] = requester
	kvg.requester = requester
	coord.granters[KVWork] = kvg

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	requester = makeRequester(
		ambientCtx, SQLKVResponseWork, tg, st, makeWorkQueueOptions(SQLKVResponseWork))
	coord.queues[SQLKVResponseWork] = requester
	tg.requester = requester
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	requester = makeRequester(ambientCtx,
		SQLSQLResponseWork, tg, st, makeWorkQueueOptions(SQLSQLResponseWork))
	coord.queues[SQLSQLResponseWork] = requester
	tg.requester = requester
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	requester = makeRequester(ambientCtx,
		SQLStatementLeafStartWork, sg, st, makeWorkQueueOptions(SQLStatementLeafStartWork))
	coord.queues[SQLStatementLeafStartWork] = requester
	sg.requester = requester
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	requester = makeRequester(ambientCtx,
		SQLStatementRootStartWork, sg, st, makeWorkQueueOptions(SQLStatementRootStartWork))
	coord.queues[SQLStatementRootStartWork] = requester
	sg.requester = requester
	coord.granters[SQLStatementRootStartWork] = sg

	metricStructs = appendMetricStructsForQueues(metricStructs, coord)

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
	requester := makeRequester(ambientCtx,
		SQLKVResponseWork, tg, st, makeWorkQueueOptions(SQLKVResponseWork))
	coord.queues[SQLKVResponseWork] = requester
	tg.requester = requester
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          sqlNodeCPU,
	}
	requester = makeRequester(ambientCtx,
		SQLSQLResponseWork, tg, st, makeWorkQueueOptions(SQLSQLResponseWork))
	coord.queues[SQLSQLResponseWork] = requester
	tg.requester = requester
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	requester = makeRequester(ambientCtx,
		SQLStatementLeafStartWork, sg, st, makeWorkQueueOptions(SQLStatementLeafStartWork))
	coord.queues[SQLStatementLeafStartWork] = requester
	sg.requester = requester
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	requester = makeRequester(ambientCtx,
		SQLStatementRootStartWork, sg, st, makeWorkQueueOptions(SQLStatementRootStartWork))
	coord.queues[SQLStatementRootStartWork] = requester
	sg.requester = requester
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
				s.Printf("%s%s: used: %d, total: %d", curSep, workKindString(kind), g.usedSlots,
					g.totalSlots)
			case *kvStoreTokenGranter:
				s.Printf(" io-avail: %d", g.availableIOTokens)
			}
		case SQLStatementLeafStartWork, SQLStatementRootStartWork:
			if coord.granters[i] != nil {
				g := coord.granters[i].(*slotGranter)
				s.Printf("%s%s: used: %d, total: %d", curSep, workKindString(kind), g.usedSlots, g.totalSlots)
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
	startupCtx context.Context, pmp PebbleMetricsProvider,
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
		coord:                           coord,
		ioTokensExhaustedDurationMetric: sgc.kvIOTokensExhaustedDuration,
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
	requesters := storeReq.getRequesters()
	coord.queues[KVWork] = storeReq
	kvg.regularRequester = requesters[regularWorkClass]
	kvg.elasticRequester = requesters[elasticWorkClass]
	coord.granters[KVWork] = kvg
	coord.ioLoadListener = &ioLoadListener{
		storeID:     storeID,
		settings:    sgc.settings,
		kvRequester: storeReq,
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

// GetCompactionSlot is used before starting any compaction, whether it is a
// regular or optional compaction. A regular compaction should set force=true.
func (sgc *StoreGrantCoordinators) GetCompactionSlot(storeID int32, force bool) (bool, error) {
	if unsafeGranter, ok := sgc.gcMap.Load(int64(storeID)); ok {
		granter := (*GrantCoordinator)(unsafeGranter)
		return granter.ioLoadListener.diskBandwidthLimiter.compactionLimiter.getCompactionSlot(force), nil
	}
	return true, errors.Errorf("unknown storeID %d", storeID)
}

// ReturnCompactionSlot is called when a compaction completes.
func (sgc *StoreGrantCoordinators) ReturnCompactionSlot(storeID int32) error {
	if unsafeGranter, ok := sgc.gcMap.Load(int64(storeID)); ok {
		granter := (*GrantCoordinator)(unsafeGranter)
		granter.ioLoadListener.diskBandwidthLimiter.compactionLimiter.returnCompactionSlot()
		return nil
	}
	return errors.Errorf("unknown storeID %d", storeID)
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

	totalSlotsMetric *metric.Gauge
}

var _ cpuOverloadIndicator = &kvSlotAdjuster{}
var _ CPULoadListener = &kvSlotAdjuster{}

func (kvsa *kvSlotAdjuster) CPULoad(runnable int, procs int, _ time.Duration) {
	threshold := int(KVSlotAdjusterOverloadThreshold.Get(&kvsa.settings.SV))

	// Simple heuristic, which worked ok in experiments. More sophisticated ones
	// could be devised.
	if runnable >= threshold*procs {
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
		if kvsa.granter.usedSlots > 0 && kvsa.granter.totalSlots > kvsa.minCPUSlots &&
			kvsa.granter.usedSlots <= kvsa.granter.totalSlots {
			kvsa.granter.totalSlots--
		}
	} else if float64(runnable) <= float64((threshold*procs)/2) {
		// Underload.
		// Used all its slots and can increase further, so additive increase.
		if kvsa.granter.usedSlots >= kvsa.granter.totalSlots &&
			kvsa.granter.totalSlots < kvsa.maxCPUSlots {
			// NB: If the workload is IO bound, the slot count here will keep
			// incrementing until these slots are no longer the bottleneck for
			// admission. So it is not unreasonable to see this slot count go into
			// the 1000s. If the workload switches to being CPU bound, we can
			// decrease by 1000 slots every second (because the CPULoad ticks are at
			// 1ms intervals, and we do additive decrease).
			kvsa.granter.totalSlots++
		}
	}
	kvsa.totalSlotsMetric.Update(int64(kvsa.granter.totalSlots))
}

func (kvsa *kvSlotAdjuster) isOverloaded() bool {
	return kvsa.granter.usedSlots >= kvsa.granter.totalSlots && !kvsa.granter.skipSlotEnforcement
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

// StoreMetrics are the metrics for a store.
type StoreMetrics struct {
	StoreID int32
	*pebble.Metrics
	WriteStallCount int64
	*pebble.InternalIntervalMetrics

	// TODO: populate. IntervalCompactionInfo should be folded into
	// pebble.InternalIntervalMetrics.
	IntervalDiskLoadInfo   IntervalDiskLoadInfo
	IntervalCompactionInfo IntervalCompactionInfo
}

// granterWithIOTokens is used to abstract kvStoreTokenGranter for testing.
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
	setAvailableElasticDiskTokensLocked(tokens int64)
	// getDiskTokensUsed returns the disk tokens used since the last such call.
	getDiskTokensUsedAndResetLocked() [numWorkClasses]int64
	// setAdmittedDoneByteAddition supplies the adjustments to use when
	// storeWriteDone is called. Note that this is not the adjustment to use at
	// admission time -- that is handled by StoreWorkQueue and is not in scope
	// of this granter. This asymmetry is due to the need to use all the
	// functionality of WorkQueue at admission time.
	setAdmittedDoneByteAdditionLocked(bytes int64, l0Bytes int64)
}

// storeAdmissionStats are stats maintained by a storeRequester. The non-test
// implementation of storeRequester is StoreWorkQueue. StoreWorkQueue updates
// all of these when StoreWorkQueue.AdmittedWorkDone is called, so that these
// cumulative values are mutually consistent.
type storeAdmissionStats struct {
	// Total admission requests.
	admittedCount uint64

	// Bytes mentioned for the requests in admittedCount.
	//
	// TODO(sumeer): since these are a mix of regular Batch writes and ingestion
	// requests, and the two kinds of bytes are not actually comparable (the
	// Batch writes are uncompressed), we may need to fix this inaccuracy if it
	// turns out to be an issue.
	atAdmitAccountedBytes          uint64
	atAdmittedDoneAccountedBytes   uint64
	atAdmittedDoneAccountedL0Bytes uint64
}

// storeRequestEstimates are estimates that the storeRequester should use for
// its future requests.
type storeRequestEstimates struct {
	// This estimate is used to adjust work bytes, to compensate for any
	// observed under-counting at Admit time. Under-counting happens due to:
	// - Requests unobserved by admission control.
	// - Requests that do not provide their bytes at admission time.
	// Must be > 0.
	atAdmitWorkByteAddition int64
}

// storeRequester is used to abstract *StoreWorkQueue for testing.
type storeRequester interface {
	requesterClose
	getRequesters() [numWorkClasses]requester
	getStoreAdmissionStats() [numWorkClasses]storeAdmissionStats
	setStoreRequestEstimates(estimates storeRequestEstimates)
}

type ioLoadListenerState struct {
	// Cumulative.
	cumAdmissionStats [numWorkClasses]storeAdmissionStats
	// Cumulative.
	cumL0AddedBytes uint64
	// Gauge.
	curL0Bytes int64
	// Cumulative.
	cumWriteStallCount int64
	// Cumulative incoming bytes is flushed+ingested bytes into the LSM (any
	// level).
	cumIncomingBytes uint64

	// Exponentially smoothed per interval values.

	smoothedIntL0CompactedBytes int64 // bytes leaving L0
	// Smoothed history of byte tokens calculated based on compactions out of L0.
	smoothedCompactionByteTokens float64
	// smoothed unaccounted bytes per request.
	smoothedIntPerWorkAtAdmitUnaccountedBytes          float64
	smoothedIntPerWorkAtAdmittedDoneUnaccountedBytes   float64
	smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes float64

	// Number of flush tokens, before multiplying by target fraction.
	smoothedNumFlushTokens float64
	// The target fraction to be used for the effective flush tokens. It is in
	// the interval
	// [MinFlushUtilizationPercentage/100,maxFlushUtilTargetFraction].
	flushUtilTargetFraction float64

	// totalNumByteTokens represents the tokens to give out until the next call to
	// adjustTokens. They are parceled out in small intervals. tokensAllocated
	// represents what has been given out.
	totalNumByteTokens int64
	tokensAllocated    int64
	// Used tokens can be negative if some tokens taken in one interval were
	// returned in another, but that will be extremely rare.
	tokensUsed int64

	// elasticDiskTokens represents the tokens to give out until the next call
	// to adjustTokens. They are parceled out in snall intervals.
	// elasticDiskTokensAllocated represents what has been given out.
	elasticDiskTokens          int64
	elasticDiskTokensAllocated int64
}

// ioLoadListener adjusts tokens in kvStoreTokenGranter for IO, specifically due to
// overload caused by writes. IO uses tokens and not slots since work
// completion is not an indicator that the "resource usage" has ceased -- it
// just means that the write has been applied to the WAL. Most of the work is
// in flushing to sstables and the following compactions, which happens later.
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

	diskBandwidthLimiter diskBandwidthLimiter
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

// pebbleMetricsTicks is called every adjustmentInterval seconds, and decides
// the token allocations until the next call.
func (io *ioLoadListener) pebbleMetricsTick(ctx context.Context, metrics StoreMetrics) {
	m := metrics.Metrics
	if !io.statsInitialized {
		io.statsInitialized = true
		io.ioLoadListenerState = ioLoadListenerState{
			cumAdmissionStats:  io.kvRequester.getStoreAdmissionStats(),
			cumL0AddedBytes:    m.Levels[0].BytesFlushed + m.Levels[0].BytesIngested,
			curL0Bytes:         m.Levels[0].Size,
			cumWriteStallCount: metrics.WriteStallCount,
			cumIncomingBytes:   cumIncomingBytes(m),
			// No initial limit, i.e, the first interval is unlimited.
			totalNumByteTokens: unlimitedTokens,
			elasticDiskTokens:  unlimitedTokens,
		}
		return
	}
	io.adjustTokens(ctx, metrics)
}

func cumIncomingBytes(m *pebble.Metrics) uint64 {
	var result uint64
	for i := range m.Levels {
		result += m.Levels[i].BytesFlushed + m.Levels[i].BytesIngested
	}
	return result
}

// allocateTokensTick gives out 1/ticksInAdjustmentInterval of the
// totalNumByteTokens every 250ms.
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
	toAllocateIOTokens := allocateFunc(io.totalNumByteTokens, io.tokensAllocated)
	toAllocateElasticDiskTokens := allocateFunc(io.elasticDiskTokens, io.elasticDiskTokensAllocated)
	io.mu.Lock()
	defer io.mu.Unlock()
	io.tokensAllocated += toAllocateIOTokens
	if io.tokensAllocated < 0 {
		panic(errors.AssertionFailedf("tokens allocated is negative %d", io.tokensAllocated))
	}
	io.tokensUsed += io.mu.kvGranter.setAvailableIOTokensLocked(toAllocateIOTokens)
	io.mu.kvGranter.setAvailableElasticDiskTokensLocked(toAllocateElasticDiskTokens)
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
	io.mu.Lock()
	diskTokensUsed := io.mu.kvGranter.getDiskTokensUsedAndResetLocked()
	io.mu.Unlock()
	intIncomingBytes := int64(cumIncomingBytes(metrics.Metrics)) -
		int64(io.ioLoadListenerState.cumIncomingBytes)
	intervalLSMInfo := intervalLSMInfo{
		incomingBytes:     intIncomingBytes,
		regularTokensUsed: diskTokensUsed[regularWorkClass],
		elasticTokensUsed: diskTokensUsed[elasticWorkClass],
	}
	elasticTokens := io.diskBandwidthLimiter.adjust(
		metrics.IntervalDiskLoadInfo, metrics.IntervalCompactionInfo, intervalLSMInfo)

	res := io.adjustTokensInner(ctx, io.ioLoadListenerState, io.kvRequester.getStoreAdmissionStats(),
		metrics.Levels[0], metrics.WriteStallCount, metrics.InternalIntervalMetrics,
		cumIncomingBytes(metrics.Metrics),
		L0FileCountOverloadThreshold.Get(&io.settings.SV),
		L0SubLevelCountOverloadThreshold.Get(&io.settings.SV),
		MinFlushUtilizationPercentage.Get(&io.settings.SV),
	)
	io.adjustTokensResult = res
	io.elasticDiskTokens = elasticTokens
	io.elasticDiskTokensAllocated = 0
	io.kvRequester.setStoreRequestEstimates(res.requestEstimates)
	io.mu.Lock()
	io.mu.kvGranter.setAdmittedDoneByteAdditionLocked(
		int64(res.smoothedIntPerWorkAtAdmitUnaccountedBytes),
		int64(res.smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes))
	if res.aux.shouldLog {
		log.Infof(logtags.AddTag(ctx, "s", io.storeID), "IO overload: %s", res)
	}
}

type adjustTokensAuxComputations struct {
	shouldLog                        bool
	curL0NumFiles, curL0NumSublevels int64

	intIncomingBytes    int64
	intL0AddedBytes     int64
	intL0CompactedBytes int64

	intAdmittedCount                  uint64
	intAtAdmitAccountedBytes          uint64
	intAtAdmittedDoneAccountedBytes   uint64
	intAtAdmittedDoneAccountedL0Bytes uint64

	intFlushTokens      float64
	intFlushUtilization float64
	intWriteStalls      int64
}

func (*ioLoadListener) adjustTokensInner(
	ctx context.Context,
	prev ioLoadListenerState,
	cumAdmissionStats [numWorkClasses]storeAdmissionStats,
	l0Metrics pebble.LevelMetrics,
	cumWriteStallCount int64,
	im *pebble.InternalIntervalMetrics,
	cumIncomingBytes uint64,
	threshNumFiles, threshNumSublevels int64,
	minFlushUtilizationPercentage int64,
) adjustTokensResult {
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
	// Compute the deltas of the storeAdmissionStats.

	// Estimation for unaccounted bytes.

	// intAdmittedCount is the number of requests admitted since the last token adjustment.
	var intAdmittedCount uint64
	var intAtAdmitAccountedBytes uint64
	var intAtAdmittedDoneAccountedBytes uint64
	var intAtAdmittedDoneAccountedL0Bytes uint64
	for i := workClass(0); i < numWorkClasses; i++ {
		intAdmittedCount += cumAdmissionStats[i].admittedCount - prev.cumAdmissionStats[i].admittedCount
		intAtAdmitAccountedBytes += cumAdmissionStats[i].atAdmitAccountedBytes -
			prev.cumAdmissionStats[i].atAdmitAccountedBytes
		intAtAdmittedDoneAccountedBytes += cumAdmissionStats[i].atAdmittedDoneAccountedBytes -
			prev.cumAdmissionStats[i].atAdmittedDoneAccountedBytes
		intAtAdmittedDoneAccountedL0Bytes += cumAdmissionStats[i].atAdmittedDoneAccountedL0Bytes -
			prev.cumAdmissionStats[i].atAdmittedDoneAccountedL0Bytes
	}
	doLog := true
	if intAdmittedCount <= 0 {
		intAdmittedCount = 1
		// Admission control is likely disabled, given there was no KVWork
		// admitted for 15s. And even if it is enabled, this is not an interesting
		// situation.
		doLog = false
	}
	intIncomingBytes := int64(cumIncomingBytes - prev.cumIncomingBytes)
	unaccountedFunc := func(
		intUnaccountedBytes int64, prevSmoothedIntPerWorkUnaccountedBytes float64, intAddedBytes int64,
	) (adjustedIntUnaccountedBytes int64, smoothedIntPerWorkUnaccountedBytes float64) {
		// Clamp the unaccounted bytes to >= 0. Negative values can happen because
		// of the different points in time the two stats a, b are collected, for
		// which we are computing a-b. The compensation we do below is limited to
		// exponential smoothing, which hopefully is good enough.
		if intUnaccountedBytes < 0 {
			intUnaccountedBytes = 0
		}
		intPerWorkUnaccountedBytes := float64(intUnaccountedBytes) / float64(intAdmittedCount)
		if intAdmittedCount > 1 && intAddedBytes > 0 {
			// Track an exponentially smoothed estimate of unaccounted bytes added per
			// work when there was some work actually admitted. Note we treat having
			// admitted one item as the same as having admitted zero both because we
			// clamp admitted to 1 and if we only admitted one thing, do we really
			// want to use that for our estimate? The conjunction includes intAddedBytes
			// > 0 (and not just intAdmittedCount), since we have seen situation where no
			// bytes were added and intAdmittedCount > 1. This can happen since the stats
			// from Pebble and those from the requester are not synchronized, and in
			// that case we don't want to include this sample in the smoothing.
			if prevSmoothedIntPerWorkUnaccountedBytes == 0 {
				smoothedIntPerWorkUnaccountedBytes = intPerWorkUnaccountedBytes
			} else {
				smoothedIntPerWorkUnaccountedBytes = alpha*intPerWorkUnaccountedBytes +
					(1-alpha)*prevSmoothedIntPerWorkUnaccountedBytes
			}
		} else {
			// We've not had work flow through admission control, so we don't have any
			// reasonable interval data to update our previous estimates. Keep the old
			// smoothed value around, as it is likely more useful for future admission
			// control computations than smoothing down to zero over time.
			smoothedIntPerWorkUnaccountedBytes = prevSmoothedIntPerWorkUnaccountedBytes
		}
		return intAddedBytes, smoothedIntPerWorkUnaccountedBytes
	}
	// Bytes that were added to the LSM or to L0 based on LSM stats, but we
	// don't know where they came from.
	intAtAdmitUnaccountedBytes := intIncomingBytes - int64(intAtAdmitAccountedBytes)
	intAtAdmittedDoneUnaccountedBytes := intIncomingBytes - int64(intAtAdmittedDoneAccountedBytes)
	intAtAdmittedDoneUnaccountedL0Bytes := intL0AddedBytes -
		int64(intAtAdmittedDoneAccountedL0Bytes)
	var smoothedIntPerWorkAtAdmitUnaccountedBytes float64
	intAtAdmitUnaccountedBytes, smoothedIntPerWorkAtAdmitUnaccountedBytes = unaccountedFunc(
		intAtAdmitUnaccountedBytes, prev.smoothedIntPerWorkAtAdmitUnaccountedBytes, intIncomingBytes)
	var smoothedIntPerWorkAtAdmittedDoneUnaccountedBytes float64
	intAtAdmittedDoneUnaccountedBytes, smoothedIntPerWorkAtAdmittedDoneUnaccountedBytes =
		unaccountedFunc(
			intAtAdmittedDoneUnaccountedBytes,
			prev.smoothedIntPerWorkAtAdmittedDoneUnaccountedBytes,
			intIncomingBytes)
	var smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes float64
	intAtAdmittedDoneUnaccountedL0Bytes, smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes =
		unaccountedFunc(
			intAtAdmittedDoneUnaccountedL0Bytes,
			prev.smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes,
			intL0AddedBytes)

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
	// There is a dedicated flush thread. In comparison, with a write amp of W
	// and maximum compaction concurrency of C, only approximately C/W threads
	// are working on reading from L0 to compact out of L0. Since W can be 20+
	// and C defaults to 3 (we plan to dynamically adjust C but one can expect C
	// to be <= 10), C/W < 1. So the main reason we are considering flush tokens
	// is transient flush bottlenecks, and workloads where W is small.

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
	minFlushUtilTargetFraction := float64(minFlushUtilizationPercentage) / 100
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
		// Have we used >= 90% of the tokens.
		highTokenUsage :=
			float64(prev.tokensUsed) >= 0.9*smoothedNumFlushTokens*flushUtilTargetFraction
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
	// determination based on a very low utilization. Note that flush
	// utilization has been observed to fluctuate from 0.16 to 0.9 in a single
	// interval, when compaction tokens are not limited, hence we have set
	// flushUtilIgnoreThreshold to a very low value. If we've erred towards it
	// being too low, we run the risk of computing incorrect tokens. If we've
	// erred towards being too high, we run the risk of giving out
	// unlimitedTokens and causing write stalls.

	// We constrain admission based on compactions, if the store is over the L0
	// threshold.
	var totalNumByteTokens int64
	var smoothedCompactionByteTokens float64
	curL0NumFiles := l0Metrics.NumFiles
	curL0NumSublevels := int64(l0Metrics.Sublevels)
	if curL0NumFiles > threshNumFiles || curL0NumSublevels > threshNumSublevels {
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
		doLog = false
	}
	doLog = doLog || doLogFlush
	// Use the minimum of the token count calculated using compactions and
	// flushes.
	if totalNumByteTokens > numFlushTokens {
		totalNumByteTokens = numFlushTokens
	}
	// Install the latest cumulative stats.
	return adjustTokensResult{
		ioLoadListenerState: ioLoadListenerState{
			cumAdmissionStats:                                  cumAdmissionStats,
			cumL0AddedBytes:                                    cumL0AddedBytes,
			curL0Bytes:                                         curL0Bytes,
			cumWriteStallCount:                                 cumWriteStallCount,
			cumIncomingBytes:                                   cumIncomingBytes,
			smoothedIntL0CompactedBytes:                        smoothedIntL0CompactedBytes,
			smoothedCompactionByteTokens:                       smoothedCompactionByteTokens,
			smoothedIntPerWorkAtAdmitUnaccountedBytes:          smoothedIntPerWorkAtAdmitUnaccountedBytes,
			smoothedIntPerWorkAtAdmittedDoneUnaccountedBytes:   smoothedIntPerWorkAtAdmittedDoneUnaccountedBytes,
			smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes: smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes,
			smoothedNumFlushTokens:                             smoothedNumFlushTokens,
			flushUtilTargetFraction:                            flushUtilTargetFraction,
			totalNumByteTokens:                                 totalNumByteTokens,
			tokensAllocated:                                    0,
			tokensUsed:                                         0,
			// elasticDiskTokens* will be written in the caller.
		},
		requestEstimates: storeRequestEstimates{
			atAdmitWorkByteAddition: max(1, int64(smoothedIntPerWorkAtAdmitUnaccountedBytes)),
		},
		aux: adjustTokensAuxComputations{
			shouldLog:                         doLog,
			curL0NumFiles:                     curL0NumFiles,
			curL0NumSublevels:                 curL0NumSublevels,
			intIncomingBytes:                  intIncomingBytes,
			intL0AddedBytes:                   intL0AddedBytes,
			intL0CompactedBytes:               intL0CompactedBytes,
			intAdmittedCount:                  intAdmittedCount,
			intAtAdmitAccountedBytes:          intAtAdmitAccountedBytes,
			intAtAdmittedDoneAccountedBytes:   intAtAdmittedDoneAccountedBytes,
			intAtAdmittedDoneAccountedL0Bytes: intAtAdmittedDoneAccountedL0Bytes,
			intFlushTokens:                    intFlushTokens,
			intFlushUtilization:               intFlushUtilization,
			intWriteStalls:                    intWriteStalls,
		},
	}
}

type adjustTokensResult struct {
	ioLoadListenerState
	requestEstimates storeRequestEstimates
	aux              adjustTokensAuxComputations
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
	p.Printf("%d ssts, %d sub-levels, ", res.aux.curL0NumFiles, res.aux.curL0NumSublevels)
	p.Printf("L0 growth %s: ", ib(res.aux.intL0AddedBytes))
	// Writes + Ingests to L0 that we expected because requests asked admission
	// control for them. This is the "happy path".
	p.Printf("%s acc + ", ib(int64(res.aux.intAtAdmittedDoneAccountedL0Bytes)))
	// The unhappy case - extra bytes that showed up in L0 but we didn't account
	// for them with any request. If this is large, traffic patterns possibly changed
	// recently.
	p.Printf("%s unacc [≈%s/req, n=%d], ",
		ib(res.aux.intL0AddedBytes-int64(res.aux.intAtAdmittedDoneAccountedL0Bytes)),
		ib(int64(res.smoothedIntPerWorkAtAdmittedDoneUnaccountedL0Bytes)), res.aux.intAdmittedCount)
	// How much got compacted out of L0 recently.
	p.Printf("compacted %s [≈%s], ", ib(res.aux.intL0CompactedBytes), ib(res.smoothedIntL0CompactedBytes))
	p.Printf("flush tokens %s with multiplier %.3f [≈%s]; ", ib(int64(res.aux.intFlushTokens)),
		res.flushUtilTargetFraction, ib(int64(res.smoothedNumFlushTokens*res.flushUtilTargetFraction)))
	p.Printf("admitting ")
	if n := res.ioLoadListenerState.totalNumByteTokens; n < unlimitedTokens {
		p.Printf("%s", ib(n))
		if n := res.ioLoadListenerState.tokensAllocated; 0 < n &&
			n < unlimitedTokens/ticksInAdjustmentInterval {
			p.Printf("(used %s)", ib(n))
		}
		p.Printf(" with L0 penalty: +%s/req", ib(res.requestEstimates.atAdmitWorkByteAddition))
	} else {
		p.SafeString("all")
	}
	p.Printf(" LSM growth %s: ", ib(res.aux.intIncomingBytes))
	p.Printf(" %s acc + ", ib(int64(res.aux.intAtAdmittedDoneAccountedBytes)))
	p.Printf("%s unacc [≈%s/req, admit≈%s/req] ", ib(res.aux.intIncomingBytes-
		int64(res.aux.intAtAdmittedDoneAccountedBytes)),
		ib(int64(res.smoothedIntPerWorkAtAdmittedDoneUnaccountedBytes)),
		ib(int64(res.smoothedIntPerWorkAtAdmitUnaccountedBytes)))
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
	usedSlots = metric.Metadata{
		// Note: we append a WorkKind string to this name.
		Name:        "admission.granter.used_slots.",
		Help:        "Used slots",
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

// GranterMetrics are metrics associated with a GrantCoordinator.
type GranterMetrics struct {
	KVTotalSlots                *metric.Gauge
	KVUsedSlots                 *metric.Gauge
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
