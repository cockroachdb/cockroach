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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
)

// KVSlotAdjusterOverloadThreshold sets a goroutine runnable threshold at
// which the CPU will be considered overloaded, when running in a node that
// executes KV operations.
var KVSlotAdjusterOverloadThreshold = settings.RegisterIntSetting(
	"admission.kv_slot_adjuster.overload_threshold",
	"when the number of runnable goroutines per CPU is greater than this threshold, the "+
		"slot adjuster considers the cpu to be overloaded",
	32, settings.PositiveInt)

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
	// granted is called by a granter to grant admission to queued requests. It
	// returns true if the grant was accepted, else returns false. A grant may
	// not be accepted if the grant raced with request cancellation and there
	// are now no waiting requests. The grantChainID is used when calling
	// continueGrantChain -- see the comment with that method below.
	granted(grantChainID grantChainID) bool
	// getAdmittedCount returns the cumulative count of admitted work.
	getAdmittedCount() uint64
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
	// canceled, and the grantee did not end up doing any work. The latter case
	// occurs despite the bool return value on the requester.granted method --
	// it is possible that the work was not canceled at the time when
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
	// local constraint -- insufficient tokens or slots.
	grantFailLocal
)

// granterWithLockedCalls is an extension of the granter and requester
// interfaces that is used as an internal implementation detail of the
// GrantCoordinator. Note that an implementer of granterWithLockedCalls is
// mainly passing things through to the GrantCoordinator where the main logic
// lives. The *Locked() methods are where the differences in slots and tokens
// are handled.
type granterWithLockedCalls interface {
	granter
	// tryGetLocked is the real implementation of tryGet in the granter interface.
	// Additionally, it is also used when continuing a grant chain.
	tryGetLocked() grantResult
	// returnGrantLocked is the real implementation of returnGrant.
	returnGrantLocked()
	// tookWithoutPermissionLocked is the real implementation of
	// tookWithoutPermission.
	tookWithoutPermissionLocked()

	// getPairedRequester returns the requester implementation that this granter
	// interacts with.
	getPairedRequester() requester
}

// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord      *GrantCoordinator
	workKind   WorkKind
	requester  requester
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

func (sg *slotGranter) getPairedRequester() requester {
	return sg.requester
}

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

func (sg *slotGranter) continueGrantChain(grantChainID grantChainID) {
	sg.coord.continueGrantChain(sg.workKind, grantChainID)
}

// tokenGranter implements granterWithLockedCalls.
type tokenGranter struct {
	coord                *GrantCoordinator
	workKind             WorkKind
	requester            requester
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

func (tg *tokenGranter) getPairedRequester() requester {
	return tg.requester
}

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

func (tg *tokenGranter) continueGrantChain(grantChainID grantChainID) {
	tg.coord.continueGrantChain(tg.workKind, grantChainID)
}

// kvGranter implements granterWithLockedCalls. It is used for grants to
// KVWork, that are limited by both slots (CPU bound work) and tokens (IO
// bound work).
type kvGranter struct {
	coord           *GrantCoordinator
	requester       requester
	usedSlots       int
	totalSlots      int
	ioTokensEnabled bool
	// There is no rate limiting in granting these tokens. That is, they are all
	// burst tokens.
	availableIOTokens int64

	usedSlotsMetric                 *metric.Gauge
	IOTokensExhaustedDurationMetric *metric.Counter
	exhaustedStart                  time.Time
}

var _ granterWithLockedCalls = &kvGranter{}

func (sg *kvGranter) getPairedRequester() requester {
	return sg.requester
}

func (sg *kvGranter) grantKind() grantKind {
	// Slot represents that there is a completion indicator, and it does not
	// matter that kvGranter internally uses both slots and tokens.
	return slot
}

func (sg *kvGranter) tryGet() bool {
	return sg.coord.tryGet(KVWork)
}

func (sg *kvGranter) tryGetLocked() grantResult {
	if sg.usedSlots < sg.totalSlots {
		if !sg.ioTokensEnabled || sg.availableIOTokens > 0 {
			sg.usedSlots++
			sg.usedSlotsMetric.Update(int64(sg.usedSlots))
			if sg.ioTokensEnabled {
				sg.availableIOTokens--
				if sg.availableIOTokens == 0 {
					sg.exhaustedStart = timeutil.Now()
				}
			}
			return grantSuccess
		}
		return grantFailLocal
	}
	return grantFailDueToSharedResource
}

func (sg *kvGranter) returnGrant() {
	sg.coord.returnGrant(KVWork)
}

func (sg *kvGranter) returnGrantLocked() {
	sg.usedSlots--
	if sg.usedSlots < 0 {
		panic(errors.AssertionFailedf("used slots is negative %d", sg.usedSlots))
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

func (sg *kvGranter) tookWithoutPermission() {
	sg.coord.tookWithoutPermission(KVWork)
}

func (sg *kvGranter) tookWithoutPermissionLocked() {
	sg.usedSlots++
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
	if sg.ioTokensEnabled {
		sg.availableIOTokens--
		if sg.availableIOTokens == 0 {
			sg.exhaustedStart = timeutil.Now()
		}
	}
}

func (sg *kvGranter) continueGrantChain(grantChainID grantChainID) {
	sg.coord.continueGrantChain(KVWork, grantChainID)
}

func (sg *kvGranter) setAvailableIOTokensLocked(tokens int64) {
	wasExhausted := sg.ioTokensEnabled && sg.availableIOTokens <= 0
	sg.ioTokensEnabled = true
	if sg.availableIOTokens < 0 {
		// Negative because of tookWithoutPermission.
		sg.availableIOTokens += tokens
	} else {
		sg.availableIOTokens = tokens
	}
	if wasExhausted && sg.availableIOTokens > 0 {
		exhaustedMicros := timeutil.Since(sg.exhaustedStart).Microseconds()
		sg.IOTokensExhaustedDurationMetric.Inc(exhaustedMicros)
		// NB: the lock is already held.
		if !sg.coord.grantChainActive {
			sg.coord.tryGrant()
		}
		// Else, let the grant chain finish. We could terminate it, but token
		// replenishment occurs at 1s granularity which is coarse enough to not
		// bother.
	}
}

// GrantCoordinator is the top-level object that coordinates grants across
// different WorkKinds (for more context see the comment in doc.go, and the
// comment where WorkKind is declared). Typically there will one
// GrantCoordinator in a node (see the NewGrantCoordinator*() functions for
// the different kinds of nodes).
type GrantCoordinator struct {
	settings *cluster.Settings
	// mu is ordered before any mutex acquired in a requester implementation.
	mu syncutil.Mutex
	// NB: Some granters can be nil.
	granters [numWorkKinds]granterWithLockedCalls
	// The WorkQueues behaving as requesters in each granterWithLockedCalls.
	// This is kept separately only to service GetWorkQueue calls.
	queues               [numWorkKinds]requester
	cpuOverloadIndicator cpuOverloadIndicator
	cpuLoadListener      CPULoadListener
	ioLoadListener       *ioLoadListener

	// The latest value of GOMAXPROCS, received via CPULoad.
	numProcs int

	// See the comment at continueGrantChain that explains how a grant chain
	// functions and the motivation.

	// grantChainActive indicates whether a grant chain is active. If active,
	// grantChainID is the ID of that chain. If !active, grantChainID is the ID
	// of the next chain that will become active. IDs are assigned by
	// incrementing grantChainID.
	grantChainActive bool
	grantChainID     grantChainID
	// Index into granters, which represents the current WorkKind at which the
	// grant chain is operating. Only relevant when grantChainActive is true.
	grantChainIndex WorkKind
	// See the comment at delayForGrantChainTermination for motivation.
	grantChainStartTime time.Time
}

var _ CPULoadListener = &GrantCoordinator{}

// Options for constructing a GrantCoordinator.
type Options struct {
	MinCPUSlots                    int
	MaxCPUSlots                    int
	SQLKVResponseBurstTokens       int
	SQLSQLResponseBurstTokens      int
	SQLStatementLeafStartWorkSlots int
	SQLStatementRootStartWorkSlots int
	Settings                       *cluster.Settings
	// Only non-nil for tests.
	makeRequesterFunc func(
		workKind WorkKind, granter granter, usesTokens bool, tiedToRange bool,
		settings *cluster.Settings) requester
}

// NewGrantCoordinator constructs a GrantCoordinator and WorkQueues for a
// regular cluster node. Caller is responsible for hooking this up to receive
// calls to CPULoad.
func NewGrantCoordinator(opts Options) (*GrantCoordinator, []metric.Struct) {
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
		settings:             st,
		cpuOverloadIndicator: kvSlotAdjuster,
		cpuLoadListener:      kvSlotAdjuster,
		numProcs:             1,
		grantChainID:         1,
	}

	kvg := &kvGranter{
		coord:                           coord,
		totalSlots:                      opts.MinCPUSlots,
		usedSlotsMetric:                 metrics.KVUsedSlots,
		IOTokensExhaustedDurationMetric: metrics.KVIOTokensExhaustedDuration,
	}
	kvSlotAdjuster.granter = kvg
	coord.queues[KVWork] = makeRequester(
		KVWork, kvg, false /* usesTokens */, true /* tiedToRange */, st)
	kvg.requester = coord.queues[KVWork]
	coord.granters[KVWork] = kvg

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	coord.queues[SQLKVResponseWork] = makeRequester(
		SQLKVResponseWork, tg, true /* usesTokens */, false /* tiedToRange */, st)
	tg.requester = coord.queues[SQLKVResponseWork]
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	coord.queues[SQLSQLResponseWork] = makeRequester(
		SQLSQLResponseWork, tg, true /* usesTokens */, false /* tiedToRange */, st)
	tg.requester = coord.queues[SQLSQLResponseWork]
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	coord.queues[SQLStatementLeafStartWork] = makeRequester(
		SQLStatementLeafStartWork, sg, false /* usesTokens */, false /* tiedToRange */, st)
	sg.requester = coord.queues[SQLStatementLeafStartWork]
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	coord.queues[SQLStatementRootStartWork] = makeRequester(
		SQLStatementRootStartWork, sg, false /* usesTokens */, false /* tiedToRange */, st)
	sg.requester = coord.queues[SQLStatementRootStartWork]
	coord.granters[SQLStatementRootStartWork] = sg

	return coord, appendMetricStructs(metricStructs, coord)
}

// NewGrantCoordinatorMultiTenantKV constructs a GrantCoordinator and
// WorkQueues for a multi-tenant KV node. Caller is responsible for hooking
// this up to receive calls to CPULoad.
func NewGrantCoordinatorMultiTenantKV(opts Options) (*GrantCoordinator, []metric.Struct) {
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
		settings:             st,
		cpuOverloadIndicator: kvSlotAdjuster,
		cpuLoadListener:      kvSlotAdjuster,
		numProcs:             1,
		grantChainID:         1,
	}

	kvg := &kvGranter{
		coord:                           coord,
		totalSlots:                      opts.MinCPUSlots,
		usedSlotsMetric:                 metrics.KVUsedSlots,
		IOTokensExhaustedDurationMetric: metrics.KVIOTokensExhaustedDuration,
	}
	kvSlotAdjuster.granter = kvg
	coord.queues[KVWork] = makeRequester(
		KVWork, kvg, false /* usesTokens */, true /* tiedToRange */, st)
	kvg.requester = coord.queues[KVWork]
	coord.granters[KVWork] = kvg

	return coord, appendMetricStructs(metricStructs, coord)
}

// NewGrantCoordinatorSQL constructs a GrantCoordinator and WorkQueues for a
// single-tenant SQL node in a multi-tenant cluster. Caller is responsible for
// hooking this up to receive calls to CPULoad.
func NewGrantCoordinatorSQL(opts Options) (*GrantCoordinator, []metric.Struct) {
	makeRequester := makeWorkQueue
	if opts.makeRequesterFunc != nil {
		makeRequester = opts.makeRequesterFunc
	}
	st := opts.Settings

	metrics := makeGranterMetrics()
	metricStructs := append([]metric.Struct(nil), metrics)
	sqlNodeCPU := &sqlNodeCPUOverloadIndicator{}
	coord := &GrantCoordinator{
		settings:             st,
		cpuOverloadIndicator: sqlNodeCPU,
		cpuLoadListener:      sqlNodeCPU,
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
	coord.queues[SQLKVResponseWork] = makeRequester(
		SQLKVResponseWork, tg, true /* usesTokens */, false /* tiedToRange */, st)
	tg.requester = coord.queues[SQLKVResponseWork]
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          sqlNodeCPU,
	}
	coord.queues[SQLSQLResponseWork] = makeRequester(
		SQLSQLResponseWork, tg, true /* usesTokens */, false /* tiedToRange */, st)
	tg.requester = coord.queues[SQLSQLResponseWork]
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	coord.queues[SQLStatementLeafStartWork] = makeRequester(
		SQLStatementLeafStartWork, sg, false /* usesTokens */, false /* tiedToRange */, st)
	sg.requester = coord.queues[SQLStatementLeafStartWork]
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	coord.queues[SQLStatementRootStartWork] = makeRequester(
		SQLStatementRootStartWork, sg, false /* usesTokens */, false /* tiedToRange */, st)
	sg.requester = coord.queues[SQLStatementRootStartWork]
	coord.granters[SQLStatementRootStartWork] = sg

	return coord, appendMetricStructs(metricStructs, coord)
}

// Experimental observations: sub-level count of ~40 caused a node heartbeat
// latency p90, p99 of 2.5s, 4s. With the following setting that limits
// sub-level count to 10, before the system is considered overloaded, we see
// the actual sub-level count ranging from 5-30, with p90, p99 node heartbeat
// latency showing a similar wide range, with 1s, 2s being the middle of the
// range respectively.
//
// We've set these overload thresholds in a way that allows the system to
// absorb short durations (say a few minutes) of heavy write load.
//
// TODO(sumeer): make these configurable since it is possible that different
// users have different tolerance for read slowness caused by higher
// read-amplification.
const l0FileCountOverloadThreshold = 1000
const l0SubLevelCountOverloadThreshold = 10

// SetPebbleMetricsProvider sets a PebbleMetricsProvider and causes the load
// on the various storage engines to be used for admission control.
func (coord *GrantCoordinator) SetPebbleMetricsProvider(pmp PebbleMetricsProvider) {
	if coord.ioLoadListener != nil {
		panic(errors.AssertionFailedf("SetPebbleMetricsProvider called more than once"))
	}
	coord.ioLoadListener = &ioLoadListener{
		l0FileCountOverloadThreshold:     l0FileCountOverloadThreshold,
		l0SubLevelCountOverloadThreshold: l0SubLevelCountOverloadThreshold,
		pebbleMetricsProvider:            pmp,
		kvRequester:                      coord.queues[KVWork],
		mu:                               &coord.mu,
		kvGranter:                        coord.granters[KVWork].(*kvGranter),
	}
	coord.ioLoadListener.start(func(d time.Duration) timeTickerInterface {
		return timeTicker{ticker: time.NewTicker(d)}
	})
}

func appendMetricStructs(ms []metric.Struct, coord *GrantCoordinator) []metric.Struct {
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

// GetWorkQueue returns the WorkQueue for a particular WorkKind. Can be nil if
// the NewGrantCoordinator* function does not construct a WorkQueue for that
// work.
func (coord *GrantCoordinator) GetWorkQueue(workKind WorkKind) *WorkQueue {
	return coord.queues[workKind].(*WorkQueue)
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
	coord.numProcs = procs
	coord.cpuLoadListener.CPULoad(runnable, procs)
	coord.granters[SQLKVResponseWork].(*tokenGranter).refillBurstTokens()
	coord.granters[SQLSQLResponseWork].(*tokenGranter).refillBurstTokens()
	if coord.grantChainActive && !coord.tryTerminateGrantChain() {
		return
	}
	coord.tryGrant()
}

// tryGet is called by granter.tryGet with the WorkKind.
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
func (coord *GrantCoordinator) returnGrant(workKind WorkKind) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.granters[workKind].returnGrantLocked()
	if coord.grantChainActive {
		if coord.grantChainIndex > workKind &&
			coord.granters[workKind].getPairedRequester().hasWaitingRequests() {
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
func (coord *GrantCoordinator) tookWithoutPermission(workKind WorkKind) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.granters[workKind].tookWithoutPermissionLocked()
}

// continueGrantChain is called by granter.continueGrantChain with the
// WorkKind.
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

// tryGrant tries to either continue an existing grant chain, or tries to
// start a new grant chain.
func (coord *GrantCoordinator) tryGrant() {
	startingChain := false
	if !coord.grantChainActive {
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
		req := granter.getPairedRequester()
		for req.hasWaitingRequests() && !localDone {
			res := granter.tryGetLocked()
			switch res {
			case grantSuccess:
				chainID := noGrantChain
				if grantBurstCount+1 == grantBurstLimit {
					chainID = coord.grantChainID
				}
				if !req.granted(chainID) {
					granter.returnGrantLocked()
				} else {
					grantBurstCount++
					if grantBurstCount == grantBurstLimit {
						coord.grantChainActive = true
						if startingChain {
							coord.grantChainStartTime = timeutil.Now()
						}
						return
					}
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
	// since it represents the ID to be used for the next chain.
	if !startingChain {
		coord.grantChainID++
	}
}

// Close implements the stop.Closer interface.
func (coord *GrantCoordinator) Close() {
	for i := range coord.queues {
		q, ok := coord.queues[i].(*WorkQueue)
		if ok {
			q.close()
		}
	}
	if coord.ioLoadListener != nil {
		coord.ioLoadListener.close()
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
			g := coord.granters[i].(*kvGranter)
			s.Printf("%s%s: used: %d, total: %d", curSep, workKindString(kind), g.usedSlots, g.totalSlots)
			if g.ioTokensEnabled {
				s.Printf(" io-avail: %d", g.availableIOTokens)
			}
		case SQLStatementLeafStartWork, SQLStatementRootStartWork:
			g := coord.granters[i].(*slotGranter)
			s.Printf("%s%s: used: %d, total: %d", curSep, workKindString(kind), g.usedSlots, g.totalSlots)
		case SQLKVResponseWork, SQLSQLResponseWork:
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
// expect this to be called every 1ms.
// TODO(sumeer): experiment with more smoothing. It is possible that rapid
// slot fluctuation may be resulting in under-utilization at a time scale that
// is not observable at our metrics frequency.
type CPULoadListener interface {
	CPULoad(runnable int, procs int)
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
	granter     *kvGranter
	minCPUSlots int
	maxCPUSlots int

	totalSlotsMetric *metric.Gauge
}

var _ cpuOverloadIndicator = &kvSlotAdjuster{}
var _ CPULoadListener = &kvSlotAdjuster{}

func (kvsa *kvSlotAdjuster) CPULoad(runnable int, procs int) {
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
	return kvsa.granter.usedSlots >= kvsa.granter.totalSlots
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
	GetPebbleMetrics() []*pebble.Metrics
}

// granterWithIOTokens is used to abstract kvGranter for testing.
type granterWithIOTokens interface {
	// setAvailableIOTokensLocked bounds the available tokens that can be
	// granted to the value provided in the tokens parameter. This is not a
	// tight bound when the callee has negative available tokens, due to the use
	// of granter.tookWithoutPermission, since in that the case the callee
	// increments that negative value with the value provided by tokens. This
	// method needs to be called periodically.
	setAvailableIOTokensLocked(tokens int64)
}

// TODO(sumeer): distinguish KVWork that is read-only, so that it is not
// subject to this write overload admission control. Also distinguish which
// store is needed by a KVWork and only constrain writes on stores that are
// overloaded.

// ioLoadListener adjusts tokens in kvGranter for IO, specifically due to
// overload caused by writes. Currently we are making no distinction at
// admission time between work that needs to only read and work that also
// needs to write -- both require a token. IO uses tokens and not slots since
// work completion is not an indicator that the "resource usage" has ceased --
// it just means that the write has been applied to the WAL. Most of the work
// is in flushing to sstables and the following compactions, which happens
// later.
type ioLoadListener struct {
	l0FileCountOverloadThreshold     int64
	l0SubLevelCountOverloadThreshold int32
	pebbleMetricsProvider            PebbleMetricsProvider
	kvRequester                      requester
	// mu is used when changing state in kvGranter.
	mu        *syncutil.Mutex
	kvGranter granterWithIOTokens

	// Cumulative stats used to compute interval stats.
	admittedCount uint64
	l0Bytes       int64
	l0AddedBytes  uint64
	// Exponentially smoothed per interval values.
	smoothedBytesRemoved int64
	smoothedNumAdmit     float64

	// totalTokens represents the tokens to give out until the next call to
	// adjustTokens. They are given out with smoothing -- tokensAllocated
	// represents what has been given out.
	totalTokens     int64
	tokensAllocated int64
	closeCh         chan struct{}
}

// timeTickerInterface abstracts time.Ticker for testing.
type timeTickerInterface interface {
	tickChannel() <-chan time.Time
	stop()
}

type timeTicker struct {
	ticker *time.Ticker
}

func (tt timeTicker) tickChannel() <-chan time.Time {
	return tt.ticker.C
}

func (tt timeTicker) stop() {
	tt.ticker.Stop()
}

type newTickerFunc func(time.Duration) timeTickerInterface

const unlimitedTokens = math.MaxInt64

// Token changes are made at a coarse time granularity of 1min since
// compactions can take ~10s to complete. The totalTokens to give out over
// the 1min interval are given out in a smoothed manner, at 1s intervals.
// This has similarities with the following kinds of token buckets:
// - Zero replenishment rate and a burst value that is changed every 1min. We
//   explicitly don't want a huge burst every 1min.
// - A replenishment rate equal to totalTokens/60, with a burst capped at
//   totalTokens/60. The only difference with the code here is that if
//   totalTokens is small, the integer rounding effects are compensated for.
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
const adjustmentInterval = 60

func (io *ioLoadListener) start(newTickerFunc newTickerFunc) {
	io.closeCh = make(chan struct{})
	// Initialize cumulative stats.
	io.admittedCount = io.kvRequester.getAdmittedCount()
	m := io.pebbleMetricsProvider.GetPebbleMetrics()
	for i := range m {
		io.l0Bytes += m[i].Levels[0].Size
		io.l0AddedBytes += m[i].Levels[0].BytesFlushed + m[i].Levels[0].BytesIngested
	}
	// allocateTokens gives out 1/60th of totalTokens every 1s.
	allocateTokens := func() {
		toAllocate := int64(math.Ceil(float64(io.totalTokens) / adjustmentInterval))
		if io.totalTokens != unlimitedTokens && toAllocate+io.tokensAllocated > io.totalTokens {
			toAllocate = io.totalTokens - io.tokensAllocated
		}
		if toAllocate > 0 {
			io.mu.Lock()
			defer io.mu.Unlock()
			io.tokensAllocated += toAllocate
			io.kvGranter.setAvailableIOTokensLocked(toAllocate)
		}
	}
	// First tick.
	io.totalTokens = unlimitedTokens
	allocateTokens()
	go func() {
		var ticks int64
		ticker := newTickerFunc(time.Second)
		tickCh := ticker.tickChannel()
		done := false
		for !done {
			select {
			case <-tickCh:
				ticks++
				if ticks%adjustmentInterval == 0 {
					io.adjustTokens()
				}
				allocateTokens()
			case <-io.closeCh:
				done = true
			}
		}
		ticker.stop()
		close(io.closeCh)
	}()
}

// adjustTokens computes a new value of totalTokens (and resets
// tokensAllocated). The new value, when overloaded, is based on comparing how
// many bytes are being moved out of L0 via compactions with the average
// number of bytes being added to L0 per KV work. We want the former to be
// (significantly) larger so that L0 returns to a healthy state.
func (io *ioLoadListener) adjustTokens() {
	io.tokensAllocated = 0
	// Grab the cumulative stats.
	admittedCount := io.kvRequester.getAdmittedCount()
	m := io.pebbleMetricsProvider.GetPebbleMetrics()
	if len(m) == 0 {
		// No store, which is odd. Setting totalTokens to unlimitedTokens is
		// defensive.
		io.totalTokens = unlimitedTokens
		return
	}
	var l0Bytes int64
	var l0AddedBytes uint64
	for i := range m {
		l0Bytes = m[i].Levels[0].Size
		l0AddedBytes = m[i].Levels[0].BytesFlushed + m[i].Levels[0].BytesIngested
	}

	// Compute the stats for the interval.
	bytesAdded := int64(l0AddedBytes - io.l0AddedBytes)
	// bytesRemoved are due to finished compactions.
	bytesRemoved := io.l0Bytes + bytesAdded - l0Bytes
	if bytesRemoved < 0 {
		bytesRemoved = 0
	}
	const alpha = 0.5
	// Compaction scheduling can be uneven in prioritizing L0 for compactions,
	// so smooth out what is being removed by compactions.
	io.smoothedBytesRemoved =
		int64(alpha*float64(bytesRemoved) + (1-alpha)*float64(io.smoothedBytesRemoved))
	// admitted represents what we actually admitted.
	admitted := admittedCount - io.admittedCount
	doLog := true
	if admitted == 0 {
		admitted = 1
		// Admission control is likely disabled, given there was no KVWork
		// admitted for 60s. And even if it is enabled, this is not an interesting
		// situation.
		doLog = false
	}
	// We constrain admission if any store if over the threshold.
	overThreshold := false
	for i := range m {
		if m[i].Levels[0].NumFiles > io.l0FileCountOverloadThreshold ||
			m[i].Levels[0].Sublevels > io.l0SubLevelCountOverloadThreshold {
			overThreshold = true
			break
		}
	}
	if overThreshold {
		// Attribute the bytesAdded equally to all the admitted work.
		bytesAddedPerWork := float64(bytesAdded) / float64(admitted)
		// Don't admit more work than we can remove via compactions. numAdmit
		// tracks our goal for admission.
		numAdmit := float64(io.smoothedBytesRemoved) / bytesAddedPerWork
		// Scale down since we want to get under the thresholds over time. This
		// scaling could be adjusted based on how much above the threshold we are,
		// but for now we just use a constant.
		numAdmit /= 2.0
		// Smooth it out in case our estimation of numAdmit goes awry in some
		// intervals.
		io.smoothedNumAdmit = alpha*numAdmit + (1-alpha)*io.smoothedNumAdmit
		io.totalTokens = int64(io.smoothedNumAdmit)
		if doLog {
			log.Infof(context.Background(),
				"IO overload: admitted: %d, added: %d, removed (%d, %d), admit: (%f, %d)",
				admitted, bytesAdded, bytesRemoved, io.smoothedBytesRemoved, numAdmit, io.totalTokens)
		}
	} else {
		// Under the threshold. Maintain a smoothedNumAdmit so that it is not 0
		// when we first go over the threshold. Instead use what we actually
		// admitted.
		io.smoothedNumAdmit = alpha*float64(admitted) + (1-alpha)*io.smoothedNumAdmit
		io.totalTokens = unlimitedTokens
	}
	// Install the latest cumulative stats.
	io.admittedCount = admittedCount
	io.l0Bytes = l0Bytes
	io.l0AddedBytes = l0AddedBytes
}

func (io *ioLoadListener) close() {
	io.closeCh <- struct{}{}
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
var _ = NewGrantCoordinatorMultiTenantKV
var _ = NewGrantCoordinatorSQL
var _ = (*GrantCoordinator)(nil).GetWorkQueue

// TODO(sumeer): experiment with approaches to adjust slots for
// SQLStatementLeafStartWork and SQLStatementRootStartWork for SQL nodes. Note
// that for these WorkKinds we are currently setting very high slot counts
// since we rely on other signals like memory and cpuOverloadIndicator to gate
// admission. One could debate whether we should be using rate limiting
// instead of counting slots for such work. The only reason the above code
// uses the term "slot" for these is that we have a completion indicator, and
// when we do have such an indicator it can be beneficial to be able to keep
// track of how many ongoing work items we have.
