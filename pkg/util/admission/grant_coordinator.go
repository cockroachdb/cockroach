// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// GrantCoordinators holds {regular,elastic} GrantCoordinators for
// {regular,elastic} work, and a StoreGrantCoordinators that allows for
// per-store GrantCoordinators for KVWork that involves writes.
type GrantCoordinators struct {
	RegularCPU *GrantCoordinator
	ElasticCPU *ElasticCPUGrantCoordinator
	Stores     *StoreGrantCoordinators
}

// Close implements the stop.Closer interface.
func (gcs GrantCoordinators) Close() {
	gcs.Stores.close()
	gcs.RegularCPU.Close()
	gcs.ElasticCPU.close()
}

// GrantCoordinator is the top-level object that coordinates grants across
// different WorkKinds (for more context see the comment in admission.go, and
// the comment where WorkKind is declared). Typically there will be one
// GrantCoordinator in a node for CPU intensive regular work, and for nodes that
// also have the KV layer, one GrantCoordinator per store (these are managed by
// StoreGrantCoordinators) for KVWork that uses that store. See the
// NewGrantCoordinators and NewGrantCoordinatorSQL functions.
type GrantCoordinator struct {
	ambientCtx log.AmbientContext

	settings *cluster.Settings

	// mu is ordered before any mutex acquired in a requester implementation.
	mu struct {
		syncutil.Mutex
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

		// The cpu fields can be nil, and the IO field below (ioLoadListener)
		// can be nil, since a GrantCoordinator typically handles one of these
		// two resources.
		cpuOverloadIndicator cpuOverloadIndicator
		cpuLoadListener      CPULoadListener

		// The latest value of GOMAXPROCS, received via CPULoad. Only initialized if
		// the cpu resource is being handled by this GrantCoordinator.
		numProcs int
	}

	lastCPULoadSamplePeriod time.Duration

	// NB: Some granters can be nil.
	// None of the references are changing, so mu protection is unnecessary
	granters [numWorkKinds]granterWithLockedCalls
	// The WorkQueues behaving as requesters in each granterWithLockedCalls.
	// This is kept separately only to service GetWorkQueue calls and to call
	// close().
	queues [numWorkKinds]requesterClose

	ioLoadListener *ioLoadListener

	// See the comment at continueGrantChain that explains how a grant chain
	// functions and the motivation. When !useGrantChains, grant chains are
	// disabled.
	useGrantChains bool

	// The admission control code needs high sampling frequency of the cpu load,
	// and turns off admission control enforcement when the sampling frequency
	// is too low. For testing queueing behavior, we do not want the enforcement
	// to be turned off in a non-deterministic manner so add a testing flag to
	// disable that feature. False in production.
	//
	// TODO(irfansharif): Fold into the testing knobs struct below.
	testingDisableSkipEnforcement bool

	knobs *TestingKnobs
}

var _ CPULoadListener = &GrantCoordinator{}

// Options for constructing GrantCoordinators.
type Options struct {
	MinCPUSlots                   int
	MaxCPUSlots                   int
	SQLKVResponseBurstTokens      int64
	SQLSQLResponseBurstTokens     int64
	TestingDisableSkipEnforcement bool
	// Only non-nil for tests.
	makeRequesterFunc      makeRequesterFunc
	makeStoreRequesterFunc makeStoreRequesterFunc
}

var _ base.ModuleTestingKnobs = &Options{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*Options) ModuleTestingKnobs() {}

// DefaultOptions are the default settings for various admission control knobs.
var DefaultOptions = Options{
	MinCPUSlots:               1,
	MaxCPUSlots:               100000, /* TODO(sumeer): add cluster setting */
	SQLKVResponseBurstTokens:  100000, /* TODO(sumeer): add cluster setting */
	SQLSQLResponseBurstTokens: 100000, /* TODO(sumeer): add cluster setting */
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
	if override.TestingDisableSkipEnforcement {
		o.TestingDisableSkipEnforcement = true
	}
}

type makeRequesterFunc func(
	_ log.AmbientContext, workKind WorkKind, granter granter, settings *cluster.Settings,
	metrics *WorkQueueMetrics, opts workQueueOptions) requester

// NewGrantCoordinators constructs GrantCoordinators and WorkQueues for a
// node. Caller is responsible for:
// - hooking up GrantCoordinators.RegularCPU to receive calls to CPULoad, and
// - to set a PebbleMetricsProvider on GrantCoordinators.Stores
//
// Regular and elastic requests pass through GrantCoordinators.{Regular,Elastic}
// respectively, and a subset of requests pass through each store's
// GrantCoordinator. We arrange these such that requests (that need to) first
// pass through a store's GrantCoordinator and then through the
// {regular,elastic} one. This ensures that we are not using slots/elastic CPU
// tokens in the latter level on requests that are blocked elsewhere for
// admission. Additionally, we don't want the CPU scheduler signal that is
// implicitly used in grant chains to delay admission through the per store
// GrantCoordinators since they are not trying to control CPU usage, so we turn
// off grant chaining in those coordinators.
func NewGrantCoordinators(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	opts Options,
	registry *metric.Registry,
	onLogEntryAdmitted OnLogEntryAdmitted,
	knobs *TestingKnobs,
) GrantCoordinators {
	metrics := makeGrantCoordinatorMetrics()
	registry.AddMetricStruct(metrics)

	if knobs == nil {
		knobs = &TestingKnobs{}
	}

	return GrantCoordinators{
		Stores:     makeStoresGrantCoordinators(ambientCtx, opts, st, onLogEntryAdmitted, knobs),
		RegularCPU: makeRegularGrantCoordinator(ambientCtx, opts, st, metrics, registry, knobs),
		ElasticCPU: makeElasticCPUGrantCoordinator(ambientCtx, st, registry),
	}
}

func makeRegularGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	st *cluster.Settings,
	metrics GrantCoordinatorMetrics,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *GrantCoordinator {
	makeRequester := makeWorkQueue
	if opts.makeRequesterFunc != nil {
		makeRequester = opts.makeRequesterFunc
	}

	kvSlotAdjuster := &kvSlotAdjuster{
		settings:                         st,
		minCPUSlots:                      opts.MinCPUSlots,
		maxCPUSlots:                      opts.MaxCPUSlots,
		totalSlotsMetric:                 metrics.KVTotalSlots,
		cpuLoadShortPeriodDurationMetric: metrics.KVCPULoadShortPeriodDuration,
		cpuLoadLongPeriodDurationMetric:  metrics.KVCPULoadLongPeriodDuration,
		slotAdjusterIncrementsMetric:     metrics.KVSlotAdjusterIncrements,
		slotAdjusterDecrementsMetric:     metrics.KVSlotAdjusterDecrements,
	}
	coord := &GrantCoordinator{
		ambientCtx:                    ambientCtx,
		settings:                      st,
		useGrantChains:                true,
		testingDisableSkipEnforcement: opts.TestingDisableSkipEnforcement,
		knobs:                         knobs,
	}
	coord.mu.grantChainID = 1
	coord.mu.cpuOverloadIndicator = kvSlotAdjuster
	coord.mu.cpuLoadListener = kvSlotAdjuster
	coord.mu.numProcs = 1

	kvg := &slotGranter{
		coord:                        coord,
		workKind:                     KVWork,
		totalSlots:                   opts.MinCPUSlots,
		skipSlotEnforcement:          !goschedstats.Supported,
		usedSlotsMetric:              metrics.KVUsedSlots,
		slotsExhaustedDurationMetric: metrics.KVSlotsExhaustedDuration,
	}

	kvSlotAdjuster.granter = kvg
	wqMetrics := makeWorkQueueMetrics(KVWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req := makeRequester(ambientCtx, KVWork, kvg, st, wqMetrics, makeWorkQueueOptions(KVWork))
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
	wqMetrics = makeWorkQueueMetrics(SQLKVResponseWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req = makeRequester(
		ambientCtx, SQLKVResponseWork, tg, st, wqMetrics, makeWorkQueueOptions(SQLKVResponseWork))
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
	wqMetrics = makeWorkQueueMetrics(SQLSQLResponseWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req = makeRequester(ambientCtx,
		SQLSQLResponseWork, tg, st, wqMetrics, makeWorkQueueOptions(SQLSQLResponseWork))
	coord.queues[SQLSQLResponseWork] = req
	tg.requester = req
	coord.granters[SQLSQLResponseWork] = tg

	return coord
}

// pebbleMetricsTick is called every adjustmentInterval seconds and passes
// through to the ioLoadListener, so that it can adjust the plan for future IO
// token allocations.
func (coord *GrantCoordinator) pebbleMetricsTick(ctx context.Context, m StoreMetrics) bool {
	return coord.ioLoadListener.pebbleMetricsTick(ctx, m)
}

// allocateIOTokensTick tells the ioLoadListener to allocate tokens.
func (coord *GrantCoordinator) allocateIOTokensTick(remainingTicks int64) {
	coord.ioLoadListener.allocateTokensTick(remainingTicks)
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if !coord.mu.grantChainActive {
		coord.tryGrantLocked()
	}
	// Else, let the grant chain finish. NB: we turn off grant chains on the
	// GrantCoordinators used for IO, so the if-condition is always true.
}

// adjustDiskTokenError is used to account for errors in disk read and write
// token estimation. Refer to the comment in adjustDiskTokenErrorLocked for more
// details.
func (coord *GrantCoordinator) adjustDiskTokenError(m StoreMetrics) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if storeGranter, ok := coord.granters[KVWork].(*kvStoreTokenGranter); ok {
		storeGranter.adjustDiskTokenErrorLocked(m.DiskStats.BytesRead, m.DiskStats.BytesWritten)
	}
}

// testingTryGrant is only for unit tests, since they sometimes cut out
// support classes like the ioLoadListener.
func (coord *GrantCoordinator) testingTryGrant() {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if !coord.mu.grantChainActive {
		coord.tryGrantLocked()
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

	if log.V(1) {
		if coord.lastCPULoadSamplePeriod != 0 && coord.lastCPULoadSamplePeriod != samplePeriod &&
			KVAdmissionControlEnabled.Get(&coord.settings.SV) {
			log.Infof(ctx, "CPULoad switching to period %s", samplePeriod.String())
		}
	}
	coord.lastCPULoadSamplePeriod = samplePeriod

	coord.mu.Lock()
	defer coord.mu.Unlock()

	coord.mu.numProcs = procs
	coord.mu.cpuLoadListener.CPULoad(runnable, procs, samplePeriod)

	// Slot adjustment and token refilling requires 1ms periods to work well. If
	// the CPULoad ticks are less frequent, there is no guarantee that the
	// tokens or slots will be sufficient to service requests. This is
	// particularly the case for slots where we dynamically adjust them, and
	// high contention can suddenly result in high slot utilization even while
	// cpu utilization stays low. We don't want to artificially bottleneck
	// request processing when we are in this slow CPULoad ticks regime since we
	// can't adjust slots or refill tokens fast enough. So we explicitly tell
	// the granters to not do token or slot enforcement.
	skipEnforcement := samplePeriod > time.Millisecond || !goschedstats.Supported
	coord.granters[SQLKVResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	coord.granters[SQLSQLResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	if coord.testingDisableSkipEnforcement {
		// This testing option only applies to KV work.
		skipEnforcement = false
	}
	kvg := coord.granters[KVWork].(*slotGranter)
	kvg.skipSlotEnforcement = skipEnforcement

	if coord.mu.grantChainActive && !coord.tryTerminateGrantChain() {
		return
	}
	coord.tryGrantLocked()
}

// tryGet is called by granter.tryGet with the WorkKind.
func (coord *GrantCoordinator) tryGet(
	workKind WorkKind, count int64, demuxHandle int8,
) (granted bool) {
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
		if coord.mu.grantChainActive && coord.mu.grantChainIndex >= workKind {
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
	if coord.mu.grantChainActive {
		if coord.mu.grantChainIndex > workKind &&
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
	coord.tryGrantLocked()
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
func (coord *GrantCoordinator) continueGrantChain(_ WorkKind, grantChainID grantChainID) {
	if grantChainID == noGrantChain {
		return
	}
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if coord.mu.grantChainID != grantChainID {
		// Someone terminated grantChainID by incrementing coord.grantChainID.
		return
	}
	coord.tryGrantLocked()
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
		now.Sub(coord.mu.grantChainStartTime) < delayForGrantChainTermination {
		return false
	}
	// Incrementing the ID will cause the existing grant chain to die out when
	// the grantee calls continueGrantChain.
	coord.mu.grantChainID++
	coord.mu.grantChainActive = false
	coord.mu.grantChainStartTime = time.Time{}
	return true
}

// tryGrantLocked tries to either continue an existing grant chain, or if no grant
// chain is active, tries to start a new grant chain when grant chaining is
// enabled, or grants as much as it can when grant chaining is disabled.
func (coord *GrantCoordinator) tryGrantLocked() {
	startingChain := false
	if !coord.mu.grantChainActive {
		// NB: always set to true when !coord.useGrantChains, and we won't
		// actually use this to start a grant chain (see below).
		startingChain = true
		coord.mu.grantChainIndex = 0
	}
	// Assume that we will not be able to start a new grant chain, or that the
	// existing one will die out. The code below will set it to true if neither
	// is true.
	coord.mu.grantChainActive = false
	grantBurstCount := 0
	// Grant in a burst proportional to numProcs, to generate a runnable for
	// each.
	grantBurstLimit := coord.mu.numProcs
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
	for ; coord.mu.grantChainIndex < numWorkKinds; coord.mu.grantChainIndex++ {
		localDone := false

		granter := coord.granters[coord.mu.grantChainIndex]
		if granter == nil {
			// A GrantCoordinator can be limited to certain WorkKinds, and the
			// remaining will be nil.
			continue
		}
		for granter.requesterHasWaitingRequests() && !localDone {
			chainID := noGrantChain
			if grantBurstCount+1 == grantBurstLimit && coord.useGrantChains {
				chainID = coord.mu.grantChainID
			}
			res := granter.tryGrantLocked(chainID)
			switch res {
			case grantSuccess:
				grantBurstCount++
				if grantBurstCount == grantBurstLimit && coord.useGrantChains {
					coord.mu.grantChainActive = true
					if startingChain {
						coord.mu.grantChainStartTime = timeutil.Now()
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
		coord.mu.grantChainID++
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
func (coord *GrantCoordinator) SafeFormat(s redact.SafePrinter, _ rune) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	s.Printf("(chain: id: %d active: %t index: %d)",
		coord.mu.grantChainID, coord.mu.grantChainActive, coord.mu.grantChainIndex,
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
				s.Printf("%s%s: used: %d, total: %d", curSep, kind, g.usedSlots, g.totalSlots)
			case *kvStoreTokenGranter:
				s.Printf(" io-avail: %d(%d), disk-write-tokens-avail: %d, disk-read-tokens-deducted: %d",
					g.coordMu.availableIOTokens[admissionpb.RegularWorkClass],
					g.coordMu.availableIOTokens[admissionpb.ElasticWorkClass],
					g.coordMu.diskTokensAvailable.writeByteTokens,
					g.coordMu.diskTokensError.diskReadTokensAlreadyDeducted,
				)
			}
		case SQLKVResponseWork, SQLSQLResponseWork:
			if coord.granters[i] != nil {
				g := coord.granters[i].(*tokenGranter)
				s.Printf("%s%s: avail: %d", curSep, kind, g.availableBurstTokens)
				if kind == SQLKVResponseWork {
					curSep = newlineStr
				} else {
					curSep = spaceStr
				}
			}
		}
	}
}

// GrantCoordinatorMetrics are metrics associated with a GrantCoordinator.
type GrantCoordinatorMetrics struct {
	KVTotalSlots                 *metric.Gauge
	KVUsedSlots                  *metric.Gauge
	KVSlotsExhaustedDuration     *metric.Counter
	KVCPULoadShortPeriodDuration *metric.Counter
	KVCPULoadLongPeriodDuration  *metric.Counter
	KVSlotAdjusterIncrements     *metric.Counter
	KVSlotAdjusterDecrements     *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (GrantCoordinatorMetrics) MetricStruct() {}

func makeGrantCoordinatorMetrics() GrantCoordinatorMetrics {
	return GrantCoordinatorMetrics{
		KVTotalSlots:                 metric.NewGauge(totalSlots),
		KVUsedSlots:                  metric.NewGauge(addName(KVWork.String(), usedSlots)),
		KVSlotsExhaustedDuration:     metric.NewCounter(kvSlotsExhaustedDuration),
		KVCPULoadShortPeriodDuration: metric.NewCounter(kvCPULoadShortPeriodDuration),
		KVCPULoadLongPeriodDuration:  metric.NewCounter(kvCPULoadLongPeriodDuration),
		KVSlotAdjusterIncrements:     metric.NewCounter(kvSlotAdjusterIncrements),
		KVSlotAdjusterDecrements:     metric.NewCounter(kvSlotAdjusterDecrements),
	}
}
