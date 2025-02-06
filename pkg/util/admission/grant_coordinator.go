// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
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
	Regular *GrantCoordinator
	Elastic *ElasticCPUGrantCoordinator
	Stores  *StoreGrantCoordinators
}

// Close implements the stop.Closer interface.
func (gcs GrantCoordinators) Close() {
	gcs.Stores.close()
	gcs.Regular.Close()
	gcs.Elastic.close()
}

// StoreGrantCoordinators is a container for GrantCoordinators for each store,
// that is used for KV work admission that takes into account store health.
// Currently, it is intended only for writes to stores.
type StoreGrantCoordinators struct {
	ambientCtx log.AmbientContext

	settings               *cluster.Settings
	makeStoreRequesterFunc makeStoreRequesterFunc

	gcMap syncutil.Map[roachpb.StoreID, GrantCoordinator]
	// numStores is used to track the number of stores which have been added
	// to the gcMap. This is used because the IntMap doesn't expose a size
	// api.
	numStores                      int
	setPebbleMetricsProviderCalled bool
	onLogEntryAdmitted             OnLogEntryAdmitted
	closeCh                        chan struct{}

	disableTickerForTesting bool // TODO(irfansharif): Fold into the testing knobs struct below.
	knobs                   *TestingKnobs
}

// SetPebbleMetricsProvider sets a PebbleMetricsProvider and causes the load
// on the various storage engines to be used for admission control.
func (sgc *StoreGrantCoordinators) SetPebbleMetricsProvider(
	startupCtx context.Context,
	pmp PebbleMetricsProvider,
	mrp MetricsRegistryProvider,
	iotc IOThresholdConsumer,
) {
	if sgc.setPebbleMetricsProviderCalled {
		panic(errors.AssertionFailedf("SetPebbleMetricsProvider called more than once"))
	}
	sgc.setPebbleMetricsProviderCalled = true
	pebbleMetricsProvider := pmp
	sgc.closeCh = make(chan struct{})
	metrics := pebbleMetricsProvider.GetPebbleMetrics()
	for _, m := range metrics {
		gc := sgc.initGrantCoordinator(m.StoreID, mrp.GetMetricsRegistry(m.StoreID))
		// Defensive call to LoadAndStore even though Store ought to be sufficient
		// since SetPebbleMetricsProvider can only be called once. This code
		// guards against duplication of stores returned by GetPebbleMetrics.
		_, loaded := sgc.gcMap.LoadOrStore(m.StoreID, gc)
		if !loaded {
			sgc.numStores++
		}
		gc.pebbleMetricsTick(startupCtx, m)
		gc.allocateIOTokensTick(unloadedDuration.ticksInAdjustmentInterval())
	}
	if sgc.disableTickerForTesting {
		return
	}
	// Attach tracer and log tags.
	ctx := sgc.ambientCtx.AnnotateCtx(context.Background())

	go func() {
		t := tokenAllocationTicker{}
		done := false
		// The first adjustment interval is unloaded. We start as unloaded mainly
		// for tests, and do a one-way transition to do 1ms ticks once we encounter
		// load in the system.
		var systemLoaded bool
		t.adjustmentStart(false /* loaded */)
		var remainingTicks uint64
		for !done {
			select {
			case <-t.ticker.C:
				remainingTicks = t.remainingTicks()
				// We do error accounting for disk reads and writes. This is important
				// since disk token accounting is based on estimates over adjustment
				// intervals. Like any model, these linear models have error terms, and
				// need to be adjusted for greater accuracy. We adjust for these errors
				// at a higher frequency than the adjustment interval. The error
				// adjustment interval is defined by errorAdjustmentInterval.
				//
				// NB: We always do error calculation prior to making adjustments to
				// make sure we account for errors prior to starting a new adjustment
				// interval.
				if t.shouldAdjustForError(remainingTicks, systemLoaded) {
					metrics = pebbleMetricsProvider.GetPebbleMetrics()
					for _, m := range metrics {
						if gc, ok := sgc.gcMap.Load(m.StoreID); ok {
							gc.adjustDiskTokenError(m)
						} else {
							log.Warningf(ctx,
								"seeing metrics for unknown storeID %d", m.StoreID)
						}
					}
				}

				// Start a new adjustment interval.
				if remainingTicks == 0 {
					metrics = pebbleMetricsProvider.GetPebbleMetrics()
					if len(metrics) != sgc.numStores {
						log.Warningf(ctx,
							"expected %d store metrics and found %d metrics", sgc.numStores, len(metrics))
					}
					for _, m := range metrics {
						if gc, ok := sgc.gcMap.Load(m.StoreID); ok {
							// We say that the system has load if at least one store is loaded.
							storeLoaded := gc.pebbleMetricsTick(ctx, m)
							systemLoaded = systemLoaded || storeLoaded
							iotc.UpdateIOThreshold(m.StoreID, gc.ioLoadListener.ioThreshold)
						} else {
							log.Warningf(ctx,
								"seeing metrics for unknown storeID %d", m.StoreID)
						}
					}
					// Start a new adjustment interval since there are no ticks remaining
					// in the current adjustment interval. Note that the next call to
					// allocateIOTokensTick will belong to the new adjustment interval.
					t.adjustmentStart(systemLoaded)
					remainingTicks = t.remainingTicks()
				}

				// Allocate tokens to the store grant coordinator.
				sgc.gcMap.Range(func(_ roachpb.StoreID, gc *GrantCoordinator) bool {
					gc.allocateIOTokensTick(int64(remainingTicks))
					// true indicates that iteration should continue after the
					// current entry has been processed.
					return true
				})
			case <-sgc.closeCh:
				done = true
				pebbleMetricsProvider.Close()
			}
		}
		t.stop()
	}()
}

func (sgc *StoreGrantCoordinators) initGrantCoordinator(
	storeID roachpb.StoreID, metricsRegistry *metric.Registry,
) *GrantCoordinator {
	coord := &GrantCoordinator{
		settings:       sgc.settings,
		useGrantChains: false,
		knobs:          sgc.knobs,
	}
	coord.mu.numProcs = 1

	// Initialize metrics.
	sgcMetrics := makeStoreGrantCoordinatorMetrics(metricsRegistry)
	regularStoreWorkQueueMetrics :=
		makeWorkQueueMetrics(fmt.Sprintf("%s-stores", KVWork), metricsRegistry,
			admissionpb.NormalPri, admissionpb.LockingNormalPri)
	elasticStoreWorkQueueMetrics :=
		makeWorkQueueMetrics(fmt.Sprintf("%s-stores", admissionpb.ElasticWorkClass), metricsRegistry,
			admissionpb.BulkLowPri, admissionpb.BulkNormalPri)
	storeWorkQMetrics := [admissionpb.NumWorkClasses]*WorkQueueMetrics{
		regularStoreWorkQueueMetrics, elasticStoreWorkQueueMetrics,
	}
	snapshotQMetrics := makeSnapshotQueueMetrics(metricsRegistry)

	kvg := &kvStoreTokenGranter{
		coord: coord,
		// Setting tokens to unlimited is defensive. We expect that
		// pebbleMetricsTick and allocateIOTokensTick will get called during
		// initialization, which will also set these to unlimited.
		startingIOTokens:                unlimitedTokens / unloadedDuration.ticksInAdjustmentInterval(),
		ioTokensExhaustedDurationMetric: sgcMetrics.KVIOTokensExhaustedDuration,
		availableTokensMetric:           sgcMetrics.KVIOTokensAvailable,
		tokensTakenMetric:               sgcMetrics.KVIOTokensTaken,
		tokensReturnedMetric:            sgcMetrics.KVIOTokensReturned,
	}
	kvg.coordMu.availableIOTokens[admissionpb.RegularWorkClass] = unlimitedTokens / unloadedDuration.ticksInAdjustmentInterval()
	kvg.coordMu.availableIOTokens[admissionpb.ElasticWorkClass] = kvg.coordMu.availableIOTokens[admissionpb.RegularWorkClass]
	kvg.coordMu.diskTokensAvailable.writeByteTokens = unlimitedTokens / unloadedDuration.ticksInAdjustmentInterval()

	opts := makeWorkQueueOptions(KVWork)
	// This is IO work, so override the usesTokens value.
	opts.usesTokens = true
	storeGranters := [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted{
		&kvStoreTokenChildGranter{
			workType: admissionpb.RegularStoreWorkType,
			parent:   kvg,
		},
		&kvStoreTokenChildGranter{
			workType: admissionpb.ElasticStoreWorkType,
			parent:   kvg,
		},
	}
	snapshotGranter := &kvStoreTokenChildGranter{
		workType: admissionpb.SnapshotIngestStoreWorkType,
		parent:   kvg,
	}

	storeReq := sgc.makeStoreRequesterFunc(
		sgc.ambientCtx,
		storeID,
		storeGranters,
		sgc.settings,
		storeWorkQMetrics,
		opts,
		sgc.knobs,
		sgc.onLogEntryAdmitted,
		sgcMetrics.KVIOTokensBypassed,
		&coord.mu.Mutex,
	)
	coord.queues[KVWork] = storeReq
	requesters := storeReq.getRequesters()
	kvg.regularRequester = requesters[admissionpb.RegularWorkClass]
	kvg.elasticRequester = requesters[admissionpb.ElasticWorkClass]
	kvg.snapshotRequester = makeSnapshotQueue(snapshotGranter, snapshotQMetrics)
	coord.granters[KVWork] = kvg
	coord.ioLoadListener = &ioLoadListener{
		storeID:               storeID,
		settings:              sgc.settings,
		kvRequester:           storeReq,
		perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
		diskBandwidthLimiter:  newDiskBandwidthLimiter(),
		kvGranter:             kvg,
		l0CompactedBytes:      sgcMetrics.L0CompactedBytes,
		l0TokensProduced:      sgcMetrics.L0TokensProduced,
	}
	return coord
}

// TryGetQueueForStore returns a WorkQueue for the given storeID, or nil if
// the storeID is not known.
func (sgc *StoreGrantCoordinators) TryGetQueueForStore(storeID roachpb.StoreID) *StoreWorkQueue {
	if granter, ok := sgc.gcMap.Load(storeID); ok {
		return granter.queues[KVWork].(*StoreWorkQueue)
	}
	return nil
}

func (sgc *StoreGrantCoordinators) TryGetSnapshotQueueForStore(storeID roachpb.StoreID) requester {
	if granter, ok := sgc.gcMap.Load(storeID); ok {
		return granter.granters[KVWork].(*kvStoreTokenGranter).snapshotRequester
	}
	return nil
}

func (sgc *StoreGrantCoordinators) close() {
	// closeCh can be nil in tests that never called SetPebbleMetricsProvider.
	if sgc.closeCh != nil {
		close(sgc.closeCh)
	}

	sgc.gcMap.Range(func(_ roachpb.StoreID, gc *GrantCoordinator) bool {
		gc.Close()
		// true indicates that iteration should continue after the
		// current entry has been processed.
		return true
	})
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
	// disable that feature.
	//
	// TODO(irfansharif): Fold into the testing knobs struct below.
	testingDisableSkipEnforcement bool

	knobs *TestingKnobs
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
	metrics *WorkQueueMetrics, opts workQueueOptions) requester

type makeStoreRequesterFunc func(
	_ log.AmbientContext, storeID roachpb.StoreID, granters [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted,
	settings *cluster.Settings, metrics [admissionpb.NumWorkClasses]*WorkQueueMetrics, opts workQueueOptions, knobs *TestingKnobs,
	onLogEntryAdmitted OnLogEntryAdmitted, ioTokensBypassedMetric *metric.Counter, coordMu *syncutil.Mutex,
) storeRequester

// NewGrantCoordinators constructs GrantCoordinators and WorkQueues for a
// regular cluster node. Caller is responsible for:
// - hooking up GrantCoordinators.Regular to receive calls to CPULoad, and
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
		Stores:  makeStoresGrantCoordinators(ambientCtx, opts, st, onLogEntryAdmitted, knobs),
		Regular: makeRegularGrantCoordinator(ambientCtx, opts, st, metrics, registry, knobs),
		Elastic: makeElasticGrantCoordinator(ambientCtx, st, registry),
	}
}

func makeElasticGrantCoordinator(
	ambientCtx log.AmbientContext, st *cluster.Settings, registry *metric.Registry,
) *ElasticCPUGrantCoordinator {
	schedulerLatencyListenerMetrics := makeSchedulerLatencyListenerMetrics()
	registry.AddMetricStruct(schedulerLatencyListenerMetrics)
	elasticCPUGranterMetrics := makeElasticCPUGranterMetrics()
	registry.AddMetricStruct(elasticCPUGranterMetrics)

	elasticWorkQueueMetrics := makeWorkQueueMetrics("elastic-cpu", registry,
		admissionpb.BulkNormalPri, admissionpb.NormalPri)

	elasticCPUGranter := newElasticCPUGranter(ambientCtx, st, elasticCPUGranterMetrics)
	schedulerLatencyListener := newSchedulerLatencyListener(ambientCtx, st, schedulerLatencyListenerMetrics, elasticCPUGranter)

	elasticCPUInternalWorkQueue := &WorkQueue{}
	initWorkQueue(elasticCPUInternalWorkQueue, ambientCtx, KVWork, "kv-elastic-cpu-queue", elasticCPUGranter, st,
		elasticWorkQueueMetrics,
		workQueueOptions{usesTokens: true}, nil /* knobs */) // will be closed by the embedding *ElasticCPUWorkQueue
	elasticCPUWorkQueue := makeElasticCPUWorkQueue(st, elasticCPUInternalWorkQueue, elasticCPUGranter, elasticCPUGranterMetrics)
	elasticCPUGrantCoordinator := makeElasticCPUGrantCoordinator(elasticCPUGranter, elasticCPUWorkQueue, schedulerLatencyListener)
	elasticCPUGranter.setRequester(elasticCPUInternalWorkQueue)
	schedulerLatencyListener.setCoord(elasticCPUGrantCoordinator)
	return elasticCPUGrantCoordinator
}

func makeStoresGrantCoordinators(
	ambientCtx log.AmbientContext,
	opts Options,
	st *cluster.Settings,
	onLogEntryAdmitted OnLogEntryAdmitted,
	knobs *TestingKnobs,
) *StoreGrantCoordinators {
	makeStoreRequester := makeStoreWorkQueue
	if opts.makeStoreRequesterFunc != nil {
		makeStoreRequester = opts.makeStoreRequesterFunc
	}
	storeCoordinators := &StoreGrantCoordinators{
		ambientCtx:             ambientCtx,
		settings:               st,
		makeStoreRequesterFunc: makeStoreRequester,
		onLogEntryAdmitted:     onLogEntryAdmitted,
		knobs:                  knobs,
	}
	return storeCoordinators
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

	sg := &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	wqMetrics = makeWorkQueueMetrics(SQLStatementLeafStartWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req = makeRequester(ambientCtx,
		SQLStatementLeafStartWork, sg, st, wqMetrics, makeWorkQueueOptions(SQLStatementLeafStartWork))
	coord.queues[SQLStatementLeafStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     kvSlotAdjuster,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	wqMetrics = makeWorkQueueMetrics(SQLStatementRootStartWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req = makeRequester(ambientCtx,
		SQLStatementRootStartWork, sg, st, wqMetrics, makeWorkQueueOptions(SQLStatementRootStartWork))
	coord.queues[SQLStatementRootStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementRootStartWork] = sg
	return coord
}

// Prevent the linter from emitting unused warnings until this is hooked up.
var _ = NewGrantCoordinatorSQL

// NewGrantCoordinatorSQL constructs a GrantCoordinator and WorkQueues for a
// single-tenant SQL node in a multi-tenant cluster. Caller is responsible for
// hooking this up to receive calls to CPULoad.
func NewGrantCoordinatorSQL(
	ambientCtx log.AmbientContext, st *cluster.Settings, registry *metric.Registry, opts Options,
) *GrantCoordinator {
	makeRequester := makeWorkQueue
	if opts.makeRequesterFunc != nil {
		makeRequester = opts.makeRequesterFunc
	}

	metrics := makeGrantCoordinatorMetrics()
	registry.AddMetricStruct(metrics)
	sqlNodeCPU := &sqlNodeCPUOverloadIndicator{}
	coord := &GrantCoordinator{
		ambientCtx:     ambientCtx,
		settings:       st,
		useGrantChains: true,
	}
	coord.mu.grantChainID = 1
	coord.mu.cpuOverloadIndicator = sqlNodeCPU
	coord.mu.cpuLoadListener = sqlNodeCPU
	coord.mu.numProcs = 1

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          sqlNodeCPU,
	}
	wqMetrics := makeWorkQueueMetrics(SQLKVResponseWork.String(), registry)
	req := makeRequester(ambientCtx,
		SQLKVResponseWork, tg, st, wqMetrics, makeWorkQueueOptions(SQLKVResponseWork))
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
	wqMetrics = makeWorkQueueMetrics(SQLSQLResponseWork.String(), registry)
	req = makeRequester(ambientCtx,
		SQLSQLResponseWork, tg, st, wqMetrics, makeWorkQueueOptions(SQLSQLResponseWork))
	coord.queues[SQLSQLResponseWork] = req
	tg.requester = req
	coord.granters[SQLSQLResponseWork] = tg

	sg := &slotGranter{
		coord:           coord,
		workKind:        SQLStatementLeafStartWork,
		totalSlots:      opts.SQLStatementLeafStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLLeafStartUsedSlots,
	}
	wqMetrics = makeWorkQueueMetrics(SQLStatementLeafStartWork.String(), registry)
	req = makeRequester(ambientCtx,
		SQLStatementLeafStartWork, sg, st, wqMetrics, makeWorkQueueOptions(SQLStatementLeafStartWork))
	coord.queues[SQLStatementLeafStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementLeafStartWork] = sg

	sg = &slotGranter{
		coord:           coord,
		workKind:        SQLStatementRootStartWork,
		totalSlots:      opts.SQLStatementRootStartWorkSlots,
		cpuOverload:     sqlNodeCPU,
		usedSlotsMetric: metrics.SQLRootStartUsedSlots,
	}
	wqMetrics = makeWorkQueueMetrics(SQLStatementRootStartWork.String(), registry)
	req = makeRequester(ambientCtx,
		SQLStatementRootStartWork, sg, st, wqMetrics, makeWorkQueueOptions(SQLStatementRootStartWork))
	coord.queues[SQLStatementRootStartWork] = req
	sg.requester = req
	coord.granters[SQLStatementRootStartWork] = sg

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
	skipEnforcement := samplePeriod > time.Millisecond
	coord.granters[SQLKVResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	coord.granters[SQLSQLResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	if coord.granters[KVWork] != nil {
		if !coord.testingDisableSkipEnforcement {
			kvg := coord.granters[KVWork].(*slotGranter)
			kvg.skipSlotEnforcement = skipEnforcement
		}
	}
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
		case SQLStatementLeafStartWork, SQLStatementRootStartWork:
			if coord.granters[i] != nil {
				g := coord.granters[i].(*slotGranter)
				s.Printf("%s%s: used: %d, total: %d", curSep, kind, g.usedSlots, g.totalSlots)
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
	SQLLeafStartUsedSlots        *metric.Gauge
	SQLRootStartUsedSlots        *metric.Gauge
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
		SQLLeafStartUsedSlots:        metric.NewGauge(addName(SQLStatementLeafStartWork.String(), usedSlots)),
		SQLRootStartUsedSlots:        metric.NewGauge(addName(SQLStatementRootStartWork.String(), usedSlots)),
	}
}

type StoreGrantCoordinatorMetrics struct {
	KVIOTokensTaken             *metric.Counter
	KVIOTokensReturned          *metric.Counter
	KVIOTokensBypassed          *metric.Counter
	KVIOTokensAvailable         [admissionpb.NumWorkClasses]*metric.Gauge
	KVIOTokensExhaustedDuration [admissionpb.NumWorkClasses]*metric.Counter
	L0CompactedBytes            *metric.Counter
	L0TokensProduced            *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (StoreGrantCoordinatorMetrics) MetricStruct() {}

func makeStoreGrantCoordinatorMetrics(registry *metric.Registry) StoreGrantCoordinatorMetrics {
	m := StoreGrantCoordinatorMetrics{
		KVIOTokensTaken:    metric.NewCounter(kvIOTokensTaken),
		KVIOTokensReturned: metric.NewCounter(kvIOTokensReturned),
		KVIOTokensBypassed: metric.NewCounter(kvIOTokensBypassed),
		L0CompactedBytes:   metric.NewCounter(l0CompactedBytes),
		L0TokensProduced:   metric.NewCounter(l0TokensProduced),
	}
	m.KVIOTokensAvailable[admissionpb.RegularWorkClass] = metric.NewGauge(kvIOTokensAvailable)
	m.KVIOTokensAvailable[admissionpb.ElasticWorkClass] = metric.NewGauge(kvElasticIOTokensAvailable)
	m.KVIOTokensExhaustedDuration = [admissionpb.NumWorkClasses]*metric.Counter{
		metric.NewCounter(kvIOTokensExhaustedDuration),
		metric.NewCounter(kvElasticIOTokensExhaustedDuration),
	}
	registry.AddMetricStruct(m)
	return m
}

// ElasticCPUGrantCoordinator coordinates grants for elastic CPU tokens, it has
// a single granter-requester pair. Since it's used for elastic CPU work, and
// the total allotment of CPU available for such work is reduced before getting
// close to CPU saturation (we observe 1ms+ p99 scheduling latencies when
// running at 65% utilization on 8vCPU machines, which is enough to affect
// foreground latencies), we don't want it to serve as a gatekeeper for
// SQL-level admission. All this informs why its structured as a separate grant
// coordinator.
//
// TODO(irfansharif): Ideally we wouldn't use this separate
// ElasticGrantCoordinator and just make this part of the one GrantCoordinator
// above but given we're dealing with a different workClass (elasticWorkClass)
// but for an existing WorkKind (KVWork), and not all APIs on the grant
// coordinator currently segment across the two, it was easier to copy over some
// of the mediating code instead (grant chains also don't apply in this scheme).
// Try to do something better here and revisit the existing abstractions; see
// github.com/cockroachdb/cockroach/pull/86638#pullrequestreview-1084437330.
type ElasticCPUGrantCoordinator struct {
	SchedulerLatencyListener SchedulerLatencyListener
	ElasticCPUWorkQueue      *ElasticCPUWorkQueue
	elasticCPUGranter        *elasticCPUGranter
}

func makeElasticCPUGrantCoordinator(
	elasticCPUGranter *elasticCPUGranter,
	elasticCPUWorkQueue *ElasticCPUWorkQueue,
	listener *schedulerLatencyListener,
) *ElasticCPUGrantCoordinator {
	return &ElasticCPUGrantCoordinator{
		elasticCPUGranter:        elasticCPUGranter,
		ElasticCPUWorkQueue:      elasticCPUWorkQueue,
		SchedulerLatencyListener: listener,
	}
}

func (e *ElasticCPUGrantCoordinator) close() {
	e.ElasticCPUWorkQueue.close()
}

// tryGrant is used to attempt to grant to waiting requests.
func (e *ElasticCPUGrantCoordinator) tryGrant() {
	e.elasticCPUGranter.tryGrant()
}

// NewPacer implements the PacerMaker interface.
func (e *ElasticCPUGrantCoordinator) NewPacer(unit time.Duration, wi WorkInfo) *Pacer {
	if e == nil {
		return nil
	}
	return &Pacer{
		unit: unit,
		wi:   wi,
		wq:   e.ElasticCPUWorkQueue,
	}
}
