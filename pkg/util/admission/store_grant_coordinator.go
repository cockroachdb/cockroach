// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// makeStoreRequesterFunc abstracts makeStoreWorkQueue for testing.
type makeStoreRequesterFunc func(
	_ log.AmbientContext, storeID roachpb.StoreID, granters [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted,
	settings *cluster.Settings, metrics [admissionpb.NumWorkClasses]*WorkQueueMetrics, opts workQueueOptions, knobs *TestingKnobs,
	onLogEntryAdmitted OnLogEntryAdmitted, ioTokensBypassedMetric *metric.Counter, coordMu *syncutil.Mutex,
) storeRequester

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

// PebbleMetricsProvider provides the pebble.Metrics for all stores.
type PebbleMetricsProvider interface {
	GetPebbleMetrics() []StoreMetrics
	Close()
}

// MetricsRegistryProvider provides the store metric.Registry for a given store.
type MetricsRegistryProvider interface {
	GetMetricsRegistry(roachpb.StoreID) *metric.Registry
}

// IOThresholdConsumer is informed about updated IOThresholds.
type IOThresholdConsumer interface {
	UpdateIOThreshold(roachpb.StoreID, *admissionpb.IOThreshold)
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
		// Ensure that the goroutine has observed the close and will no longer
		// call GetPebbleMetrics, since the engines will be closed soon after this
		// method returns, and calling GetPebbleMetrics on closed engines is not
		// permitted.
		sgc.closeCh <- struct{}{}
		// Close the channel, so that if close gets called twice due to a bug,
		// sending on the closed channel will panic instead of the send being
		// blocked forever.
		close(sgc.closeCh)
	}

	sgc.gcMap.Range(func(_ roachpb.StoreID, gc *GrantCoordinator) bool {
		gc.Close()
		// true indicates that iteration should continue after the
		// current entry has been processed.
		return true
	})
}

// StoreGrantCoordinatorMetrics are per-store metrics for a store
// GrantCoordinator.
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
