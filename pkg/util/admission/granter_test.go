// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// TestGranterBasic is a datadriven test with the following commands:
//
// init-grant-coordinator min-cpu=<int> max-cpu=<int> sql-kv-tokens=<int>
// sql-sql-tokens=<int>
// set-has-waiting-requests work=<kind> v=<true|false>
// set-return-value-from-granted work=<kind> v=<int>
// try-get work=<kind> [v=<int>]
// return-grant work=<kind> [v=<int>]
// took-without-permission work=<kind> [v=<int>]
// continue-grant-chain work=<kind>
// cpu-load runnable=<int> procs=<int> [infrequent=<bool>]
// init-store-grant-coordinator
// set-tokens io-tokens=<int> disk-write-tokens=<int>
// adjust-disk-error actual-write-bytes=<int> actual-read-bytes=<int>
func TestGranterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if !goschedstats.Supported {
		skip.IgnoreLint(t, "goschedstats not supported")
	}
	var ambientCtx log.AmbientContext
	// requesters[numWorkKinds] is used for kv elastic work, when working with a
	// store grant coordinator.
	// requesters[numWorkKinds + 1] is used for snapshot ingest, when working with a
	// store grant coordinator.
	var requesters [numWorkKinds + 2]*testRequester
	var coord *GrantCoordinator
	clearRequesterAndCoord := func() {
		coord = nil
		for i := range requesters {
			requesters[i] = nil
		}
	}
	var buf strings.Builder
	flushAndReset := func() string {
		fmt.Fprintf(&buf, "GrantCoordinator:\n%s\n", coord.String())
		str := buf.String()
		buf.Reset()
		return str
	}
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	KVSlotAdjusterOverloadThreshold.Override(context.Background(), &settings.SV, 1)
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "granter"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init-grant-coordinator":
			clearRequesterAndCoord()
			var opts Options
			d.ScanArgs(t, "min-cpu", &opts.MinCPUSlots)
			d.ScanArgs(t, "max-cpu", &opts.MaxCPUSlots)
			var burstTokens int
			d.ScanArgs(t, "sql-kv-tokens", &burstTokens)
			opts.SQLKVResponseBurstTokens = int64(burstTokens)
			d.ScanArgs(t, "sql-sql-tokens", &burstTokens)
			opts.SQLSQLResponseBurstTokens = int64(burstTokens)
			opts.makeRequesterFunc = func(
				_ log.AmbientContext, workKind WorkKind, granter granter, _ *cluster.Settings,
				metrics *WorkQueueMetrics, opts workQueueOptions) requester {
				req := &testRequester{
					workKind:               workKind,
					granter:                granter,
					usesTokens:             opts.usesTokens,
					buf:                    &buf,
					returnValueFromGranted: 1,
				}
				requesters[workKind] = req
				return req
			}
			delayForGrantChainTermination = 0
			coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, nil)
			defer coords.Close()
			coord = coords.Regular
			return flushAndReset()

		case "init-store-grant-coordinator":
			clearRequesterAndCoord()
			storeCoordinators := &StoreGrantCoordinators{
				settings: settings,
				makeStoreRequesterFunc: func(
					ambientCtx log.AmbientContext, _ roachpb.StoreID, granters [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted,
					settings *cluster.Settings, metrics [admissionpb.NumWorkClasses]*WorkQueueMetrics, opts workQueueOptions, knobs *TestingKnobs,
					_ OnLogEntryAdmitted, _ *metric.Counter, _ *syncutil.Mutex,
				) storeRequester {
					makeTestRequester := func(wc admissionpb.WorkClass) *testRequester {
						req := &testRequester{
							workKind:               KVWork,
							granter:                granters[wc],
							usesTokens:             true,
							buf:                    &buf,
							returnValueFromGranted: 0,
						}
						switch wc {
						case admissionpb.RegularWorkClass:
							req.additionalID = "-regular"
						case admissionpb.ElasticWorkClass:
							req.additionalID = "-elastic"
						}
						return req
					}
					req := &storeTestRequester{}
					req.requesters[admissionpb.RegularWorkClass] = makeTestRequester(admissionpb.RegularWorkClass)
					req.requesters[admissionpb.ElasticWorkClass] = makeTestRequester(admissionpb.ElasticWorkClass)
					requesters[KVWork] = req.requesters[admissionpb.RegularWorkClass]
					requesters[numWorkKinds] = req.requesters[admissionpb.ElasticWorkClass]
					return req
				},
				disableTickerForTesting: true,
				knobs:                   &TestingKnobs{},
			}
			var metricsProvider testMetricsProvider
			metricsProvider.setMetricsForStores([]int32{1}, pebble.Metrics{})
			registryProvider := &testRegistryProvider{registry: registry}
			storeCoordinators.SetPebbleMetricsProvider(context.Background(), &metricsProvider, registryProvider, &metricsProvider)
			var ok bool
			coord, ok = storeCoordinators.gcMap.Load(1)
			require.True(t, ok)
			kvStoreGranter := coord.granters[KVWork].(*kvStoreTokenGranter)
			// Defensive check: `SetPebbleMetricsProvider` should initialize the SnapshotQueue.
			require.NotNil(t, kvStoreGranter.snapshotRequester)
			snapshotGranter := kvStoreGranter.snapshotRequester.(*SnapshotQueue).snapshotGranter
			require.NotNil(t, snapshotGranter)
			snapshotReq := &testRequester{
				workKind:               KVWork,
				granter:                snapshotGranter,
				additionalID:           "-snapshot",
				usesTokens:             true,
				buf:                    &buf,
				returnValueFromGranted: 0,
			}
			kvStoreGranter.snapshotRequester = snapshotReq
			snapshotQueue := storeCoordinators.TryGetSnapshotQueueForStore(1)
			require.NotNil(t, snapshotQueue)
			requesters[numWorkKinds+1] = snapshotReq
			// Use the same model for the IO linear models.
			tlm := tokensLinearModel{multiplier: 0.5, constant: 50}
			// Use w-amp of 1 for the purpose of this test.
			wamplm := tokensLinearModel{multiplier: 1, constant: 0}
			kvStoreGranter.setLinearModels(tlm, tlm, tlm, wamplm)
			return flushAndReset()

		case "set-has-waiting-requests":
			var v bool
			d.ScanArgs(t, "v", &v)
			requesters[scanWorkKind(t, d)].waitingRequests = v
			return flushAndReset()

		case "set-return-value-from-granted":
			var v int
			d.ScanArgs(t, "v", &v)
			requesters[scanWorkKind(t, d)].returnValueFromGranted = int64(v)
			return flushAndReset()

		case "try-get":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			requesters[scanWorkKind(t, d)].tryGet(int64(v))
			return flushAndReset()

		case "return-grant":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			requesters[scanWorkKind(t, d)].returnGrant(int64(v))
			return flushAndReset()

		case "took-without-permission":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			requesters[scanWorkKind(t, d)].tookWithoutPermission(int64(v))
			return flushAndReset()

		case "continue-grant-chain":
			requesters[scanWorkKind(t, d)].continueGrantChain()
			return flushAndReset()

		case "cpu-load":
			var runnable, procs int
			d.ScanArgs(t, "runnable", &runnable)
			d.ScanArgs(t, "procs", &procs)
			infrequent := false
			if d.HasArg("infrequent") {
				d.ScanArgs(t, "infrequent", &infrequent)
			}

			samplePeriod := time.Millisecond
			if infrequent {
				samplePeriod = 250 * time.Millisecond
			}
			coord.CPULoad(runnable, procs, samplePeriod)
			str := flushAndReset()
			kvsa := coord.mu.cpuLoadListener.(*kvSlotAdjuster)
			microsToMillis := func(micros int64) int64 {
				return micros * int64(time.Microsecond) / int64(time.Millisecond)
			}
			return fmt.Sprintf("%sSlotAdjuster metrics: slots: %d, duration (short, long) millis: (%d, %d), inc: %d, dec: %d\n",
				str, kvsa.totalSlotsMetric.Value(),
				microsToMillis(kvsa.cpuLoadShortPeriodDurationMetric.Count()),
				microsToMillis(kvsa.cpuLoadLongPeriodDurationMetric.Count()),
				kvsa.slotAdjusterIncrementsMetric.Count(), kvsa.slotAdjusterDecrementsMetric.Count(),
			)

		case "set-tokens-loop":
			var ioTokens int
			var elasticDiskWriteTokens int
			var elasticDiskReadTokens int
			var loop int
			d.ScanArgs(t, "io-tokens", &ioTokens)
			d.ScanArgs(t, "disk-write-tokens", &elasticDiskWriteTokens)
			if d.HasArg("disk-read-tokens") {
				d.ScanArgs(t, "disk-read-tokens", &elasticDiskReadTokens)
			}
			d.ScanArgs(t, "loop", &loop)

			for loop > 0 {
				loop--
				// We are not using a real ioLoadListener, and simply setting the
				// tokens (the ioLoadListener has its own test).
				coord.granters[KVWork].(*kvStoreTokenGranter).setAvailableTokens(
					int64(ioTokens),
					int64(ioTokens),
					int64(elasticDiskWriteTokens),
					int64(elasticDiskReadTokens),
					int64(ioTokens*250),
					int64(ioTokens*250),
					int64(elasticDiskWriteTokens*250),
					false, // lastTick
				)
			}
			coord.testingTryGrant()
			return flushAndReset()

		case "set-tokens":
			var ioTokens int
			var elasticDiskWriteTokens int
			var elasticDiskReadTokens int
			var tickInterval int
			d.ScanArgs(t, "io-tokens", &ioTokens)
			d.ScanArgs(t, "disk-write-tokens", &elasticDiskWriteTokens)
			if d.HasArg("disk-read-tokens") {
				d.ScanArgs(t, "disk-read-tokens", &elasticDiskReadTokens)
			}
			elasticIOTokens := ioTokens
			if d.HasArg("elastic-io-tokens") {
				d.ScanArgs(t, "elastic-io-tokens", &elasticIOTokens)
			}
			if d.HasArg("tick-interval") {
				d.ScanArgs(t, "tick-interval", &tickInterval)
			}
			var burstMultiplier = 1
			if tickInterval == 1 {
				burstMultiplier = 250
			} else if tickInterval == 250 {
			} else {
				return "unsupported tick rate"
			}

			// We are not using a real ioLoadListener, and simply setting the
			// tokens (the ioLoadListener has its own test).
			coord.granters[KVWork].(*kvStoreTokenGranter).setAvailableTokens(
				int64(ioTokens),
				int64(elasticIOTokens),
				int64(elasticDiskWriteTokens),
				int64(elasticDiskReadTokens),
				int64(ioTokens*burstMultiplier),
				int64(elasticIOTokens*burstMultiplier),
				int64(elasticDiskWriteTokens*burstMultiplier),
				false, // lastTick
			)
			coord.testingTryGrant()
			return flushAndReset()

		case "store-write-done":
			var origTokens, writeBytes int
			d.ScanArgs(t, "orig-tokens", &origTokens)
			d.ScanArgs(t, "write-bytes", &writeBytes)
			requesters[scanWorkKind(t, d)].granter.(granterWithStoreReplicatedWorkAdmitted).storeWriteDone(
				int64(origTokens), StoreWorkDoneInfo{WriteBytes: int64(writeBytes)})
			coord.testingTryGrant()
			return flushAndReset()

		case "adjust-disk-error":
			var readBytes, writeBytes int
			d.ScanArgs(t, "actual-write-bytes", &writeBytes)
			d.ScanArgs(t, "actual-read-bytes", &readBytes)
			m := StoreMetrics{DiskStats: DiskStats{
				BytesRead:    uint64(readBytes),
				BytesWritten: uint64(writeBytes),
			}}
			coord.adjustDiskTokenError(m)
			return flushAndReset()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// TestStoreCoordinators tests only the setup of GrantCoordinators per store.
// Testing of IO load functionality happens in TestIOLoadListener.
func TestStoreCoordinators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	var buf strings.Builder
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	// All the KVWork requesters. The first one is for all KVWork and the
	// remaining are the per-store ones.
	var requesters []*testRequester
	makeRequesterFunc := func(
		_ log.AmbientContext, workKind WorkKind, granter granter, _ *cluster.Settings,
		_ *WorkQueueMetrics,
		opts workQueueOptions) requester {
		req := &testRequester{
			workKind:   workKind,
			granter:    granter,
			usesTokens: opts.usesTokens,
			buf:        &buf,
		}
		if workKind == KVWork {
			requesters = append(requesters, req)
		}
		return req
	}
	opts := Options{
		makeRequesterFunc: makeRequesterFunc,
		makeStoreRequesterFunc: func(
			ctx log.AmbientContext, _ roachpb.StoreID, granters [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted,
			settings *cluster.Settings, metrics [admissionpb.NumWorkClasses]*WorkQueueMetrics, opts workQueueOptions, _ *TestingKnobs, _ OnLogEntryAdmitted,
			_ *metric.Counter, _ *syncutil.Mutex) storeRequester {
			reqReg := makeRequesterFunc(ctx, KVWork, granters[admissionpb.RegularWorkClass], settings, metrics[admissionpb.RegularWorkClass], opts)
			reqElastic := makeRequesterFunc(ctx, KVWork, granters[admissionpb.ElasticWorkClass], settings, metrics[admissionpb.ElasticWorkClass], opts)
			str := &storeTestRequester{}
			str.requesters[admissionpb.RegularWorkClass] = reqReg.(*testRequester)
			str.requesters[admissionpb.RegularWorkClass].additionalID = "-regular"
			str.requesters[admissionpb.ElasticWorkClass] = reqElastic.(*testRequester)
			str.requesters[admissionpb.ElasticWorkClass].additionalID = "-elastic"
			return str
		},
	}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, nil)
	// There is only 1 KVWork requester at this point in initialization, for the
	// Regular GrantCoordinator.
	require.Equal(t, 1, len(requesters))
	storeCoords := coords.Stores
	metrics := pebble.Metrics{}
	mp := testMetricsProvider{}
	mp.setMetricsForStores([]int32{10, 20}, metrics)
	registryProvider := &testRegistryProvider{registry: registry}
	// Setting the metrics provider will cause the initialization of two
	// GrantCoordinators for the two stores.
	storeCoords.SetPebbleMetricsProvider(context.Background(), &mp, registryProvider, &mp)
	// Now we have 1+2*2 = 5 KVWork requesters.
	require.Equal(t, 5, len(requesters))
	// Confirm that the store IDs are as expected.
	var actualStores []roachpb.StoreID

	storeCoords.gcMap.Range(func(s roachpb.StoreID, _ *GrantCoordinator) bool {
		actualStores = append(actualStores, s)
		// true indicates that iteration should continue after the
		// current entry has been processed.
		return true
	})
	sort.Slice(actualStores, func(i, j int) bool { return actualStores[i] < actualStores[j] })
	require.Equal(t, []roachpb.StoreID{10, 20}, actualStores)
	// Do tryGet on all store requesters, which have unlimited tokens at this
	// point in time, so will return true.
	requesters = requesters[1:]
	for i := range requesters {
		requesters[i].tryGet(1)
	}
	require.Equal(t,
		"kv-regular: tryGet(1) returned true\nkv-elastic: tryGet(1) returned true\n"+
			"kv-regular: tryGet(1) returned true\nkv-elastic: tryGet(1) returned true\n",
		buf.String())
	coords.Close()
}

type testRequester struct {
	workKind     WorkKind
	additionalID string
	granter      granter
	usesTokens   bool
	buf          *strings.Builder

	waitingRequests        bool
	returnValueFromGranted int64
	grantChainID           grantChainID
}

var _ requester = &testRequester{}

func (tr *testRequester) hasWaitingRequests() bool {
	return tr.waitingRequests
}

func (tr *testRequester) granted(grantChainID grantChainID) int64 {
	fmt.Fprintf(tr.buf, "%s%s: granted in chain %d, and returning %d\n",
		tr.workKind, tr.additionalID,
		grantChainID, tr.returnValueFromGranted)
	tr.grantChainID = grantChainID
	return tr.returnValueFromGranted
}

func (tr *testRequester) close() {}

func (tr *testRequester) tryGet(count int64) {
	rv := tr.granter.tryGet(count)
	fmt.Fprintf(tr.buf, "%s%s: tryGet(%d) returned %t\n", tr.workKind,
		tr.additionalID, count, rv)
}

func (tr *testRequester) returnGrant(count int64) {
	fmt.Fprintf(tr.buf, "%s%s: returnGrant(%d)\n", tr.workKind, tr.additionalID,
		count)
	tr.granter.returnGrant(count)
}

func (tr *testRequester) tookWithoutPermission(count int64) {
	fmt.Fprintf(tr.buf, "%s%s: tookWithoutPermission(%d)\n", tr.workKind,
		tr.additionalID, count)
	tr.granter.tookWithoutPermission(count)
}

func (tr *testRequester) continueGrantChain() {
	fmt.Fprintf(tr.buf, "%s%s: continueGrantChain\n", tr.workKind,
		tr.additionalID)
	tr.granter.continueGrantChain(tr.grantChainID)
}

type storeTestRequester struct {
	requesters [admissionpb.NumWorkClasses]*testRequester
}

var _ storeRequester = &storeTestRequester{}

func (str *storeTestRequester) getRequesters() [admissionpb.NumWorkClasses]requester {
	var rv [admissionpb.NumWorkClasses]requester
	for i := range str.requesters {
		rv[i] = str.requesters[i]
	}
	return rv
}

func (str *storeTestRequester) close() {}

func (str *storeTestRequester) getStoreAdmissionStats() storeAdmissionStats {
	// Only used by ioLoadListener, so don't bother.
	return storeAdmissionStats{}
}

func (str *storeTestRequester) setStoreRequestEstimates(estimates storeRequestEstimates) {
	// Only used by ioLoadListener, so don't bother.
}

func scanWorkKind(t *testing.T, d *datadriven.TestData) int8 {
	var kindStr string
	d.ScanArgs(t, "work", &kindStr)
	switch kindStr {
	case "kv":
		return int8(KVWork)
	case "sql-kv-response":
		return int8(SQLKVResponseWork)
	case "sql-sql-response":
		return int8(SQLSQLResponseWork)
	case "kv-elastic":
		return int8(numWorkKinds)
	case "kv-snapshot":
		return int8(numWorkKinds + 1)
	}
	panic("unknown WorkKind")
}

// TODO(sumeer):
// - Test metrics
// - Test GrantCoordinator with multi-tenant configurations

type testMetricsProvider struct {
	metrics []StoreMetrics
}

func (m *testMetricsProvider) GetPebbleMetrics() []StoreMetrics {
	return m.metrics
}

func (m *testMetricsProvider) Close() {}

func (m *testMetricsProvider) UpdateIOThreshold(
	id roachpb.StoreID, threshold *admissionpb.IOThreshold,
) {
}

func (m *testMetricsProvider) setMetricsForStores(stores []int32, metrics pebble.Metrics) {
	m.metrics = m.metrics[:0]
	for _, s := range stores {
		m.metrics = append(m.metrics, StoreMetrics{
			StoreID: roachpb.StoreID(s),
			Metrics: &metrics,
		})
	}
}

type noopOnLogEntryAdmitted struct{}

func (n *noopOnLogEntryAdmitted) AdmittedLogEntry(context.Context, LogEntryAdmittedCallbackState) {
}

var _ OnLogEntryAdmitted = &noopOnLogEntryAdmitted{}

type testRegistryProvider struct {
	registry *metric.Registry
}

func (r *testRegistryProvider) GetMetricsRegistry(roachpb.StoreID) *metric.Registry {
	return r.registry
}

func TestCPUTokenSim(t *testing.T) {
	// Same as cpu rate s/s, or number of vCPUs to give this work.
	tokenRateMillisPerMillis := 4
	burstIntervalMillis := 1000
	workInitialTokenMillis := 2
	workUsageMillis := 1
	if workInitialTokenMillis < workUsageMillis {
		panic("")
	}
	workDurationMillis := 50
	if workDurationMillis < workUsageMillis {
		panic("")
	}
	// Keep this short since care about under-utilization at short time intervals.
	simulateIntervalMillis := 2 * 1000

	type work struct {
		start int
	}
	var started []work
	bucketBurstTokens := tokenRateMillisPerMillis * burstIntervalMillis
	tokens := bucketBurstTokens
	usageMillis := 0

	// Tick every 1ms.
	for i := 0; i <= simulateIntervalMillis; i++ {
		// Pop work that finished
		j := 0
		for ; j < len(started); j++ {
			if started[j].start+workDurationMillis > i {
				break
			}
			usageMillis += workUsageMillis
			returnMillis := workInitialTokenMillis - workUsageMillis
			tokens += returnMillis
		}
		started = started[j:]
		// Add tokens for tick.
		tokens += tokenRateMillisPerMillis
		// Cap tokens to burst.
		if tokens > bucketBurstTokens {
			tokens = bucketBurstTokens
		}
		// Try starting work.
		for tokens > 0 {
			tokens -= workInitialTokenMillis
			started = append(started, work{start: i})
		}
	}
	// End of simulation.
	for _, w := range started {
		interval := simulateIntervalMillis - w.start
		if interval == 0 {
			continue
		}
		intervalFraction := float64(interval) / float64(workDurationMillis)
		usageMillis += int(intervalFraction * float64(workUsageMillis))
	}
	fmt.Printf("work (dur: %d usage: %d): rate: %d, usage-rate: %f\n",
		workDurationMillis, workUsageMillis,
		tokenRateMillisPerMillis,
		float64(usageMillis)/float64(simulateIntervalMillis))
}

func TestMultiTenantCPUTokenSim(t *testing.T) {
	// 16ms of tokens per tick, represent 80%. want 10% consumed by tenant 0 and
	// tenant 1 combined. Would be 2ms per tick.
	// NO MORE: But want 1 in 8 to see arrival.
	const tokensPerTick = 16
	const burstTokens = 1000 * 16
	tokens := burstTokens
	const workInitialTokens = 100
	const workUsageTokens = workInitialTokens
	const workDuration = workInitialTokens
	const numSmallTenants = 4
	type waitingWork struct {
		startWait int
	}
	var smallTenantWaiting [numSmallTenants][]waitingWork
	type work struct {
		tenant int
		start  int
	}
	var started []work
	var tokensUsed [numSmallTenants + 1]int
	type waitTime struct {
		count    int
		waitTime int
	}
	var waitTimes [numSmallTenants]waitTime

	simulateIntervalMillis := 50 * 1000
	for i := 0; i <= simulateIntervalMillis; i++ {
		// Pop work that finished
		j := 0
		for ; j < len(started); j++ {
			if started[j].start+workDuration > i {
				break
			}
			returnTokens := workInitialTokens - workUsageTokens
			tokens += returnTokens
			tokensUsed[started[j].tenant] -= returnTokens
		}
		started = started[j:]
		// Add tokens for tick.
		tokens += tokensPerTick
		// Cap tokens to burst.
		if tokens > burstTokens {
			tokens = burstTokens
		}
		// Try starting work.
		for tokens > 0 {
			tenant := -1
			used := math.MaxInt
			for j := range smallTenantWaiting {
				if len(smallTenantWaiting[j]) > 0 && tokensUsed[j] < used {
					tenant = j
					used = tokensUsed[j]
				}
			}
			if tokensUsed[numSmallTenants] < used {
				tenant = numSmallTenants
			}
			tokensUsed[tenant] += workInitialTokens
			started = append(started, work{tenant: tenant, start: i})
			tokens -= workInitialTokens
			// fmt.Printf("%d: picked tenant %d\n", i, tenant)
			if tenant != numSmallTenants {
				waitTimes[tenant].count++
				waitTimes[tenant].waitTime += (i - smallTenantWaiting[tenant][0].startWait)
				smallTenantWaiting[tenant] = smallTenantWaiting[tenant][1:]
			}
		}
		if i%100 == 0 {
			for j := range smallTenantWaiting {
				for k := 0; k < 1; k++ {
					smallTenantWaiting[j] = append(smallTenantWaiting[j], waitingWork{startWait: i})
				}
			}
		}
	}
	sumTokensUsed := 0
	for j := range tokensUsed {
		sumTokensUsed += tokensUsed[j]
	}
	fmt.Printf("rate %.2f\n", float64(sumTokensUsed)/float64(simulateIntervalMillis))
	fmt.Printf("tenant fractions:")
	for j := range tokensUsed {
		fmt.Printf(" %d:%.2f", j, float64(tokensUsed[j])/float64(sumTokensUsed))
	}
	fmt.Printf("\n")
	fmt.Printf("wait times:")
	for j := range waitTimes {
		fmt.Printf(" %d:%.2fms", j, float64(waitTimes[j].waitTime)/float64(waitTimes[j].count))
	}
	fmt.Printf("\n")
}

func TestMultiTenantUncontrolledCPUTokenSim(t *testing.T) {
	// 16ms of tokens per tick, represent 80%. want 20% consumed by tenant 0, 1,
	// 2, 3 combined.
	const tokensPerTick = 16
	const burstTokens = 1000 * 16
	tokens := burstTokens
	const workInitialTokens = 100
	const workUsageTokens = workInitialTokens
	const workDuration = workInitialTokens
	const numSmallTenants = 4
	const smallTenantArrivalInterval = 100
	type waitingWork struct {
		startWait int
	}
	var smallTenantWaiting [numSmallTenants][]waitingWork
	type work struct {
		tenant int
		start  int
	}
	var started []work
	var tokensUsed [numSmallTenants + 1]int
	var tenantTokensNanos [numSmallTenants + 1]int64
	tenantBurstTokensNanos := int64(burstTokens) * 1e6
	for i := range tenantTokensNanos {
		tenantTokensNanos[i] = tenantBurstTokensNanos
	}
	uncontrolledTokens := 0
	type waitTime struct {
		count    int
		waitTime int
	}
	var waitTimes [numSmallTenants]waitTime

	simulateIntervalMillis := 8 * 1000
	lastTotalTokensUsed := 0
	lastTokens := burstTokens
	for i := 0; i <= simulateIntervalMillis; i++ {
		// Pop work that finished
		{
			j := 0
			for ; j < len(started); j++ {
				if started[j].start+workDuration > i {
					break
				}
				returnTokens := workInitialTokens - workUsageTokens
				tokens += returnTokens
				tenantTokensNanos[started[j].tenant] += int64(returnTokens) * 1e6
				tokensUsed[started[j].tenant] -= returnTokens
			}
			started = started[j:]
		}
		// Add tokens for tick.
		tokens += tokensPerTick
		// Cap tokens to burst.
		if tokens > burstTokens {
			tokens = burstTokens
		}
		if i%1000 == 0 {
			totalTokensUsed := 0
			for i := range tokensUsed {
				totalTokensUsed += tokensUsed[i]
			}
			deltaTokensUsed := totalTokensUsed - lastTotalTokensUsed
			lastTotalTokensUsed = totalTokensUsed
			// Ignoring the fact that tokens are capped.
			maxControlledTokensAvailable := burstTokens + lastTokens
			if tokens < 0 {
				// Don't let these become too negative.
				tokens = 0
			}
			lastTokens = tokens
			// For logging.
			prevTenantBurstTokensNanos := tenantBurstTokensNanos
			if uncontrolledTokens != 0 {
				excessTokens := deltaTokensUsed - maxControlledTokensAvailable
				if excessTokens < 0 {
					excessTokens = 0
				}
				toDeduct := min(excessTokens, uncontrolledTokens)
				// Some tenants are exhausing our aggregate. We need to restrict
				// them. How to restrict them?
				//
				// When we do tenantBurstTokensNanos -= int64(uncontrolledTokens) *
				// 1e6 we could reduce tenantBurstTokens by a much larger value than
				// what we should be doing. Cap this to 25% of burstTokens, which will
				// be 4000.
				//
				// Second problem. When we deduct this from tenantTokensNanos we may
				// still not become the restriction. The large tenant is being shaped
				// by the aggregate, but keeping the burst close to 0. This is ok. In that
				// case we don't want to reduce the tt. But also gone over because of
				// initial burst.
				tenantBurstTokensNanos -= int64(toDeduct) * 1e6
				fmt.Printf("%d: uncontrolledTokens: %d, excessTokens: %d, tenantBurstTokens: %d, tokens: %d\n",
					i, uncontrolledTokens, excessTokens,
					tenantBurstTokensNanos/1e6, tokens)
			}
			uncontrolledTokens = 0
			// Under-utilization of aggregate tokens.
			if tokens > (burstTokens / 8) {
				tenantBurstTokensNanos = int64(burstTokens) * 1e6
				fmt.Printf("%d: tenantBurstTokens: %d\n", i, tenantBurstTokensNanos/1e6)
			}
			deltaTenantBurstTokensNanos := tenantBurstTokensNanos - prevTenantBurstTokensNanos
			fmt.Printf("%d: delta tenant burst tokens: %d\n", i, deltaTenantBurstTokensNanos/1e6)
			for tenant := range tenantTokensNanos {
				tenantTokensNanos[tenant] += deltaTenantBurstTokensNanos
			}

			fmt.Printf("%d: delta tokens used: %d\n", i, deltaTokensUsed)
		}
		for i := range tenantTokensNanos {
			tenantTokensNanos[i] += tenantBurstTokensNanos / 1000
			if tenantTokensNanos[i] > tenantBurstTokensNanos {
				tenantTokensNanos[i] = tenantBurstTokensNanos
			}
		}
		fmt.Printf("%d: tokens: %d tt:", i, tokens)
		for i := range tenantTokensNanos {
			fmt.Printf(" %d", tenantTokensNanos[i]/1e6)
		}
		fmt.Printf("\n")

		// Try starting work using tokens.
		for tokens > 0 {
			tenant := -1
			used := math.MaxInt
			for j := range smallTenantWaiting {
				if len(smallTenantWaiting[j]) > 0 && tokensUsed[j] < used && tenantTokensNanos[j] > 0 {
					tenant = j
					used = tokensUsed[j]
				}
			}
			if tokensUsed[numSmallTenants] < used && tenantTokensNanos[numSmallTenants] > 0 {
				tenant = numSmallTenants
			}
			if tenant < 0 {
				fmt.Printf("%d: no tenant picked\n", i)
				break
			} else {
				tokensUsed[tenant] += workInitialTokens
				started = append(started, work{tenant: tenant, start: i})
				tokens -= workInitialTokens
				tenantTokensNanos[tenant] -= int64(workInitialTokens) * 1e6
				if tenant != numSmallTenants {
					waitTimes[tenant].count++
					wt := (i - smallTenantWaiting[tenant][0].startWait)
					waitTimes[tenant].waitTime += wt
					smallTenantWaiting[tenant] = smallTenantWaiting[tenant][1:]
					fmt.Printf("%d: picked tenant %d, wt: %d\n", i, tenant, wt)
				} else {
					fmt.Printf("%d: picked tenant %d\n", i, tenant)
				}
			}
		}
		// Either tokens < 0, or all tenants with waiting work have no more
		// tenantTokensNanos[tenant]. In the latter case the following loop will
		// by definition be unsuccessful. In the former case, it can do something.

		// Try starting work using tenantTokensNanos.
		for tenant := 0; tenant <= numSmallTenants; tenant++ {
			for tenantTokensNanos[tenant] > (3*tenantBurstTokensNanos)/4 {
				if tenant == numSmallTenants || len(smallTenantWaiting[tenant]) > 0 {
					require.GreaterOrEqual(t, 0, tokens)
					tokensUsed[tenant] += workInitialTokens
					started = append(started, work{tenant: tenant, start: i})
					tokens -= workInitialTokens
					tenantTokensNanos[tenant] -= int64(workInitialTokens) * 1e6
					uncontrolledTokens += workInitialTokens
					if tenant != numSmallTenants {
						waitTimes[tenant].count++
						wt := (i - smallTenantWaiting[tenant][0].startWait)
						waitTimes[tenant].waitTime += wt
						smallTenantWaiting[tenant] = smallTenantWaiting[tenant][1:]
						fmt.Printf("%d: picked tenant %d, wt: %d\n", i, tenant, wt)
					} else {
						fmt.Printf("%d: uncontrolled picked tenant %d\n", i, tenant)
					}
				} else {
					break
				}
			}
		}
		if i%100 == 0 {
			// Add a work unit for small tenants.
			for tenant := range smallTenantWaiting {
				haveTenantTokens := tenantTokensNanos[tenant] > 0
				canBeUncontrolled := tenantTokensNanos[tenant] > (3*tenantBurstTokensNanos)/4
				haveTokens := tokens > 0
				if haveTenantTokens && (haveTokens || canBeUncontrolled) {
					tokensUsed[tenant] += workInitialTokens
					started = append(started, work{tenant: tenant, start: i})
					tokens -= workInitialTokens
					tenantTokensNanos[tenant] -= int64(workInitialTokens) * 1e6
					if !haveTokens {
						uncontrolledTokens += workInitialTokens
						fmt.Printf("%d: arrived-admitted uncontrolled tenant %d\n", i, tenant)
					} else {
						fmt.Printf("%d: arrived-admitted tenant %d\n", i, tenant)
					}
					waitTimes[tenant].count++
				} else {
					smallTenantWaiting[tenant] = append(smallTenantWaiting[tenant], waitingWork{startWait: i})
					fmt.Printf("%d: arrived-queued tenant %d\n", i, tenant)
				}
			}
		}
	}
	sumTokensUsed := 0
	for j := range tokensUsed {
		sumTokensUsed += tokensUsed[j]
	}
	fmt.Printf("rate %.2f\n", float64(sumTokensUsed)/float64(simulateIntervalMillis))
	fmt.Printf("tenant fractions:")
	for j := range tokensUsed {
		fmt.Printf(" %d:%.2f", j, float64(tokensUsed[j])/float64(sumTokensUsed))
	}
	fmt.Printf("\n")
	fmt.Printf("wait times:")
	for j := range waitTimes {
		fmt.Printf(" %d:%.2fms", j, float64(waitTimes[j].waitTime)/float64(waitTimes[j].count))
	}
	fmt.Printf("\n")
}
