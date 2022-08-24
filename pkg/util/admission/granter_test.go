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
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// TestGranterBasic is a datadriven test with the following commands:
//
// init-grant-coordinator min-cpu=<int> max-cpu=<int> sql-kv-tokens=<int>
// sql-sql-tokens=<int> sql-leaf=<int> sql-root=<int>
// enabled-soft-slot-granting=<bool>
// set-has-waiting-requests work=<kind> v=<true|false>
// set-return-value-from-granted work=<kind> v=<int>
// try-get work=<kind> [v=<int>]
// return-grant work=<kind> [v=<int>]
// took-without-permission work=<kind> [v=<int>]
// continue-grant-chain work=<kind>
// cpu-load runnable=<int> procs=<int> [infrequent=<bool>] [clamp=<int>]
// init-store-grant-coordinator
// set-io-tokens tokens=<int>
// try-get-soft-slots slots=<int>
// return-soft-slots slots=<int>
func TestGranterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	// requesters[numWorkKinds] is used for kv elastic work, when working with a
	// store grant coordinator.
	var requesters [numWorkKinds + 1]*testRequester
	var coord *GrantCoordinator
	var ssg *SoftSlotGranter
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
	KVSlotAdjusterOverloadThreshold.Override(context.Background(), &settings.SV, 1)
	datadriven.RunTest(t, testutils.TestDataPath(t, "granter"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init-grant-coordinator":
			clearRequesterAndCoord()
			var opts Options
			opts.Settings = settings
			d.ScanArgs(t, "min-cpu", &opts.MinCPUSlots)
			d.ScanArgs(t, "max-cpu", &opts.MaxCPUSlots)
			var burstTokens int
			d.ScanArgs(t, "sql-kv-tokens", &burstTokens)
			opts.SQLKVResponseBurstTokens = int64(burstTokens)
			d.ScanArgs(t, "sql-sql-tokens", &burstTokens)
			opts.SQLSQLResponseBurstTokens = int64(burstTokens)
			d.ScanArgs(t, "sql-leaf", &opts.SQLStatementLeafStartWorkSlots)
			d.ScanArgs(t, "sql-root", &opts.SQLStatementRootStartWorkSlots)
			opts.makeRequesterFunc = func(
				_ log.AmbientContext, workKind WorkKind, granter granter, _ *cluster.Settings,
				opts workQueueOptions) requester {
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
			opts.RunnableAlphaOverride = 1 // This gives weight to only the most recent sample.
			coords, _ := NewGrantCoordinators(ambientCtx, opts)
			defer coords.Close()
			coord = coords.Regular
			var err error
			ssg, err = MakeSoftSlotGranter(coord)
			require.NoError(t, err)
			if d.HasArg("enabled-soft-slot-granting") {
				var enabledSoftSlotGranting bool
				d.ScanArgs(t, "enabled-soft-slot-granting", &enabledSoftSlotGranting)
				if !enabledSoftSlotGranting {
					EnabledSoftSlotGranting.Override(context.Background(), &settings.SV, false)
				}
			}

			return flushAndReset()

		case "init-store-grant-coordinator":
			clearRequesterAndCoord()
			metrics := makeGrantCoordinatorMetrics()
			storeCoordinators := &StoreGrantCoordinators{
				settings: settings,
				makeStoreRequesterFunc: func(
					ambientCtx log.AmbientContext, granters [numWorkClasses]granterWithStoreWriteDone,
					settings *cluster.Settings, opts workQueueOptions) storeRequester {
					makeTestRequester := func(wc workClass) *testRequester {
						req := &testRequester{
							workKind:               KVWork,
							granter:                granters[wc],
							usesTokens:             true,
							buf:                    &buf,
							returnValueFromGranted: 0,
						}
						switch wc {
						case regularWorkClass:
							req.additionalID = "-regular"
						case elasticWorkClass:
							req.additionalID = "-elastic"
						}
						return req
					}
					req := &storeTestRequester{}
					req.requesters[regularWorkClass] = makeTestRequester(regularWorkClass)
					req.requesters[elasticWorkClass] = makeTestRequester(elasticWorkClass)
					requesters[KVWork] = req.requesters[regularWorkClass]
					requesters[numWorkKinds] = req.requesters[elasticWorkClass]
					return req
				},
				kvIOTokensExhaustedDuration: metrics.KVIOTokensExhaustedDuration,
				workQueueMetrics:            makeWorkQueueMetrics(""),
				disableTickerForTesting:     true,
			}
			var metricsProvider testMetricsProvider
			metricsProvider.setMetricsForStores([]int32{1}, pebble.Metrics{})
			storeCoordinators.SetPebbleMetricsProvider(context.Background(), &metricsProvider, &metricsProvider)
			unsafeGranter, ok := storeCoordinators.gcMap.Load(int64(1))
			require.True(t, ok)
			coord = (*GrantCoordinator)(unsafeGranter)
			kvStoreGranter := coord.granters[KVWork].(*kvStoreTokenGranter)
			// Use the same model for all 3 kinds of models.
			tlm := tokensLinearModel{multiplier: 0.5, constant: 50}
			coord.mu.Lock()
			kvStoreGranter.setAdmittedDoneModelsLocked(tlm, tlm, tlm)
			coord.mu.Unlock()
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
			if d.HasArg("clamp") {
				var clamp int
				d.ScanArgs(t, "clamp", &clamp)
				kvsa := coord.cpuLoadListener.(*kvSlotAdjuster)
				kvsa.setModerateSlotsClamp(clamp)
			}

			samplePeriod := time.Millisecond
			if infrequent {
				samplePeriod = 250 * time.Millisecond
			}
			coord.CPULoad(runnable, procs, samplePeriod)
			return flushAndReset()

		case "set-io-tokens":
			var tokens int
			d.ScanArgs(t, "tokens", &tokens)
			// We are not using a real ioLoadListener, and simply setting the
			// tokens (the ioLoadListener has its own test).
			coord.mu.Lock()
			coord.granters[KVWork].(*kvStoreTokenGranter).setAvailableIOTokensLocked(int64(tokens))
			coord.mu.Unlock()
			coord.testingTryGrant()
			return flushAndReset()

		case "set-elastic-disk-bw-tokens":
			var tokens int
			d.ScanArgs(t, "tokens", &tokens)
			// We are not using a real ioLoadListener, and simply setting the
			// tokens (the ioLoadListener has its own test).
			coord.mu.Lock()
			coord.granters[KVWork].(*kvStoreTokenGranter).setAvailableElasticDiskBandwidthTokensLocked(
				int64(tokens))
			coord.mu.Unlock()
			coord.testingTryGrant()
			return flushAndReset()

		case "store-write-done":
			var origTokens, writeBytes int
			d.ScanArgs(t, "orig-tokens", &origTokens)
			d.ScanArgs(t, "write-bytes", &writeBytes)
			requesters[scanWorkKind(t, d)].granter.(granterWithStoreWriteDone).storeWriteDone(
				int64(origTokens), StoreWorkDoneInfo{WriteBytes: int64(writeBytes)})
			coord.testingTryGrant()
			return flushAndReset()

		case "try-get-soft-slots":
			var slots int
			d.ScanArgs(t, "slots", &slots)
			granted := ssg.TryGetSlots(slots)
			fmt.Fprintf(&buf, "requested: %d, granted: %d\n", slots, granted)
			return flushAndReset()

		case "return-soft-slots":
			var slots int
			d.ScanArgs(t, "slots", &slots)
			ssg.ReturnSlots(slots)
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
	// All the KVWork requesters. The first one is for all KVWork and the
	// remaining are the per-store ones.
	var requesters []*testRequester
	makeRequesterFunc := func(
		_ log.AmbientContext, workKind WorkKind, granter granter, _ *cluster.Settings,
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
		Settings:          settings,
		makeRequesterFunc: makeRequesterFunc,
		makeStoreRequesterFunc: func(
			ctx log.AmbientContext, granters [numWorkClasses]granterWithStoreWriteDone,
			settings *cluster.Settings, opts workQueueOptions) storeRequester {
			reqReg := makeRequesterFunc(ctx, KVWork, granters[regularWorkClass], settings, opts)
			reqElastic := makeRequesterFunc(ctx, KVWork, granters[elasticWorkClass], settings, opts)
			str := &storeTestRequester{}
			str.requesters[regularWorkClass] = reqReg.(*testRequester)
			str.requesters[regularWorkClass].additionalID = "-regular"
			str.requesters[elasticWorkClass] = reqElastic.(*testRequester)
			str.requesters[elasticWorkClass].additionalID = "-elastic"
			return str
		},
	}
	coords, _ := NewGrantCoordinators(ambientCtx, opts)
	// There is only 1 KVWork requester at this point in initialization, for the
	// Regular GrantCoordinator.
	require.Equal(t, 1, len(requesters))
	storeCoords := coords.Stores
	metrics := pebble.Metrics{}
	mp := testMetricsProvider{}
	mp.setMetricsForStores([]int32{10, 20}, metrics)
	// Setting the metrics provider will cause the initialization of two
	// GrantCoordinators for the two stores.
	storeCoords.SetPebbleMetricsProvider(context.Background(), &mp, &mp)
	// Now we have 1+2*2 = 5 KVWork requesters.
	require.Equal(t, 5, len(requesters))
	// Confirm that the store IDs are as expected.
	var actualStores []int32

	storeCoords.gcMap.Range(func(s int64, _ unsafe.Pointer) bool {
		// The int32 conversion is lossless since we only store int32s in the
		// gcMap.
		actualStores = append(actualStores, int32(s))
		// true indicates that iteration should continue after the
		// current entry has been processed.
		return true
	})
	sort.Slice(actualStores, func(i, j int) bool { return actualStores[i] < actualStores[j] })
	require.Equal(t, []int32{10, 20}, actualStores)
	// Do tryGet on all requesters. The requester for the Regular
	// GrantCoordinator will return false since it has 0 CPU slots. We are
	// interested in the other ones, which have unlimited slots at this point in
	// time, so will return true.
	for i := range requesters {
		requesters[i].tryGet(1)
	}
	require.Equal(t,
		"kv: tryGet(1) returned false\n"+
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
		workKindString(tr.workKind), tr.additionalID,
		grantChainID, tr.returnValueFromGranted)
	tr.grantChainID = grantChainID
	return tr.returnValueFromGranted
}

func (tr *testRequester) close() {}

func (tr *testRequester) tryGet(count int64) {
	rv := tr.granter.tryGet(count)
	fmt.Fprintf(tr.buf, "%s%s: tryGet(%d) returned %t\n", workKindString(tr.workKind),
		tr.additionalID, count, rv)
}

func (tr *testRequester) returnGrant(count int64) {
	fmt.Fprintf(tr.buf, "%s%s: returnGrant(%d)\n", workKindString(tr.workKind), tr.additionalID,
		count)
	tr.granter.returnGrant(count)
}

func (tr *testRequester) tookWithoutPermission(count int64) {
	fmt.Fprintf(tr.buf, "%s%s: tookWithoutPermission(%d)\n", workKindString(tr.workKind),
		tr.additionalID, count)
	tr.granter.tookWithoutPermission(count)
}

func (tr *testRequester) continueGrantChain() {
	fmt.Fprintf(tr.buf, "%s%s: continueGrantChain\n", workKindString(tr.workKind),
		tr.additionalID)
	tr.granter.continueGrantChain(tr.grantChainID)
}

type storeTestRequester struct {
	requesters [numWorkClasses]*testRequester
}

var _ storeRequester = &storeTestRequester{}

func (str *storeTestRequester) getRequesters() [numWorkClasses]requester {
	var rv [numWorkClasses]requester
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

// setModerateSlotsClamp is used in testing to force a value for kvsa.moderateSlotsClamp.
func (kvsa *kvSlotAdjuster) setModerateSlotsClamp(val int) {
	kvsa.moderateSlotsClampOverride = val
	kvsa.moderateSlotsClamp = val
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
	case "sql-leaf-start":
		return int8(SQLStatementLeafStartWork)
	case "sql-root-start":
		return int8(SQLStatementRootStartWork)
	case "kv-elastic":
		return int8(numWorkKinds)
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

func (m *testMetricsProvider) UpdateIOThreshold(
	id roachpb.StoreID, threshold *admissionpb.IOThreshold,
) {
}

func (m *testMetricsProvider) setMetricsForStores(stores []int32, metrics pebble.Metrics) {
	m.metrics = m.metrics[:0]
	for _, s := range stores {
		m.metrics = append(m.metrics, StoreMetrics{
			StoreID: s,
			Metrics: &metrics,
		})
	}
}
