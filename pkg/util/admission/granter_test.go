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
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

type testRequester struct {
	workKind   WorkKind
	granter    granter
	usesTokens bool
	buf        *strings.Builder

	waitingRequests        bool
	returnValueFromGranted int64
	grantChainID           grantChainID
}

var _ requester = &testRequester{}
var _ storeRequester = &testRequester{}

func (tr *testRequester) hasWaitingRequests() bool {
	return tr.waitingRequests
}

func (tr *testRequester) granted(grantChainID grantChainID) int64 {
	fmt.Fprintf(tr.buf, "%s: granted in chain %d, and returning %d\n", workKindString(tr.workKind),
		grantChainID, tr.returnValueFromGranted)
	tr.grantChainID = grantChainID
	return tr.returnValueFromGranted
}

func (tr *testRequester) close() {}

func (tr *testRequester) tryGet(count int64) {
	rv := tr.granter.tryGet(count)
	fmt.Fprintf(tr.buf, "%s: tryGet(%d) returned %t\n", workKindString(tr.workKind), count, rv)
}

func (tr *testRequester) returnGrant(count int64) {
	fmt.Fprintf(tr.buf, "%s: returnGrant(%d)\n", workKindString(tr.workKind), count)
	tr.granter.returnGrant(count)
}

func (tr *testRequester) tookWithoutPermission(count int64) {
	fmt.Fprintf(tr.buf, "%s: tookWithoutPermission(%d)\n", workKindString(tr.workKind), count)
	tr.granter.tookWithoutPermission(count)
}

func (tr *testRequester) continueGrantChain() {
	fmt.Fprintf(tr.buf, "%s: continueGrantChain\n", workKindString(tr.workKind))
	tr.granter.continueGrantChain(tr.grantChainID)
}

func (tr *testRequester) getStoreAdmissionStats() storeAdmissionStats {
	// Only used by ioLoadListener, so don't bother.
	return storeAdmissionStats{}
}

func (tr *testRequester) setStoreRequestEstimates(estimates storeRequestEstimates) {
	// Only used by ioLoadListener, so don't bother.
}

// setModerateSlotsClamp is used in testing to force a value for kvsa.moderateSlotsClamp.
func (kvsa *kvSlotAdjuster) setModerateSlotsClamp(val int) {
	kvsa.moderateSlotsClampOverride = val
	kvsa.moderateSlotsClamp = val
}

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
	var requesters [numWorkKinds]*testRequester
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
			metrics := makeGranterMetrics()
			storeCoordinators := &StoreGrantCoordinators{
				settings: settings,
				makeStoreRequesterFunc: func(
					ambientCtx log.AmbientContext, granter granter, settings *cluster.Settings,
					opts workQueueOptions) storeRequester {
					req := &testRequester{
						workKind:               KVWork,
						granter:                granter,
						usesTokens:             true,
						buf:                    &buf,
						returnValueFromGranted: 0,
					}
					requesters[KVWork] = req
					return req
				},
				kvIOTokensExhaustedDuration: metrics.KVIOTokensExhaustedDuration,
				workQueueMetrics:            makeWorkQueueMetrics(""),
				disableTickerForTesting:     true,
			}
			var testMetricsProvider testMetricsProvider
			testMetricsProvider.setMetricsForStores([]int32{1}, pebble.Metrics{})
			storeCoordinators.SetPebbleMetricsProvider(context.Background(), &testMetricsProvider, &testMetricsProvider)
			unsafeGranter, ok := storeCoordinators.gcMap.Load(int64(1))
			require.True(t, ok)
			coord = (*GrantCoordinator)(unsafeGranter)
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

func scanWorkKind(t *testing.T, d *datadriven.TestData) WorkKind {
	var kindStr string
	d.ScanArgs(t, "work", &kindStr)
	switch kindStr {
	case "kv":
		return KVWork
	case "sql-kv-response":
		return SQLKVResponseWork
	case "sql-sql-response":
		return SQLSQLResponseWork
	case "sql-leaf-start":
		return SQLStatementLeafStartWork
	case "sql-root-start":
		return SQLStatementRootStartWork
	}
	panic("unknown WorkKind")
}

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
			ctx log.AmbientContext, granter granter, settings *cluster.Settings,
			opts workQueueOptions) storeRequester {
			req := makeRequesterFunc(ctx, KVWork, granter, settings, opts)
			return req.(*testRequester)
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
	// Now we have 1+2 = 3 KVWork requesters.
	require.Equal(t, 3, len(requesters))
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
		"kv: tryGet(1) returned false\nkv: tryGet(1) returned true\nkv: tryGet(1) returned true\n",
		buf.String())
	coords.Close()
}

type testRequesterForIOLL struct {
	stats storeAdmissionStats
	buf   strings.Builder
}

var _ storeRequester = &testRequesterForIOLL{}

func (r *testRequesterForIOLL) hasWaitingRequests() bool {
	panic("unimplemented")
}

func (r *testRequesterForIOLL) granted(grantChainID grantChainID) int64 {
	panic("unimplemented")
}

func (r *testRequesterForIOLL) close() {}

func (r *testRequesterForIOLL) getStoreAdmissionStats() storeAdmissionStats {
	return r.stats
}

func (r *testRequesterForIOLL) setStoreRequestEstimates(estimates storeRequestEstimates) {
	fmt.Fprintf(&r.buf,
		"store-request-estimates: fractionOfIngestIntoL0: %.2f, workByteAddition: %d",
		estimates.fractionOfIngestIntoL0, estimates.workByteAddition)
}

type testGranterWithIOTokens struct {
	buf           strings.Builder
	allTokensUsed bool
}

func (g *testGranterWithIOTokens) setAvailableIOTokensLocked(tokens int64) (tokensUsed int64) {
	fmt.Fprintf(&g.buf, "setAvailableIOTokens: %s", tokensForTokenTickDurationToString(tokens))
	if g.allTokensUsed {
		return tokens * 2
	}
	return 0
}

func tokensForTokenTickDurationToString(tokens int64) string {
	if tokens >= unlimitedTokens/ticksInAdjustmentInterval {
		return "unlimited"
	}
	return fmt.Sprintf("%d", tokens)
}

type rawTokenResult adjustTokensResult

// TestIOLoadListener is a datadriven test with the following command that
// sets the state for token calculation and then ticks adjustmentInterval
// times to cause tokens to be set in the testGranterWithIOTokens:
// set-state admitted=<int> l0-bytes=<int> l0-added=<int> l0-files=<int> l0-sublevels=<int> ...
func TestIOLoadListener(t *testing.T) {
	req := &testRequesterForIOLL{}
	kvGranter := &testGranterWithIOTokens{}
	var ioll *ioLoadListener
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	datadriven.RunTest(t, testutils.TestDataPath(t, "io_load_listener"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				ioll = &ioLoadListener{
					settings:    st,
					kvRequester: req,
				}
				// The mutex is needed by ioLoadListener but is not useful in this
				// test -- the channels provide synchronization and prevent this
				// test code and the ioLoadListener from being concurrently
				// active.
				ioll.mu.Mutex = &syncutil.Mutex{}
				ioll.mu.kvGranter = kvGranter
				return ""

			case "prep-admission-stats":
				req.stats = storeAdmissionStats{
					admittedCount:            0,
					admittedWithBytesCount:   0,
					admittedAccountedBytes:   0,
					ingestedAccountedBytes:   0,
					ingestedAccountedL0Bytes: 0,
				}
				d.ScanArgs(t, "admitted", &req.stats.admittedCount)
				if d.HasArg("admitted-bytes") {
					d.ScanArgs(t, "admitted-bytes", &req.stats.admittedAccountedBytes)
				}
				if d.HasArg("ingested-bytes") {
					d.ScanArgs(t, "ingested-bytes", &req.stats.ingestedAccountedBytes)
				}
				if d.HasArg("ingested-into-l0") {
					d.ScanArgs(t, "ingested-into-l0", &req.stats.ingestedAccountedL0Bytes)
				}
				return fmt.Sprintf("%+v", req.stats)

			case "set-min-flush-util":
				var percent int
				d.ScanArgs(t, "percent", &percent)
				MinFlushUtilizationFraction.Override(ctx, &st.SV, float64(percent)/100)
				return ""

			case "set-state":
				// Setup state used as input for token adjustment.
				var metrics pebble.Metrics
				var l0Bytes uint64
				d.ScanArgs(t, "l0-bytes", &l0Bytes)
				metrics.Levels[0].Size = int64(l0Bytes)
				var l0Added uint64
				d.ScanArgs(t, "l0-added", &l0Added)
				metrics.Levels[0].BytesIngested = l0Added / 2
				metrics.Levels[0].BytesFlushed = l0Added - metrics.Levels[0].BytesIngested
				var l0Files int
				d.ScanArgs(t, "l0-files", &l0Files)
				metrics.Levels[0].NumFiles = int64(l0Files)
				var l0SubLevels int
				d.ScanArgs(t, "l0-sublevels", &l0SubLevels)
				metrics.Levels[0].Sublevels = int32(l0SubLevels)
				var flushBytes, flushWorkSec, flushIdleSec int
				if d.HasArg("flush-bytes") {
					d.ScanArgs(t, "flush-bytes", &flushBytes)
					d.ScanArgs(t, "flush-work-sec", &flushWorkSec)
					d.ScanArgs(t, "flush-idle-sec", &flushIdleSec)
				}
				flushMetric := pebble.ThroughputMetric{
					Bytes:        int64(flushBytes),
					WorkDuration: time.Duration(flushWorkSec) * time.Second,
					IdleDuration: time.Duration(flushIdleSec) * time.Second,
				}
				im := &pebble.InternalIntervalMetrics{}
				im.Flush.WriteThroughput = flushMetric
				var writeStallCount int
				if d.HasArg("write-stall-count") {
					d.ScanArgs(t, "write-stall-count", &writeStallCount)
				}
				var allTokensUsed bool
				if d.HasArg("all-tokens-used") {
					d.ScanArgs(t, "all-tokens-used", &allTokensUsed)
				}
				kvGranter.allTokensUsed = allTokensUsed
				var printOnlyFirstTick bool
				if d.HasArg("print-only-first-tick") {
					d.ScanArgs(t, "print-only-first-tick", &printOnlyFirstTick)
				}
				ioll.pebbleMetricsTick(ctx, StoreMetrics{
					Metrics:                 &metrics,
					WriteStallCount:         int64(writeStallCount),
					InternalIntervalMetrics: im,
				})
				var buf strings.Builder
				// Do the ticks until just before next adjustment.
				res := ioll.adjustTokensResult
				fmt.Fprintln(&buf, redact.StringWithoutMarkers(&res))
				res.ioThreshold = nil // avoid nondeterminism
				fmt.Fprintf(&buf, "%+v\n", (rawTokenResult)(res))
				if req.buf.Len() > 0 {
					fmt.Fprintf(&buf, "%s\n", req.buf.String())
					req.buf.Reset()
				}
				for i := 0; i < ticksInAdjustmentInterval; i++ {
					ioll.allocateTokensTick()
					if i == 0 || !printOnlyFirstTick {
						fmt.Fprintf(&buf, "tick: %d, %s\n", i, kvGranter.buf.String())
					}
					kvGranter.buf.Reset()
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestIOLoadListenerOverflow(t *testing.T) {
	req := &testRequesterForIOLL{}
	kvGranter := &testGranterWithIOTokens{}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	ioll := ioLoadListener{
		settings:    st,
		kvRequester: req,
	}
	ioll.mu.Mutex = &syncutil.Mutex{}
	ioll.mu.kvGranter = kvGranter
	// Bug 1: overflow when totalNumByteTokens is too large.
	for i := int64(0); i < adjustmentInterval; i++ {
		// Override the totalNumByteTokens manually to trigger the overflow bug.
		ioll.totalNumByteTokens = math.MaxInt64 - i
		ioll.tokensAllocated = 0
		for j := 0; j < ticksInAdjustmentInterval; j++ {
			ioll.allocateTokensTick()
		}
	}
	// Bug2: overflow when bytes added delta is 0.
	m := pebble.Metrics{}
	m.Levels[0] = pebble.LevelMetrics{
		Sublevels: 100,
		NumFiles:  10000,
	}
	ioll.pebbleMetricsTick(ctx,
		StoreMetrics{Metrics: &m, InternalIntervalMetrics: &pebble.InternalIntervalMetrics{}})
	ioll.pebbleMetricsTick(ctx,
		StoreMetrics{Metrics: &m, InternalIntervalMetrics: &pebble.InternalIntervalMetrics{}})
	ioll.allocateTokensTick()
}

type testGranterNonNegativeTokens struct {
	t *testing.T
}

func (g *testGranterNonNegativeTokens) setAvailableIOTokensLocked(tokens int64) (tokensUsed int64) {
	require.LessOrEqual(g.t, int64(0), tokens)
	return 0
}

func TestAdjustTokensInnerAndLogging(t *testing.T) {
	const mb = 12 + 1<<20
	prevAdmStats := storeAdmissionStats{
		admittedCount:            100,
		admittedWithBytesCount:   80,
		admittedAccountedBytes:   420 * mb,
		ingestedAccountedBytes:   200 * mb,
		ingestedAccountedL0Bytes: 100 * mb,
	}
	admStats := prevAdmStats
	admStats.admittedCount = 721
	admStats.admittedWithBytesCount += 123
	admStats.admittedAccountedBytes += 371 * mb
	admStats.ingestedAccountedBytes += 193 * mb
	admStats.ingestedAccountedL0Bytes += 73 * mb
	tests := []struct {
		name           redact.SafeString
		prev           ioLoadListenerState
		admissionStats storeAdmissionStats
		l0Metrics      pebble.LevelMetrics
	}{
		{
			name: "zero",
		},
		{
			name: "real-numbers",
			prev: ioLoadListenerState{
				cumAdmissionStats:                           prevAdmStats,
				cumL0AddedBytes:                             1402 * mb,
				curL0Bytes:                                  400 * mb,
				smoothedIntL0CompactedBytes:                 47 * mb,
				smoothedIntPerWorkUnaccountedL0Bytes:        2204, // 2kb
				smoothedIntIngestedAccountedL0BytesFraction: 0.3,
				smoothedCompactionByteTokens:                201 * mb,
				totalNumByteTokens:                          int64(201 * mb),
			},
			admissionStats: admStats,
			l0Metrics: pebble.LevelMetrics{
				Sublevels:     27,
				NumFiles:      195,
				Size:          900 * mb,
				BytesIngested: 1801 * mb,
				BytesFlushed:  178 * mb,
			},
		},
	}
	ctx := context.Background()
	var buf redact.StringBuilder
	for _, tt := range tests {
		buf.Printf("%s:\n", tt.name)
		res := (*ioLoadListener)(nil).adjustTokensInner(
			ctx, tt.prev, tt.admissionStats, tt.l0Metrics, 0,
			&pebble.InternalIntervalMetrics{}, 100, 10, 0.50)
		buf.Printf("%s\n", res)
	}
	echotest.Require(t, string(redact.Sprint(buf)), filepath.Join(testutils.TestDataPath(t, "format_adjust_tokens_stats.txt")))
}

// TODO(sumeer):
// - Test metrics
// - Test GrantCoordinator with multi-tenant configurations

// TestBadIOLoadListenerStats tests that bad stats (non-monotonic cumulative
// stats and negative values) don't cause panics or tokens to be negative.
func TestBadIOLoadListenerStats(t *testing.T) {
	var m pebble.Metrics
	req := &testRequesterForIOLL{}
	ctx := context.Background()

	randomValues := func() {
		// Use uints, and cast so that we get bad negative values.
		m.Levels[0].Sublevels = int32(rand.Uint32())
		m.Levels[0].NumFiles = int64(rand.Uint64())
		m.Levels[0].Size = int64(rand.Uint64())
		m.Levels[0].BytesFlushed = rand.Uint64()
		m.Levels[0].BytesIngested = rand.Uint64()
		req.stats.admittedCount = rand.Uint64()
		req.stats.admittedWithBytesCount = rand.Uint64()
		req.stats.admittedAccountedBytes = rand.Uint64()
		req.stats.ingestedAccountedBytes = rand.Uint64()
		req.stats.ingestedAccountedL0Bytes = rand.Uint64()
	}
	kvGranter := &testGranterNonNegativeTokens{t: t}
	st := cluster.MakeTestingClusterSettings()
	ioll := ioLoadListener{
		settings:    st,
		kvRequester: req,
	}
	ioll.mu.Mutex = &syncutil.Mutex{}
	ioll.mu.kvGranter = kvGranter
	for i := 0; i < 100; i++ {
		randomValues()
		ioll.pebbleMetricsTick(ctx, StoreMetrics{
			Metrics:                 &m,
			InternalIntervalMetrics: &pebble.InternalIntervalMetrics{},
		})
		for j := 0; j < ticksInAdjustmentInterval; j++ {
			ioll.allocateTokensTick()
			require.LessOrEqual(t, int64(0), ioll.smoothedIntL0CompactedBytes)
			require.LessOrEqual(t, float64(0), ioll.smoothedIntPerWorkUnaccountedL0Bytes)
			require.LessOrEqual(t, float64(0), ioll.smoothedIntIngestedAccountedL0BytesFraction)
			require.LessOrEqual(t, float64(0), ioll.smoothedCompactionByteTokens)
			require.LessOrEqual(t, int64(0), ioll.totalNumByteTokens)
			require.LessOrEqual(t, int64(0), ioll.tokensAllocated)
		}
	}
}
