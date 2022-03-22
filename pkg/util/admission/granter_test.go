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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

type testRequester struct {
	workKind   WorkKind
	granter    granter
	usesTokens bool
	buf        *strings.Builder

	waitingRequests        bool
	returnFalseFromGranted bool
	grantChainID           grantChainID
}

func (tr *testRequester) hasWaitingRequests() bool {
	return tr.waitingRequests
}

func (tr *testRequester) granted(grantChainID grantChainID) bool {
	fmt.Fprintf(tr.buf, "%s: granted in chain %d, and returning %t\n", workKindString(tr.workKind),
		grantChainID, !tr.returnFalseFromGranted)
	tr.grantChainID = grantChainID
	return !tr.returnFalseFromGranted
}

func (tr *testRequester) tryGet() {
	rv := tr.granter.tryGet()
	fmt.Fprintf(tr.buf, "%s: tryGet returned %t\n", workKindString(tr.workKind), rv)
}

func (tr *testRequester) returnGrant() {
	fmt.Fprintf(tr.buf, "%s: returnGrant\n", workKindString(tr.workKind))
	tr.granter.returnGrant()
}

func (tr *testRequester) tookWithoutPermission() {
	fmt.Fprintf(tr.buf, "%s: tookWithoutPermission\n", workKindString(tr.workKind))
	tr.granter.tookWithoutPermission()
}

func (tr *testRequester) continueGrantChain() {
	fmt.Fprintf(tr.buf, "%s: continueGrantChain\n", workKindString(tr.workKind))
	tr.granter.continueGrantChain(tr.grantChainID)
}

func (tr *testRequester) getAdmittedCount() uint64 {
	// Only used by ioLoadListener, so don't bother.
	return 0
}

// TestGranterBasic is a datadriven test with the following commands:
//
// init-grant-coordinator min-cpu=<int> max-cpu=<int> sql-kv-tokens=<int>
//   sql-sql-tokens=<int> sql-leaf=<int> sql-root=<int>
// set-has-waiting-requests work=<kind> v=<true|false>
// set-return-value-from-granted work=<kind> v=<true|false>
// try-get work=<kind>
// return-grant work=<kind>
// took-without-permission work=<kind>
// continue-grant-chain work=<kind>
// cpu-load runnable=<int> procs=<int> [infrequent=<bool>]
// set-io-tokens tokens=<int>
func TestGranterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	var requesters [numWorkKinds]*testRequester
	var coord *GrantCoordinator
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
			var opts Options
			opts.Settings = settings
			d.ScanArgs(t, "min-cpu", &opts.MinCPUSlots)
			d.ScanArgs(t, "max-cpu", &opts.MaxCPUSlots)
			d.ScanArgs(t, "sql-kv-tokens", &opts.SQLKVResponseBurstTokens)
			d.ScanArgs(t, "sql-sql-tokens", &opts.SQLSQLResponseBurstTokens)
			d.ScanArgs(t, "sql-leaf", &opts.SQLStatementLeafStartWorkSlots)
			d.ScanArgs(t, "sql-root", &opts.SQLStatementRootStartWorkSlots)
			opts.makeRequesterFunc = func(
				_ log.AmbientContext, workKind WorkKind, granter granter, _ *cluster.Settings,
				opts workQueueOptions) requester {
				req := &testRequester{
					workKind:   workKind,
					granter:    granter,
					usesTokens: opts.usesTokens,
					buf:        &buf,
				}
				requesters[workKind] = req
				return req
			}
			delayForGrantChainTermination = 0
			coords, _ := NewGrantCoordinators(ambientCtx, opts)
			coord = coords.Regular
			return flushAndReset()

		case "set-has-waiting-requests":
			var v bool
			d.ScanArgs(t, "v", &v)
			requesters[scanWorkKind(t, d)].waitingRequests = v
			return flushAndReset()

		case "set-return-value-from-granted":
			var v bool
			d.ScanArgs(t, "v", &v)
			requesters[scanWorkKind(t, d)].returnFalseFromGranted = !v
			return flushAndReset()

		case "try-get":
			requesters[scanWorkKind(t, d)].tryGet()
			return flushAndReset()

		case "return-grant":
			requesters[scanWorkKind(t, d)].returnGrant()
			return flushAndReset()

		case "took-without-permission":
			requesters[scanWorkKind(t, d)].tookWithoutPermission()
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
			return flushAndReset()

		case "set-io-tokens":
			var tokens int
			d.ScanArgs(t, "tokens", &tokens)
			// We are not using a real ioLoadListener, and simply setting the
			// tokens (the ioLoadListener has its own test).
			coord.mu.Lock()
			coord.granters[KVWork].(*kvGranter).setAvailableIOTokensLocked(int64(tokens))
			coord.mu.Unlock()
			coord.testingTryGrant()
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
	opts := Options{
		Settings: settings,
		makeRequesterFunc: func(
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
	storeCoords.SetPebbleMetricsProvider(context.Background(), &mp)
	// Now we have 1+2 = 3 KVWork requesters.
	require.Equal(t, 3, len(requesters))
	// Confirm that the store IDs are as expected.
	var actualStores []int32
	for s := range storeCoords.gcMap {
		actualStores = append(actualStores, s)
	}
	sort.Slice(actualStores, func(i, j int) bool { return actualStores[i] < actualStores[j] })
	require.Equal(t, []int32{10, 20}, actualStores)
	// Do tryGet on all requesters. The requester for the Regular
	// GrantCoordinator will return false since it has 0 CPU slots. We are
	// interested in the other ones, which have unlimited slots at this point in
	// time, so will return true.
	for i := range requesters {
		requesters[i].tryGet()
	}
	require.Equal(t,
		"kv: tryGet returned false\nkv: tryGet returned true\nkv: tryGet returned true\n",
		buf.String())
	coords.Close()
}

type testRequesterForIOLL struct {
	admittedCount uint64
}

func (r *testRequesterForIOLL) hasWaitingRequests() bool {
	panic("unimplemented")
}

func (r *testRequesterForIOLL) granted(grantChainID grantChainID) bool {
	panic("unimplemented")
}

func (r *testRequesterForIOLL) getAdmittedCount() uint64 {
	return r.admittedCount
}

type testGranterWithIOTokens struct {
	buf strings.Builder
}

func (g *testGranterWithIOTokens) setAvailableIOTokensLocked(tokens int64) {
	fmt.Fprintf(&g.buf, "setAvailableIOTokens: %s", tokensFor1sToString(tokens))
}

func tokensForIntervalToString(tokens int64) string {
	if tokens == unlimitedTokens {
		return "unlimited"
	}
	return fmt.Sprintf("%d", tokens)
}

func tokensFor1sToString(tokens int64) string {
	if tokens >= unlimitedTokens/adjustmentInterval {
		return "unlimited"
	}
	return fmt.Sprintf("%d", tokens)
}

// TestIOLoadListener is a datadriven test with the following command that
// sets the state for token calculation and then ticks adjustmentInterval
// times to cause tokens to be set in the testGranterWithIOTokens:
// set-state admitted=<int> l0-bytes=<int> l0-added=<int> l0-files=<int> l0-sublevels=<int>
func TestIOLoadListener(t *testing.T) {
	req := &testRequesterForIOLL{}
	kvGranter := &testGranterWithIOTokens{}
	var ioll *ioLoadListener
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	datadriven.RunTest(t, testutils.TestDataPath(t, "io_load_listener"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "set-state":
				// Setup state used as input for token adjustment.
				var admitted uint64
				d.ScanArgs(t, "admitted", &admitted)
				req.admittedCount = admitted
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
				if ioll == nil {
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
				}
				ioll.pebbleMetricsTick(ctx, metrics)
				// Do the ticks until just before next adjustment.
				var buf strings.Builder
				fmt.Fprintf(&buf, "admitted: %d, bytes: %d, added-bytes: %d,\nsmoothed-removed: %d, "+
					"smoothed-admit: %d,\ntokens: %s, tokens-allocated: %s\n", ioll.admittedCount,
					ioll.l0Bytes, ioll.l0AddedBytes, ioll.smoothedBytesRemoved,
					int64(ioll.smoothedNumAdmit), tokensForIntervalToString(ioll.totalTokens),
					tokensFor1sToString(ioll.tokensAllocated))
				for i := 0; i < adjustmentInterval; i++ {
					ioll.allocateTokensTick()
					fmt.Fprintf(&buf, "tick: %d, %s\n", i, kvGranter.buf.String())
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
	// Bug 1: overflow when totalTokens is too large.
	for i := int64(0); i < adjustmentInterval; i++ {
		// Override the totalTokens manually to trigger the overflow bug.
		ioll.totalTokens = math.MaxInt64 - i
		ioll.tokensAllocated = 0
		for j := 0; j < adjustmentInterval; j++ {
			ioll.allocateTokensTick()
		}
	}
	// Bug2: overflow when bytes added delta is 0.
	m := pebble.Metrics{}
	m.Levels[0] = pebble.LevelMetrics{
		Sublevels: 100,
		NumFiles:  10000,
	}
	ioll.pebbleMetricsTick(ctx, m)
	ioll.pebbleMetricsTick(ctx, m)
	ioll.allocateTokensTick()
}

type testGranterNonNegativeTokens struct {
	t *testing.T
}

func (g *testGranterNonNegativeTokens) setAvailableIOTokensLocked(tokens int64) {
	require.LessOrEqual(g.t, int64(0), tokens)
}

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
		req.admittedCount = rand.Uint64()
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
		ioll.pebbleMetricsTick(ctx, m)
		for j := 0; j < adjustmentInterval; j++ {
			ioll.allocateTokensTick()
			require.LessOrEqual(t, int64(0), ioll.totalTokens)
			require.LessOrEqual(t, int64(0), ioll.tokensAllocated)
		}
	}
}

// TODO(sumeer):
// - Test metrics
// - Test GrantCoordinator with multi-tenant configurations
