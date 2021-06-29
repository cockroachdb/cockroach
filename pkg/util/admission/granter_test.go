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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
// cpu-load runnable=<int> procs=<int>
// set-io-tokens tokens=<int>
func TestGranterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	datadriven.RunTest(t, "testdata/granter", func(t *testing.T, d *datadriven.TestData) string {
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
				workKind WorkKind, granter granter, usesTokens bool, tiedToRange bool,
				_ *cluster.Settings) requester {
				req := &testRequester{
					workKind:   workKind,
					granter:    granter,
					usesTokens: usesTokens,
					buf:        &buf,
				}
				requesters[workKind] = req
				return req
			}
			delayForGrantChainTermination = 0
			coord, _ = NewGrantCoordinator(opts)
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
			coord.CPULoad(runnable, procs)
			return flushAndReset()

		case "set-io-tokens":
			var tokens int
			d.ScanArgs(t, "tokens", &tokens)
			// We are not using a real ioLoadListener, and simply setting the
			// tokens (the ioLoadListener has its own test).
			coord.mu.Lock()
			coord.granters[KVWork].(*kvGranter).setAvailableIOTokensLocked(int64(tokens))
			coord.mu.Unlock()
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

type testPebbleMetricsProvider struct {
	metrics []*pebble.Metrics
}

func (m *testPebbleMetricsProvider) GetPebbleMetrics() []*pebble.Metrics {
	return m.metrics
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
	buf         strings.Builder
	tokensSetCh chan struct{}
}

func (g *testGranterWithIOTokens) setAvailableIOTokensLocked(tokens int64) {
	fmt.Fprintf(&g.buf, "setAvailableIOTokens: %s", tokensFor1sToString(tokens))
	g.tokensSetCh <- struct{}{}
}

type testTicker struct {
	ch chan time.Time
}

func (tt *testTicker) tickChannel() <-chan time.Time {
	return tt.ch
}

func (tt *testTicker) stop() {
	close(tt.ch)
}

func tokensFor60sToString(tokens int64) string {
	if tokens == unlimitedTokens {
		return "unlimited"
	}
	return fmt.Sprintf("%d", tokens)
}

func tokensFor1sToString(tokens int64) string {
	// ioLoadListener works with floats, so we just approximate the unlimited
	// calculation here.
	if tokens >= (unlimitedTokens/60 - 5) {
		return "unlimited"
	}
	return fmt.Sprintf("%d", tokens)
}

// TestIOLoadListener is a datadriven test with the following command that
// sets the state for token calculation and then runs the timeTickerInterface 60 times to
// cause tokens to be set in the testGranterWithIOTokens:
// set-state admitted=<int> l0-bytes=<int> l0-added=<int> l0-files=<int> l0-sublevels=<int>
func TestIOLoadListener(t *testing.T) {
	pmp := &testPebbleMetricsProvider{}
	req := &testRequesterForIOLL{}
	kvGranter := &testGranterWithIOTokens{
		tokensSetCh: make(chan struct{}, 1),
	}
	tickerCh := make(chan time.Time)
	newTestTicker := func(d time.Duration) timeTickerInterface {
		require.Equal(t, time.Second, d)
		return &testTicker{ch: tickerCh}
	}
	var ioll *ioLoadListener

	datadriven.RunTest(t, "testdata/io_load_listener",
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
				pmp.metrics = []*pebble.Metrics{&metrics}

				created := false
				if ioll == nil {
					created = true
					ioll = &ioLoadListener{
						l0FileCountOverloadThreshold:     l0FileCountOverloadThreshold,
						l0SubLevelCountOverloadThreshold: l0SubLevelCountOverloadThreshold,
						pebbleMetricsProvider:            pmp,
						kvRequester:                      req,
						// The mutex is needed by ioLoadListener but is not useful in this
						// test -- the channels provide synchronization and prevent this
						// test code and the ioLoadListener from being concurrently
						// active.
						mu:        &syncutil.Mutex{},
						kvGranter: kvGranter,
					}
					ioll.start(newTestTicker)
				}
				// Do the ticks until just before next adjustment.
				var buf strings.Builder
				for i := 0; i < adjustmentInterval; i++ {
					if i != 0 || !created {
						tickerCh <- timeutil.Now()
					}
					// Else, skip first tick for created ioLoadListener, since it reads
					// state and sets tokens immediately.
					<-kvGranter.tokensSetCh
					if i == 0 {
						fmt.Fprintf(&buf, "admitted: %d, bytes: %d, added-bytes: %d,\nsmoothed-removed: %d, "+
							"smoothed-admit: %d,\ntokens: %s, tokens-allocated: %s\n", ioll.admittedCount,
							ioll.l0Bytes, ioll.l0AddedBytes, ioll.smoothedBytesRemoved,
							int64(ioll.smoothedNumAdmit), tokensFor60sToString(ioll.totalTokens),
							tokensFor1sToString(ioll.tokensAllocated))
					}
					fmt.Fprintf(&buf, "tick: %d, %s\n", i, kvGranter.buf.String())
					kvGranter.buf.Reset()
				}
				return buf.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	ioll.close()
}

// TODO(sumeer):
// - Test metrics
// - Test GrantCoordinator with multi-tenant configurations
