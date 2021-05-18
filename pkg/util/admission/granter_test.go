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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
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

// TODO(sumeer):
// - Test metrics
// - Test GrantCoordinator with multi-tenant configurations
