// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execstats_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestTraceAnalyzer verifies that the TraceAnalyzer correctly calculates
// expected top-level statistics from a physical plan and an accompanying trace
// from that plan's execution. It does this by starting up a multi-node cluster,
// enabling tracing, capturing the physical plan of a test statement, and
// constructing a TraceAnalyzer from the resulting trace and physical plan.
func TestTraceAnalyzer(t *testing.T) {
	defer log.Scope(t).Close(t)
	defer leaktest.AfterTest(t)()

	const (
		testStmt = "SELECT * FROM test.foo"
		numNodes = 3
	)

	ctx := context.Background()
	analyzerChan := make(chan *execstats.TraceAnalyzer, 1)
	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					TestingSaveFlows: func(stmt string) func(map[roachpb.NodeID]*execinfrapb.FlowSpec) error {
						if stmt != testStmt {
							return func(map[roachpb.NodeID]*execinfrapb.FlowSpec) error { return nil }
						}
						return func(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) error {
							analyzer := execstats.NewTraceAnalyzer(flows)
							analyzerChan <- analyzer
							return nil
						}
					},
				},
				DistSQL: &execinfra.TestingKnobs{
					// DeterministicStats are set to eliminate variability when
					// calculating expected results. Note that this sets some fields to 0
					// (such as execution time), so a more dynamic approach might be
					// needed for those kinds of tests.
					DeterministicStats: true,
				},
			},
		}})
	defer tc.Stopper().Stop(ctx)

	const gatewayNode = 0
	db := tc.ServerConn(gatewayNode)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT",
		30,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2)),
	)

	sqlDB.Exec(t, "ALTER TABLE test.foo SPLIT AT VALUES (10), (20)")
	sqlDB.Exec(
		t,
		fmt.Sprintf("ALTER TABLE test.foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 10), (ARRAY[%d], 20)",
			tc.Server(0).GetFirstStoreID(),
			tc.Server(1).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID(),
		),
	)

	execCfg := tc.Server(gatewayNode).ExecutorConfig().(sql.ExecutorConfig)

	var (
		rowexecTraceAnalyzer *execstats.TraceAnalyzer
		colexecTraceAnalyzer *execstats.TraceAnalyzer
	)
	for _, vectorizeMode := range []sessiondatapb.VectorizeExecMode{sessiondatapb.VectorizeOff, sessiondatapb.VectorizeOn} {
		var sp *tracing.Span
		ctx, sp = tracing.StartSnowballTrace(ctx, execCfg.AmbientCtx.Tracer, t.Name())
		ie := execCfg.InternalExecutor
		ie.SetSessionData(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{
					VectorizeMode: vectorizeMode,
				},
				LocalOnlySessionData: sessiondata.LocalOnlySessionData{
					DistSQLMode: sessiondata.DistSQLOn,
				},
			},
		)
		_, err := ie.QueryEx(
			ctx,
			t.Name(),
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			testStmt,
		)
		sp.Finish()
		require.NoError(t, err)
		trace := sp.GetRecording()
		analyzer := <-analyzerChan
		require.NoError(t, analyzer.AddTrace(trace))
		switch vectorizeMode {
		case sessiondatapb.VectorizeOff:
			rowexecTraceAnalyzer = analyzer
		case sessiondatapb.VectorizeOn:
			colexecTraceAnalyzer = analyzer
		default:
			t.Fatalf("programming error, vectorize mode %s not handled", vectorizeMode)
		}
	}

	t.Run("NetworkBytesSent", func(t *testing.T) {
		for _, tc := range []struct {
			analyzer *execstats.TraceAnalyzer
			// expectedBytes are defined as a range in order to not have to change
			// this test too often if the wire format changes.
			expectedBytesRange [2]int64
		}{
			{
				analyzer:           rowexecTraceAnalyzer,
				expectedBytesRange: [2]int64{32, 128},
			},
			{
				analyzer: colexecTraceAnalyzer,
				// Note that the expectedBytes in the colexec case is larger than the
				// rowexec case because the DeterministicStats flag behaves differently
				// in each case. In rowexec, the outbox row bytes are returned. In
				// colexec, an artificial number proportional to the number of tuples is
				// returned.
				expectedBytesRange: [2]int64{128, 256},
			},
		} {
			networkBytesGroupedByNode, err := tc.analyzer.GetNetworkBytesSent()
			require.NoError(t, err)
			require.Equal(
				t, numNodes-1, len(networkBytesGroupedByNode), "expected all nodes minus the gateway node to have sent bytes",
			)

			var actualBytes int64
			for _, bytes := range networkBytesGroupedByNode {
				actualBytes += bytes
			}
			require.GreaterOrEqual(t, actualBytes, tc.expectedBytesRange[0])
			require.LessOrEqual(t, actualBytes, tc.expectedBytesRange[1])
		}
	})
}
