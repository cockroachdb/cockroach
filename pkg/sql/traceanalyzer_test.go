// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	context "context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
	flowsChan := make(chan map[roachpb.NodeID]*execinfrapb.FlowSpec, 1)
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
							flowsChan <- flows
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
		rowexecTraceAnalyzer = &sql.TraceAnalyzer{}
		colexecTraceAnalyzer = &sql.TraceAnalyzer{}
	)
	for _, vectorizeMode := range []sessiondatapb.VectorizeExecMode{sessiondatapb.VectorizeOff, sessiondatapb.VectorizeOn} {
		ctx, sp := tracing.StartSnowballTrace(ctx, execCfg.AmbientCtx.Tracer, t.Name())
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
			sessiondata.InternalExecutorOverride{User: security.RootUser},
			testStmt,
		)
		sp.Finish()
		require.NoError(t, err)
		trace := tracing.GetRecording(sp)
		analyzer := &sql.TraceAnalyzer{}
		flows := <-flowsChan
		require.Equal(t, numNodes, len(flows), "expected one flow on each node")
		require.NoError(t, analyzer.Reset(flows, trace))
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
			analyzer      *sql.TraceAnalyzer
			expectedBytes int64
		}{
			{
				analyzer:      rowexecTraceAnalyzer,
				expectedBytes: 63,
			},
			{
				analyzer: colexecTraceAnalyzer,
				// Note that the expectedBytes in the colexec case is larger than the
				// rowexec case mostly because arrow serialization requires more bytes
				// for the representation (e.g. header, padding, a distinct vector for
				// null representation etc...). See RecordBatchSerializer for more
				// information.
				expectedBytes: 1872,
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
			require.Equal(t, tc.expectedBytes, actualBytes)
		}
	})
}
