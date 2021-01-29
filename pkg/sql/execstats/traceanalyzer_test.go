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
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
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
		testStmt = "SELECT * FROM test.foo ORDER BY v"
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
							flowMetadata := execstats.NewFlowMetadata(flows)
							analyzer := execstats.MakeTraceAnalyzer(flowMetadata)
							analyzerChan <- analyzer
							return nil
						}
					},
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
		ctx, sp = tracing.StartVerboseTrace(ctx, execCfg.AmbientCtx.Tracer, t.Name())
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
		require.NoError(t, analyzer.AddTrace(trace, true /* makeDeterministic */))
		require.NoError(t, analyzer.ProcessStats())
		switch vectorizeMode {
		case sessiondatapb.VectorizeOff:
			rowexecTraceAnalyzer = analyzer
		case sessiondatapb.VectorizeOn:
			colexecTraceAnalyzer = analyzer
		default:
			t.Fatalf("programming error, vectorize mode %s not handled", vectorizeMode)
		}
	}

	for _, tc := range []struct {
		name                string
		analyzer            *execstats.TraceAnalyzer
		expectedMaxMemUsage int64
	}{
		{
			name:                "RowExec",
			analyzer:            rowexecTraceAnalyzer,
			expectedMaxMemUsage: int64(20480),
		},
		{
			name:                "ColExec",
			analyzer:            colexecTraceAnalyzer,
			expectedMaxMemUsage: int64(51200),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeLevelStats := tc.analyzer.GetNodeLevelStats()
			require.Equal(
				t, numNodes-1, len(nodeLevelStats.NetworkBytesSentGroupedByNode), "expected all nodes minus the gateway node to have sent bytes",
			)
			require.Equal(
				t, numNodes, len(nodeLevelStats.MaxMemoryUsageGroupedByNode), "expected all nodes to have specified maximum memory usage",
			)

			queryLevelStats := tc.analyzer.GetQueryLevelStats()

			// The stats don't count the actual bytes, but they are a synthetic value
			// based on the number of tuples. In this test 21 tuples flow over the
			// network.
			require.Equal(t, int64(21*8), queryLevelStats.NetworkBytesSent)

			require.Equal(t, tc.expectedMaxMemUsage, queryLevelStats.MaxMemUsage)

			require.Equal(t, int64(30), queryLevelStats.KVRowsRead)
			// For tests, the bytes read is based on the number of rows read, rather
			// than actual bytes read.
			require.Equal(t, int64(30*8), queryLevelStats.KVBytesRead)

			// For tests, network messages is a synthetic value based on the number of
			// network tuples. In this test 21 tuples flow over the network.
			require.Equal(t, int64(21/2), queryLevelStats.NetworkMessages)
		})
	}
}

func TestTraceAnalyzerProcessStats(t *testing.T) {
	const (
		node1Time      = 3 * time.Second
		node2Time      = 5 * time.Second
		cumulativeTime = node1Time + node2Time
	)
	a := &execstats.TraceAnalyzer{FlowMetadata: &execstats.FlowMetadata{}}
	a.AddComponentStats(
		1, /* nodeID */
		&execinfrapb.ComponentStats{
			Component: execinfrapb.ProcessorComponentID(
				execinfrapb.FlowID{UUID: uuid.MakeV4()},
				1, /* processorID */
			),
			KV: execinfrapb.KVStats{
				KVTime:         optional.MakeTimeValue(node1Time),
				ContentionTime: optional.MakeTimeValue(node1Time),
			},
		},
	)

	a.AddComponentStats(
		2, /* nodeID */
		&execinfrapb.ComponentStats{
			Component: execinfrapb.ProcessorComponentID(
				execinfrapb.FlowID{UUID: uuid.MakeV4()},
				2, /* processorID */
			),
			KV: execinfrapb.KVStats{
				KVTime:         optional.MakeTimeValue(node2Time),
				ContentionTime: optional.MakeTimeValue(node2Time),
			},
		},
	)

	expected := execstats.QueryLevelStats{
		KVTime:         cumulativeTime,
		ContentionTime: cumulativeTime,
	}

	assert.NoError(t, a.ProcessStats())
	if got := a.GetQueryLevelStats(); !reflect.DeepEqual(got, expected) {
		t.Errorf("ProcessStats() = %v, want %v", got, expected)
	}
}
