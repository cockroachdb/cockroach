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
	"github.com/stretchr/testify/require"
)

// TestTraceAnalyzerAnalyze verifies that the TraceAnalyzer correctly calculates
// expected query-level statistics from the trace. It does this by starting up a
// multi-node cluster, enabling tracing, and constructing a TraceAnalyzer from
// the resulting trace.
func TestTraceAnalyzerAnalyze(t *testing.T) {
	defer log.Scope(t).Close(t)
	defer leaktest.AfterTest(t)()

	const (
		testStmt = "SELECT * FROM test.foo ORDER BY v"
		numNodes = 3
	)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      base.TestServerArgs{UseDatabase: "test"},
	})
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
		var analyzer execstats.TraceAnalyzer
		require.NoError(t, analyzer.Analyze(trace, true /* makeDeterministic */))
		switch vectorizeMode {
		case sessiondatapb.VectorizeOff:
			rowexecTraceAnalyzer = &analyzer
		case sessiondatapb.VectorizeOn:
			colexecTraceAnalyzer = &analyzer
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

func TestTraceAnalyzerProcessComponentStats(t *testing.T) {
	const (
		node1Time      = 3 * time.Second
		node2Time      = 5 * time.Second
		cumulativeTime = node1Time + node2Time
	)
	var a execstats.TraceAnalyzer
	require.NoError(t, a.ProcessComponentStats(
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
	))

	require.NoError(t, a.ProcessComponentStats(
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
	))

	expected := execstats.QueryLevelStats{
		KVTime:         cumulativeTime,
		ContentionTime: cumulativeTime,
	}

	if got := a.GetQueryLevelStats(); !reflect.DeepEqual(got, expected) {
		t.Errorf("ProcessStats() = %v, want %v", got, expected)
	}
}
