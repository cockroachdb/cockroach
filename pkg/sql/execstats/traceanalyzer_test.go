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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
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
					TestingSaveFlows: func(stmt string) func(map[roachpb.NodeID]*execinfrapb.FlowSpec, execinfra.OpChains) error {
						if stmt != testStmt {
							return func(map[roachpb.NodeID]*execinfrapb.FlowSpec, execinfra.OpChains) error { return nil }
						}
						return func(flows map[roachpb.NodeID]*execinfrapb.FlowSpec, _ execinfra.OpChains) error {
							flowsMetadata := execstats.NewFlowsMetadata(flows)
							analyzer := execstats.NewTraceAnalyzer(flowsMetadata)
							analyzerChan <- analyzer
							return nil
						}
					},
				},
				DistSQL: &execinfra.TestingKnobs{
					ForceDiskSpill: true,
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
		_, err := ie.ExecEx(
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
		name     string
		analyzer *execstats.TraceAnalyzer
	}{
		{
			name:     "RowExec",
			analyzer: rowexecTraceAnalyzer,
		},
		{
			name:     "ColExec",
			analyzer: colexecTraceAnalyzer,
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
			require.Equal(
				t, numNodes, len(nodeLevelStats.MaxDiskUsageGroupedByNode), "expected all nodes to have specified maximum disk usage",
			)

			queryLevelStats := tc.analyzer.GetQueryLevelStats()

			// The stats don't count the actual bytes, but they are a synthetic value
			// based on the number of tuples. In this test 21 tuples flow over the
			// network.
			require.Equal(t, int64(21*8), queryLevelStats.NetworkBytesSent)

			// Soft check that MaxMemUsage is set to a non-zero value. The actual
			// value differs between test runs due to metamorphic randomization.
			require.Greater(t, queryLevelStats.MaxMemUsage, int64(0))

			require.Equal(t, int64(1048576), queryLevelStats.MaxDiskUsage)

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
		node1KVTime              = 1 * time.Second
		node1ContentionTime      = 2 * time.Second
		node2KVTime              = 3 * time.Second
		node2ContentionTime      = 4 * time.Second
		cumulativeKVTime         = node1KVTime + node2KVTime
		cumulativeContentionTime = node1ContentionTime + node2ContentionTime
	)
	a := &execstats.TraceAnalyzer{FlowsMetadata: &execstats.FlowsMetadata{}}
	n1 := base.SQLInstanceID(1)
	n2 := base.SQLInstanceID(2)
	a.AddComponentStats(
		&execinfrapb.ComponentStats{
			Component: execinfrapb.ProcessorComponentID(
				n1,
				execinfrapb.FlowID{UUID: uuid.MakeV4()},
				1, /* processorID */
			),
			KV: execinfrapb.KVStats{
				KVTime:         optional.MakeTimeValue(node1KVTime),
				ContentionTime: optional.MakeTimeValue(node1ContentionTime),
			},
		},
	)

	a.AddComponentStats(
		&execinfrapb.ComponentStats{
			Component: execinfrapb.ProcessorComponentID(
				n2,
				execinfrapb.FlowID{UUID: uuid.MakeV4()},
				2, /* processorID */
			),
			KV: execinfrapb.KVStats{
				KVTime:         optional.MakeTimeValue(node2KVTime),
				ContentionTime: optional.MakeTimeValue(node2ContentionTime),
			},
		},
	)

	expected := execstats.QueryLevelStats{
		KVTime:         cumulativeKVTime,
		ContentionTime: cumulativeContentionTime,
	}

	assert.NoError(t, a.ProcessStats())
	if got := a.GetQueryLevelStats(); !reflect.DeepEqual(got, expected) {
		t.Errorf("ProcessStats() = %v, want %v", got, expected)
	}
}

func TestQueryLevelStatsAccumulate(t *testing.T) {
	a := execstats.QueryLevelStats{
		NetworkBytesSent: 1,
		MaxMemUsage:      2,
		KVBytesRead:      3,
		KVRowsRead:       4,
		KVTime:           5 * time.Second,
		NetworkMessages:  6,
		ContentionTime:   7 * time.Second,
		MaxDiskUsage:     8,
		Regions:          []string{"gcp-us-east1"},
	}
	b := execstats.QueryLevelStats{
		NetworkBytesSent: 8,
		MaxMemUsage:      9,
		KVBytesRead:      10,
		KVRowsRead:       11,
		KVTime:           12 * time.Second,
		NetworkMessages:  13,
		ContentionTime:   14 * time.Second,
		MaxDiskUsage:     15,
		Regions:          []string{"gcp-us-west1"},
	}
	expected := execstats.QueryLevelStats{
		NetworkBytesSent: 9,
		MaxMemUsage:      9,
		KVBytesRead:      13,
		KVRowsRead:       15,
		KVTime:           17 * time.Second,
		NetworkMessages:  19,
		ContentionTime:   21 * time.Second,
		MaxDiskUsage:     15,
		Regions:          []string{"gcp-us-east1", "gcp-us-west1"},
	}

	aCopy := a
	a.Accumulate(b)
	require.Equal(t, expected, a)

	reflectedAccumulatedStats := reflect.ValueOf(a)
	reflectedOriginalStats := reflect.ValueOf(aCopy)
	for i := 0; i < reflectedAccumulatedStats.NumField(); i++ {
		require.NotEqual(
			t,
			reflectedAccumulatedStats.Field(i).Interface(),
			reflectedOriginalStats.Field(i).Interface(),
			"no struct field should be the same after accumulation in this test but %s was unchanged, did you forget to update Accumulate?",
			reflectedAccumulatedStats.Type().Field(i).Name,
		)
	}
}

// TestGetQueryLevelStatsAccumulates does a sanity check that GetQueryLevelStats
// accumulates the stats for all flows passed into it. It does so by creating
// two FlowsMetadata objects and, thus, simulating a subquery and a main query.
func TestGetQueryLevelStatsAccumulates(t *testing.T) {
	const f1KVTime = 1 * time.Second
	const f2KVTime = 3 * time.Second

	// Artificially inject component stats directly into the FlowsMetadata (in
	// the non-testing setting the stats come from the trace).
	var f1, f2 execstats.FlowsMetadata
	n1 := base.SQLInstanceID(1)
	n2 := base.SQLInstanceID(2)
	f1.AddComponentStats(
		&execinfrapb.ComponentStats{
			Component: execinfrapb.ProcessorComponentID(
				n1,
				execinfrapb.FlowID{UUID: uuid.MakeV4()},
				1, /* processorID */
			),
			KV: execinfrapb.KVStats{
				KVTime: optional.MakeTimeValue(f1KVTime),
			},
		},
	)
	f2.AddComponentStats(
		&execinfrapb.ComponentStats{
			Component: execinfrapb.ProcessorComponentID(
				n2,
				execinfrapb.FlowID{UUID: uuid.MakeV4()},
				2, /* processorID */
			),
			KV: execinfrapb.KVStats{
				KVTime: optional.MakeTimeValue(f2KVTime),
			},
		},
	)

	queryLevelStats, err := execstats.GetQueryLevelStats(
		nil,   /* trace */
		false, /* deterministicExplainAnalyze */
		[]*execstats.FlowsMetadata{&f1, &f2},
	)
	require.NoError(t, err)
	require.Equal(t, f1KVTime+f2KVTime, queryLevelStats.KVTime)
}
