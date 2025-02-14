// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstats_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
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
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestTraceAnalyzer verifies that the TraceAnalyzer correctly calculates
// expected top-level statistics from a physical plan and an accompanying trace
// from that plan's execution. It does this by starting up a multi-node cluster,
// enabling tracing, capturing the physical plan of a test statement, and
// constructing a TraceAnalyzer from the resulting trace and physical plan.
func TestTraceAnalyzer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		testStmt = "SELECT * FROM test.foo ORDER BY v"
		numNodes = 3
	)

	ctx := context.Background()
	analyzerChan := make(chan *execstats.TraceAnalyzer, 1)
	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					TestingSaveFlows: func(stmt string) sql.SaveFlowsFunc {
						if stmt != testStmt {
							return func(map[base.SQLInstanceID]*execinfrapb.FlowSpec, execopnode.OpChains, []execinfra.LocalProcessor, bool) error {
								return nil
							}
						}
						return func(flows map[base.SQLInstanceID]*execinfrapb.FlowSpec, _ execopnode.OpChains, _ []execinfra.LocalProcessor, _ bool) error {
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
	srv, s := tc.Server(gatewayNode), tc.ApplicationLayer(gatewayNode)
	if srv.DeploymentMode().IsExternal() {
		require.NoError(t, srv.GrantTenantCapabilities(
			ctx, serverutils.TestTenantID(),
			map[tenantcapabilities.ID]string{tenantcapabilities.CanAdminRelocateRange: "true"}))
	}
	db := s.SQLConn(t)
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

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	var (
		rowexecTraceAnalyzer *execstats.TraceAnalyzer
		colexecTraceAnalyzer *execstats.TraceAnalyzer
	)
	for _, vectorizeMode := range []sessiondatapb.VectorizeExecMode{sessiondatapb.VectorizeOff, sessiondatapb.VectorizeOn} {
		execCtx, finishAndCollect := tracing.ContextWithRecordingSpan(ctx, execCfg.AmbientCtx.Tracer, t.Name())
		defer finishAndCollect()
		ie := execCfg.InternalDB.NewInternalExecutor(&sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{
				VectorizeMode: vectorizeMode,
			},
			LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
				DistSQLMode: sessiondatapb.DistSQLOn,
			},
		})
		_, err := ie.ExecEx(
			execCtx,
			redact.RedactableString(t.Name()),
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			testStmt,
		)
		require.NoError(t, err)
		trace := finishAndCollect()
		analyzer := <-analyzerChan
		require.NoError(t, analyzer.AddTrace(trace, true /* makeDeterministic */))
		analyzer.ProcessStats()
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

	a.ProcessStats()
	require.Equal(t, a.GetQueryLevelStats(), expected)
}

func TestQueryLevelStatsAccumulate(t *testing.T) {
	aEvent := kvpb.ContentionEvent{Duration: 7 * time.Second}
	a := execstats.QueryLevelStats{
		NetworkBytesSent:                   1,
		MaxMemUsage:                        2,
		KVBytesRead:                        3,
		KVPairsRead:                        4,
		KVRowsRead:                         4,
		KVBatchRequestsIssued:              4,
		KVTime:                             5 * time.Second,
		NetworkMessages:                    6,
		ContentionTime:                     7 * time.Second,
		ContentionEvents:                   []kvpb.ContentionEvent{aEvent},
		MaxDiskUsage:                       8,
		RUEstimate:                         9,
		CPUTime:                            10 * time.Second,
		MvccSteps:                          11,
		MvccStepsInternal:                  12,
		MvccSeeks:                          13,
		MvccSeeksInternal:                  14,
		MvccBlockBytes:                     15,
		MvccBlockBytesInCache:              16,
		MvccKeyBytes:                       17,
		MvccValueBytes:                     18,
		MvccPointCount:                     19,
		MvccPointsCoveredByRangeTombstones: 20,
		MvccRangeKeyCount:                  21,
		MvccRangeKeyContainedPoints:        22,
		MvccRangeKeySkippedPoints:          23,
		SQLInstanceIDs:                     []int32{1},
		KVNodeIDs:                          []int32{1, 2},
		Regions:                            []string{"east-usA"},
		UsedFollowerRead:                   false,
		ClientTime:                         time.Second,
	}
	bEvent := kvpb.ContentionEvent{Duration: 14 * time.Second}
	b := execstats.QueryLevelStats{
		NetworkBytesSent:                   8,
		MaxMemUsage:                        9,
		KVBytesRead:                        10,
		KVPairsRead:                        11,
		KVRowsRead:                         11,
		KVBatchRequestsIssued:              11,
		KVTime:                             12 * time.Second,
		NetworkMessages:                    13,
		ContentionTime:                     14 * time.Second,
		ContentionEvents:                   []kvpb.ContentionEvent{bEvent},
		MaxDiskUsage:                       15,
		RUEstimate:                         16,
		CPUTime:                            17 * time.Second,
		MvccSteps:                          18,
		MvccStepsInternal:                  19,
		MvccSeeks:                          20,
		MvccSeeksInternal:                  21,
		MvccBlockBytes:                     22,
		MvccBlockBytesInCache:              23,
		MvccKeyBytes:                       24,
		MvccValueBytes:                     25,
		MvccPointCount:                     26,
		MvccPointsCoveredByRangeTombstones: 27,
		MvccRangeKeyCount:                  28,
		MvccRangeKeyContainedPoints:        29,
		MvccRangeKeySkippedPoints:          30,
		SQLInstanceIDs:                     []int32{2},
		KVNodeIDs:                          []int32{1, 3},
		Regions:                            []string{"east-usB"},
		UsedFollowerRead:                   true,
		ClientTime:                         2 * time.Second,
	}
	expected := execstats.QueryLevelStats{
		NetworkBytesSent:                   9,
		MaxMemUsage:                        9,
		KVBytesRead:                        13,
		KVPairsRead:                        15,
		KVRowsRead:                         15,
		KVBatchRequestsIssued:              15,
		KVTime:                             17 * time.Second,
		NetworkMessages:                    19,
		ContentionTime:                     21 * time.Second,
		ContentionEvents:                   []kvpb.ContentionEvent{aEvent, bEvent},
		MaxDiskUsage:                       15,
		RUEstimate:                         25,
		CPUTime:                            27 * time.Second,
		MvccSteps:                          29,
		MvccStepsInternal:                  31,
		MvccSeeks:                          33,
		MvccSeeksInternal:                  35,
		MvccBlockBytes:                     37,
		MvccBlockBytesInCache:              39,
		MvccKeyBytes:                       41,
		MvccValueBytes:                     43,
		MvccPointCount:                     45,
		MvccPointsCoveredByRangeTombstones: 47,
		MvccRangeKeyCount:                  49,
		MvccRangeKeyContainedPoints:        51,
		MvccRangeKeySkippedPoints:          53,
		SQLInstanceIDs:                     []int32{1, 2},
		KVNodeIDs:                          []int32{1, 2, 3},
		Regions:                            []string{"east-usA", "east-usB"},
		UsedFollowerRead:                   true,
		ClientTime:                         3 * time.Second,
	}

	aCopy := a
	// Copy will point to the same array.
	aCopy.SQLInstanceIDs = []int32{1}
	aCopy.KVNodeIDs = []int32{1, 2}
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
