// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	tablemetadatacacheutil "github.com/cockroachdb/cockroach/pkg/sql/tablemetadatacache/util"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDrivenTableMetadataCacheUpdater tests the operations performed by
// tableMetadataCacheUpdater. It reads data written to system.table_metadata
// by the cache updater to ensure the updates are valid.
func TestDataDrivenTableMetadataCacheUpdater(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	knobs := tablemetadatacacheutil.CreateTestingKnobs()
	// We won't check the job progress in this test.
	mockUpdateProgress := func(ctx context.Context, progress float32) {}
	datadriven.Walk(t, datapathutils.TestDataPath(t, ""), func(t *testing.T, path string) {
		metrics := newTableMetadataUpdateJobMetrics().(TableMetadataUpdateJobMetrics)
		mockTimeSrc := timeutil.NewManualTime(timeutil.FromUnixMicros(0))
		s := serverutils.StartServerOnly(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				TableMetadata: knobs,
			},
		})
		defer s.Stopper().Stop(ctx)

		queryConn := s.ApplicationLayer().SQLConn(t)
		s.ApplicationLayer().DB()
		spanStatsServer := s.TenantStatusServer().(serverpb.TenantStatusServer)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			batchSize := updateJobBatchSizeSetting.Get(&s.ClusterSettings().SV)
			switch d.Cmd {
			case "set-time":
				var unixSecs int64
				d.ScanArgs(t, "unixSecs", &unixSecs)
				mockTimeSrc.AdvanceTo(timeutil.FromUnixMicros(unixSecs * 1e6))
				return ""
			case "query":
				rows, err := queryConn.Query(d.Input)
				if err != nil {
					return err.Error()
				}
				res, err := sqlutils.RowsToDataDrivenOutput(rows)
				if err != nil {
					return err.Error()
				}
				return res
			case "update-cache":
				var spanStatsErrors string
				// The batch number to inject an error on.
				var spanStatsErrBatch int64
				var spanStatsSrv spanStatsFetcher = spanStatsServer
				d.MaybeScanArgs(t, "injectSpanStatsErrors", &spanStatsErrors)
				d.MaybeScanArgs(t, "spanStatsErrBatch", &spanStatsErrBatch)
				batchCount := atomic.Int64{}
				if spanStatsErrors != "" {
					spanStatsSrv = &mockSpanStatsFetcher{
						getSpanStats: func(ctx context.Context, request *roachpb.SpanStatsRequest) (*roachpb.SpanStatsResponse, error) {
							var errs []string
							batchCount.Add(1)
							if spanStatsErrBatch == 0 || batchCount.Load() == spanStatsErrBatch {
								errs = strings.Split(spanStatsErrors, ",")
							}
							return &roachpb.SpanStatsResponse{
								SpanToStats: nil,
								Errors:      errs,
							}, nil
						},
					}
				}
				updater := newTableMetadataUpdater(mockUpdateProgress, &metrics, spanStatsSrv, s.InternalExecutor().(isql.Executor), mockTimeSrc, batchSize, knobs)
				prevUpdatedTables := metrics.UpdatedTables.Count()
				prevErrs := metrics.Errors.Count()
				err := updater.RunUpdater(ctx)
				if err != nil {
					return err.Error()
				}
				return fmt.Sprintf("updatedTables: %d, errors: %d, run #: %d, duration > 0: %t",
					metrics.UpdatedTables.Count()-prevUpdatedTables,
					metrics.Errors.Count()-prevErrs,
					metrics.NumRuns.Count(),
					metrics.Duration.CumulativeSnapshot().Mean() > 0)
			case "prune-cache":
				updater := newTableMetadataUpdater(mockUpdateProgress, &metrics, spanStatsServer, s.InternalExecutor().(isql.Executor), mockTimeSrc, batchSize, knobs)
				pruned, err := updater.pruneCache(ctx)
				if err != nil {
					return err.Error()
				}
				return fmt.Sprintf("pruned %d table(s)", pruned)
			case "flush-stores":
				// Flush store so that bytes return non-zero from span stats.
				s := s.StorageLayer()
				err := s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
					return store.TODOEngine().Flush()
				})
				if err != nil {
					return err.Error()
				}
				return "success"
			default:
				return "unknown command"
			}
		})
	})

}

func TestTableMetadataUpdateJobProgressAndMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var currentProgress float32
	// Server setup.
	knobs := tablemetadatacacheutil.CreateTestingKnobs()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TableMetadata: knobs,
		},
	})
	defer s.Stopper().Stop(ctx)
	conn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	batchSize := updateJobBatchSizeSetting.Get(&s.ClusterSettings().SV)
	metrics := newTableMetadataUpdateJobMetrics().(TableMetadataUpdateJobMetrics)
	count := 0
	updater := newTableMetadataUpdater(
		func(ctx context.Context, progress float32) {
			require.Greater(t, progress, currentProgress, "progress should be greater than current progress")
			currentProgress = progress
			count++
		}, // onProgressUpdated
		&metrics,
		s.TenantStatusServer().(serverpb.TenantStatusServer),
		s.ExecutorConfig().(sql.ExecutorConfig).InternalDB.Executor(),
		timeutil.DefaultTimeSource{},
		batchSize,
		knobs,
	)
	require.NoError(t, updater.RunUpdater(ctx))
	updatedTables := metrics.UpdatedTables.Count()
	require.Greater(t, updatedTables, int64(0))
	// Since there are only system tables in the cluster, the threshold to call updateProgress isn't met.
	// Update progress will only be called once.
	require.Equal(t, 1, count)
	require.Equal(t, int64(0), metrics.Errors.Count())
	require.Equal(t, float32(.99), currentProgress)
	require.Equal(t, int64(1), metrics.NumRuns.Count())
	require.Greater(t, metrics.Duration.CumulativeSnapshot().Mean(), float64(0))
	count = 0
	currentProgress = 0
	sw := timeutil.NewStopWatch()
	sw.Start()
	// generate 500 random tables
	conn.Exec(t,
		`SELECT crdb_internal.generate_test_objects('{"names": "random_table_", "counts": [500], "randomize_columns": true}'::JSONB)`)
	sw.Stop()
	t.Logf("Time elapsed to generate tables: %s", sw.Elapsed())
	sw.Start()
	require.NoError(t, updater.RunUpdater(ctx))
	sw.Stop()
	t.Logf("Time elapsed to run update job: %s", sw.Elapsed())
	// The updated tables metric doesn't reset between runs, so it should increase by updatedTables + 500, because 500
	// random tables were previously generated
	expectedTablesUpdated := (updatedTables * 2) + 500
	require.Equal(t, expectedTablesUpdated, metrics.UpdatedTables.Count())
	estimatedBatches := int(math.Ceil(float64(expectedTablesUpdated) / float64(batchSize)))
	estimatedProgressUpdates := estimatedBatches / batchesPerProgressUpdate
	require.GreaterOrEqual(t, count, estimatedProgressUpdates)
	require.Equal(t, int64(0), metrics.Errors.Count())
	require.Equal(t, float32(.99), currentProgress)
	require.Equal(t, int64(2), metrics.NumRuns.Count())
}

type mockSpanStatsFetcher struct {
	getSpanStats func(ctx context.Context, request *roachpb.SpanStatsRequest) (*roachpb.SpanStatsResponse, error)
}

var _ spanStatsFetcher = &mockSpanStatsFetcher{}

func (m *mockSpanStatsFetcher) SpanStats(
	ctx context.Context, request *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	if m.getSpanStats == nil {
		return nil, nil
	}
	return m.getSpanStats(ctx, request)
}
