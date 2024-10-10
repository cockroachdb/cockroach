// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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
// by the cache updater to ensure the udpates are valid.
func TestDataDrivenTableMetadataCacheUpdater(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	knobs := tablemetadatacacheutil.CreateTestingKnobs()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TableMetadata: knobs,
		},
	})
	defer s.Stopper().Stop(ctx)

	queryConn := s.ApplicationLayer().SQLConn(t)
	s.ApplicationLayer().DB()

	// We won't check the job progress in this test.
	mockUpdateProgress := func(ctx context.Context, progress float32) {}
	metrics := newTableMetadataUpdateJobMetrics().(TableMetadataUpdateJobMetrics)
	datadriven.Walk(t, datapathutils.TestDataPath(t, ""), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
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
				updater := newTableMetadataUpdater(mockUpdateProgress, &metrics, s.InternalExecutor().(isql.Executor), knobs)
				updated, err := updater.updateCache(ctx)
				if err != nil {
					return err.Error()
				}
				return fmt.Sprintf("updated %d table(s)", updated)
			case "prune-cache":
				updater := newTableMetadataUpdater(mockUpdateProgress, &metrics, s.InternalExecutor().(isql.Executor), knobs)
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
	metrics := newTableMetadataUpdateJobMetrics().(TableMetadataUpdateJobMetrics)
	count := 0
	updater := tableMetadataUpdater{
		ie:      s.ExecutorConfig().(sql.ExecutorConfig).InternalDB.Executor(),
		metrics: &metrics,
		updateProgress: func(ctx context.Context, progress float32) {
			require.Greater(t, progress, currentProgress, "progress should be greater than current progress")
			currentProgress = progress
			count++
		},
		testKnobs: knobs,
	}
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
	estimatedBatches := int(math.Ceil(float64(expectedTablesUpdated) / float64(tableBatchSize)))
	estimatedProgressUpdates := estimatedBatches / batchesPerProgressUpdate
	require.GreaterOrEqual(t, count, estimatedProgressUpdates)
	require.Equal(t, int64(0), metrics.Errors.Count())
	require.Equal(t, float32(.99), currentProgress)
	require.Equal(t, int64(2), metrics.NumRuns.Count())
}
