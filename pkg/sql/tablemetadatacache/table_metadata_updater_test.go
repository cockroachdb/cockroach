// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tablemetadatacache

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	queryConn := s.ApplicationLayer().SQLConn(t)
	s.ApplicationLayer().DB()
	jobRegistry := s.JobRegistry().(*jobs.Registry)
	jobId := jobRegistry.MakeJobID()

	// Create a new job so that we don't have to wait for the existing one be claimed
	jr := jobs.Record{
		JobID:         jobId,
		Description:   jobspb.TypeUpdateTableMetadataCache.String(),
		Details:       jobspb.UpdateTableMetadataCacheDetails{},
		Progress:      jobspb.UpdateTableMetadataCacheProgress{},
		CreatedBy:     &jobs.CreatedByInfo{Name: username.NodeUser, ID: username.NodeUserID},
		Username:      username.NodeUserName(),
		NonCancelable: true,
	}
	job, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, jr, jobId, nil)
	metrics := newTableMetadataUpdateJobMetrics().(TableMetadataUpdateJobMetrics)
	require.NoError(t, err)
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
				updater := newTableMetadataUpdater(job, &metrics, s.InternalExecutor().(isql.Executor)).(*tableMetadataUpdater)
				updated, err := updater.updateCache(ctx)
				if err != nil {
					return err.Error()
				}
				return fmt.Sprintf("updated %d table(s)", updated)
			case "prune-cache":
				updater := newTableMetadataUpdater(job, &metrics, s.InternalExecutor().(isql.Executor)).(*tableMetadataUpdater)
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
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
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
	count = 0
	currentProgress = 0

	// generate 500 random tables
	conn.Exec(t,
		`SELECT crdb_internal.generate_test_objects('{"names": "random_table_", "counts": [500], "randomize_columns": true}'::JSONB)`)
	require.NoError(t, updater.RunUpdater(ctx))
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
