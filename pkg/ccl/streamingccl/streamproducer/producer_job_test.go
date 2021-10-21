package streamproducer

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/base"
	//_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl" // Load the builtins
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStreamReplicationProducerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	shortInterval := 2 * time.Millisecond
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: &jobs.TestingKnobs{
					IntervalOverrides: jobs.TestingIntervalOverrides{
						Adopt:  &shortInterval,
						Cancel: &shortInterval,
					},
				},
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	source := tc.Server(0)
	sqlDB := tc.ServerConn(0)
	sql := sqlutils.MakeSQLRunner(sqlDB)
	registry := source.JobRegistry().(*jobs.Registry)

	t.Run("inactive-job-eventually-fails", func(t *testing.T) {
		tenantID := roachpb.MakeTenantID(10)
		rows := sql.QueryStr(t, "SELECT crdb_internal.init_stream($1, $2)", tenantID.ToUint64(), "2s")
		streamID := rows[0][0]
		jobsQuery := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %s", streamID)
		sql.CheckQueryResults(t, jobsQuery, [][]string{{"running"}})
		sql.SucceedsSoonDuration = time.Minute
		sql.CheckQueryResultsRetry(t, jobsQuery, [][]string{{"failed"}})
	})

	t.Run("active-job-continuously-running", func(t *testing.T) {
		tenantID := roachpb.MakeTenantID(20)
		row := sql.QueryRow(t, "SELECT crdb_internal.init_stream($1, $2)", tenantID.ToUint64(), "3s")
		var streamID uint64
		row.Scan(&streamID)

		testDuration := 30 * time.Second
		progressUpdater := func() {
			now := timeutil.Now()
			for start, stop := now, now.Add(testDuration); start.Before(stop); start = start.Add(50 * time.Millisecond) {
				_ = source.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return registry.UpdateJobWithTxn(ctx, jobspb.JobID(streamID), txn, false,
						func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
							 progress := md.Progress
							 progress.GetStreamReplicationProgress().LastActive = timeutil.ToUnixMicros(timeutil.Now())
							 ju.UpdateProgress(progress)
							 return nil
						})
				})
			}
		}
		go progressUpdater()

		progressRead := make(map[int64]bool)
		now := timeutil.Now()
		for start, stop := now, now.Add(testDuration); start.Before(stop); start = start.Add(50 * time.Millisecond) {
			if j, err := registry.LoadJob(ctx, jobspb.JobID(streamID)); err != nil {
				t.Fatal(err)
			} else {
				p := j.Progress()
				progressRead[p.GetStreamReplicationProgress().LastActive] = true
			}
			jobsQuery := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID)
			sql.CheckQueryResults(t, jobsQuery, [][]string{{"running"}})
		}

		require.Greater(t, len(progressRead), 1, "progress.LastActive should have been set more than once")
	})
}
