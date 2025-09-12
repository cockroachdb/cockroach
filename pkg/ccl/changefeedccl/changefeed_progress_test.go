// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestChangefeedFrontierPersistence verifies that changefeeds periodically
// persist their span frontiers to the job info table.
func TestChangefeedFrontierPersistence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		ctx := context.Background()

		// Set a short interval for frontier persistence.
		sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.progress.frontier_persistence.interval = '5s'")

		// Get frontier persistence metric.
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metric := registry.MetricsStruct().Changefeed.(*Metrics).AggMetrics.Timers.FrontierPersistence

		// Verify metric count starts at zero.
		initialCount, _ := metric.CumulativeSnapshot().Total()
		require.Equal(t, int64(0), initialCount)

		// Create a table and insert some data.
		sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY, b STRING)")
		sqlDB.Exec(t, "INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (3, 'c')")

		// Start a changefeed.
		foo := feed(t, f, "CREATE CHANGEFEED FOR foo")
		defer closeFeed(t, foo)

		// Make sure frontier gets persisted to job_info table.
		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()
		testutils.SucceedsSoon(t, func() error {
			var found bool
			var allSpans []jobspb.ResolvedSpan
			if err := s.Server.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				var err error
				allSpans, found, err = jobfrontier.GetAllResolvedSpans(ctx, txn, jobID)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			if !found {
				return errors.Newf("frontier not yet persisted")
			}
			t.Logf("found resolved spans in job_info table: %+v", allSpans)
			return nil
		})

		// Verify metric count and average latency have sensible values.
		testutils.SucceedsSoon(t, func() error {
			metricSnapshot := metric.CumulativeSnapshot()
			count, _ := metricSnapshot.Total()
			if count == 0 {
				return errors.Newf("metrics not yet updated")
			}
			avgLatency := time.Duration(metricSnapshot.Mean())
			t.Logf("frontier persistence metrics - count: %d, avg latency: %s", count, avgLatency)
			require.Greater(t, count, int64(0))
			require.Greater(t, avgLatency, time.Duration(0))
			return nil
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}
