// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"

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

		// Set a short interval for frontier persistence
		sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.progress.frontier_persistence.interval = '5s'")

		// Create a table and insert some data
		sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY, b STRING)")
		sqlDB.Exec(t, "INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (3, 'c')")

		// Start a changefeed
		foo := feed(t, f, "CREATE CHANGEFEED FOR foo")
		defer closeFeed(t, foo)

		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()

		// Get changefeed metrics from job registry
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metric := registry.MetricsStruct().Changefeed.(*Metrics).AggMetrics.Timers.FrontierPersistence

		// Verify metric starts at zero
		initialCount, _ := metric.CumulativeSnapshot().Total()
		require.Equal(t, int64(0), initialCount, "frontier persistence metric should start at zero")

		testutils.SucceedsSoon(t, func() error {
			var found bool
			var allSpans []jobspb.ResolvedSpan
			err := s.Server.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				// Check if any frontier data has been persisted
				var err error
				allSpans, found, err = jobfrontier.GetAllResolvedSpans(ctx, txn, jobID)
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			if !found {
				return errors.Newf("frontier not yet persisted")
			}
			t.Logf("Found %d resolved spans persisted for job %d", len(allSpans), jobID)
			for i, span := range allSpans {
				t.Logf("  Span %d: %s -> %s", i, span.Span, span.Timestamp)
			}
			return nil
		})

		count, _ := metric.CumulativeSnapshot().Total()
		mean := metric.CumulativeSnapshot().Mean()
		require.Greater(t, count, int64(0), "frontier persistence should have been called at least once")
		require.Greater(t, mean, float64(0), "frontier persistence mean latency should be greater than 0")

		t.Logf("Frontier persistence metrics - count: %d, mean latency: %.2fms", count, mean/1e6)
	}

	// Run test with enterprise changefeed
	cdcTest(t, testFn, feedTestEnterpriseSinks)
}
