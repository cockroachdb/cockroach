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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
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
		var allSpans []jobspb.ResolvedSpan
		testutils.SucceedsSoon(t, func() error {
			var found bool
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

		// Make sure the persisted spans cover the entire table.
		fooTableSpan := desctestutils.
			TestingGetPublicTableDescriptor(s.Server.DB(), s.Codec, "d", "foo").
			PrimaryIndexSpan(s.Codec)
		var spanGroup roachpb.SpanGroup
		spanGroup.Add(fooTableSpan)
		for _, rs := range allSpans {
			spanGroup.Sub(rs.Span)
		}
		require.Zero(t, spanGroup.Len())

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

// TestChangefeedFrontierRestore verifies that changefeeds will correctly
// restore progress from persisted span frontiers.
func TestChangefeedFrontierRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		ctx := context.Background()

		// Disable span-level checkpointing.
		changefeedbase.SpanCheckpointInterval.Override(ctx, &s.Server.ClusterSettings().SV, 0)

		// Create a table and a changefeed on it.
		sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")
		foo := feed(t, f, "CREATE CHANGEFEED FOR foo WITH initial_scan='no'")
		defer closeFeed(t, foo)
		jobFeed := foo.(cdctest.EnterpriseTestFeed)

		// Pause the changefeed.
		require.NoError(t, jobFeed.Pause())

		// Insert a few rows into the table and save the insert time.
		var tsStr string
		sqlDB.QueryRow(t, `INSERT INTO foo VALUES (1), (2), (3), (4), (5), (6)
RETURNING cluster_logical_timestamp()`).Scan(&tsStr)
		ts := parseTimeToHLC(t, tsStr)

		// Make function to create spans for single rows in the table.
		codec := s.Server.Codec()
		fooDesc := desctestutils.TestingGetPublicTableDescriptor(s.Server.DB(), codec, "d", "foo")
		rowSpan := func(key int64) roachpb.Span {
			keyPrefix := func() []byte {
				return rowenc.MakeIndexKeyPrefix(codec, fooDesc.GetID(), fooDesc.GetPrimaryIndexID())
			}
			return roachpb.Span{
				Key:    encoding.EncodeVarintAscending(keyPrefix(), key),
				EndKey: encoding.EncodeVarintAscending(keyPrefix(), key+1),
			}
		}

		// Manually persist a span frontier that manually marks some of the
		// inserted rows as resolved already.
		hw, err := jobFeed.HighWaterMark()
		require.NoError(t, err)
		frontier, err := span.MakeFrontierAt(hw, fooDesc.PrimaryIndexSpan(codec))
		require.NoError(t, err)
		for _, id := range []int64{2, 5} {
			_, err := frontier.Forward(rowSpan(id), ts)
			require.NoError(t, err)
		}
		err = s.Server.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return jobfrontier.Store(ctx, txn, jobFeed.JobID(), "test frontier", frontier)
		})
		require.NoError(t, err)

		// Resume the changefeed.
		require.NoError(t, jobFeed.Resume())

		// We should receive rows 1, 3, 4, 6 (rows 2 and 5 were marked as resolved).
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [3]->{"after": {"a": 3}}`,
			`foo: [4]->{"after": {"a": 4}}`,
			`foo: [6]->{"after": {"a": 6}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}
