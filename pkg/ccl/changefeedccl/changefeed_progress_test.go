// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
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
		changefeedbase.FrontierPersistenceInterval.Override(ctx,
			&s.Server.ClusterSettings().SV, 5*time.Second)

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

func TestChangefeedProgressSkewMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "per-table tracking", func(t *testing.T, perTableTracking bool) {
		testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			ctx := context.Background()

			// Enable/disable per-table tracking.
			changefeedbase.TrackPerTableProgress.Override(ctx,
				&s.Server.ClusterSettings().SV, perTableTracking)

			registry := s.Server.JobRegistry().(*jobs.Registry)
			aggMetrics := registry.MetricsStruct().Changefeed.(*Metrics).AggMetrics
			const scope = "skew"
			scopedMetrics, err := aggMetrics.getOrCreateScope(scope)
			require.NoError(t, err)

			// Progress skew metrics should start at zero.
			require.Zero(t, aggMetrics.SpanProgressSkew.Value())
			require.Zero(t, aggMetrics.TableProgressSkew.Value())
			require.Zero(t, scopedMetrics.SpanProgressSkew.Value())
			require.Zero(t, scopedMetrics.TableProgressSkew.Value())

			// Create two tables and insert some initial data.
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `CREATE TABLE bar (b INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (1), (2), (3)`)
			sqlDB.Exec(t, `INSERT INTO bar VALUES (1), (2), (3)`)

			// Set up testing knobs to block all progress updates for bar.
			var blockBarProgress atomic.Bool
			blockBarProgress.Store(true)
			{
				barTableSpan := desctestutils.
					TestingGetPublicTableDescriptor(s.Server.DB(), s.Codec, "d", "bar").
					PrimaryIndexSpan(s.Codec)

				knobs := s.TestingKnobs.
					DistSQL.(*execinfra.TestingKnobs).
					Changefeed.(*TestingKnobs)

				knobs.FilterSpanWithMutation = func(rs *jobspb.ResolvedSpan) (bool, error) {
					if blockBarProgress.Load() && barTableSpan.Contains(rs.Span) {
						return true, nil
					}
					return false, nil
				}
			}

			// Create changefeed for both tables with no initial scan.
			feed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo, bar
WITH no_initial_scan, min_checkpoint_frequency='1s', resolved, metrics_label='%s'`, scope))
			defer closeFeed(t, feed)

			assertSpanSkewInRange := func(start int64, end int64) int64 {
				var spanSkew int64
				testutils.SucceedsSoon(t, func() error {
					spanSkew = aggMetrics.SpanProgressSkew.Value()
					scopedSpanSkew := scopedMetrics.SpanProgressSkew.Value()
					if spanSkew != scopedSpanSkew {
						return errors.Newf("aggregate and scoped span skew don't match")
					}
					if spanSkew < start {
						return errors.Newf("expected span skew to be at least %d, got %d", start, spanSkew)
					}
					if spanSkew >= end {
						return errors.Newf("expected span skew to be less than %d, got %d", end, spanSkew)
					}
					return nil
				})
				return spanSkew
			}
			assertTableSkewInRange := func(start int64, end int64) int64 {
				var tableSkew int64
				testutils.SucceedsSoon(t, func() error {
					tableSkew = aggMetrics.TableProgressSkew.Value()
					scopedTableSkew := scopedMetrics.TableProgressSkew.Value()
					if tableSkew != scopedTableSkew {
						return errors.Newf("aggregate and scoped table skew don't match")
					}
					if !perTableTracking {
						if tableSkew != 0 {
							return errors.Newf("expected table skew to be 0, got %d", tableSkew)
						}
						return nil
					}
					if tableSkew < start {
						return errors.Newf("expected table skew to be at least %d, got %d", start, tableSkew)
					}
					if tableSkew >= end {
						return errors.Newf("expected table skew to be less than %d, got %d", end, tableSkew)
					}
					return nil
				})
				return tableSkew
			}

			// Verify that progress skew metrics show a non-negligible amount of lag
			// since bar progress is blocked. Some amount of skew is often unavoidable
			// due to the fact the aggregator processes the rangefeed checkpoints for
			// different spans separately and at the time of a flush, may have only
			// processed a portion of the checkpoints for a specific closed timestamp.
			// The duration of 5s has been chosen given the default closed timestamp
			// interval is 3s.
			startingSpanSkew := assertSpanSkewInRange(int64(5*time.Second), math.MaxInt64)
			startingTableSkew := assertTableSkewInRange(int64(5*time.Second), math.MaxInt64)

			// Verify that skew continues to increase since bar progress is still blocked.
			assertSpanSkewInRange(startingSpanSkew+int64(5*time.Second), math.MaxInt64)
			assertTableSkewInRange(startingTableSkew+int64(5*time.Second), math.MaxInt64)

			// Re-enable progress updates for bar.
			blockBarProgress.Store(false)

			// Verify that skew drops below the skew observed at the start.
			assertSpanSkewInRange(0, startingSpanSkew)
			assertTableSkewInRange(0, startingTableSkew)
		}

		cdcTest(t, testFn)
	})
}
