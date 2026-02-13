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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/resolvedspan"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

				knobs.ShouldFlushFrontier = func(rs jobspb.ResolvedSpan) bool {
					return true
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

// TestCoreChangefeedProgress verifies that core (sinkless) changefeeds
// save and restore their full span frontier across retries. It creates
// a changefeed over two tables with one table's resolved timestamps
// frozen, waits for the frontier to be persisted with distinct
// per-span timestamps, injects a retryable error, and checks that the
// restored frontier on retry matches the saved one.
func TestCoreChangefeedProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		// Create two tables so the frontier has multiple spans.
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1), (2), (3)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (4), (5), (6)`)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Force the aggregator to flush its frontier on every resolved
		// span. Without this, the aggregator batches updates and the
		// coordinator may not receive rows frequently enough.
		knobs.ShouldFlushFrontier = func(_ jobspb.ResolvedSpan) bool {
			return true
		}

		// Freeze bar's resolved timestamps at the first non-zero value.
		// Foo continues advancing, so the frontier has two spans at
		// different timestamps. This ensures the test verifies per-span
		// frontier restoration, not just highwater restoration.
		barTableSpan := desctestutils.
			TestingGetPublicTableDescriptor(s.Server.DB(), s.Codec, "d", "bar").
			PrimaryIndexSpan(s.Codec)
		var barFrozenAt atomic.Int64
		knobs.FilterSpanWithMutation = func(rs *jobspb.ResolvedSpan) (bool, error) {
			if barTableSpan.ContainsKey(rs.Span.Key) && !rs.Timestamp.IsEmpty() {
				if frozen := barFrozenAt.Load(); frozen != 0 {
					rs.Timestamp.WallTime = frozen
				} else {
					barFrozenAt.Store(rs.Timestamp.WallTime)
				}
			}
			return false, nil
		}

		// Snapshot the restored frontier on each run so we can compare.
		var hasRestoredFrontier atomic.Bool
		var initialFrontier, retryFrontier []jobspb.ResolvedSpan
		knobs.AfterCoordinatorFrontierRestore = func(frontier *resolvedspan.CoordinatorFrontier) {
			var spans []jobspb.ResolvedSpan
			for rs := range frontier.All() {
				spans = append(spans, rs)
			}
			if initialFrontier == nil {
				initialFrontier = spans
			} else {
				retryFrontier = spans
				hasRestoredFrontier.Store(true)
			}
		}

		// Track the initial highwater on each run.
		var startedOnce atomic.Bool
		var initialHighwater, retryHighwater hlc.Timestamp
		knobs.StartDistChangefeedInitialHighwater = func(_ context.Context, hw hlc.Timestamp) {
			if startedOnce.Load() {
				retryHighwater = hw
			} else {
				initialHighwater = hw
				startedOnce.Store(true)
			}
		}

		// Record the last persisted frontier spans. Once the frontier has
		// multiple spans with distinct non-empty timestamps, inject a
		// retryable error exactly once so the retry can restore the frontier.
		var errorInjected atomic.Bool
		var savedSpans []jobspb.ResolvedSpan
		var savedHighwater hlc.Timestamp
		knobs.AfterPersistFrontier = func(state eval.CoreChangefeedState) error {
			if errorInjected.Load() {
				return nil
			}
			cp := state.(*coreProgress)
			require.NotNil(t, cp)
			// Wait until the frontier has multiple spans with non-empty,
			// distinct timestamps before signaling. Distinct timestamps
			// ensure we're testing per-span restoration, not just highwater.
			if len(cp.frontierSpans) < 2 {
				return nil
			}
			timestamps := make(map[int64]struct{})
			for _, rs := range cp.frontierSpans {
				if rs.Timestamp.IsEmpty() {
					return nil
				}
				timestamps[rs.Timestamp.WallTime] = struct{}{}
			}
			if len(timestamps) < 2 {
				return nil
			}
			savedSpans = make([]jobspb.ResolvedSpan, len(cp.frontierSpans))
			copy(savedSpans, cp.frontierSpans)
			if hw := cp.progress.GetHighWater(); hw != nil {
				savedHighwater = *hw
			}
			if errorInjected.CompareAndSwap(false, true) {
				return changefeedbase.MarkRetryableError(errors.New("AfterPersistFrontier"))
			}
			return nil
		}

		// Run changefeed.
		cf := feed(t, f, `CREATE CHANGEFEED FOR foo, bar
WITH resolved='1ns', min_checkpoint_frequency='1ns'`)
		defer closeFeed(t, cf)

		// Drain initial rows so the changefeed can advance past them.
		assertPayloads(t, cf, []string{
			`foo: [1]->{"after": {"a": 1}}`,
			`foo: [2]->{"after": {"a": 2}}`,
			`foo: [3]->{"after": {"a": 3}}`,
			`bar: [4]->{"after": {"a": 4}}`,
			`bar: [5]->{"after": {"a": 5}}`,
			`bar: [6]->{"after": {"a": 6}}`,
		})

		// Wait for the frontier to be restored on retry.
		testutils.SucceedsSoon(t, func() error {
			if !hasRestoredFrontier.Load() {
				return errors.New("waiting for retry")
			}
			return nil
		})

		// The persisted frontier should have multiple spans with non-empty
		// timestamps, and at least two distinct timestamps (bar is frozen
		// behind foo) to verify per-span restoration.
		require.Greater(t, len(savedSpans), 1)
		timestamps := make(map[hlc.Timestamp]struct{})
		for _, rs := range savedSpans {
			require.False(t, rs.Timestamp.IsEmpty())
			timestamps[rs.Timestamp] = struct{}{}
		}
		require.Greater(t, len(timestamps), 1,
			"expected spans at different timestamps to verify per-span restoration")

		// On first run, the highwater and frontier start empty.
		require.True(t, initialHighwater.IsEmpty())
		require.NotEmpty(t, initialFrontier)
		for _, rs := range initialFrontier {
			require.True(t, rs.Timestamp.IsEmpty())
		}

		// On retry, the highwater and frontier should match what was saved.
		require.False(t, savedHighwater.IsEmpty())
		require.Equal(t, savedHighwater, retryHighwater)
		require.NotEmpty(t, retryFrontier)
		require.ElementsMatch(t, savedSpans, retryFrontier)
	}

	cdcTest(t, testFn, feedTestForceSink("sinkless"),
		withAllowChangefeedErr("test injects retryable error"))
}
