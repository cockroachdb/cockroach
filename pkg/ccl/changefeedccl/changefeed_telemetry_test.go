package changefeedccl

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChangefeedProgressMetrics tests the changefeed.aggregator_progress and
// changefeed.checkpoint_progress metrics.
func TestChangefeedProgressMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Verify the aggmetric functional gauges work correctly
	t.Run("aggregate functional gauge", func(t *testing.T) {
		cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			registry := s.Server.JobRegistry().(*jobs.Registry)
			metrics := registry.MetricsStruct().Changefeed.(*Metrics)
			defaultSLI, err := metrics.getSLIMetrics(defaultSLIScope)
			require.NoError(t, err)
			sliA, err := metrics.getSLIMetrics("scope_a")
			require.NoError(t, err)
			sliB, err := metrics.getSLIMetrics("scope_b")
			require.NoError(t, err)

			defaultSLI.mu.checkpoint[5] = hlc.Timestamp{WallTime: 1}

			sliA.mu.checkpoint[1] = hlc.Timestamp{WallTime: 2}
			sliA.mu.checkpoint[2] = hlc.Timestamp{WallTime: 5}
			sliA.mu.checkpoint[3] = hlc.Timestamp{WallTime: 0} // Zero timestamp should be ignored.

			sliB.mu.checkpoint[1] = hlc.Timestamp{WallTime: 4}
			sliB.mu.checkpoint[2] = hlc.Timestamp{WallTime: 9}

			// Ensure each scope gets the correct value
			require.Equal(t, int64(1), defaultSLI.CheckpointProgress.Value())
			require.Equal(t, int64(2), sliA.CheckpointProgress.Value())
			require.Equal(t, int64(4), sliB.CheckpointProgress.Value())

			// Ensure the value progresses upon changefeed progress
			defaultSLI.mu.checkpoint[5] = hlc.Timestamp{WallTime: 20}
			require.Equal(t, int64(20), defaultSLI.CheckpointProgress.Value())

			// Ensure the value updates correctly upon changefeeds completing
			delete(sliB.mu.checkpoint, 1)
			require.Equal(t, int64(9), sliB.CheckpointProgress.Value())
			delete(sliB.mu.checkpoint, 2)
			require.Equal(t, int64(0), sliB.CheckpointProgress.Value())

			// Ensure the aggregate value is correct after progress / completion
			require.Equal(t, int64(2), metrics.AggMetrics.CheckpointProgress.Value())
			sliA.mu.checkpoint[1] = hlc.Timestamp{WallTime: 30}
			require.Equal(t, int64(5), metrics.AggMetrics.CheckpointProgress.Value())
			delete(sliA.mu.checkpoint, 2)
			require.Equal(t, int64(20), metrics.AggMetrics.CheckpointProgress.Value())
			delete(defaultSLI.mu.checkpoint, 5)
			require.Equal(t, int64(30), metrics.AggMetrics.CheckpointProgress.Value())
			delete(sliA.mu.checkpoint, 1)
			require.Equal(t, int64(0), metrics.AggMetrics.CheckpointProgress.Value())
		})
	})

	// Verify that ids must be registered to have an effect.
	t.Run("id registration", func(t *testing.T) {
		cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			registry := s.Server.JobRegistry().(*jobs.Registry)
			metrics := registry.MetricsStruct().Changefeed.(*Metrics)
			sliA, err := metrics.getSLIMetrics("scope_a")
			require.NoError(t, err)

			unregisteredID := int64(999)
			id1 := sliA.claimId()
			id2 := sliA.claimId()
			id3 := sliA.claimId()
			sliA.setResolved(unregisteredID, hlc.Timestamp{WallTime: 0})
			sliA.setResolved(id1, hlc.Timestamp{WallTime: 1})
			sliA.setResolved(id2, hlc.Timestamp{WallTime: 2})
			sliA.setResolved(id3, hlc.Timestamp{WallTime: 3})

			sliA.setCheckpoint(unregisteredID, hlc.Timestamp{WallTime: 0})
			sliA.setCheckpoint(id1, hlc.Timestamp{WallTime: 1})
			sliA.setCheckpoint(id2, hlc.Timestamp{WallTime: 2})
			sliA.setCheckpoint(id3, hlc.Timestamp{WallTime: 3})

			require.Equal(t, int64(1), metrics.AggMetrics.CheckpointProgress.Value())
			require.Equal(t, int64(1), metrics.AggMetrics.AggregatorProgress.Value())

			sliA.closeId(id1)

			require.Equal(t, int64(2), metrics.AggMetrics.CheckpointProgress.Value())
			require.Equal(t, int64(2), metrics.AggMetrics.AggregatorProgress.Value())

		})
	})

	// Verify that a changefeed updates the timestamps as it progresses
	t.Run("running changefeed", func(t *testing.T) {
		cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)

			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
			fooA := feed(t, f, `CREATE CHANGEFEED FOR foo WITH metrics_label='label_a', resolved='100ms'`)

			registry := s.Server.JobRegistry().(*jobs.Registry)
			metrics := registry.MetricsStruct().Changefeed.(*Metrics)
			sliA, err := metrics.getSLIMetrics("label_a")
			require.NoError(t, err)

			// Verify that aggregator_progress has recurring updates
			var lastTimestamp int64 = 0
			for i := 0; i < 3; i++ {
				testutils.SucceedsSoon(t, func() error {
					progress := sliA.AggregatorProgress.Value()
					if progress > lastTimestamp {
						lastTimestamp = progress
						return nil
					}
					return errors.Newf("waiting for aggregator_progress to advance from %d (value=%d)",
						lastTimestamp, progress)
				})
			}

			// Verify that checkpoint_progress has recurring updates
			for i := 0; i < 3; i++ {
				testutils.SucceedsSoon(t, func() error {
					progress := sliA.CheckpointProgress.Value()
					if progress > lastTimestamp {
						lastTimestamp = progress
						return nil
					}
					return errors.Newf("waiting for checkpoint_progress to advance from %d (value=%d)",
						lastTimestamp, progress)
				})
			}

			// Verify that max_behind_nanos has recurring updates
			var lastValue int64 = 0
			for i := 0; i < 3; i++ {
				testutils.SucceedsSoon(t, func() error {
					value := sliA.MaxBehindNanos.Value()
					if value != lastValue {
						lastValue = value
						return nil
					}
					return errors.Newf("waiting for max_behind_nanos to update %d",
						lastValue)
				})
			}

			sliB, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics("label_b")
			require.Equal(t, int64(0), sliB.AggregatorProgress.Value())
			fooB := feed(t, f, `CREATE CHANGEFEED FOR foo WITH metrics_label='label_b', resolved='100ms'`)
			defer closeFeed(t, fooB)
			require.NoError(t, err)
			// Verify that aggregator_progress has recurring updates
			testutils.SucceedsSoon(t, func() error {
				progress := sliB.AggregatorProgress.Value()
				if progress > 0 {
					return nil
				}
				return errors.Newf("waiting for second aggregator_progress to advance (value=%d)", progress)
			})

			closeFeed(t, fooA)
			testutils.SucceedsSoon(t, func() error {
				aggregatorProgress := sliA.AggregatorProgress.Value()
				checkpointProgress := sliA.CheckpointProgress.Value()
				maxBehindNanos := sliA.MaxBehindNanos.Value()
				if aggregatorProgress == 0 && checkpointProgress == 0 && maxBehindNanos == 0 {
					return nil
				}
				return errors.Newf("waiting for progress metrics to be 0 (ap=%d, cp=%d)",
					aggregatorProgress, checkpointProgress)
			})
		})
	})
}

// TestChangefeedLaggingRangesMetrics tests the behavior of the
// changefeed.lagging_ranges metric.
func TestChangefeedLaggingRangesMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Ensure a fast closed timestamp interval so ranges can catch up fast.
		kvserver.RangeFeedRefreshInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 20*time.Millisecond)
		closedts.SideTransportCloseInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 20*time.Millisecond)
		closedts.TargetDuration.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 20*time.Millisecond)

		skipMu := syncutil.Mutex{}
		skippedRanges := map[string]struct{}{}
		numRanges := 10
		numRangesToSkip := int64(4)
		var stopSkip atomic.Bool
		// `shouldSkip` continuously skips checkpoints for the first `numRangesToSkip` ranges it sees.
		// skipping is disabled by setting `stopSkip` to true.
		shouldSkip := func(event *kvpb.RangeFeedEvent) bool {
			if stopSkip.Load() {
				return false
			}
			switch event.GetValue().(type) {
			case *kvpb.RangeFeedCheckpoint:
				sp := event.Checkpoint.Span
				skipMu.Lock()
				defer skipMu.Unlock()
				if _, ok := skippedRanges[sp.String()]; ok || int64(len(skippedRanges)) < numRangesToSkip {
					skippedRanges[sp.String()] = struct{}{}
					return true
				}
			}
			return false
		}

		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)

		knobs.FeedKnobs.RangefeedOptions = append(knobs.FeedKnobs.RangefeedOptions, kvcoord.TestingWithOnRangefeedEvent(
			func(ctx context.Context, s roachpb.Span, _ int64, event *kvpb.RangeFeedEvent) (skip bool, _ error) {
				return shouldSkip(event), nil
			}),
		)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		sli1, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics("t1")
		require.NoError(t, err)
		laggingRangesTier1 := sli1.LaggingRanges
		sli2, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics("t2")
		require.NoError(t, err)
		laggingRangesTier2 := sli2.LaggingRanges

		assertLaggingRanges := func(tier string, expected int64) {
			testutils.SucceedsWithin(t, func() error {
				var laggingRangesObserved int64
				if tier == "t1" {
					laggingRangesObserved = laggingRangesTier1.Value()
				} else {
					laggingRangesObserved = laggingRangesTier2.Value()
				}
				if laggingRangesObserved != expected {
					return fmt.Errorf("expected %d lagging ranges, but found %d", expected, laggingRangesObserved)
				}
				return nil
			}, 10*time.Second)
		}

		sqlDB.Exec(t, fmt.Sprintf(`
		  CREATE TABLE foo (key INT PRIMARY KEY);
		  INSERT INTO foo (key) SELECT * FROM generate_series(1, %d);
		  ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, %d, 1));
  		`, numRanges, numRanges-1))
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM [SHOW RANGES FROM TABLE foo]`,
			[][]string{{fmt.Sprint(numRanges)}},
		)

		const laggingRangesOpts = `lagging_ranges_threshold="250ms", lagging_ranges_polling_interval="25ms"`
		feed1Tier1 := feed(t, f,
			fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH initial_scan='no', metrics_label="t1", %s`, laggingRangesOpts))
		feed2Tier1 := feed(t, f,
			fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH initial_scan='no', metrics_label="t1", %s`, laggingRangesOpts))
		feed3Tier2 := feed(t, f,
			fmt.Sprintf(`CREATE CHANGEFEED FOR foo WITH initial_scan='no', metrics_label="t2", %s`, laggingRangesOpts))

		assertLaggingRanges("t1", numRangesToSkip*2)
		assertLaggingRanges("t2", numRangesToSkip)

		stopSkip.Store(true)
		assertLaggingRanges("t1", 0)
		assertLaggingRanges("t2", 0)

		stopSkip.Store(false)
		assertLaggingRanges("t1", numRangesToSkip*2)
		assertLaggingRanges("t2", numRangesToSkip)

		require.NoError(t, feed1Tier1.Close())
		assertLaggingRanges("t1", numRangesToSkip)
		assertLaggingRanges("t2", numRangesToSkip)

		require.NoError(t, feed2Tier1.Close())
		assertLaggingRanges("t1", 0)
		assertLaggingRanges("t2", numRangesToSkip)

		require.NoError(t, feed3Tier2.Close())
		assertLaggingRanges("t1", 0)
		assertLaggingRanges("t2", 0)
	}
	// Can't run on tenants due to lack of SPLIT AT support (#54254)
	cdcTest(t, testFn, feedTestNoTenants, feedTestEnterpriseSinks)
}

func TestChangefeedTotalRangesMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		defaultSLI, err := metrics.getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		totalRanges := defaultSLI.TotalRanges

		// Total ranges should start at zero.
		require.Zero(t, totalRanges.Value())

		assertTotalRanges := func(expected int64) {
			testutils.SucceedsSoon(t, func() error {
				if actual := totalRanges.Value(); expected != actual {
					return errors.Newf("expected total ranges to be %d, but got %d", expected, actual)
				}
				return nil
			})
		}

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, "CREATE TABLE foo (x int)")

		// We expect one range after creating a changefeed on a single table.
		fooFeed := feed(t, f, "CREATE CHANGEFEED FOR foo WITH lagging_ranges_polling_interval='1s'")
		assertTotalRanges(1)

		// We expect total ranges to be zero again after pausing the changefeed.
		require.NoError(t, fooFeed.(cdctest.EnterpriseTestFeed).Pause())
		assertTotalRanges(0)

		// We once again expect one range after resuming the changefeed.
		require.NoError(t, fooFeed.(cdctest.EnterpriseTestFeed).Resume())
		assertTotalRanges(1)

		// We expect two ranges after starting another changefeed on a single table.
		barFeed := feed(t, f, "CREATE CHANGEFEED FOR foo WITH lagging_ranges_polling_interval='1s'")
		assertTotalRanges(2)

		// We expect there to still be one range after cancelling one of the changefeeds.
		require.NoError(t, fooFeed.Close())
		assertTotalRanges(1)

		// We expect there to be no ranges left after cancelling the other changefeed.
		require.NoError(t, barFeed.Close())
		assertTotalRanges(0)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestChangefeedBackfillObservability(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		registry := s.Server.JobRegistry().(*jobs.Registry)
		sli, err := registry.MetricsStruct().Changefeed.(*Metrics).getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)
		pendingRanges := sli.BackfillPendingRanges

		// Create a table with multiple ranges
		numRanges := 10
		rowsPerRange := 20
		sqlDB.Exec(t, fmt.Sprintf(`
  CREATE TABLE foo (key INT PRIMARY KEY);
  INSERT INTO foo (key) SELECT * FROM generate_series(1, %d);
  ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(%d, %d, %d));
  `, numRanges*rowsPerRange, rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange))
		sqlDB.CheckQueryResults(t, `SELECT count(*) FROM [SHOW RANGES FROM TABLE foo]`,
			[][]string{{fmt.Sprint(numRanges)}},
		)

		// Allow control of the scans
		scanCtx, scanCancel := context.WithCancel(context.Background())
		scanChan := make(chan struct{})
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			select {
			case <-scanCtx.Done():
				return scanCtx.Err()
			case <-scanChan:
				return nil
			}
		}

		require.Equal(t, pendingRanges.Value(), int64(0))
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)

		// Progress the initial backfill halfway through its ranges
		for i := 0; i < numRanges/2; i++ {
			scanChan <- struct{}{}
		}
		testutils.SucceedsSoon(t, func() error {
			count := pendingRanges.Value()
			if count != int64(numRanges/2) {
				return fmt.Errorf("range count %d should be %d", count, numRanges/2)
			}
			return nil
		})

		// Ensure that the pending count is cleared if the backfill completes
		// regardless of successful scans
		scanCancel()
		testutils.SucceedsSoon(t, func() error {
			count := pendingRanges.Value()
			if count > 0 {
				return fmt.Errorf("range count %d should be 0", count)
			}
			return nil
		})
	}

	// Can't run on tenants due to lack of SPLIT AT support (#54254)
	cdcTest(t, testFn, feedTestNoTenants, feedTestEnterpriseSinks)
}

func TestChangefeedMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		if c := s.Server.MustGetSQLCounter(`changefeed.emitted_messages`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.flushed_bytes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.flushes`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
			t.Errorf(`expected %d got %d`, 0, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.in`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.out`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.schemafeed.table_metadata_nanos`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}
		if c := s.Server.MustGetSQLCounter(`changefeed.schemafeed.table_history_scans`); c != 0 {
			t.Errorf(`expected 0 got %d`, c)
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH metrics_label='tier0'`)
		_, err := foo.Next()
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_messages`); c != 1 {
				return errors.Errorf(`expected 1 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_bytes`); c != 22 {
				return errors.Errorf(`expected 22 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.flushed_bytes`); c != 22 {
				return errors.Errorf(`expected 22 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.flushes`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.running`); c != 1 {
				return errors.Errorf(`expected 1 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.max_behind_nanos`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.in`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.buffer_entries.out`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.schemafeed.table_history_scans`); c <= 0 {
				return errors.Errorf(`expected > 0 got %d`, c)
			}
			return nil
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)

		// Check that two changefeeds add correctly.
		// Set cluster settings back so we don't interfere with schema changes.
		sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		fooCopy := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		_, _ = fooCopy.Next()
		_, _ = fooCopy.Next()
		testutils.SucceedsSoon(t, func() error {
			// We can't assert exactly 4 or 88 in case we get (allowed) duplicates
			// from RangeFeed.
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_messages`); c < 4 {
				return errors.Errorf(`expected >= 4 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.emitted_bytes`); c < 88 {
				return errors.Errorf(`expected >= 88 got %d`, c)
			}
			return nil
		})

		// Cancel all the changefeeds and check that max_behind_nanos returns to 0
		// and the number running returns to 0.
		require.NoError(t, foo.Close())
		require.NoError(t, fooCopy.Close())
		testutils.SucceedsSoon(t, func() error {
			if c := s.Server.MustGetSQLCounter(`changefeed.max_behind_nanos`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			if c := s.Server.MustGetSQLCounter(`changefeed.running`); c != 0 {
				return errors.Errorf(`expected 0 got %d`, c)
			}
			return nil
		})
	}

	cdcTestWithSystem(t, testFn, feedTestForceSink("sinkless"))
}

func TestChangefeedTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)

		// Reset the counts.
		_ = telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

		// Start some feeds (and read from them to make sure they've started.
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		fooBar := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH format=json`)
		defer closeFeed(t, fooBar)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})
		assertPayloads(t, fooBar, []string{
			`bar: [1]->{"after": {"a": 1}}`,
			`foo: [1]->{"after": {"a": 1}}`,
		})

		var expectedSink string
		if strings.Contains(t.Name(), `sinkless`) || strings.Contains(t.Name(), `poller`) {
			expectedSink = `sinkless`
		} else {
			expectedSink = `experimental-sql`
		}

		counts := telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)
		require.Equal(t, int32(2), counts[`changefeed.create.sink.`+expectedSink])
		require.Equal(t, int32(2), counts[`changefeed.create.format.json`])
		require.Equal(t, int32(1), counts[`changefeed.create.num_tables.1`])
		require.Equal(t, int32(1), counts[`changefeed.create.num_tables.2`])
	}

	cdcTest(t, testFn, feedTestForceSink("sinkless"))
	cdcTest(t, testFn, feedTestForceSink("enterprise"))
}

func TestChangefeedContinuousTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		// Hack: since setting a zero value disabled, set a negative value to ensure we always log.
		interval := -10 * time.Millisecond
		continuousTelemetryInterval.Override(context.Background(), &s.Server.ClusterSettings().SV, interval)

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY)`)
		// NB: In order for this test to work for sinkless feeds, we must
		// have at least one row before creating the feed.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (-1)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, foo)
		var jobID jobspb.JobID
		if foo, ok := foo.(cdctest.EnterpriseTestFeed); ok {
			jobID = foo.JobID()
		}

		for i := 0; i < 5; i++ {
			beforeCreate := timeutil.Now()
			sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO foo VALUES (%d) RETURNING cluster_logical_timestamp()`, i))
			verifyLogsWithEmittedBytesAndMessages(t, jobID, beforeCreate.UnixNano(), interval.Nanoseconds(), false)
		}
	}

	cdcTest(t, testFn)
}

func TestChangefeedContinuousTelemetryOnTermination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		interval := 24 * time.Hour
		continuousTelemetryInterval.Override(context.Background(), &s.Server.ClusterSettings().SV, interval)
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY)`)
		// NB: In order for this test to work for sinkless feeds, we must
		// have at least one row before creating the feed.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		var seen atomic.Bool
		waitForIncEmittedCounters := func() error {
			if !seen.Load() {
				return errors.Newf("emitted counters have not been incremented yet")
			}
			return nil
		}
		var numPeriodicTelemetryLogger atomic.Int32
		// Synchronization to prevent a race between the changefeed closing
		// and the telemetry logger getting emitted counts after messages
		// have been emitted to the sink.
		s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs).
			WrapTelemetryLogger = func(logger telemetryLogger) telemetryLogger {
			return &testTelemetryLogger{
				telemetryLogger: logger,
				afterIncEmittedCounters: func(numMessages int, _ int) {
					if numMessages > 0 {
						seen.Store(true)
					}
				},
				id: numPeriodicTelemetryLogger.Add(1),
			}
		}

		// Insert a row and wait for logs to be created.
		beforeFirstLog := timeutil.Now()
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		var jobID jobspb.JobID
		if foo, ok := foo.(cdctest.EnterpriseTestFeed); ok {
			jobID = foo.JobID()
		}
		testutils.SucceedsSoon(t, waitForIncEmittedCounters)
		verifyLogsWithEmittedBytesAndMessages(t, jobID, beforeFirstLog.UnixNano(), interval.Nanoseconds(), false /* closing */)

		// Insert more rows. No logs should be created for these since we recently
		// published them above and the interval is 24h.
		afterFirstLog := timeutil.Now()
		seen.Store(false)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"id": 1}}`,
			`foo: [2]->{"after": {"id": 2}}`,
			`foo: [3]->{"after": {"id": 3}}`,
		})
		testutils.SucceedsSoon(t, waitForIncEmittedCounters)

		// Close the changefeed and ensure logs were created after closing.
		require.NoError(t, foo.Close())

		if numPeriodicTelemetryLogger.Load() > 1 {
			t.Log("transient error")
		}

		verifyLogsWithEmittedBytesAndMessages(
			t, jobID, afterFirstLog.UnixNano(), interval.Nanoseconds(), true, /* closing */
		)
	}

	cdcTest(t, testFn)
}

func TestChangefeedContinuousTelemetryDifferentJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		// Hack: since setting a zero value disabled, set a negative value to ensure we always log.
		interval := -100 * time.Millisecond
		continuousTelemetryInterval.Override(context.Background(), &s.Server.ClusterSettings().SV, interval)
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE foo2 (id INT PRIMARY KEY)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		foo2 := feed(t, f, `CREATE CHANGEFEED FOR foo2`)
		job1 := foo.(cdctest.EnterpriseTestFeed).JobID()
		job2 := foo2.(cdctest.EnterpriseTestFeed).JobID()

		beforeInsert := timeutil.Now()
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `INSERT INTO foo2 VALUES (1)`)
		verifyLogsWithEmittedBytesAndMessages(t, job1, beforeInsert.UnixNano(), interval.Nanoseconds(), false)
		verifyLogsWithEmittedBytesAndMessages(t, job2, beforeInsert.UnixNano(), interval.Nanoseconds(), false)
		require.NoError(t, foo.Close())

		beforeInsert = timeutil.Now()
		sqlDB.Exec(t, `INSERT INTO foo2 VALUES (2)`)
		verifyLogsWithEmittedBytesAndMessages(t, job2, beforeInsert.UnixNano(), interval.Nanoseconds(), false)
		require.NoError(t, foo2.Close())
	}

	cdcTest(t, testFn, feedTestOmitSinks("sinkless"))
}

func TestDistSenderRangeFeedPopulatesVirtualTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	scanner := keysutil.MakePrettyScanner(nil, nil)

	observeTables := func(sqlDB *sqlutils.SQLRunner, codec keys.SQLCodec) []int {
		rows := sqlDB.Query(t, "SELECT range_start FROM crdb_internal.active_range_feeds")
		defer rows.Close()
		var tableIDs []int
		for rows.Next() {
			var prettyKey string
			require.NoError(t, rows.Scan(&prettyKey))
			key, err := scanner.Scan(prettyKey)
			require.NoError(t, err)
			_, tableID, err := codec.DecodeTablePrefix(key)
			require.NoError(t, err)
			tableIDs = append(tableIDs, int(tableID))
		}
		return tableIDs
	}

	cases := []struct {
		user           string
		shouldSeeTable bool
	}{
		{`feedCreator`, false},
		{`regularUser`, false},
		{`adminUser`, true},
		{`viewClusterMetadataUser`, true},
	}

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Creates several different tables, users, and roles for us to use.
		ChangefeedJobPermissionsTestSetup(t, s)

		var tableID int
		sqlDB.QueryRow(t, "SELECT table_id FROM crdb_internal.tables WHERE name = 'table_a'").Scan(&tableID)

		var cf cdctest.TestFeed
		asUser(t, f, `feedCreator`, func(userDB *sqlutils.SQLRunner) {
			cf = feed(t, f, `CREATE CHANGEFEED FOR table_a;`)
		})
		defer closeFeed(t, cf)

		for _, c := range cases {
			testutils.SucceedsSoon(t, func() error {
				asUser(t, f, c.user, func(userDB *sqlutils.SQLRunner) {
					tableIDs := observeTables(userDB, s.Codec)
					if c.shouldSeeTable {
						require.Containsf(t, tableIDs, tableID, "user %s should see table %d", c.user, tableID)
					} else {
						require.Emptyf(t, tableIDs, "user %s should not see any tables", c.user)
					}
				})
				return nil
			})
		}

	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestCreateChangefeedTelemetryLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
	sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)

	t.Run(`core_sink_type`, func(t *testing.T) {
		coreFeedFactory, cleanup := makeFeedFactory(t, "sinkless", s.Server, s.DB)
		defer cleanup()

		beforeCreateSinkless := timeutil.Now()
		coreFeed := feed(t, coreFeedFactory, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, coreFeed)

		createLogs := checkCreateChangefeedLogs(t, beforeCreateSinkless.UnixNano())
		require.Equal(t, 1, len(createLogs))
		require.Equal(t, "core", createLogs[0].SinkType)
	})

	t.Run(`gcpubsub_sink_type_with_options`, func(t *testing.T) {
		pubsubFeedFactory := makePubsubFeedFactory(s.Server, s.DB)
		beforeCreatePubsub := timeutil.Now()
		pubsubFeed := feed(t, pubsubFeedFactory, `CREATE CHANGEFEED FOR foo, bar WITH resolved="10s", no_initial_scan`)
		defer closeFeed(t, pubsubFeed)

		createLogs := checkCreateChangefeedLogs(t, beforeCreatePubsub.UnixNano())
		require.Equal(t, 1, len(createLogs))
		require.Equal(t, `gcpubsub`, createLogs[0].SinkType)
		require.Equal(t, int32(2), createLogs[0].NumTables)
		require.Equal(t, `10s`, createLogs[0].Resolved)
		require.Equal(t, `no`, createLogs[0].InitialScan)
		require.Equal(t, false, createLogs[0].Transformation)
	})

	t.Run(`with_transformation`, func(t *testing.T) {
		pubsubFeedFactory := makePubsubFeedFactory(s.Server, s.DB)
		beforeCreateWithTransformation := timeutil.Now()
		pubsubFeed := feed(t, pubsubFeedFactory, `CREATE CHANGEFEED AS SELECT b FROM foo`)
		defer closeFeed(t, pubsubFeed)

		createLogs := checkCreateChangefeedLogs(t, beforeCreateWithTransformation.UnixNano())
		require.Equal(t, 1, len(createLogs))
		require.Equal(t, true, createLogs[0].Transformation)
	})
}

func TestAlterChangefeedTelemetryLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cdcTest(t, func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		beforeCreate := timeutil.Now()
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`)
		defer closeFeed(t, testFeed)
		feed := testFeed.(cdctest.EnterpriseTestFeed)

		// Alter changefeed to drop bar as a target and set resolved.
		require.NoError(t, feed.Pause())
		sqlDB.Exec(t, `ALTER CHANGEFEED $1 DROP bar SET resolved`, feed.JobID())
		require.NoError(t, feed.Resume())

		var logs []eventpb.AlterChangefeed
		testutils.SucceedsSoon(t, func() error {
			logs = checkAlterChangefeedLogs(t, beforeCreate.UnixNano())
			if len(logs) < 1 {
				return errors.New("no logs found")
			}
			return nil
		})

		require.Len(t, logs, 1)
		l := logs[0]
		require.EqualValues(t, feed.JobID(), l.JobId)
		require.Equal(t, `alter_changefeed`, l.EventType)
		require.Contains(t, l.PreviousDescription, `bar`)
		require.NotContains(t, l.Description, `bar`)
		require.Equal(t, "yes", l.Resolved)
	}, feedTestEnterpriseSinks)
}

// Note that closeFeed needs to be called in order for the logs to be detected
func TestChangefeedFailedTelemetryLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	waitForLogs := func(t *testing.T, startTime time.Time) []eventpb.ChangefeedFailed {
		var logs []eventpb.ChangefeedFailed
		testutils.SucceedsSoon(t, func() error {
			logs = checkChangefeedFailedLogs(t, startTime.UnixNano())
			if len(logs) < 1 {
				return fmt.Errorf("no logs found")
			}
			return nil
		})
		return logs
	}

	t.Run(`connection_closed`, func(t *testing.T) {
		s, stopServer := makeServer(t, withAllowChangefeedErr("expects error"))
		defer stopServer()

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		coreFactory, sinkCleanup := makeFeedFactory(t, "sinkless", s.Server, s.DB)
		coreFeed := feed(t, coreFactory, `CREATE CHANGEFEED FOR foo`)
		assertPayloads(t, coreFeed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "updated"}}`,
		})
		beforeCoreSinkClose := timeutil.Now()

		sinkCleanup()
		closeFeed(t, coreFeed)

		failLogs := waitForLogs(t, beforeCoreSinkClose)
		require.Equal(t, 1, len(failLogs))
		require.Equal(t, failLogs[0].FailureType, changefeedbase.ConnectionClosed)
	})

	cdcTestNamed(t, "user_input", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		beforeCreate := timeutil.Now()
		_, err := f.Feed(`CREATE CHANGEFEED FOR foo, invalid_table`)
		require.Error(t, err)

		failLogs := waitForLogs(t, beforeCreate)
		require.Equal(t, 1, len(failLogs))
		require.Equal(t, failLogs[0].FailureType, changefeedbase.UserInput)
	}, feedTestEnterpriseSinks, withAllowChangefeedErr("expects error"))

	cdcTestNamed(t, "unknown_error", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.BeforeEmitRow = func(_ context.Context) error {
			return changefeedbase.WithTerminalError(errors.New("should fail"))
		}

		beforeCreate := timeutil.Now()
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH on_error=FAIL`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'next')`)
		feedJob := foo.(cdctest.EnterpriseTestFeed)
		require.NoError(t, feedJob.WaitForState(func(s jobs.State) bool { return s == jobs.StateFailed }))

		closeFeed(t, foo)
		failLogs := waitForLogs(t, beforeCreate)
		require.Equal(t, 1, len(failLogs))
		require.Equal(t, failLogs[0].FailureType, changefeedbase.UnknownError)
		require.Contains(t, []string{`gcpubsub`, `external`}, failLogs[0].SinkType)
		require.Equal(t, failLogs[0].NumTables, int32(1))
	}, feedTestForceSink("pubsub"), withAllowChangefeedErr("expects error"))
}

func TestChangefeedCanceledTelemetryLogs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	waitForLogs := func(t *testing.T, startTime time.Time) []eventpb.ChangefeedCanceled {
		var logs []eventpb.ChangefeedCanceled
		testutils.SucceedsSoon(t, func() error {
			logs = checkChangefeedCanceledLogs(t, startTime.UnixNano())
			if len(logs) < 1 {
				return fmt.Errorf("no logs found")
			}
			return nil
		})
		return logs
	}

	cdcTestNamed(t, "canceled enterprise changefeeds", func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		beforeCreate := timeutil.Now()
		feed, err := f.Feed(`CREATE CHANGEFEED FOR foo`)
		require.NoError(t, err)
		enterpriseFeed := feed.(cdctest.EnterpriseTestFeed)

		sqlDB.Exec(t, `CANCEL JOB $1`, enterpriseFeed.JobID())

		canceledLogs := waitForLogs(t, beforeCreate)
		require.Equal(t, 1, len(canceledLogs))
		require.Equal(t, enterpriseFeed.JobID().String(), strconv.FormatInt(canceledLogs[0].JobId, 10))
		require.Equal(t, "changefeed_canceled", canceledLogs[0].EventType)
		require.NoError(t, feed.Close())
	}, feedTestEnterpriseSinks)
}

// Regression for #85902.
func TestRedactedSchemaRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE test_table (id INT PRIMARY KEY, i int, j int)`)

		userInfoToRedact := "7JHKUXMWYD374NV:secret-key"
		registryURI := fmt.Sprintf("https://%s@psrc-x77pq.us-central1.gcp.confluent.cloud:443", userInfoToRedact)

		changefeedDesc := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE test_table WITH updated,
					confluent_schema_registry =
					"%s";`, registryURI)
		registryURIWithRedaction := strings.Replace(registryURI, userInfoToRedact, "redacted", 1)
		cf := feed(t, f, changefeedDesc)
		defer closeFeed(t, cf)

		var description string
		sqlDB.QueryRow(t, "SELECT description from [SHOW CHANGEFEED JOBS]").Scan(&description)

		assert.Contains(t, description, registryURIWithRedaction)
	}

	// kafka supports the confluent_schema_registry option.
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestChangefeedMetricsScopeNotice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()
	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, "CREATE table foo (i int)")
	sqlDB.Exec(t, `SET CLUSTER SETTING server.child_metrics.enabled = false`)

	sqlCreate := "CREATE CHANGEFEED FOR d.foo INTO 'null://' WITH metrics_label='scope'"
	expectNotice(t, s.Server, sqlCreate, `server.child_metrics.enabled is set to false, metrics will only be published to the 'scope' label when it is set to true`)

	var jobID string
	sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] where job_type='CHANGEFEED'`).Scan(&jobID)
	sqlDB.Exec(t, "PAUSE JOB $1", jobID)
	sqlDB.CheckQueryResultsRetry(
		t,
		fmt.Sprintf(`SELECT count(*) FROM [SHOW JOBS] WHERE job_type='CHANGEFEED' AND status='%s'`, jobs.StatePaused),
		[][]string{{"1"}},
	)

	sqlAlter := fmt.Sprintf("ALTER CHANGEFEED %s SET metrics_label='other'", jobID)
	expectNotice(t, s.Server, sqlAlter, `server.child_metrics.enabled is set to false, metrics will only be published to the 'other' label when it is set to true`)
}

func TestChangefeedExecLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	str := strconv.Itoa

	const nodes = 4
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TODOTestTenantDisabled, // need nodelocal and splits.
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}
	for i := 0; i < nodes; i++ {
		args.ServerArgsPerNode[i] = base.TestServerArgs{
			ExternalIODir: path.Join(dir, str(i)),
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "x", Value: str(i / 2)}, {Key: "y", Value: str(i % 2)}}},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, args)
	defer tc.Stopper().Stop(ctx)
	tc.ToggleReplicateQueues(false)

	n2 := sqlutils.MakeSQLRunner(tc.Conns[1])

	// Setup a table with at least one range on each node to be sure we will see a
	// file from that node if it isn't excluded by filter. Relocate can fail with
	// errors like `change replicas... descriptor changed` thus the SucceedsSoon.
	n2.ExecMultiple(t,
		"SET CLUSTER SETTING kv.rangefeed.enabled = true",
		"CREATE TABLE x (id INT PRIMARY KEY)",
		"INSERT INTO x SELECT generate_series(1, 40)",
		"ALTER TABLE x SPLIT AT SELECT id FROM x WHERE id % 5 = 0",
	)
	for _, i := range []string{
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[1, 2, 3], 0)`,
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[1, 3, 4], 5)`,
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[2, 1, 3], 10)`,
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[2, 1, 4], 15)`,
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[3, 4, 2], 20)`,
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[3, 4, 1], 25)`,
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[4, 2, 1], 30)`,
		`ALTER TABLE x EXPERIMENTAL_RELOCATE VALUES (ARRAY[4, 2, 3], 35)`,
	} {
		n2.ExecSucceedsSoon(t, i)
	}

	test := func(t *testing.T, name, filter string, expect []bool) {
		t.Run(name, func(t *testing.T) {
			// Run and wait for the changefeed.
			var job int
			n2.QueryRow(t, "CREATE CHANGEFEED FOR x INTO $1 WITH initial_scan='only', execution_locality=$2",
				"nodelocal://0/"+name, filter).Scan(&job)
			n2.Exec(t, "SHOW JOB WHEN COMPLETE $1", job)
			// Now check each dir against expectation.
			filesSomewhere := false
			for i := range expect {
				where := path.Join(dir, str(i), name)
				x, err := os.ReadDir(where)
				filesHere := err == nil && len(x) > 0
				if !expect[i] {
					require.False(t, filesHere, where)
				}
				filesSomewhere = filesSomewhere || filesHere
			}
			require.True(t, filesSomewhere)
		})
	}

	test(t, "all", "", []bool{true, true, true, true})
	test(t, "x", "x=0", []bool{true, true, false, false})
	test(t, "y", "y=1", []bool{false, true, false, true})
}

// TestCloudstorageBufferedBytesMetric tests the metric which tracks the number
// of buffered bytes in the cloudstorage sink.
func TestCloudstorageBufferedBytesMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		defaultSLI, err := metrics.getSLIMetrics(defaultSLIScope)
		require.NoError(t, err)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		var shouldEmit atomic.Bool
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			return !shouldEmit.Load(), nil
		}
		db := sqlutils.MakeSQLRunner(s.DB)
		db.Exec(t, `
		  CREATE TABLE foo (key INT PRIMARY KEY);
		  INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000);
  		`)

		require.Equal(t, int64(0), defaultSLI.CloudstorageBufferedBytes.Value())

		format := "json"
		if rng.Float32() < 0.5 {
			format = "parquet"
		}
		foo, err := f.Feed(fmt.Sprintf("CREATE CHANGEFEED FOR TABLE foo WITH format='%s'", format))
		require.NoError(t, err)

		// Because checkpoints are disabled, we should have some bytes build up
		// in the sink.
		targetBytes := int64(40000)
		if format == "parquet" {
			// Parquet is a much more efficient format, so the buffered files will
			// be much smaller.
			targetBytes = 2000
		}
		testutils.SucceedsSoon(t, func() error {
			numBytes := defaultSLI.CloudstorageBufferedBytes.Value()
			if defaultSLI.CloudstorageBufferedBytes.Value() < targetBytes {
				return errors.Newf("expected at least %d buffered bytes but found %d", targetBytes, numBytes)
			}
			return nil
		})

		// Allow checkpoints to pass through and flush the sink. We should see
		// zero bytes buffered after that.
		shouldEmit.Store(true)
		testutils.SucceedsSoon(t, func() error {
			numBytes := defaultSLI.CloudstorageBufferedBytes.Value()
			if defaultSLI.CloudstorageBufferedBytes.Value() != 0 {
				return errors.Newf("expected at least %d buffered bytes but found %d", 0, numBytes)
			}
			return nil
		})

		require.NoError(t, foo.Close())
	}

	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}

// TestBatchSizeMetric the emitted batch size histogram metric.
func TestBatchSizeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		batchSizeHist := registry.MetricsStruct().Changefeed.(*Metrics).AggMetrics.EmittedBatchSizes

		db := sqlutils.MakeSQLRunner(s.DB)
		db.Exec(t, `
		  CREATE TABLE foo (key INT PRIMARY KEY);
		  INSERT INTO foo (key) VALUES (1), (2), (3);
		`)

		numSamples, sum := batchSizeHist.WindowedSnapshot().Total()
		require.Equal(t, int64(0), numSamples)
		require.Equal(t, 0.0, sum)

		foo, err := f.Feed("CREATE CHANGEFEED FOR TABLE foo")
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			numSamples, sum = batchSizeHist.WindowedSnapshot().Total()
			if numSamples <= 0 && sum <= 0.0 {
				return errors.Newf("waiting for metric %d %d", numSamples, sum)
			}
			return nil
		})
		require.NoError(t, foo.Close())
	}
	cdcTest(t, testFn)
}

// TestParallelIOMetrics tests parallel io metrics.
func TestParallelIOMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test relies on messing with timings to see pending rows build up,
	//  so skip it when the system is loaded.
	skip.UnderDuress(t)

	// Add delay so queuing occurs, which results in the below metrics being
	// nonzero.
	defer testingEnableQueuingDelay()()

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics).AggMetrics

		db := sqlutils.MakeSQLRunner(s.DB)
		db.Exec(t, `SET CLUSTER SETTING changefeed.sink_io_workers = 1`)
		db.Exec(t, `
		  CREATE TABLE foo (a INT PRIMARY KEY);
		`)

		// Keep writing data to the same key to ensure contention.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		g := ctxgroup.WithContext(ctx)
		done := make(chan struct{})
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-done:
					return nil
				default:
					_, err := s.DB.Exec(`UPSERT INTO foo (a)  SELECT * FROM generate_series(1, 10)`)
					if err != nil {
						return err
					}
				}
			}
		})
		// Set the frequency to 1s. The default frequency at the time of writing is
		foo, err := f.Feed("CREATE CHANGEFEED FOR TABLE foo WITH pubsub_sink_config=" +
			"'{\"Flush\": {\"Frequency\": \"100ms\"}}'")
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			numSamples, sum := metrics.ParallelIOPendingQueueNanos.WindowedSnapshot().Total()
			if numSamples <= 0 && sum <= 0.0 {
				return errors.Newf("waiting for queue nanos: %d %f", numSamples, sum)
			}
			return nil
		})
		testutils.SucceedsSoon(t, func() error {
			pendingKeys := metrics.ParallelIOPendingRows.Value()
			if pendingKeys <= 0 {
				return errors.Newf("waiting for pending keys: %d", pendingKeys)
			}
			return nil
		})
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < 50; i++ {
				inFlightKeys := metrics.ParallelIOInFlightKeys.Value()
				if inFlightKeys > 0 {
					return nil
				}
			}
			return errors.New("waiting for in-flight keys")
		})
		testutils.SucceedsSoon(t, func() error {
			numSamples, sum := metrics.ParallelIOResultQueueNanos.WindowedSnapshot().Total()
			if numSamples <= 0 && sum <= 0.0 {
				return errors.Newf("waiting for result queue nanos: %d %f", numSamples, sum)
			}
			return nil
		})
		close(done)
		require.NoError(t, g.Wait())
		require.NoError(t, foo.Close())
	}
	cdcTest(t, testFn, feedTestForceSink("pubsub"))
}

// TestSinkBackpressureMetric tests that the sink backpressure metric is recorded
// when quota limits are hit.
func TestSinkBackpressureMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics).AggMetrics

		db := sqlutils.MakeSQLRunner(s.DB)
		db.Exec(t, `SET CLUSTER SETTING changefeed.sink_io_workers = 1`)
		db.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		g := ctxgroup.WithContext(ctx)
		done := make(chan struct{})
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-done:
					return nil
				default:
					_, err := s.DB.Exec(`UPSERT INTO foo (a)  SELECT * FROM generate_series(1, 10)`)
					if err != nil {
						return err
					}
				}
			}
		})

		foo, err := f.Feed(`CREATE CHANGEFEED FOR TABLE foo WITH pubsub_sink_config='{"Flush": {"Frequency": "100ms"}}'`)
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			numSamples, sum := metrics.SinkBackpressureNanos.WindowedSnapshot().Total()
			if numSamples <= 0 && sum <= 0.0 {
				return errors.Newf("waiting for backpressure nanos: %d %f", numSamples, sum)
			}
			return nil
		})

		close(done)
		require.NoError(t, g.Wait())
		require.NoError(t, foo.Close())
	}
	cdcTest(t, testFn, feedTestForceSink("pubsub"))
}
