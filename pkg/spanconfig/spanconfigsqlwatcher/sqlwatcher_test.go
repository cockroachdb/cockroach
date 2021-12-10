// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSQLWatcherReactsToUpdates verifies that the SQLWatcher emits the correct
// updates following changes made to system.descriptor or system.zones. It also
// ensures that the timestamp supplied to the handler function is monotonically
// increasing.
func TestSQLWatcherReactsToUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		stmt        string
		expectedIDs descpb.IDs
	}{
		{
			stmt:        "CREATE TABLE t()",
			expectedIDs: descpb.IDs{54},
		},
		{
			stmt:        "CREATE TABLE t2(); ALTER TABLE t2 CONFIGURE ZONE USING num_replicas = 3",
			expectedIDs: descpb.IDs{55},
		},
		{
			stmt:        "CREATE DATABASE d; CREATE TABLE d.t1(); CREATE TABLE d.t2()",
			expectedIDs: descpb.IDs{56, 57, 58, 59},
		},
		{
			stmt:        "ALTER DATABASE d CONFIGURE ZONE USING num_replicas=5",
			expectedIDs: descpb.IDs{56},
		},
		{
			stmt:        "CREATE TABLE t3(); CREATE TABLE t4()",
			expectedIDs: descpb.IDs{60, 61},
		},
		{
			stmt:        "ALTER TABLE t3 CONFIGURE ZONE USING num_replicas=5; CREATE TABLE t5(); DROP TABLE t4;",
			expectedIDs: descpb.IDs{60, 61, 62},
		},
		// Named zone tests.
		{
			stmt:        "ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.RootNamespaceID},
		},
		{
			stmt:        "ALTER RANGE liveness CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.LivenessRangesID},
		},
		{
			stmt:        "ALTER RANGE meta CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.MetaRangesID},
		},
		{
			stmt:        "ALTER RANGE system CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.SystemRangesID},
		},
		{
			stmt:        "ALTER RANGE timeseries CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.TimeseriesRangesID},
		},
		// Test that events on types/schemas are also captured.
		{
			stmt: "CREATE DATABASE db; CREATE SCHEMA db.sc",
			// One ID each for the parent database, the public schema and the schema.
			expectedIDs: descpb.IDs{63, 64, 65},
		},
		{
			stmt: "CREATE TYPE typ AS ENUM()",
			// One ID each for the enum and the array type.
			expectedIDs: descpb.IDs{66, 67},
		},
	}

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation.
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speed up schema changes.
			},
		},
	})
	defer tc.Stopper().Stop(context.Background())

	ts := tc.Server(0 /* idx */)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)

	noopCheckpointDuration := 100 * time.Millisecond
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		noopCheckpointDuration,
		nil, /* knobs */
	)

	var mu struct {
		syncutil.Mutex
		receivedIDs    map[descpb.ID]struct{}
		lastCheckpoint hlc.Timestamp
	}
	mu.receivedIDs = make(map[descpb.ID]struct{})

	var wg sync.WaitGroup
	watcherStartTS := ts.Clock().Now()

	watcherCtx, watcherCancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()

		_ = sqlWatcher.WatchForSQLUpdates(watcherCtx, watcherStartTS,
			func(ctx context.Context, updates []spanconfig.DescriptorUpdate, checkpointTS hlc.Timestamp) error {
				mu.Lock()
				defer mu.Unlock()

				require.True(t, mu.lastCheckpoint.LessEq(checkpointTS))
				mu.lastCheckpoint = checkpointTS

				for _, update := range updates {
					mu.receivedIDs[update.ID] = struct{}{}
				}
				return nil
			})
	}()

	for _, tc := range testCases {
		tdb.Exec(t, tc.stmt)
		afterStmtTS := ts.Clock().Now()

		testutils.SucceedsSoon(t, func() error {
			mu.Lock()
			defer mu.Unlock()

			if mu.lastCheckpoint.Less(afterStmtTS) {
				return errors.New("checkpoint precedes statement timestamp")
			}
			return nil
		})

		// Rangefeed events aren't guaranteed to be in any particular order for
		// different keys.
		mu.Lock()
		require.Equal(t, len(tc.expectedIDs), len(mu.receivedIDs))
		for _, id := range tc.expectedIDs {
			_, seen := mu.receivedIDs[id]
			require.True(t, seen)
			delete(mu.receivedIDs, id)
		}
		mu.Unlock()
	}

	// Stop the watcher and wait for the goroutine to complete.
	watcherCancel()
	wg.Wait()
}

// TestSQLWatcherMultiple tests that we're able to fire off multiple sql watcher
// processes, both sequentially and concurrently.
func TestSQLWatcherMultiple(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation.
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speed up schema changes.
			},
		},
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0 /* idx */)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)

	noopCheckpointDuration := 100 * time.Millisecond
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		noopCheckpointDuration,
		nil, /* knobs */
	)

	beforeStmtTS := ts.Clock().Now()
	tdb.Exec(t, "CREATE TABLE t()")
	afterStmtTS := ts.Clock().Now()
	var expDescID descpb.ID
	row := tdb.QueryRow(t, `SELECT id FROM system.namespace WHERE name='t'`)
	row.Scan(&expDescID)

	var wg sync.WaitGroup
	mu := struct {
		syncutil.Mutex
		w1LastCheckpoint, w2LastCheckpoint, w3LastCheckpoint hlc.Timestamp
	}{}

	f := func(ctx context.Context, onExit func(), onCheckpoint func(hlc.Timestamp)) {
		defer wg.Done()

		receivedIDs := make(map[descpb.ID]struct{})
		err := sqlWatcher.WatchForSQLUpdates(ctx, beforeStmtTS,
			func(_ context.Context, updates []spanconfig.DescriptorUpdate, checkpointTS hlc.Timestamp) error {
				onCheckpoint(checkpointTS)

				for _, update := range updates {
					receivedIDs[update.ID] = struct{}{}
				}
				return nil
			})
		require.True(t, testutils.IsError(err, "context canceled"))
		require.Equal(t, 1, len(receivedIDs))
		_, seen := receivedIDs[expDescID]
		require.True(t, seen)
	}

	{
		// Run the first watcher; wait for it to observe the update before
		// tearing it down.

		watcher1Ctx, watcher1Cancel := context.WithCancel(ctx)
		wg.Add(1)
		go f(watcher1Ctx, func() { wg.Done() }, func(ts hlc.Timestamp) {
			mu.Lock()
			mu.w1LastCheckpoint = ts
			mu.Unlock()
		})

		testutils.SucceedsSoon(t, func() error {
			mu.Lock()
			defer mu.Unlock()

			if mu.w1LastCheckpoint.Less(afterStmtTS) {
				return errors.New("w1 checkpoint precedes statement timestamp")
			}
			return nil
		})
		watcher1Cancel()
		wg.Wait()
	}

	{
		// Run two more watchers; wait for each to independently observe the
		// update before tearing them down.

		watcher2Ctx, watcher2Cancel := context.WithCancel(ctx)
		wg.Add(1)
		go f(watcher2Ctx, func() { wg.Done() }, func(ts hlc.Timestamp) {
			mu.Lock()
			mu.w2LastCheckpoint = ts
			mu.Unlock()
		})

		watcher3Ctx, watcher3Cancel := context.WithCancel(ctx)
		wg.Add(1)
		go f(watcher3Ctx, func() { wg.Done() }, func(ts hlc.Timestamp) {
			mu.Lock()
			mu.w3LastCheckpoint = ts
			mu.Unlock()
		})

		testutils.SucceedsSoon(t, func() error {
			mu.Lock()
			defer mu.Unlock()

			if mu.w2LastCheckpoint.Less(afterStmtTS) {
				return errors.New("w2 checkpoint precedes statement timestamp")
			}
			if mu.w3LastCheckpoint.Less(afterStmtTS) {
				return errors.New("w3 checkpoint precedes statement timestamp")
			}
			return nil
		})

		watcher2Cancel()
		watcher3Cancel()
		wg.Wait()
	}
}

// TestSQLWatcherOnEventError ensures that if there is an error processing a
// rangefeed event the handler is never run and the error is bubbled back to
// the caller.
func TestSQLWatcherOnEventError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation.
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speed up schema changes.
			},
		},
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0 /* idx */)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)

	noopCheckpointDuration := time.Hour // effectively disable no-op checkpoints
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		noopCheckpointDuration,
		&spanconfig.TestingKnobs{
			SQLWatcherOnEventInterceptor: func() error {
				return errors.New("boom")
			},
		},
	)

	startTS := ts.Clock().Now()
	tdb.Exec(t, "CREATE TABLE t()")

	err := sqlWatcher.WatchForSQLUpdates(ctx, startTS, func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error {
		t.Fatal("handler should never run")
		return nil
	})
	require.Error(t, err)
	require.True(t, testutils.IsError(err, "boom"))
}

// TestSQLWatcherHandlerError ensures that no further calls are made to the
// handler after it returns an error.
func TestSQLWatcherHandlerError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation.
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speed up schema changes.
			},
		},
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0 /* idx */)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)

	noopCheckpointDuration := 100 * time.Millisecond
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		noopCheckpointDuration,
		nil, /* knobs */
	)

	tdb.Exec(t, "CREATE TABLE t()")

	stopTraffic := make(chan struct{})
	startTS := ts.Clock().Now()

	var wg sync.WaitGroup
	wg.Add(1)

	// Start a background thread that modifies zone configs on a table to trigger
	// events on system.zones.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopTraffic:
				return
			default:
				time.Sleep(100 * time.Millisecond)
			}
			tdb.Exec(t, "ALTER TABLE t CONFIGURE ZONE USING num_replicas=5")
		}
	}()

	var numCalled int32
	// Wrap the call to WatchForSQLUpdates in a SucceedsSoon to ensure it
	// evaluates within 45 seconds.
	testutils.SucceedsSoon(t, func() error {
		err := sqlWatcher.WatchForSQLUpdates(ctx, startTS, func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error {
			atomic.AddInt32(&numCalled, 1)
			return errors.New("handler error")
		})
		require.Error(t, err)
		require.True(t, testutils.IsError(err, "handler error"))
		return nil
	})

	// Shutdown the background goroutine.
	close(stopTraffic)
	wg.Wait()

	// Ensure that the handler was called only once.
	require.Equal(t, int32(1), atomic.LoadInt32(&numCalled))
}

func TestWatcherReceivesNoopCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation.
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speed up schema changes.
			},
		},
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0 /* idx */)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)

	noopCheckpointDuration := 25 * time.Millisecond
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		noopCheckpointDuration,
		nil, /* knobs */
	)

	beforeStmtTS := ts.Clock().Now()
	tdb.Exec(t, "CREATE TABLE t()")
	afterStmtTS := ts.Clock().Now()
	var expDescID descpb.ID
	row := tdb.QueryRow(t, `SELECT id FROM system.namespace WHERE name='t'`)
	row.Scan(&expDescID)

	var wg sync.WaitGroup
	mu := struct {
		syncutil.Mutex
		numCheckpoints int
		lastCheckpoint hlc.Timestamp
	}{}

	watch := func(ctx context.Context, onExit func(), onCheckpoint func(hlc.Timestamp)) {
		defer wg.Done()

		receivedIDs := make(map[descpb.ID]struct{})
		err := sqlWatcher.WatchForSQLUpdates(ctx, beforeStmtTS,
			func(_ context.Context, updates []spanconfig.DescriptorUpdate, checkpointTS hlc.Timestamp) error {
				onCheckpoint(checkpointTS)

				for _, update := range updates {
					receivedIDs[update.ID] = struct{}{}
				}
				return nil
			})
		require.True(t, testutils.IsError(err, "context canceled"))
		require.Equal(t, 1, len(receivedIDs))
		_, seen := receivedIDs[expDescID]
		require.True(t, seen)
	}

	{
		// Run the first watcher; wait for it to observe the update before
		// tearing it down.

		watcherCtx, watcherCancel := context.WithCancel(ctx)
		wg.Add(1)
		go watch(watcherCtx, func() { wg.Done() }, func(ts hlc.Timestamp) {
			mu.Lock()
			mu.lastCheckpoint = ts
			mu.numCheckpoints++
			mu.Unlock()
		})

		testutils.SucceedsSoon(t, func() error {
			mu.Lock()
			defer mu.Unlock()

			if mu.lastCheckpoint.Less(afterStmtTS) {
				return errors.New("last checkpoint precedes statement timestamp")
			}
			if mu.numCheckpoints < 3 {
				return errors.New("didn't receive no-op checkpoints")
			}
			return nil
		})
		watcherCancel()
		wg.Wait()
	}
}
