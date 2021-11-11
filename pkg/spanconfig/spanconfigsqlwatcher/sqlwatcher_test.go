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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		setup       string
		stmt        string
		expectedIDs descpb.IDs
	}{
		{
			stmt:        "CREATE TABLE t()",
			expectedIDs: descpb.IDs{52},
		},
		{
			setup:       "CREATE TABLE t2()",
			stmt:        "ALTER TABLE t2 CONFIGURE ZONE USING num_replicas = 3",
			expectedIDs: descpb.IDs{53},
		},
		{
			setup:       "CREATE DATABASE d; CREATE TABLE d.t1(); CREATE TABLE d.t2()",
			stmt:        "ALTER DATABASE d CONFIGURE ZONE USING num_replicas=5",
			expectedIDs: descpb.IDs{54},
		},
		{
			setup:       "CREATE TABLE t3(); CREATE TABLE t4()",
			stmt:        "ALTER TABLE t3 CONFIGURE ZONE USING num_replicas=5; CREATE TABLE t5(); DROP TABLE t4;",
			expectedIDs: descpb.IDs{57, 58, 59},
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
			setup: "CREATE DATABASE db",
			stmt:  "CREATE SCHEMA db.sc",
			// One ID each for the parent database and the schema.
			expectedIDs: descpb.IDs{60, 61},
		},
		{
			stmt: "CREATE TYPE typ AS ENUM()",
			// One ID each for the enum and the array type.
			expectedIDs: descpb.IDs{62, 63},
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

	sqlDB := tc.ServerConn(0 /* idx */)
	for _, tc := range testCases {
		sqlWatcher := spanconfigsqlwatcher.NewFactory(
			keys.SystemSQLCodec,
			ts.ClusterSettings(),
			ts.RangeFeedFactory().(*rangefeed.Factory),
			1<<20, /* 1 MB, bufferMemLimit */
			ts.Stopper(),
			nil, /* knobs */
		).New()

		_, err := sqlDB.Exec(tc.setup)
		require.NoError(t, err)

		// Save the startTS here before the test case is executed to ensure the
		// watcher can pick up the change when we start it in a separate goroutine.
		startTS := ts.Clock().Now()

		var mu struct {
			syncutil.Mutex
			receivedIDs map[descpb.ID]struct{}
		}
		mu.receivedIDs = make(map[descpb.ID]struct{})

		watcherCtx, watcherCancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			prevCheckpointTS := startTS
			_ = sqlWatcher.WatchForSQLUpdates(watcherCtx,
				startTS,
				func(ctx context.Context, updates []spanconfig.DescriptorUpdate, checkpointTS hlc.Timestamp) error {
					require.True(t, prevCheckpointTS.Less(checkpointTS))
					mu.Lock()
					defer mu.Unlock()
					for _, update := range updates {
						mu.receivedIDs[update.ID] = struct{}{}
					}
					prevCheckpointTS = checkpointTS
					return nil
				})
		}()

		_, err = sqlDB.Exec(tc.stmt)
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			mu.Lock()
			defer mu.Unlock()
			if len(mu.receivedIDs) == len(tc.expectedIDs) {
				return nil
			}
			return errors.Newf("expected to receive %d IDs, but found %d", len(tc.expectedIDs), len(mu.receivedIDs))
		})

		// Rangefeed events aren't guaranteed to be in any particular order for
		// different keys.
		mu.Lock()
		for _, id := range tc.expectedIDs {
			_, seen := mu.receivedIDs[id]
			require.True(t, seen)
		}
		mu.Unlock()

		// Stop the watcher and wait for the goroutine to complete.
		watcherCancel()
		wg.Wait()
	}
}

// TestSQLWatcherFactory tests that the SQLWatcherFactory can create multiple
// SQLWatchers and that every SQLWatcher is only good for a single
// WatchForSQLUpdates.
func TestSQLWatcherFactory(t *testing.T) {
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
	sqlDB := tc.ServerConn(0 /* idx */)

	sqlWatcherFactory := spanconfigsqlwatcher.NewFactory(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		nil, /* knobs */
	)

	startTS := ts.Clock().Now()
	_, err := sqlDB.Exec("CREATE TABLE t()")
	require.NoError(t, err)

	sqlWatcher := sqlWatcherFactory.New()

	var wg sync.WaitGroup

	watcherCtx, watcherCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = sqlWatcher.WatchForSQLUpdates(watcherCtx, startTS, func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error {
			t.Error("handler should never run")
			return nil
		})
	}()

	watcherCancel()
	wg.Wait()

	err = sqlWatcher.WatchForSQLUpdates(watcherCtx, startTS, func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error {
		t.Fatal("handler should never run")
		return nil
	})
	require.Error(t, err)
	require.True(t, testutils.IsError(err, "watcher already started watching"))

	newSQLWatcher := sqlWatcherFactory.New()
	newWatcherCtx, newWatcherCancel := context.WithCancel(ctx)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = newSQLWatcher.WatchForSQLUpdates(newWatcherCtx, startTS, func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error {
			t.Error("handler should never run")
			return nil
		})
		require.True(t, testutils.IsError(err, "context canceled"))
	}()

	newWatcherCancel()
	wg.Wait()
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
	sqlDB := tc.ServerConn(0 /* idx */)

	sqlWatcherFactory := spanconfigsqlwatcher.NewFactory(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		&spanconfig.TestingKnobs{
			SQLWatcherOnEventInterceptor: func() error {
				return errors.New("boom")
			},
		},
	)

	startTS := ts.Clock().Now()
	_, err := sqlDB.Exec("CREATE TABLE t()")
	require.NoError(t, err)

	sqlWatcher := sqlWatcherFactory.New()
	err = sqlWatcher.WatchForSQLUpdates(ctx, startTS, func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error {
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
	sqlDB := tc.ServerConn(0 /* idx */)

	sqlWatcher := spanconfigsqlwatcher.NewFactory(
		keys.SystemSQLCodec,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		1<<20, /* 1 MB, bufferMemLimit */
		ts.Stopper(),
		nil, /* knobs */
	).New()

	_, err := sqlDB.Exec("CREATE TABLE t()")
	require.NoError(t, err)

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
			}
			_, err = sqlDB.Exec("ALTER TABLE t CONFIGURE ZONE USING num_replicas=5")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
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
