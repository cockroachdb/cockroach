// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ie := s.InternalExecutor().(sqlutil.InternalExecutor)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	setup := func(t *testing.T) (
		*hlc.Clock, *timeutil.ManualTime, *cluster.Settings, *stop.Stopper, *slstorage.Storage,
	) {
		dbName := t.Name()
		tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
		schema := strings.Replace(systemschema.SqllivenessTableSchema,
			`CREATE TABLE system.sqlliveness`,
			`CREATE TABLE "`+dbName+`".sqlliveness`, 1)
		tDB.Exec(t, schema)

		timeSource := timeutil.NewManualTime(t0)
		clock := hlc.NewClock(func() int64 {
			return timeSource.Now().UnixNano()
		}, base.DefaultMaxClockOffset)
		settings := cluster.MakeTestingClusterSettings()
		stopper := stop.NewStopper()
		storage := slstorage.NewTestingStorage(stopper, clock, kvDB, ie, settings,
			dbName, timeSource.NewTimer)
		return clock, timeSource, settings, stopper, storage
	}

	t.Run("basic-insert-is-alive", func(t *testing.T) {
		clock, _, _, stopper, storage := setup(t)
		defer stopper.Stop(ctx)

		exp := clock.Now().Add(time.Second.Nanoseconds(), 0)
		const id = "asdf"
		metrics := storage.Metrics()

		{
			require.NoError(t, storage.Insert(ctx, id, exp))
			require.Equal(t, int64(1), metrics.WriteSuccesses.Count())
		}
		{
			isAlive, err := storage.IsAlive(ctx, id)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(1), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(0), metrics.IsAliveCacheHits.Count())
		}
		{
			isAlive, err := storage.IsAlive(ctx, id)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(1), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(1), metrics.IsAliveCacheHits.Count())
		}
	})
	t.Run("delete-update", func(t *testing.T) {
		clock, timeSource, settings, stopper, storage := setup(t)
		defer stopper.Stop(ctx)
		storage.Start(ctx)
		metrics := storage.Metrics()

		// GC will run right on startup and then will not run again until gc
		// interval passes.
		gcInterval := slstorage.DefaultGCInterval.Get(&settings.SV)
		nextGC := timeSource.Now().Add(gcInterval)
		testutils.SucceedsSoon(t, func() error {
			timers := timeSource.Timers()
			if len(timers) != 1 {
				return errors.Errorf("expected 1 timer, saw %d", len(timers))
			}
			require.Equal(t, []time.Time{nextGC}, timers)
			return nil
		})
		// Ensure that we saw the first gc run.
		require.Equal(t, int64(1), metrics.SessionDeletionsRuns.Count())

		// Create two records which will expire before nextGC.
		exp := clock.Now().Add(gcInterval.Nanoseconds()-1, 0)
		const id1 = "asdf"
		const id2 = "ghjk"
		{
			require.NoError(t, storage.Insert(ctx, id1, exp))
			require.NoError(t, storage.Insert(ctx, id2, exp))
			require.Equal(t, int64(2), metrics.WriteSuccesses.Count())
		}

		// Verify they are alive.
		{
			isAlive1, err := storage.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.True(t, isAlive1)
			isAlive2, err := storage.IsAlive(ctx, id2)
			require.NoError(t, err)
			require.True(t, isAlive2)
			require.Equal(t, int64(2), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(0), metrics.IsAliveCacheHits.Count())
		}

		// Update the expiration for id2.
		{
			exists, err := storage.Update(ctx, id2, hlc.Timestamp{WallTime: nextGC.UnixNano() + 1})
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, int64(3), metrics.WriteSuccesses.Count())
		}

		// Ensure that the cached value is still in use for id2.
		{
			isAlive, err := storage.IsAlive(ctx, id2)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(2), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(1), metrics.IsAliveCacheHits.Count())
		}

		// Advance time to nextGC, wait for there to be a new timer.
		timeSource.Advance(gcInterval)
		followingGC := nextGC.Add(gcInterval)
		testutils.SucceedsSoon(t, func() error {
			timers := timeSource.Timers()
			if len(timers) != 1 {
				return errors.Errorf("expected 1 timer, saw %d", len(timers))
			}
			if timers[0].Equal(followingGC) {
				return nil
			}
			return errors.Errorf("expected %v, saw %v", followingGC, timers[0])
		})
		// Ensure that we saw the second gc run and the deletion.
		require.Equal(t, int64(2), metrics.SessionDeletionsRuns.Count())
		require.Equal(t, int64(1), metrics.SessionsDeleted.Count())

		// Ensure that we now see the id1 as dead.
		{
			isAlive, err := storage.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(3), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(1), metrics.IsAliveCacheHits.Count())
		}
		// Ensure that the fact that it's dead is cached.
		{
			isAlive, err := storage.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(3), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(2), metrics.IsAliveCacheHits.Count())
		}
		// Ensure that attempts to update the now dead session fail.
		{
			exists, err := storage.Update(ctx, id1, hlc.Timestamp{WallTime: nextGC.UnixNano() + 1})
			require.NoError(t, err)
			require.False(t, exists)
			require.Equal(t, int64(1), metrics.WriteFailures.Count())
		}

		// Ensure that we now see the id2 as alive.
		{
			isAlive, err := storage.IsAlive(ctx, id2)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(4), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(2), metrics.IsAliveCacheHits.Count())
		}
		// Ensure that the fact that it's still alive is cached.
		{
			isAlive, err := storage.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(4), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(3), metrics.IsAliveCacheHits.Count())
		}
	})
}
