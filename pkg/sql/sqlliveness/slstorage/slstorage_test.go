// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slstorage_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	setup := func(t *testing.T) (
		*hlc.Clock, *timeutil.ManualTime, *cluster.Settings, *stop.Stopper, *slstorage.Storage,
	) {
		table := newSqllivenessTable(t, tDB, s)
		timeSource := timeutil.NewManualTime(t0)
		clock := hlc.NewClockForTesting(timeSource)
		settings := cluster.MakeTestingClusterSettings()
		stopper := stop.NewStopper(stop.WithTracer(s.TracerI().(*tracing.Tracer)))
		var ambientCtx log.AmbientContext
		storage := slstorage.NewTestingStorage(ambientCtx, stopper, clock, kvDB, s.Codec(), settings,
			s.SettingsWatcher().(*settingswatcher.SettingsWatcher), table, timeSource.NewTimer, true /*withSyntheticClock*/)
		return clock, timeSource, settings, stopper, storage
	}

	t.Run("basic-insert-is-alive", func(t *testing.T) {
		clock, _, _, stopper, storage := setup(t)
		storage.Start(ctx)
		defer stopper.Stop(ctx)
		reader := storage.BlockingReader()

		exp := clock.Now().Add(time.Second.Nanoseconds(), 0)
		id, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		metrics := storage.Metrics()

		{
			require.NoError(t, storage.Insert(ctx, id, exp))
			require.Equal(t, int64(1), metrics.WriteSuccesses.Count())
		}
		{
			isAlive, err := reader.IsAlive(ctx, id)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(1), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(0), metrics.IsAliveCacheHits.Count())
		}
		{
			isAlive, err := reader.IsAlive(ctx, id)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(1), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(1), metrics.IsAliveCacheHits.Count())
		}
	})
	t.Run("delete-update", func(t *testing.T) {
		clock, timeSource, settings, stopper, storage := setup(t)
		defer stopper.Stop(ctx)
		slstorage.GCJitter.Override(ctx, &settings.SV, 0)
		storage.Start(ctx)
		reader := storage.BlockingReader()
		metrics := storage.Metrics()

		// GC will run some time after startup.
		gcInterval := slstorage.GCInterval.Get(&settings.SV)
		nextGC := timeSource.Now().Add(gcInterval)
		testutils.SucceedsSoon(t, func() error {
			timers := timeSource.Timers()
			if len(timers) != 1 {
				return errors.Errorf("expected 1 timer, saw %d", len(timers))
			}
			require.Equal(t, []time.Time{nextGC}, timers)
			return nil
		})
		require.Equal(t, int64(0), metrics.SessionDeletionsRuns.Count())

		// Create two records which will expire before nextGC.
		exp := clock.Now().Add(gcInterval.Nanoseconds()-1, 0)
		id1, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		id2, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		{
			require.NoError(t, storage.Insert(ctx, id1, exp))
			require.NoError(t, storage.Insert(ctx, id2, exp))
			require.Equal(t, int64(2), metrics.WriteSuccesses.Count())
		}

		// Verify they are alive.
		{
			isAlive1, err := reader.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.True(t, isAlive1)
			isAlive2, err := reader.IsAlive(ctx, id2)
			require.NoError(t, err)
			require.True(t, isAlive2)
			require.Equal(t, int64(2), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(0), metrics.IsAliveCacheHits.Count())
		}

		// Update the expiration for id2.
		{
			exists, _, err := storage.Update(ctx, id2, hlc.Timestamp{WallTime: nextGC.UnixNano() + 1})
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, int64(3), metrics.WriteSuccesses.Count())
		}

		// Ensure that the cached value is still in use for id2.
		{
			isAlive, err := reader.IsAlive(ctx, id2)
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
		require.Equal(t, int64(1), metrics.SessionDeletionsRuns.Count())
		require.Equal(t, int64(1), metrics.SessionsDeleted.Count())

		// Ensure that we now see the id1 as dead. That fact will be cached.
		{
			isAlive, err := reader.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(2), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(2), metrics.IsAliveCacheHits.Count())
		}
		// Ensure that the fact that it's dead is cached.
		{
			isAlive, err := reader.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(2), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(3), metrics.IsAliveCacheHits.Count())
		}
		// Ensure that attempts to update the now dead session fail.
		{
			exists, _, err := storage.Update(ctx, id1, hlc.Timestamp{WallTime: nextGC.UnixNano() + 1})
			require.NoError(t, err)
			require.False(t, exists)
			require.Equal(t, int64(1), metrics.WriteFailures.Count())
		}

		// Ensure that we now see the id2 as alive.
		{
			isAlive, err := reader.IsAlive(ctx, id2)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(3), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(3), metrics.IsAliveCacheHits.Count())
		}
		// Ensure that the fact that it's still alive is cached.
		{
			isAlive, err := reader.IsAlive(ctx, id1)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(3), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(4), metrics.IsAliveCacheHits.Count())
		}
	})
	t.Run("delete-expired-on-is-alive", func(t *testing.T) {
		clock, timeSource, _, stopper, storage := setup(t)
		defer stopper.Stop(ctx)
		storage.Start(ctx)
		reader := storage.BlockingReader()

		exp := clock.Now().Add(time.Second.Nanoseconds(), 0)
		id, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		metrics := storage.Metrics()

		{
			require.NoError(t, storage.Insert(ctx, id, exp))
			require.Equal(t, int64(1), metrics.WriteSuccesses.Count())
		}
		{
			isAlive, err := reader.IsAlive(ctx, id)
			require.NoError(t, err)
			require.True(t, isAlive)
			require.Equal(t, int64(1), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(0), metrics.IsAliveCacheHits.Count())
			require.Equal(t, int64(0), metrics.SessionsDeleted.Count())
		}
		// Advance to the point where the session is expired.
		timeSource.Advance(time.Second + time.Nanosecond)
		// Ensure that we discover it is no longer alive.
		{
			isAlive, err := reader.IsAlive(ctx, id)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(2), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(0), metrics.IsAliveCacheHits.Count())
			require.Equal(t, int64(1), metrics.SessionsDeleted.Count())
		}
		// Ensure that the fact that it is no longer alive is cached.
		{
			isAlive, err := reader.IsAlive(ctx, id)
			require.NoError(t, err)
			require.False(t, isAlive)
			require.Equal(t, int64(2), metrics.IsAliveCacheMisses.Count())
			require.Equal(t, int64(1), metrics.IsAliveCacheHits.Count())
			require.Equal(t, int64(1), metrics.SessionsDeleted.Count())
		}
		// Ensure it cannot be updated.
		{
			exists, _, err := storage.Update(ctx, id, exp.Add(time.Second.Nanoseconds(), 0))
			require.NoError(t, err)
			require.False(t, exists)
			require.Equal(t, int64(1), metrics.WriteFailures.Count())
		}
	})
	t.Run("test-jitter", func(t *testing.T) {
		// We want to test that the GC runs a number of times but is jitterred.
		_, timeSource, settings, stopper, storage := setup(t)
		defer stopper.Stop(ctx)
		storage.Start(ctx)
		waitForGCTimer := func() (timer time.Time) {
			testutils.SucceedsSoon(t, func() error {
				timers := timeSource.Timers()
				if len(timers) != 1 {
					return errors.Errorf("expected 1 timer, saw %d", len(timers))
				}
				timer = timers[0]
				return nil
			})
			return timer
		}
		const N = 10
		for i := 0; i < N; i++ {
			timer := waitForGCTimer()
			timeSource.Advance(timer.Sub(timeSource.Now()))
		}
		jitter := slstorage.GCJitter.Get(&settings.SV)
		interval := slstorage.GCInterval.Get(&settings.SV)
		storage.Start(ctx)
		minTime := t0.Add(time.Duration((1 - jitter) * float64(interval.Nanoseconds()) * N))
		maxTime := t0.Add(time.Duration((1 + jitter) * float64(interval.Nanoseconds()) * N))
		noJitterTime := t0.Add(interval * N)
		now := timeSource.Now()
		require.Truef(t, now.After(minTime), "%v > %v", now, minTime)
		require.Truef(t, now.Before(maxTime), "%v < %v", now, maxTime)
		require.Truef(t, !now.Equal(noJitterTime), "%v != %v", now, noJitterTime)
	})
}

func TestConcurrentAccessesAndEvictions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	table := newSqllivenessTable(t, tDB, s)

	timeSource := timeutil.NewManualTime(t0)
	clock := hlc.NewClockForTesting(timeSource)
	settings := cluster.MakeTestingClusterSettings()
	stopper := stop.NewStopper(stop.WithTracer(s.TracerI().(*tracing.Tracer)))
	defer stopper.Stop(ctx)
	slstorage.CacheSize.Override(ctx, &settings.SV, 10)
	var ambientCtx log.AmbientContext
	storage := slstorage.NewTestingStorage(ambientCtx, stopper, clock, kvDB, s.Codec(), settings,
		s.SettingsWatcher().(*settingswatcher.SettingsWatcher), table, timeSource.NewTimer, true /*withSyntheticClock*/)
	storage.Start(ctx)
	reader := storage.BlockingReader()

	const (
		runsPerWorker   = 100
		workers         = 100
		expiration      = time.Minute
		controllerSteps = 100
	)
	type session struct {
		id         sqlliveness.SessionID
		expiration hlc.Timestamp
	}
	var (
		state = struct {
			syncutil.RWMutex
			liveSessions map[int]struct{}
			sessions     []session
		}{
			liveSessions: make(map[int]struct{}),
		}
		makeSession = func(t *testing.T) {
			t.Helper()
			state.Lock()
			defer state.Unlock()

			sid, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
			require.NoError(t, err)
			s := session{
				id:         sid,
				expiration: clock.Now().Add(expiration.Nanoseconds(), 0),
			}
			require.NoError(t, storage.Insert(ctx, s.id, s.expiration))
			state.liveSessions[len(state.sessions)] = struct{}{}
			state.sessions = append(state.sessions, s)
		}
		updateSession = func(t *testing.T) {
			t.Helper()
			state.Lock()
			defer state.Unlock()
			now := clock.Now()
			i := -1
			for i = range state.liveSessions {
			}
			if i == -1 {
				return
			}
			newExp := now.Add(expiration.Nanoseconds(), 0)
			s := &state.sessions[i]
			s.expiration = newExp
			alive, _, err := storage.Update(ctx, s.id, s.expiration)
			require.True(t, alive)
			require.NoError(t, err)
		}
		moveTimeForward = func(t *testing.T) {
			dur := time.Duration((1.25 - rand.Float64()) * float64(expiration.Nanoseconds()))
			state.Lock()
			defer state.Unlock()
			timeSource.Advance(dur)
			now := clock.Now()
			for i := range state.liveSessions {
				if state.sessions[i].expiration.Less(now) {
					delete(state.liveSessions, i)
				}
			}
		}
		step = func(t *testing.T) {
			r := rand.Float64()
			switch {
			case r < .5:
				makeSession(t)
			case r < .75:
				updateSession(t)
			default:
				moveTimeForward(t)
			}
		}
		pickSession = func() (int, sqlliveness.SessionID) {
			state.RLock()
			defer state.RUnlock()
			i := rand.Intn(len(state.sessions))
			return i, state.sessions[i].id
		}
		// checkIsAlive verifies that if false was returned that the session really
		// no longer is alive.
		checkIsAlive = func(t *testing.T, i int, isAlive bool) {
			t.Helper()
			state.RLock()
			defer state.RUnlock()
			now := clock.Now()
			if exp := state.sessions[i].expiration; !isAlive && !exp.Less(now) {
				t.Errorf("expiration for %v (%v) is not less than %v %v", i, exp, now, isAlive)
			}
		}
		wg        sync.WaitGroup
		runWorker = func() {
			defer wg.Done()
			for i := 0; i < runsPerWorker; i++ {
				time.Sleep(time.Microsecond)
				i, id := pickSession()
				isAlive, err := reader.IsAlive(ctx, id)
				assert.NoError(t, err)
				checkIsAlive(t, i, isAlive)
			}
		}
	)

	// Ensure that there's at least one session.
	makeSession(t)
	// Run the workers.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go runWorker()
	}
	// Step the random steps.
	for i := 0; i < controllerSteps; i++ {
		step(t)
	}
	wg.Wait()
}

// TestConcurrentAccessSynchronization tests that various interactions between
// synchronous and asynchronous readers in the face of context cancellation work
// as expected.
func TestConcurrentAccessSynchronization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	type filterFunc = func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error
	var requestFilter atomic.Value
	requestFilter.Store(filterFunc(nil))
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
					if f := requestFilter.Load().(filterFunc); f != nil {
						return f(ctx, request)
					}
					return nil
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	table := newSqllivenessTable(t, tDB, s)

	timeSource := timeutil.NewManualTime(t0)
	clock := hlc.NewClockForTesting(timeSource)
	settings := cluster.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	slstorage.CacheSize.Override(ctx, &settings.SV, 10)
	var ambientCtx log.AmbientContext
	storage := slstorage.NewTestingStorage(ambientCtx, stopper, clock, kvDB, s.Codec(), settings,
		s.SettingsWatcher().(*settingswatcher.SettingsWatcher), table, timeSource.NewTimer, true /*withSyntheticClock*/)
	storage.Start(ctx)

	// Synchronize reading from the store with the blocked channel by detecting
	// a Get to the table.
	prefix := s.Codec().TablePrefix(uint32(table.GetID()))
	var blockChannel atomic.Value
	var blocked int64
	resetBlockedChannel := func() { blockChannel.Store(make(chan struct{})) }
	waitForBlocked := func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt64(&blocked) == 0 {
				return errors.New("not blocked")
			}
			return nil
		})
	}
	unblock := func() { close(blockChannel.Load().(chan struct{})) }
	requestFilter.Store(func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
		getRequest, ok := request.GetArg(kvpb.Get)
		if !ok {
			return nil
		}
		get := getRequest.(*kvpb.GetRequest)
		if !bytes.HasPrefix(get.Key, prefix) {
			return nil
		}
		atomic.AddInt64(&blocked, 1)
		defer atomic.AddInt64(&blocked, -1)
		<-blockChannel.Load().(chan struct{})
		return kvpb.NewError(ctx.Err())
	})

	t.Run("CachedReader does not block", func(t *testing.T) {
		resetBlockedChannel()
		// Perform a read from the CachedReader and ensure that it does not block.
		cached := storage.CachedReader()
		var alive bool
		var g errgroup.Group
		sid, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		g.Go(func() (err error) {
			alive, err = cached.IsAlive(ctx, sid)
			return err
		})
		// Make sure that an asynchronous read was started.
		waitForBlocked(t)
		// Make sure that the cached read did not block.
		require.NoError(t, g.Wait())
		// The storage layer has never read this session so it should be assumed to
		// be alive.
		require.True(t, alive)
		// Unblock the reader and make sure it eventually populates the cache.
		unblock()
		testutils.SucceedsSoon(t, func() error {
			alive, err := cached.IsAlive(ctx, sid)
			require.NoError(t, err)
			if alive {
				return errors.New("expected not alive")
			}
			return nil
		})
	})
	t.Run("canceled context does not interrupt read", func(t *testing.T) {
		resetBlockedChannel()
		// Perform a read from the CachedReader and ensure that it does not block.
		cached := storage.CachedReader()
		var alive bool
		var g errgroup.Group
		sid, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		toCancel, cancel := context.WithCancel(ctx)

		before := storage.Metrics().IsAliveCacheMisses.Count()

		g.Go(func() (err error) {
			alive, err = cached.IsAlive(toCancel, sid)
			return err
		})
		// Make sure that an asynchronous read was started.
		waitForBlocked(t)
		require.NoError(t, g.Wait()) // make sure that the cached read did not block
		// The storage layer has never read this session so it should be assumed to
		// be alive.
		require.True(t, alive)
		cur := storage.Metrics().IsAliveCacheMisses.Count()
		require.Equal(t, int64(1), cur-before)
		// Now launch another, synchronous reader, which will join
		// the single-flight.
		g.Go(func() (err error) {
			alive, err = storage.BlockingReader().IsAlive(ctx, sid)
			return err
		})
		// Sleep some tiny amount of time to hopefully allow the other
		// goroutine to make it to the group.
		testutils.SucceedsSoon(t, func() error {
			if storage.Metrics().IsAliveCacheMisses.Count()-before != 2 {
				return errors.New("not seen cache miss yet")
			}
			return nil
		})
		// Cancel the context of the original, async call.
		cancel()
		// Unblock the reader.
		unblock()
		// Ensure that no error makes it to the synchronous call.
		require.NoError(t, g.Wait())
		// Ensure that the synchronous reader sees the session as not alive.
		require.False(t, alive)
	})
	// Test that canceling the context of a synchronous reader
	// results in the call returning promptly.
	t.Run("context cancellation returns", func(t *testing.T) {
		resetBlockedChannel()
		// Perform a read from the CachedReader and ensure that it does not block.
		cached := storage.CachedReader()
		var alive bool
		var g errgroup.Group
		sid, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		g.Go(func() (err error) {
			alive, err = cached.IsAlive(ctx, sid)
			return err
		})
		// Make sure that an asynchronous read was started.
		waitForBlocked(t)
		require.NoError(t, g.Wait()) // make sure that the cached read did not block
		// The storage layer has never read this session so it should be assumed to
		// be alive.
		require.True(t, alive)

		toCancel, cancel := context.WithCancel(ctx)
		// Now launch another, synchronous reader, which will join
		// the single-flight.
		g.Go(func() (err error) {
			alive, err = storage.BlockingReader().IsAlive(toCancel, sid)
			return err
		})

		// Cancel the context and ensure that the reader
		// returns early.
		cancel()
		require.Regexp(t, "context canceled", g.Wait())
		unblock()

		// Ensure that the cache still gets populated.
		testutils.SucceedsSoon(t, func() error {
			alive, err := cached.IsAlive(ctx, sid)
			require.NoError(t, err)
			if alive {
				return errors.New("expected not alive")
			}
			return nil
		})
	})
}

// TestDeleteMidUpdateFails ensures that a session removed while it attempts to
// update itself fails.
func TestDeleteMidUpdateFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	type filterFunc = func(context.Context, *kvpb.BatchRequest, *kvpb.BatchResponse) *kvpb.Error
	var respFilter atomic.Value
	respFilter.Store(filterFunc(nil))
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(
					ctx context.Context, request *kvpb.BatchRequest, resp *kvpb.BatchResponse,
				) *kvpb.Error {
					if f := respFilter.Load().(filterFunc); f != nil {
						return f(ctx, request, resp)
					}
					return nil
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Set up a fake storage implementation using a separate table.
	table := newSqllivenessTable(t, tdb, s)

	storage := slstorage.NewTestingStorage(
		s.DB().AmbientContext,
		s.AppStopper(), s.Clock(), kvDB, s.Codec(), s.ClusterSettings(),
		s.SettingsWatcher().(*settingswatcher.SettingsWatcher), table, timeutil.DefaultTimeSource{}.NewTimer, false, /*withSyntheticclock*/
	)

	// Insert a session.
	ID, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
	require.NoError(t, err)

	// Install a filter which will send on this channel when we attempt
	// to perform an update after the get has evaluated.
	getChan := make(chan chan struct{})
	respFilter.Store(func(
		ctx context.Context, request *kvpb.BatchRequest, _ *kvpb.BatchResponse,
	) *kvpb.Error {
		if get, ok := request.GetArg(kvpb.Get); !ok || !bytes.HasPrefix(
			get.(*kvpb.GetRequest).Key,
			s.Codec().TablePrefix(uint32(table.GetID())),
		) {
			return nil
		}
		respFilter.Store(filterFunc(nil))
		unblock := make(chan struct{})
		getChan <- unblock
		<-unblock
		return nil
	})

	// Launch the update.
	type result struct {
		exists bool
		err    error
	}
	resCh := make(chan result)
	go func() {
		var res result
		res.exists, _, res.err = storage.Update(ctx, ID, s.Clock().Now())
		resCh <- res
	}()

	// Wait for the update to block.
	unblock := <-getChan

	// Delete the session being updated.
	require.NoError(t, storage.Delete(ctx, ID))

	// Unblock the update and ensure that it saw that its session was deleted.
	close(unblock)
	res := <-resCh
	require.False(t, res.exists)
	require.NoError(t, res.err)

	// Ensure delete is idempotent
	require.NoError(t, storage.Delete(ctx, ID))
}

func newSqllivenessTable(
	t *testing.T, db *sqlutils.SQLRunner, s serverutils.ApplicationLayerInterface,
) (tableID catalog.TableDescriptor) {
	t.Helper()
	db.Exec(t, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "%s"`, t.Name()))
	db.Exec(t, strings.Replace(systemschema.SqllivenessTableSchema,
		"CREATE TABLE system.sqlliveness",
		fmt.Sprintf(`CREATE TABLE "%s".sqlliveness`, t.Name()),
		1))
	return desctestutils.TestingGetTableDescriptor(s.DB(), s.Codec(), t.Name(), "public", "sqlliveness")
}
