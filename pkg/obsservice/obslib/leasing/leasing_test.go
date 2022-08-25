// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package leasing

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/migrations"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func setupCRDBClusterForObsService(t *testing.T) (_ *pgxpool.Pool, cleanup func()) {
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(),
		"TestPersistEvents", url.User(username.RootUser),
	)
	config, err := pgxpool.ParseConfig(pgURL.String())
	require.NoError(t, err)
	config.ConnConfig.Database = "defaultdb"
	pool, err := pgxpool.ConnectConfig(ctx, config)
	require.NoError(t, err)
	require.NoError(t, migrations.RunDBMigrations(ctx, config.ConnConfig))
	return pool, func() {
		cleanupFunc()
		pool.Close()
		s.Stopper().Stop(ctx)
	}
}

func TestLeasing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	pool, cleanup := setupCRDBClusterForObsService(t)
	defer cleanup()

	ctx := context.Background()
	stop := stop.NewStopper()
	defer stop.Stop(ctx)
	clock := timeutil.NewManualTime(timeutil.Unix(123, 0))
	s1 := NewSession(uuid.MakeV4(), pool, clock, stop)
	require.NoError(t, s1.Start())
	s2 := NewSession(uuid.MakeV4(), pool, clock, stop)
	require.NoError(t, s2.Start())

	// First session succeeds in taking a lease.
	l1, err := s1.Lease(ctx, 42)
	require.NoError(t, err)
	require.True(t, clock.Now().Before(l1.ValidityEnd))
	require.True(t, clock.Now().Before(l1.Expiration))

	// Another session fails in taking the lease.
	_, err = s2.Lease(ctx, 42)
	require.Equal(t, ErrLeasedByAnother, err)
	// But it can take a lease on another instance.
	l2, err := s2.Lease(ctx, 43)
	require.NoError(t, err)

	// The lease expires.
	// Stop the sessions' heartbeat, to not race with the checks below.
	s1.testingKnobs.heartbeatSem = make(chan struct{}, 1)
	s2.testingKnobs.heartbeatSem = make(chan struct{}, 1)
	// Acquire the semaphores, so heartbeats are now blocked.
	s1.testingKnobs.heartbeatSem <- struct{}{}
	s2.testingKnobs.heartbeatSem <- struct{}{}
	clock.MustAdvanceTo(l1.ValidityEnd.Add(time.Nanosecond))

	// s1 will attempt to acquire the lease again. We'll verify that it succeeds,
	// and we'll also verify some mechanics about how that happens:
	// - With the heartbeat loop still blocked, we'll trigger the lease acquisition.
	// - We'll expect the lease acquisition to block on a heartbeat.
	// - We'll unblock the heartbeats.
	// - Now the lease acquisition should succeed.

	waitForLeaseAcqBlocked := make(chan struct{})
	s1.testingKnobs.onLeaseAcquisitionBlockedOnHeartbeatOnce = waitForLeaseAcqBlocked

	type acquisitionRes struct {
		l   LeaseResult
		err error
	}
	leaseC := make(chan acquisitionRes)
	go func() {
		l1, err = s1.Lease(ctx, 42)
		leaseC <- acquisitionRes{l: l1, err: err}
	}()

	// Wait for the lease acquisition to block.
	<-waitForLeaseAcqBlocked

	// Wait a little and verify that the lease acquisition doesn't return while
	// heartbeats are still blocked.
	select {
	case <-leaseC:
		t.Fatalf("got lease response unexpectedly before unblocking the heartbeats")
	case <-time.After(10 * time.Millisecond):
	}
	// Unblock the heartbeats for the first session.
	<-s1.testingKnobs.heartbeatSem
	// Now we should receive a response to the lease request.
	ll := <-leaseC
	require.NoError(t, ll.err)
	l1 = ll.l

	// Unblock the heartbeats for the second session.
	<-s2.testingKnobs.heartbeatSem
	_, err = s2.Lease(ctx, 42)
	require.Equal(t, ErrLeasedByAnother, err)

	// Check that s2 becomes valid, at which point we can call Lease(43) again.
	// We'll check that s2.Lease(43) doesn't block on heartbeats.
	require.NoError(t, s2.waitForValid(ctx))
	c := make(chan (struct{}))
	s2.testingKnobs.onLeaseAcquisitionBlockedOnHeartbeatOnce = c
	l22, err := s2.Lease(ctx, 43)
	require.NoError(t, err)
	require.Equal(t, l2.Epoch, l22.Epoch)
	s2.testingKnobs.onLeaseAcquisitionBlockedOnHeartbeatOnce = nil

	// The lease expires. We stop s1's heartbeats, and have session2 steal
	// the lease.

	// Block heartbeats for s1.
	s1.testingKnobs.heartbeatSem <- struct{}{}
	clock.MustAdvanceTo(l1.Expiration.Add(time.Nanosecond))
	// session2 successfully steals the lease.
	_, err = s2.Lease(ctx, 42)
	require.NoError(t, err)
	// Unblock mg1 and check that s1 cannot take the lease.
	<-s1.testingKnobs.heartbeatSem
	_, err = s1.Lease(ctx, 42)
	require.Equal(t, ErrLeasedByAnother, err)

	// Verify that s1 has incremented its epoch. It was forced to do so because
	// the lease stealing above deleted the session record with the old epoch.
	require.Equal(t, l1.Epoch+1, s1.epoch())
}

// Test that nothing bad happens when the transaction incrementing a session
// record's epoch commits, but the client gets an error (we refer to this type
// of thing as an "ambiguous error"). In this scenario, the session record's
// epoch is in advance of the epoch known by the in-memory Session. The point of
// the test is to check that nobody freaks out.
func TestAmbiguousUpsertError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	pool, cleanup := setupCRDBClusterForObsService(t)
	defer cleanup()

	ctx := context.Background()
	stop := stop.NewStopper()
	defer stop.Stop(ctx)
	clock := timeutil.NewManualTime(timeutil.Unix(123, 0))
	s := NewSession(uuid.MakeV4(), pool, clock, stop)
	otherSession := NewSession(uuid.MakeV4(), pool, clock, stop)
	require.NoError(t, otherSession.Start())
	require.NoError(t, s.Start())
	const target = 1
	l, err := s.Lease(ctx, target)
	require.NoError(t, err)

	// Stop the heartbeats, advance the clock, delete the session record through
	// the use of another session.
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	s.testingKnobs.heartbeatSem = sem
	clock.MustAdvanceTo(l.Expiration)
	_, err = otherSession.Lease(ctx, target)
	require.NoError(t, err)

	// Inject an error in the upsert transaction.
	failUpsertOnce := false
	s.testingKnobs.afterSessionRecordUpsert = func(err error) error {
		if err != nil {
			return err
		}
		if failUpsertOnce {
			failUpsertOnce = false
			return errors.Newf("test injected err")
		}
		return nil
	}

	failUpsertOnce = true
	// Unblock the session and wait until the session becomes valid. In order to
	// become valid, a successful heartbeat is needed.
	<-sem
	require.NoError(t, s.waitForValid(ctx))
}

func TestRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	pool, cleanup := setupCRDBClusterForObsService(t)
	defer cleanup()

	ctx := context.Background()
	stop := stop.NewStopper()
	defer stop.Stop(ctx)
	clock := timeutil.NewManualTime(timeutil.Unix(123, 0))
	s1 := NewSession(uuid.MakeV4(), pool, clock, stop)
	require.NoError(t, s1.Start())
	s2 := NewSession(uuid.MakeV4(), pool, clock, stop)
	require.NoError(t, s2.Start())

	// First session succeeds in taking a lease.
	_, err := s1.Lease(ctx, 42)
	require.NoError(t, err)

	err = s1.Release(ctx, 42)
	require.NoError(t, err)
	// session2 successfully steals the lease.
	_, err = s2.Lease(ctx, 42)
	require.NoError(t, err)
}

// Test repeated random races between multiple sessions trying to acquire leases
// on the same target. We check that a single one succeeds at every step.
func TestLeasersRacing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Under a race build, it's harder to ensure a random winner of the races
	// between the two sessions, as everything is very slow.
	skip.UnderRace(t, "takes too long under race, and it's less useful because the test assumes some timing")
	pool, cleanup := setupCRDBClusterForObsService(t)
	defer cleanup()

	ctx := context.Background()
	stop := stop.NewStopper()
	defer stop.Stop(ctx)
	clock := timeutil.NewManualTime(timeutil.Unix(123, 0))

	const numSessions = 4
	sessions := make([]*Session, numSessions)
	for i := 0; i < numSessions; i++ {
		sessions[i] = NewSession(uuid.MakeV4(), pool, clock, stop)
		require.NoError(t, sessions[i].Start())
	}

	const target = 1
	type leaseRes struct {
		l   LeaseResult
		err error
	}

	rnd, seed := randutil.NewTestRand()
	log.Infof(ctx, "test seed: %d", seed)

	sem := make(chan struct{}, 1)
	var l LeaseResult
	var leaseHolderIdx int
	// winners is the set of session indexes that have ever gotten the lease.
	// We'll use it to check that sessions manage to steal the lease and the test
	// is not fooling itself.
	winners := make(map[int]int)
	for iteration := 0; iteration < 50; iteration++ {
		if iteration != 0 {
			// The current leaseholder will wait a bit before heartbeating, to give a
			// fighting change to another session to steal the lease. Without the
			// artificial wait, the prior owner is heavily favored to win because it
			// must do a lot less work.
			wait := time.Duration(rnd.Intn(5000)) * time.Microsecond
			for i, s := range sessions {
				if i == leaseHolderIdx {
					s.testingKnobs.heartbeatSem = sem
				} else {
					s.testingKnobs.heartbeatSem = nil
				}
			}
			sem <- struct{}{}
			go func() {
				time.Sleep(wait)
				// Unblock heartbeats.
				<-sem
			}()

			clock.MustAdvanceTo(l.Expiration)
			require.False(t, sessions[leaseHolderIdx].valid())
		}

		resultChans := make([]chan leaseRes, numSessions)
		for i := range resultChans {
			resultChans[i] = make(chan leaseRes)
		}
		for i, s := range sessions {
			go func(i int, s *Session) {
				l1, err := s.Lease(ctx, target)
				resultChans[i] <- leaseRes{l1, err}
			}(i, s)
		}

		// Wait for resultChans from all sessions.
		results := make([]leaseRes, numSessions)
		numErrs := 0
		leaseHolderIdx = -1
		for i, resC := range resultChans {
			res := <-resC
			results[i] = res
			if res.err != nil {
				if !errors.Is(res.err, ErrLeasedByAnother) {
					t.Fatal(res.err)
				}
				numErrs++
			} else {
				leaseHolderIdx = i
				l = res.l
				winners[i]++
			}
		}
		require.NotEqual(t, -1, leaseHolderIdx)
		require.Equal(t, numSessions-1, numErrs)
	}
	log.Infof(ctx, "winning count per session: %v", winners)
	numWinners := 0
	for range winners {
		numWinners++
	}
	// Check that multiple sessions managed to take some leases, to make sure that
	// the test is not fooling itself.
	require.Greater(t, numWinners, 1)
}

// Test a lease acquisition attempt for different states of the database.
func TestLeasingScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	pool, cleanup := setupCRDBClusterForObsService(t)
	defer cleanup()

	now := timeutil.Unix(100, 0)

	const target = 42

	id1 := uuid.MakeV4()
	id2 := uuid.MakeV4()

	s1Valid := sessionData{
		id:         id1,
		epoch:      1,
		expiration: now.Add(2 * StasisDuration),
	}
	s1Expired := sessionData{
		id:         id1,
		epoch:      1,
		expiration: now,
	}
	s1ExpiredHighEpoch := sessionData{
		id:         id1,
		epoch:      100,
		expiration: now,
	}
	s2 := sessionData{
		id:         id2,
		epoch:      1,
		expiration: now.Add(LeaseDuration),
	}

	tr := true
	truePtr := &tr

	for _, tc := range []struct {
		name string
		// initialSessions lists the state of the sessions table at the start of the
		// test.
		initialSessions []sessionData
		// initialLeaseRecord, if not empty, specifies the state of the monitoring_leases
		// table at the start of the test.
		initialLeaseRecord leaseRecord
		// acquirer identifies the session that will perform the lease acquisition.
		acquirer uuid.UUID
		expErr   error
		// expSessions lists the state of sessions expected at the end of the test.
		expSessions []sessionData
		// expDelete, if set, specifies whether the lease acquisition is expected to
		// have deleted another session.
		expDelete *bool
	}{
		{
			name:               "blank",
			initialSessions:    nil,
			initialLeaseRecord: leaseRecord{},
			acquirer:           id1,
			expErr:             nil,
			expSessions:        nil,
		},
		{
			name:               "valid lease owned by another",
			initialSessions:    []sessionData{s1Valid},
			initialLeaseRecord: leaseRecord{sessionID: id1, target: target, epoch: s1Expired.epoch},
			acquirer:           id2,
			expErr:             ErrLeasedByAnother,
			expSessions:        []sessionData{s1Valid, s2},
		},
		{
			name:               "lease owned by another expired",
			initialSessions:    []sessionData{s1Expired},
			initialLeaseRecord: leaseRecord{sessionID: id1, target: target, epoch: s1Expired.epoch},
			acquirer:           id2,
			expErr:             nil,
			// We expect s1 to be deleted.
			expSessions: []sessionData{s2},
			expDelete:   truePtr,
		},
		{
			name:               "valid lease owned by us",
			initialSessions:    []sessionData{s1Valid},
			initialLeaseRecord: leaseRecord{sessionID: id1, target: target, epoch: s1Valid.epoch},
			acquirer:           id1,
			expErr:             nil,
			expSessions:        []sessionData{s1Valid.withExpiration(now.Add(LeaseDuration))},
		},
		{
			name:               "expired lease owned by us",
			initialSessions:    []sessionData{s1ExpiredHighEpoch},
			initialLeaseRecord: leaseRecord{sessionID: id1, target: target, epoch: s1ExpiredHighEpoch.epoch},
			acquirer:           id1,
			expErr:             nil,
			expSessions:        []sessionData{s1ExpiredHighEpoch.withExpiration(now.Add(LeaseDuration))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stop := stop.NewStopper()
			defer stop.Stop(ctx)
			clock := timeutil.NewManualTime(now)

			// Cleanup after the previous tests.
			_, err := pool.Exec(ctx, "DELETE FROM monitoring_leases; DELETE FROM sessions;")
			require.NoError(t, err)

			for _, data := range tc.initialSessions {
				// Populate initial sessions.
				_, err := pool.Exec(ctx, "INSERT into sessions (id, epoch, expiration) "+
					"VALUES ($1, $2, $3) ",
					data.id, data.epoch, data.expiration)
				require.NoError(t, err)
			}

			s := NewSession(tc.acquirer, pool, clock, stop)
			require.NoError(t, s.Start())
			require.NoError(t, err)

			// Populate initial lease.
			if tc.initialLeaseRecord != (leaseRecord{}) {
				_, err := pool.Exec(ctx, "INSERT into monitoring_leases (target_id, session_id, epoch) "+
					"VALUES ($1, $2, $3)",
					tc.initialLeaseRecord.target, tc.initialLeaseRecord.sessionID, tc.initialLeaseRecord.epoch)
				require.NoError(t, err)
			}

			ctx, finishAndGetRecording := tracing.ContextWithRecordingSpan(ctx, tracing.NewTracer(), "test")
			_, err = s.Lease(ctx, target)
			rec := finishAndGetRecording()
			if tc.expErr != nil {
				require.Equal(t, tc.expErr, err)
			} else {
				require.NoError(t, err)
			}
			if tc.expDelete != nil {
				_, ok := rec.FindLogMessage("deleting session")
				require.Equal(t, *tc.expDelete, ok)
			}

			// Validate the expected sessions, if specified.
			if tc.expSessions != nil {
				r := pool.QueryRow(ctx, "SELECT count(*) FROM sessions")
				var count int
				require.NoError(t, r.Scan(&count))
				require.Equal(t, len(tc.expSessions), count)
				for _, s := range tc.expSessions {
					r := pool.QueryRow(ctx, "select epoch, expiration from sessions where id = $1", s.id)
					var expiration time.Time
					var epoch int
					require.NoError(t, r.Scan(&epoch, &expiration))
					require.Equal(t, s.epoch, epoch)
					require.Equal(t, s.expiration, expiration.UTC())
				}
			}
		})
	}
}

type sessionData struct {
	id         uuid.UUID
	epoch      int
	expiration time.Time
}

type leaseRecord struct {
	sessionID uuid.UUID
	epoch     int
	target    int
}

func (s sessionData) withExpiration(when time.Time) sessionData {
	s.expiration = when
	return s
}

// Test that a session can give multiple leases on the same target.
func TestMultipleLeasesFromSameSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	pool, cleanup := setupCRDBClusterForObsService(t)
	defer cleanup()

	ctx := context.Background()
	stop := stop.NewStopper()
	defer stop.Stop(ctx)
	clock := timeutil.NewManualTime(timeutil.Unix(123, 0))
	s1 := NewSession(uuid.MakeV4(), pool, clock, stop)
	require.NoError(t, s1.Start())
	_, err := s1.Lease(ctx, 42)
	require.NoError(t, err)
	_, err = s1.Lease(ctx, 42)
	require.NoError(t, err)
}

// Test all the interleavings between a lease acquisition for session s1 and
// another session s2 deleting s1.
func TestLeaseAcquisitionRacesWithSessionDeletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const target, otherTarget = 1, 99

	// An interleaving is defined by two things, enumerated by the two enums
	// below:
	// 1. When is the s1 deleted in relation to the progress of the lease
	// acquisition.
	// 2. When is the s1's record recreated (with an incremented epoch) by the
	// heartbeat loop in relation to the lease acquisition.
	//
	// These interleaving points correspond to the structure of the code, which
	// accomodates for these possible races in different ways. There are test
	// cases for all the combinations of these twp events that make sense.

	type sessionDeletionTime int
	const (
		// s1 is deleted after the lease acquisition finds s1's record to be valid,
		// but before attempting to insert a lease record referencing it.
		delBeforeLeaseRecordInsertionTxn sessionDeletionTime = iota
		// s1 is deleted while the transaction inserting a lease record is running.
		delDuringLeaseRecordInsertionTxn
		// s1 is deleted after the transaction inserting a lease record succeeds,
		// but before the leased target is inserted into the set of leased targets.
		delAfterLeaseRecordInsertTxn
	)

	// sessionRecreationTime enumerates when s1 is recreated by its heartbeat
	// loop, after having been deleted. The first options correspond to the
	// deletion enumeration above.
	type sessionRecreationTime int
	const (
		recreateBeforeLeaseRecordInsertionTxn sessionRecreationTime = iota
		recreateDuringLeaseRecordInsertionTxn
		recreateAfterLeaseRecordInsertTxn
		// s1 is recreated once the lease acquisition blocks for s1 becoming valid
		// again, after having been deleted.
		recreateThroughLeaseRetryLoop
	)

	for _, delTime := range []sessionDeletionTime{
		delBeforeLeaseRecordInsertionTxn,
		delDuringLeaseRecordInsertionTxn,
		delAfterLeaseRecordInsertTxn,
	} {
		t.Run(fmt.Sprintf("delTime:%d", delTime), func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "conflicting lease", func(t *testing.T, conflictingLease bool) {
				var recreationTimes []sessionRecreationTime
				switch delTime {
				case delBeforeLeaseRecordInsertionTxn:
					recreationTimes = []sessionRecreationTime{
						recreateBeforeLeaseRecordInsertionTxn,
						recreateAfterLeaseRecordInsertTxn,
						// recreateDuringLeaseRecordInsertionTxn doesn't make sense since there
						// is no insertion txn.
					}
					// recreateThroughLeaseRetryLoop doesn't make sense in the
					// conflictingLease since there is no retry attempt to take a lease at
					// an incremented epoch.
					if !conflictingLease {
						recreationTimes = append(recreationTimes, recreateThroughLeaseRetryLoop)
					}
				case delDuringLeaseRecordInsertionTxn:
					recreationTimes = []sessionRecreationTime{
						recreateDuringLeaseRecordInsertionTxn,
						recreateAfterLeaseRecordInsertTxn,
					}
					// recreateThroughLeaseRetryLoop doesn't make sense in the
					// conflictingLease since there is no retry attempt to take a lease at
					// an incremented epoch.
					if !conflictingLease {
						recreationTimes = append(recreationTimes, recreateThroughLeaseRetryLoop)
					}
				case delAfterLeaseRecordInsertTxn:
					recreationTimes = []sessionRecreationTime{
						recreateAfterLeaseRecordInsertTxn,
						recreateThroughLeaseRetryLoop,
					}
				}

				for _, recreateTime := range recreationTimes {
					t.Run(fmt.Sprintf("recreateTime:%d", recreateTime), func(t *testing.T) {
						pool, cleanup := setupCRDBClusterForObsService(t)
						defer cleanup()
						ctx := context.Background()
						stop := stop.NewStopper()
						defer stop.Stop(ctx)
						clock := timeutil.NewManualTime(timeutil.Unix(123, 0))

						s1 := NewSession(uuid.MakeV4(), pool, clock, stop)
						require.NoError(t, s1.Start())
						s2 := NewSession(uuid.MakeV4(), pool, clock, stop)
						require.NoError(t, s2.Start())

						require.NoError(t, s1.waitForValid(ctx))
						// If we'll want to delete s1's record without having s2 take a
						// lease on `target`, then we have s1 take a lease on a dummy target
						// which s2 will also use in order to delete s1's record.
						if !conflictingLease {
							_, err := s1.Lease(ctx, otherTarget)
							require.NoError(t, err)
						}

						// Now we do a bunch of setup, chaining callbacks to be executed at
						// specific times in order to orchestrate the races that we want.

						beforeLeaseInsertion := []func(){}
						duringLeaseInsertion := []func(){}
						afterLeaseInsertion := []func(){}

						// s2LeaseAcquire is a callback causing s2 to acquire a lease,
						// deleting s1's session record in the process. This callback will
						// be hooked up to run at different times depending on the test
						// case.
						var epochBefore Epoch
						s2LeaseAcquire := func() {
							// Block s1's heartbeats so we can advance the clock and guarantee
							// that s1's record is expired.
							// The heartbeats will be unblocked later, depending on the test
							// case.
							s1.testingKnobs.heartbeatSem = make(chan struct{}, 1)
							s1.testingKnobs.heartbeatSem <- struct{}{}
							epochBefore = s1.epoch()
							// Advance the clock to expire the lease.
							clock.MustAdvanceTo(s1.expiration())

							targetForS2 := otherTarget
							if conflictingLease {
								targetForS2 = target
							}
							_, err := s2.Lease(ctx, targetForS2)
							require.NoError(t, err)
							// Now s1's record is deleted.
						}

						s1UnblockHeartbeatsAndWaitForRecreation := func() {
							<-s1.testingKnobs.heartbeatSem
							require.NoError(t, s1.waitForValid(ctx))
						}

						// Hookup the callback that deletes s1's record.
						switch delTime {
						case delBeforeLeaseRecordInsertionTxn:
							beforeLeaseInsertion = append(beforeLeaseInsertion, s2LeaseAcquire)
						case delDuringLeaseRecordInsertionTxn:
							duringLeaseInsertion = append(duringLeaseInsertion, s2LeaseAcquire)
						case delAfterLeaseRecordInsertTxn:
							afterLeaseInsertion = append(afterLeaseInsertion, s2LeaseAcquire)
						}

						// Hookup the callback that unblocks s1's heartbeat loop so its
						// session record is recreated.
						switch recreateTime {
						case recreateBeforeLeaseRecordInsertionTxn:
							beforeLeaseInsertion = append(beforeLeaseInsertion, s1UnblockHeartbeatsAndWaitForRecreation)
						case recreateDuringLeaseRecordInsertionTxn:
							duringLeaseInsertion = append(duringLeaseInsertion, s1UnblockHeartbeatsAndWaitForRecreation)
						case recreateAfterLeaseRecordInsertTxn:
							afterLeaseInsertion = append(afterLeaseInsertion, s1UnblockHeartbeatsAndWaitForRecreation)
						case recreateThroughLeaseRetryLoop:
							// We'll unblock the heartbeat loop when the lease acquisition
							// blocks on waiting for a valid session.
							waitForValidBlockedC := make(chan struct{})
							s1.testingKnobs.onLeaseAcquisitionBlockedOnHeartbeatOnce = waitForValidBlockedC
							go func() {
								<-waitForValidBlockedC
								<-s1.testingKnobs.heartbeatSem
							}()
						}

						s1.testingKnobs.beforeLeaseAcquisitionAttempt = func() {
							for _, f := range beforeLeaseInsertion {
								f()
							}
							s1.testingKnobs.beforeLeaseAcquisitionAttempt = nil
						}
						s1.testingKnobs.beforeLeaseRecordInsert = func() {
							for _, f := range duringLeaseInsertion {
								f()
							}
							s1.testingKnobs.beforeLeaseRecordInsert = nil
						}
						s1.testingKnobs.afterLeaseAcquisitionAttempt = func() {
							for _, f := range afterLeaseInsertion {
								f()
							}
							s1.testingKnobs.afterLeaseAcquisitionAttempt = nil
						}

						// We record s1's attempt to take the lease. We'll check the result
						// and analyze the recording below.
						leaseCtx, finish := tracing.ContextWithRecordingSpan(ctx, tracing.NewTracer(), "test lease acq")
						lease, err := s1.Lease(leaseCtx, target)
						rec := finish()

						// Check whether the lease acquisition succeeded. It's supposed to
						// succeed if s2 did not take a competing lease.
						if conflictingLease {
							require.Equal(t, ErrLeasedByAnother, err)
						} else {
							require.NoError(t, err)
							require.Equal(t, epochBefore+1, lease.Epoch)
						}

						// Check if there was a retry of the transaction inserting a lease
						// record. We expect that when the deletion of s1's session record
						// happens during the lease record insert txn, resulting in a
						// serializable retry.
						if delTime == delDuringLeaseRecordInsertionTxn {
							require.NotEqual(t, -1, tracing.FindMsgInRecording(rec, "insert lease record txn retry: 1"))
						} else {
							require.Equal(t, -1, tracing.FindMsgInRecording(rec, "insert lease record txn retry: 1"))
						}

						// Check whether the txn inserting a lease record encountered a
						// foreign key validation error. This happens when the transaction
						// finds that the session record is deleted or had its epoch
						// incremented.

						// If the deletion happens after the txn, then the txn finds the
						// session record.
						if (delTime == delBeforeLeaseRecordInsertionTxn || delTime == delDuringLeaseRecordInsertionTxn) &&
							// If the lease is recreated early, the first attempt of the txn
							// already finds the session record.
							recreateTime != recreateBeforeLeaseRecordInsertionTxn &&
							// If the lease is recreated during the txn, the second attempt of
							// the txn (which uses the incremented epoch) finds the session
							// record.
							recreateTime != recreateDuringLeaseRecordInsertionTxn &&
							// If s2 took a lease, the txn detects that early.
							!conflictingLease {
							require.NotEqual(t, -1, tracing.FindMsgInRecording(rec, "foreign key validation error"))
						} else {
							require.Equal(t, -1, tracing.FindMsgInRecording(rec, "foreign key validation error"))
						}

						// Check whether the session epoch change was detected after the
						// lease record insertion. This happens when the session is deleted
						// late enough and the recreation happens early enough after that.
						if delTime == delAfterLeaseRecordInsertTxn &&
							recreateTime != recreateThroughLeaseRetryLoop {
							require.NotEqual(t, -1, tracing.FindMsgInRecording(rec, "session epoch change detected"))
						} else {
							require.Equal(t, -1, tracing.FindMsgInRecording(rec, "session epoch change detected"))
						}
					})
				}
			})
		})
	}
}
