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
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// LeaseDuration controls how long each heartbeat pushes a session's Expiration
// by.
const LeaseDuration = 10 * time.Second

// StasisDuration is the period at the end of a lease when the lease is not
// usable. The point of the stasis period is to account for possible clock skew
// between actors that check the validity of a lease. Were it not for the
// stasis, a node with a slow clock might consider a lease to be valid, while
// another node with a fast clock considers the lease to be invalid, and steals
// it. The stasis will force the former node to extend the lease before using
// it.
const StasisDuration = time.Second

// HeartbeatBeforeStasis controls how long before the session's leases becomes
// unusable (i.e. enter their stasis period) we heartbeat the session,
// prolonging its validity.
//
// When using remaining = Lease.ValidityEnd().Sub(clock.Now()) to see how long
// is currently left on a lease, generally speaking, remaining >
// HeartbeatBeforeStasis is not expected to always be true even when database
// connectivity is fine. In particular, it will always be the case that
// remaining < LeaseDuration. Thus, users should generally not expect leases to
// be valid for more than (HeartbeatBeforeStasis - <a fudge factor accounting
// for heartbeat communication latency>).
const HeartbeatBeforeStasis = (LeaseDuration - StasisDuration) / 2

// ErrSessionStopped is returned by Lease() when called on a stopped
// Session.
var ErrSessionStopped = errors.New("session stopped")

// Session is used to acquire exclusive, time-bound leases on targets. Each Obs
// Service worker has a Session and its leases ensure mutual exclusion between
// workers: only one worker at a time pulls data from a given monitoring target.
//
// Sessions are represented in the database as "session record" rows. The
// session record is periodically heartbeated to continuously prolong its
// expiration time. If a session is not heartbeated for a while and expires,
// other sessions might delete its session record in order to steal one or more
// of its leases, at which point the owner Session is forced to increment its
// epoch.
//
// See doc.go.
type Session struct {
	db    *pgxpool.Pool
	clock timeutil.TimeSource
	// stop is used to terminate the heartbeat loops of all the sessions produced
	// by this Session.
	stop *stop.Stopper

	// heartbeatStoppedC is closed when the heartbeat loop terminates.
	heartbeatStoppedC chan struct{}

	// cancelHeartbeatLoop signals the background worker to terminate because
	// Stop() was called.
	cancelHeartbeatLoop func()

	// id is this session's identifier. Leases obtained by this session are tied
	// to this id. In order for another session to steal a lease from this
	// session, that other session will need to delete the database record
	// identified by this id.
	id           uuid.UUID
	leasingGroup singleflight.Group

	mu struct {
		syncutil.Mutex

		// stopped is set if Stop() was called.
		stopped bool

		expiration    time.Time
		epoch         int
		leasedTargets map[int]struct{}

		// onUpdateCV is signaled whenever Expiration changes, or whenever stop() is
		// called.
		onUpdateCV *sync.Cond
	}

	testingKnobs struct {
		beforeLeaseAcquisitionAttempt func()
		beforeLeaseRecordInsert       func()
		afterLeaseAcquisitionAttempt  func()

		// onLeaseAcquisitionBlockedOnHeartbeatOnce, if set, is closed when a lease acquisition
		// finds the session to be invalid and blocks on a successful heartbeat.
		// After being closed, this field is reset.
		onLeaseAcquisitionBlockedOnHeartbeatOnce chan struct{}

		// heartbeatSem, if set, is a semaphore controlling whether heartbeats are
		// blocked. To avoid data races, tests should only set this when
		// heartbeats are stopped.
		heartbeatSem chan struct{}
	}
}

// NewSession constructs a Session.
func NewSession(
	id uuid.UUID, db *pgxpool.Pool, clock timeutil.TimeSource, stop *stop.Stopper,
) *Session {
	m := &Session{
		id:    id,
		db:    db,
		clock: clock,
		stop:  stop,
	}
	m.mu.onUpdateCV = sync.NewCond(&m.mu)
	// Note: m.mu.leasedTargets is initialized the first time update() is called.
	// Until then, all Lease() calls block, so they don't access the field.
	return m
}

// Start starts a background task that maintains the database state (i.e. the
// sesson record) allowing the Session to take leases.
//
// The task runs a heartbeat loop for the session record until either the
// stopper passed to the constructor is stopped, or Stop() is called. Start()
// can be called again after Stop().
func (s *Session) Start() error {
	s.heartbeatStoppedC = make(chan struct{})
	// Reset the state in case this call comes after Stop().
	s.mu.Lock()
	s.mu.stopped = false
	var ctx context.Context
	// s.Stop() will call s.cancelHeartbeatLoop.
	ctx, s.cancelHeartbeatLoop = s.stop.WithCancelOnQuiesce(context.Background())
	s.mu.Unlock()

	return s.stop.RunAsyncTask(ctx, "session hb loop", func(ctx context.Context) {
		s.heartbeatLoop(ctx)
	})
}

// Stop stops the manager's background worker. Future calls to Lease() will
// return ErrSessionStopped.
//
// Calling Stop() after the stopper passed to the constructor is stopped is not
// necessary.
//
// Stop() can be called multiple times.
func (s *Session) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.stopped {
		return
	}

	// Mark the manager as stopped. Any racing or future Lease() call will return
	// ErrSessionStopped.
	s.mu.stopped = true
	s.mu.onUpdateCV.Broadcast()

	// Stop the background worker.
	s.cancelHeartbeatLoop()

	// Wait until the heartbeat goroutine stops. Note that this a no-op if the
	// heartbeat goroutine called Stop().
	<-s.heartbeatStoppedC
}

// upsertSessionRecord inserts or updates a session record. If a session with
// the specified id exists, its epoch is asserted to be equal to the specified
// epoch, and the record's expiration time is updated to the provided
// expiration. If a record is not found, it is inserted with epoch+1. As a
// special case, if epoch == 0, there is no assertion on the epoch of an
// existing record.
//
// The epoch of the inserted/updated record is returned.
func (s *Session) upsertSessionRecord(
	ctx context.Context, id uuid.UUID, epoch int, expiration time.Time,
) (int, error) {
	if s.testingKnobs.heartbeatSem != nil {
		s.testingKnobs.heartbeatSem <- struct{}{}
		<-s.testingKnobs.heartbeatSem
	}
	err := crdbpgx.ExecuteTx(ctx, s.db, pgx.TxOptions{}, func(txn pgx.Tx) error {
		row := txn.QueryRow(ctx, "SELECT epoch FROM sessions WHERE id=$1", id)
		var recordEpoch int
		if errors.Is(row.Scan(&recordEpoch), pgx.ErrNoRows) {
			// Sanity check: the session record should be there if we think we're valid.
			if s.valid() {
				// There might be another session that thinks it has a valid lease on the
				// target.
				log.Errorf(ctx, "session is valid but the session record was deleted from the database; "+
					"are clocks significantly out of sync? id: %s, ValidityEnd: %s, Expiration: %s.",
					s.id.Short(), s.stasisStart(), s.expiration())
			}

			epoch++
			_, err := txn.Exec(ctx,
				"INSERT INTO sessions (id, epoch, Expiration) VALUES ($1, $2, $3)",
				id, epoch, expiration)
			return err
		}

		epoch = recordEpoch
		if epoch != 0 && epoch != recordEpoch {
			panic(fmt.Sprintf("unexpected epoch in session record; expected: %d, got: %d", epoch, recordEpoch))
		}
		_, err := txn.Exec(ctx,
			"UPDATE sessions SET Expiration=$1 WHERE id=$2",
			expiration, id)
		return err
	})
	return epoch, err
}

// LeaseResult represents a lease - i.e the successful result of
// Session.Lease(). If none of the fields are of interest, the caller can ignore
// the LeaseResult completely.
type LeaseResult struct {
	// ValidityEnd specifies when the current lease should not be used anymore.
	// This is equal to Expiration-1s. The 1s accounts for clock skew between
	// nodes, and also aims to leave some gap between when a leaseholder does
	// something assuming it has the lease and when another Session takes a
	// conflicting lease.
	ValidityEnd time.Time
	// Expiration represents the moment when another Session can take a
	// conflicting lease, assuming this leasehgolder's session record is not
	// heartbeated anymore.
	Expiration time.Time
	// Epoch represents the session record's epoch that this lease is tied to. It
	// can be compared to the epoch of a prior lease to determine whether it's
	// certain that no other node held a lease in the meantime.
	//
	// The Epoch can only be compared across leases obtained from a single Session
	// instance (i.e. it cannot be compared across nodes, or even across process
	// restarts on a single node).
	Epoch int
}

// Lease acquires an exclusive time-bound lease for monitoring a target (i.e.
// CRDB node or pod). If the target is currently leased by another Session
// (i.e. by another Obs Service node), ErrLeasedByAnother is returned.
//
// Lease() might block until the Session manages to obtain a valid session.
// ErrSessionStopped is returned if the manager is stopped concurrently.
//
// It is permitted to call Lease() multiple times on a Session with the
// same target ID. All calls will succeed and point to the same lease record in
// the database if the target is not leased by another Session.
func (s *Session) Lease(ctx context.Context, targetID int) (LeaseResult, error) {
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
	}
	// Loop until we manage to insert a lease record at the right epoch.
	var retryCnt int
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		retryCnt++
		ok := s.waitForValid(ctx)
		if !ok {
			return LeaseResult{}, ErrSessionStopped
		}

		// Knowing that the Session was recently valid, check whether
		// the taget is in the leased set.
		//
		// Note that this check does not need to be atomic with the validity check
		// above. The session might no longer be valid, which is analogous to the
		// session becoming invalid immediately after returning this lease
		// information. Similarly, the session's epoch might have just changed,
		// wiping the leased set clean, in which case we'll acquire a lease.
		lease, leased := func() (LeaseResult, bool) {
			s.mu.Lock()
			defer s.mu.Unlock()
			_, found := s.mu.leasedTargets[targetID]
			if !found {
				return LeaseResult{}, false
			}
			return LeaseResult{
				Epoch:       s.mu.epoch,
				Expiration:  s.mu.expiration,
				ValidityEnd: s.stasisStartLocked(),
			}, true
		}()
		if leased {
			return lease, nil
		}

		// Protect against multiple calls with the same target using a singleflight.
		// We can't hold s.mu for all of this.
		_, _, err := s.leasingGroup.Do(strconv.Itoa(targetID), func() (interface{}, error) {
			if fn := s.testingKnobs.beforeLeaseAcquisitionAttempt; fn != nil {
				fn()
			}
			leaseEpoch, err := s.leaseInner(ctx, targetID)
			if fn := s.testingKnobs.afterLeaseAcquisitionAttempt; fn != nil {
				fn()
			}
			if err != nil {
				return nil, err
			}

			// Insert the target into the leased set, but only if we didn't race with
			// an epoch increment.
			s.mu.Lock()
			if leaseEpoch != s.mu.epoch {
				log.VEventf(ctx, 2, "session epoch change detected. lease epoch: %d, session epoch: %d", leaseEpoch, s.mu.epoch)
				// We raced with an epoch increment, so the lease record we inserted has
				// been deleted. We'll retry and insert a new record, at the new epoch.
				s.mu.Unlock()
				return nil, errSessionChanged
			}
			s.mu.leasedTargets[targetID] = struct{}{}
			s.mu.Unlock()
			return nil, nil
		})
		switch {
		case errors.Is(err, ErrLeasedByAnother):
			return LeaseResult{}, err
		case errors.Is(err, errSessionChanged):
			fallthrough
		case err == nil:
			// We expect to find the target in the leased set when we loop around.
			r.Reset()
			fallthrough
		default:
			continue
		}
	}
	// ctx must have been canceled
	return LeaseResult{}, ctx.Err()
}

// stoppedLocked returns true if Stop() has been called previously.
func (s *Session) stoppedLocked() bool {
	return s.mu.stopped
}

// valid returns whether the session has been heartbeated sufficiently recently
// such that its leases are valid.
func (s *Session) valid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.validLocked()
}

func (s *Session) validLocked() bool {
	return s.clock.Now().Before(s.stasisStartLocked())
}

// heartbeatLoop runs a heartbeat loop that periodically updates the session's
// expiration. The function returns when either the stopper is quiescing, or
// when Stop() was called.
//
// Once s.heartbeatLoop() returns, s.Lease() starts returning ErrSessionStopped.
func (s *Session) heartbeatLoop(ctx context.Context) {
	defer func() {
		close(s.heartbeatStoppedC)
		s.Stop()
	}()

	ctx = logtags.AddTag(ctx, "hbloop", nil)
	timer := s.clock.NewTimer()
	for {
		// Schedule the next heartbeat. Notice that the next heartbeat might
		// happen right away if the session is already not valid (e.g. in tests
		// that move their clock rapidly).
		hbTime := s.expiration().Add(-HeartbeatBeforeStasis)
		timer.Reset(hbTime.Sub(s.clock.Now()))
		select {
		case <-ctx.Done():
			return
		case <-timer.Ch():
			timer.MarkRead()
		}
		retryOpts := retry.Options{
			MaxBackoff: 1 * time.Second,
		}
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			expiration := s.clock.Now().Add(LeaseDuration)
			epoch, err := s.upsertSessionRecord(ctx, s.id, s.epoch(), expiration)
			if err != nil {
				log.Warningf(ctx, "failed to heartbeat session: %s", err)
				continue
			}
			s.update(epoch, expiration)
			break
		}
	}
}

// update updates the session's data.
func (s *Session) update(epoch int, expiration time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If the epoch changed, clear the leased set.
	if s.mu.epoch != epoch {
		s.mu.leasedTargets = make(map[int]struct{})
	}
	s.mu.expiration = expiration
	s.mu.epoch = epoch
	s.mu.onUpdateCV.Broadcast()
}

var errSessionChanged error = errors.New("session deleted or epoch changed")

// ErrLeasedByAnother is returned when a lease cannot be acquired because
// another valid lease for the target exists.
var ErrLeasedByAnother = errors.New("leased by another")

// leaseInner attempts to insert a lease record for a given target. If
// successful, the epoch of the inserted lease record is returned. Since the
// LeaseManger's lock is not held across the insertion, the Session's epoch
// might change after the lease record is inserted. The caller is supposed to
// check the returned epoch against the Session's epoch before telling the
// user anything about the lease.
//
// If another non-expired session has a lease on the target, ErrLeasedByAnother
// is returned.
//
// If the session record is deleted or its epoch changes concurrently with this
// call, errSessionChanged is returned. The caller is supposed to attempt the
// call again, after verifying that the session is valid.
func (s *Session) leaseInner(ctx context.Context, targetID int) (epoch int, _ error) {
	ctx = logtags.AddTag(ctx, "lease-acq", nil)
	// Run a transaction that atomically checks whether there's a lease record,
	// checks the owner's session and, if expired, deletes it, and inserts a
	// lease record.
	txnAttempt := 0
	err := crdbpgx.ExecuteTx(ctx, s.db, pgx.TxOptions{}, func(txn pgx.Tx) error {
		if txnAttempt > 0 {
			log.VEventf(ctx, 2, "insert lease record txn retry: %d", txnAttempt)
		}
		txnAttempt++
		row := txn.QueryRow(ctx,
			"SELECT l.session_id, l.epoch, s.Expiration FROM monitoring_leases l "+
				"INNER JOIN sessions s ON l.session_id=s.id WHERE l.target_id=$1",
			targetID,
		)
		var ownerSessionID uuid.UUID
		var ownerSessionExpiration time.Time
		var leaseEpoch int
		err := row.Scan(&ownerSessionID, &leaseEpoch, &ownerSessionExpiration)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		leaseRecordFound := !errors.Is(err, pgx.ErrNoRows)
		if leaseRecordFound {
			// We found a lease. See who owns it.
			owned := ownerSessionID == s.id
			if owned {
				// The lease record indicates that is owned by this session, so we have
				// a lease. Usually, the Session would be aware of this lease and
				// we wouldn't have gotten to leaseInner(), but, after a process
				// restart, the Session can discover leases in the database.
				epoch = leaseEpoch
				return nil
			}

			// The lease is owned by someone else. Check whether that session is still
			// valid.
			if s.clock.Now().Before(ownerSessionExpiration) {
				// The other owner's session is valid.
				return ErrLeasedByAnother
			}
			// The owner session is expired. Delete the session record, which will
			// also delete the lease record (and all other leases for that session)
			// through a CASCADE.
			log.VEventf(ctx, 2, "deleting session %s in order to steal lease on target: %d",
				ownerSessionID.Short(), targetID)
			if _, err := txn.Exec(ctx, "DELETE FROM sessions WHERE id=$1", ownerSessionID); err != nil {
				return err
			}
		}

		// Either there was no lease on the target, or there was one corresponding
		// to an expired session, which we deleted above. In either case, take the
		// lease.

		s.mu.Lock()
		epoch = s.mu.epoch
		s.mu.Unlock()
		if fn := s.testingKnobs.beforeLeaseRecordInsert; fn != nil {
			fn()
		}
		ok, err := insertLeaseRecord(ctx, txn, targetID, s.id, epoch)
		if err != nil {
			return err
		}
		if !ok {
			return errSessionChanged
		}
		return nil
	})
	return epoch, err
}

// insertLeaseRecord inserts a lease record using a pre-existing transaction.
//
// Returns whether the insertion succeeded. If the insertion fails
// because the referenced session record/epoch no longer exist, false is
// returned.
func insertLeaseRecord(
	ctx context.Context, txn pgx.Tx, targetID int, sessionID uuid.UUID, epoch int,
) (bool, error) {
	log.VEventf(ctx, 2, "inserting lease record for target: %d at epo: %d", targetID, epoch)
	_, err := txn.Exec(ctx,
		"INSERT INTO monitoring_leases (target_id, session_id, epoch) VALUES ($1,$2, $3)",
		targetID, sessionID.GetBytes(), epoch)
	if err != nil {
		// Detect a foreign key validation error, implying that the session record
		// was deleted by someone else from the database, or the session record
		// exists, but has the wrong epoch (which, in turn, means that it must have
		// been recently deleted and then re-inserted at a higher epoch by our
		// heartbeat loop). In either case, we signal to the caller to retry with an
		// updated epoch.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			log.VEventf(ctx, 2, "foreign key validation error; the session must have been deleted")
			// It should be rare that we end up here: the caller validated the session
			// and yet from then until now someone
			// else apparently found the session record to be expired and deleted
			// it. It can happen if there's large clock skew.
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Release releases this lease. After this call, other sessions can acquire a
// lease on the respective target. The lease should not be used any more after
// the call. For the caller's convenience, Release() returns an invalid lease.
func (s *Session) Release(ctx context.Context, targetID int) error {
	s.mu.Lock()
	_, found := s.mu.leasedTargets[targetID]
	s.mu.Unlock()
	if !found {
		return nil
	}

	_, err := s.db.Exec(ctx,
		"DELETE FROM monitoring_leases WHERE session_id=$1 AND target_id=$2",
		s.id, targetID)
	// Remove the target from the leased set regardless of err; the error might be
	// ambiguous about the result of the dabatase transaction, so better to assume
	// we no longer have a lease.
	s.mu.Lock()
	delete(s.mu.leasedTargets, targetID)
	s.mu.Unlock()
	return err
}

// expiration returns the time when, if there are no further heartbeats, the
// session will be considered expired by other sessions (according to their own
// clocks) and its leases will be available to be taken away.
//
// Note that the session's leases stop being usable before that; see
// stasisStart().
func (s *Session) expiration() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.expiration
}

// stasisStart represents the time when, if there are no further heartbeats, the
// session's leases will not be usable anymore.
//
// Note that stasisStart() does not correspond to the moment when the session's
// leases can be stolen by others; that comes a bit later, after the session's
// stasis period. See expiration().
func (s *Session) stasisStart() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stasisStartLocked()
}

func (s *Session) stasisStartLocked() time.Time {
	return s.mu.expiration.Add(-StasisDuration)
}

func (s *Session) epoch() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.epoch
}

// waitForValid waits until the session is valid. If the Session is already
// valid, the call returns immediately. Otherwise, the call blocks for the
// session's heartbeats to make the session valid again. If the session is not
// valid, there should be a heartbeat retry loop in progress, which will
// eventually unblock this call.
//
// If the session has already been stopped or is stopped concurrently with this
// call, waitForValid returns false.
//
// Note that the call returns when the session is valid instantaneously, making
// no promises about how long the validity will last.
func (s *Session) waitForValid(ctx context.Context) bool {
	// Take the lock in order to perform atomic stopped/valid checks in the loop.
	// The lock will be released while each iteration of the loop waits on the
	// cond var.
	s.mu.Lock()
	defer s.mu.Unlock()
	// Loop until the session is found to be valid. Every iteration of the loop
	// will wait for a heartbeat.
	for {
		// Lock the session and check if it's valid. If it's not, wait for an
		// update; the heartbeat loop should be working in the background.
		if s.stoppedLocked() {
			return false
		}
		if s.validLocked() {
			return true
		}

		if s.testingKnobs.onLeaseAcquisitionBlockedOnHeartbeatOnce != nil {
			close(s.testingKnobs.onLeaseAcquisitionBlockedOnHeartbeatOnce)
			s.testingKnobs.onLeaseAcquisitionBlockedOnHeartbeatOnce = nil
		}

		// Wait for an update.
		s.mu.onUpdateCV.Wait()

		// Loop around; hopefully now the session is valid.
	}
}
