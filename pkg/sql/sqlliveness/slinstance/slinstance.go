// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package slinstance provides functionality for acquiring sqlliveness leases
// via sessions that have a unique id and expiration. The creation and
// maintenance of session liveness is entrusted to the Instance. Each SQL
// server will have a handle to an instance.
package slinstance

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var (
	// DefaultTTL specifies the time to expiration when a session is created.
	DefaultTTL = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.sqlliveness.ttl",
		"default sqlliveness session ttl",
		40*time.Second,
		settings.NonNegativeDuration,
	)
	// DefaultHeartBeat specifies the period between attempts to extend a session.
	DefaultHeartBeat = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.sqlliveness.heartbeat",
		"duration heart beats to push session expiration further out in time",
		5*time.Second,
		settings.NonNegativeDuration,
	)
)

// Writer provides interactions with the storage of session records.
type Writer interface {
	// Insert stores the input Session.
	Insert(ctx context.Context, id sqlliveness.SessionID, expiration hlc.Timestamp) error
	// Update looks for a Session with the same SessionID as the input Session in
	// the storage and if found replaces it with the input returning true.
	// Otherwise it returns false to indicate that the session does not exist.
	Update(ctx context.Context, id sqlliveness.SessionID, expiration hlc.Timestamp) (bool, error)
}

type session struct {
	id    sqlliveness.SessionID
	start hlc.Timestamp

	mu struct {
		syncutil.RWMutex
		exp hlc.Timestamp
		// sessionExpiryCallbacks are invoked when the session expires. They're
		// invoked under the session's lock, so keep them small.
		sessionExpiryCallbacks []func(ctx context.Context)
	}
}

// ID implements the sqlliveness.Session interface.
func (s *session) ID() sqlliveness.SessionID { return s.id }

// Expiration implements the sqlliveness.Session interface.
func (s *session) Expiration() hlc.Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.exp
}

// Start implements the sqlliveness.Session interface.
func (s *session) Start() hlc.Timestamp {
	return s.start
}

// RegisterCallbackForSessionExpiry adds the given function to the list
// of functions called after a session expires. The functions are
// executed in a goroutine.
func (s *session) RegisterCallbackForSessionExpiry(sExp func(context.Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sessionExpiryCallbacks = append(s.mu.sessionExpiryCallbacks, sExp)
}

func (s *session) invokeSessionExpiryCallbacks(ctx context.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, callback := range s.mu.sessionExpiryCallbacks {
		callback(ctx)
	}
}

func (s *session) setExpiration(exp hlc.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.exp = exp
}

// Instance implements the sqlliveness.Instance interface by storing the
// liveness sessions in table system.sqlliveness and relying on a heart beat
// loop to extend the existing sessions' expirations or creating a new session
// to replace a session that has expired and deleted from the table.
// TODO(rima): Rename Instance to avoid confusion with sqlinstance.SQLInstance.
type Instance struct {
	clock     *hlc.Clock
	settings  *cluster.Settings
	stopper   *stop.Stopper
	storage   Writer
	ttl       func() time.Duration
	hb        func() time.Duration
	testKnobs sqlliveness.TestingKnobs
	startErr  error
	mu        struct {
		started bool
		syncutil.Mutex
		blockCh chan struct{}
		s       *session
	}
}

func (l *Instance) getSessionOrBlockCh() (*session, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.s != nil {
		return l.mu.s, nil
	}
	return nil, l.mu.blockCh
}

func (l *Instance) setSession(s *session) {
	l.mu.Lock()
	l.mu.s = s
	// Notify all calls to Session that a non-nil session is available.
	close(l.mu.blockCh)
	l.mu.Unlock()
}

func (l *Instance) clearSession(ctx context.Context) {
	l.checkExpiry(ctx)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.s = nil
	l.mu.blockCh = make(chan struct{})
}

func (l *Instance) checkExpiry(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if expiration := l.mu.s.Expiration(); expiration.Less(l.clock.Now()) {
		// If the session has expired, invoke the session expiry callbacks
		// associated with the session.
		l.mu.s.invokeSessionExpiryCallbacks(ctx)
	}
}

// createSession tries until it can create a new session and returns an error
// only if the heart beat loop should exit.
func (l *Instance) createSession(ctx context.Context) (*session, error) {
	id := sqlliveness.SessionID(uuid.MakeV4().GetBytes())
	start := l.clock.Now()
	exp := start.Add(l.ttl().Nanoseconds(), 0)
	s := &session{
		id:    id,
		start: start,
	}
	s.mu.exp = exp

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
	}
	everySecond := log.Every(time.Second)
	var err error
	for i, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
		i++
		if err = l.storage.Insert(ctx, s.id, s.Expiration()); err != nil {
			if ctx.Err() != nil {
				break
			}
			if everySecond.ShouldLog() {
				log.Errorf(ctx, "failed to create a session at %d-th attempt: %v", i, err)
			}
			// Unauthenticated errors are unrecoverable, we should break instead
			// of retrying.
			if grpcutil.IsAuthError(err) {
				break
			}
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "created new SQL liveness session %s", s.ID())
	return s, nil
}

func (l *Instance) extendSession(ctx context.Context, s *session) (bool, error) {
	exp := l.clock.Now().Add(l.ttl().Nanoseconds(), 0)

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
	}
	var err error
	var found bool
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		if found, err = l.storage.Update(ctx, s.ID(), exp); err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		break
	}
	if err != nil {
		return false, err
	}

	if !found {
		return false, nil
	}

	s.setExpiration(exp)
	return true, nil
}

func (l *Instance) heartbeatLoop(ctx context.Context) {
	defer func() {
		log.Warning(ctx, "exiting heartbeat loop")
	}()
	ctx, cancel := l.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	t := timeutil.NewTimer()
	t.Reset(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
			s, _ := l.getSessionOrBlockCh()
			if s == nil {
				var newSession *session
				if err := contextutil.RunWithTimeout(ctx, "sqlliveness create session", l.hb(), func(ctx context.Context) error {
					var err error
					newSession, err = l.createSession(ctx)
					return err
				}); err != nil {
					log.Errorf(ctx, "sqlliveness failed to create new session: %v", err)
					func() {
						l.mu.Lock()
						defer l.mu.Unlock()
						l.startErr = err
						// There was an unrecoverable error when trying to
						// create the session. Notify all calls to Session that
						// the session failed.
						close(l.mu.blockCh)
					}()
					return
				}
				l.setSession(newSession)
				t.Reset(l.hb())
				continue
			}
			var found bool
			err := contextutil.RunWithTimeout(ctx, "sqlliveness extend session", l.hb(), func(ctx context.Context) error {
				var err error
				found, err = l.extendSession(ctx, s)
				return err
			})
			switch {
			case errors.HasType(err, (*contextutil.TimeoutError)(nil)):
				// Retry without clearing the session because we don't know the current status.
				l.checkExpiry(ctx)
				t.Reset(0)
				continue
			case err != nil && ctx.Err() == nil:
				log.Errorf(ctx, "sqlliveness failed to extend session: %v", err)
				fallthrough
			case err != nil:
				// TODO(ajwerner): Decide whether we actually should exit the heartbeat loop here if the context is not
				// canceled. Consider the case of an ambiguous result error: shouldn't we try again?
				l.clearSession(ctx)
				return
			case !found:
				// No existing session found, immediately create one.
				l.clearSession(ctx)
				// Start next loop iteration immediately to insert a new session.
				t.Reset(0)
			default:
				if log.V(2) {
					log.Infof(ctx, "extended SQL liveness session %s", s.ID())
				}
				t.Reset(l.hb())
			}
		}
	}
}

// NewSQLInstance returns a new Instance struct and starts its heartbeating
// loop.
func NewSQLInstance(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	storage Writer,
	settings *cluster.Settings,
	testKnobs *sqlliveness.TestingKnobs,
) *Instance {
	l := &Instance{
		clock:    clock,
		settings: settings,
		storage:  storage,
		stopper:  stopper,
		ttl: func() time.Duration {
			return DefaultTTL.Get(&settings.SV)
		},
		hb: func() time.Duration {
			return DefaultHeartBeat.Get(&settings.SV)
		},
	}
	if testKnobs != nil {
		l.testKnobs = *testKnobs
	}
	l.mu.blockCh = make(chan struct{})
	return l
}

// Start runs the hearbeat loop.
func (l *Instance) Start(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.started {
		return
	}
	log.Infof(ctx, "starting SQL liveness instance")
	_ = l.stopper.RunAsyncTask(ctx, "slinstance", l.heartbeatLoop)
	l.mu.started = true
}

// Session returns a live session id. For each Sqlliveness instance the
// invariant is that there exists at most one live session at any point in time.
// If the current one has expired then a new one is created.
func (l *Instance) Session(ctx context.Context) (sqlliveness.Session, error) {
	if l.testKnobs.SessionOverride != nil {
		if s, err := l.testKnobs.SessionOverride(ctx); s != nil || err != nil {
			return s, err
		}
	}
	l.mu.Lock()
	if !l.mu.started {
		l.mu.Unlock()
		return nil, sqlliveness.NotStartedError
	}
	l.mu.Unlock()

	for {
		s, ch := l.getSessionOrBlockCh()
		if s != nil {
			return s, nil
		}

		select {
		case <-l.stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
			var err error
			func() {
				l.mu.Lock()
				defer l.mu.Unlock()
				err = l.startErr
			}()
			if err != nil {
				return nil, err
			}
		}
	}
}
