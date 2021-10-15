// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package slsession provides functionality for acquiring sqlliveness leases
// via sessions that have a unique id and expiration. The creation and
// maintenance of session liveness is entrusted to the Factory. Each SQL
// server will have a handle to an instance.
package slsession

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Factory implements the sqlliveness.SessionFactory interface by storing the
// liveness sessions in table system.sqlliveness and relying on a heart beat
// loop to extend the existing sessions' expirations or creating a new session
// to replace a session that has expired and deleted from the table.
type Factory struct {
	clock    *hlc.Clock
	settings *cluster.Settings
	stopper  *stop.Stopper
	storage  slstorage.Writer

	ttl        func() time.Duration
	hb         func() time.Duration
	gcInterval func() time.Duration

	testKnobs sqlliveness.TestingKnobs
	mu        struct {
		started bool
		syncutil.Mutex
		blockCh chan struct{}
		s       *session
	}
}

// NewFactory returns a new Factory struct and starts its heart-beating and
// gc loops.
func NewFactory(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	storage slstorage.Writer,
	settings *cluster.Settings,
	testKnobs *sqlliveness.TestingKnobs,
) *Factory {
	l := &Factory{
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
		gcInterval: func() time.Duration {
			baseInterval := GCInterval.Get(&settings.SV)
			jitter := GCJitter.Get(&settings.SV)
			frac := 1 + (2*rand.Float64()-1)*jitter
			return time.Duration(frac * float64(baseInterval.Nanoseconds()))
		},
	}
	if testKnobs != nil {
		l.testKnobs = *testKnobs
	}
	l.mu.blockCh = make(chan struct{})
	return l
}

func (sf *Factory) getSessionOrBlockCh() (*session, chan struct{}) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if sf.mu.s != nil {
		return sf.mu.s, nil
	}
	return nil, sf.mu.blockCh
}

func (sf *Factory) setSession(s *session) {
	sf.mu.Lock()
	sf.mu.s = s
	// Notify all calls to Session that a non-nil session is available.
	close(sf.mu.blockCh)
	sf.mu.Unlock()
}

func (sf *Factory) clearSession(ctx context.Context) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if expiration := sf.mu.s.Expiration(); expiration.Less(sf.clock.Now()) {
		// If the session has expired, invoke the session expiry callbacks
		// associated with the session.
		sf.mu.s.invokeSessionExpiryCallbacks(ctx)
	}
	sf.mu.s = nil
	sf.mu.blockCh = make(chan struct{})
}

// createSession tries until it can create a new session and returns an error
// only if the heart beat loop should exit.
func (sf *Factory) createSession(ctx context.Context) (*session, error) {
	id := sqlliveness.SessionID(uuid.MakeV4().GetBytes())
	s := &session{id: id}

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
	}
	everySecond := log.Every(time.Second)
	var err error
	for i, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
		exp := sf.clock.Now().Add(sf.ttl().Nanoseconds(), 0)
		s.mu.exp = exp
		i++
		if err = sf.storage.Insert(ctx, s.id, s.Expiration()); err != nil {
			if ctx.Err() != nil {
				break
			}
			if everySecond.ShouldLog() {
				log.Errorf(ctx, "failed to create a session at %d-th attempt: %v", i, err)
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

func (sf *Factory) extendSession(ctx context.Context, s *session) (bool, error) {

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
	}
	var err error
	var found bool
	var exp hlc.Timestamp
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		exp = sf.clock.Now().Add(sf.ttl().Nanoseconds(), 0)
		if found, err = sf.storage.Update(ctx, s.ID(), exp); err != nil {
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

func (sf *Factory) heartbeatLoop(ctx context.Context) {
	defer func() {
		log.Warning(ctx, "exiting heartbeat loop")
	}()
	ctx, cancel := sf.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	t := timeutil.NewTimer()
	t.Reset(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
			s, _ := sf.getSessionOrBlockCh()
			if s == nil {
				newSession, err := sf.createSession(ctx)
				if err != nil {
					return
				}
				sf.setSession(newSession)
				t.Reset(sf.hb())
				continue
			}
			found, err := sf.extendSession(ctx, s)
			if err != nil {
				sf.clearSession(ctx)
				return
			}
			if !found {
				sf.clearSession(ctx)
				// Start next loop iteration immediately to insert a new session.
				t.Reset(0)
				continue
			}
			if log.V(2) {
				log.Infof(ctx, "extended SQL liveness session %s", s.ID())
			}
			t.Reset(sf.hb())
		}
	}
}

// Start runs the hearbeat loop.
func (sf *Factory) Start(ctx context.Context) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if sf.mu.started {
		return
	}
	log.Infof(ctx, "starting SQL liveness instance")
	_ = sf.stopper.RunAsyncTask(ctx, "slsession-hb", sf.heartbeatLoop)
	_ = sf.stopper.RunAsyncTask(ctx, "slsession-gc", sf.deleteSessionsLoop)
	sf.mu.started = true
}

// Session returns a live session id. For each sqlliveness SessionFactory the
// invariant is that there exists at most one live session at any point in time.
// If the current one has expired then a new one is created.
func (sf *Factory) Session(ctx context.Context) (sqlliveness.Session, error) {
	if sf.testKnobs.SessionOverride != nil {
		if s, err := sf.testKnobs.SessionOverride(ctx); s != nil || err != nil {
			return s, err
		}
	}
	sf.mu.Lock()
	if !sf.mu.started {
		sf.mu.Unlock()
		return nil, sqlliveness.NotStartedError
	}
	sf.mu.Unlock()

	for {
		s, ch := sf.getSessionOrBlockCh()
		if s != nil {
			return s, nil
		}

		select {
		case <-sf.stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}

// deleteSessionsLoop is launched in start and periodically deletes sessions.
func (sf *Factory) deleteSessionsLoop(ctx context.Context) {
	ctx, cancel := sf.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	var t timeutil.TimerI
	if sf.testKnobs.NewTimer != nil {
		t = sf.testKnobs.NewTimer()
	} else {
		t = timeutil.DefaultTimeSource{}.NewTimer()
	}
	t.Reset(sf.gcInterval())
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.Ch():
			t.MarkRead()
			sf.storage.DeleteExpiredSessions(ctx, sf.clock.Now())
			t.Reset(sf.gcInterval())
		}
	}
}
