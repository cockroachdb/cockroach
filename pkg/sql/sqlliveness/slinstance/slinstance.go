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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// DefaultTTL specifies the time to expiration when a session is created.
	DefaultTTL = settings.RegisterDurationSetting(
		"server.sqlliveness.ttl",
		"default sqlliveness session ttl",
		40*time.Second,
		settings.NonNegativeDuration,
	)
	// DefaultHeartBeat specifies the period between attempts to extend a session.
	DefaultHeartBeat = settings.RegisterDurationSetting(
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
	id  sqlliveness.SessionID
	exp hlc.Timestamp
}

// ID implements the Session interface method ID.
func (s *session) ID() sqlliveness.SessionID { return s.id }

// Expiration implements the Session interface method Expiration.
func (s *session) Expiration() hlc.Timestamp { return s.exp }

// Instance implements the sqlliveness.Instance interface by storing the
// liveness sessions in table system.sqlliveness and relying on a heart beat
// loop to extend the existing sessions' expirations or creating a new session
// to replace a session that has expired and deleted from the table.
type Instance struct {
	clock    *hlc.Clock
	settings *cluster.Settings
	stopper  *stop.Stopper
	storage  Writer
	ttl      func() time.Duration
	hb       func() time.Duration
	mu       struct {
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

func (l *Instance) clearSession() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.s = nil
	l.mu.blockCh = make(chan struct{})
}

// createSession tries until it can create a new session and returns an error
// only if the heart beat loop should exit.
func (l *Instance) createSession(ctx context.Context) (*session, error) {
	id := sqlliveness.SessionID(uuid.MakeV4().GetBytes())
	exp := l.clock.Now().Add(l.ttl().Nanoseconds(), 0)
	s := &session{id: id, exp: exp}

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
	}
	everySecond := log.Every(time.Second)
	var err error
	for i, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
		i++
		if err = l.storage.Insert(ctx, s.id, s.exp); err != nil {
			if ctx.Err() != nil {
				break
			}
			if everySecond.ShouldLog() {
				log.Errorf(ctx, "failed to create a session at %d-th attempt: %v", i, err.Error())
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

func (l *Instance) extendSession(ctx context.Context, s sqlliveness.Session) (bool, error) {
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

	l.mu.Lock()
	l.mu.s.exp = exp
	l.mu.Unlock()
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
				newSession, err := l.createSession(ctx)
				if err != nil {
					return
				}
				l.setSession(newSession)
				t.Reset(l.hb())
				continue
			}
			found, err := l.extendSession(ctx, s)
			if err != nil {
				l.clearSession()
				return
			}
			if !found {
				l.clearSession()
				// Start next loop iteration immediately to insert a new session.
				t.Reset(0)
				continue
			}
			if log.V(2) {
				log.Infof(ctx, "extended SQL liveness session %s", s.ID())
			}
			t.Reset(l.hb())
		}
	}
}

// NewSQLInstance returns a new Instance struct and starts its heartbeating
// loop.
func NewSQLInstance(
	stopper *stop.Stopper, clock *hlc.Clock, storage Writer, settings *cluster.Settings,
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
		}
	}
}
