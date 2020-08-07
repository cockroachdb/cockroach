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
// maintenance of session liveness is entrusted to the SQLInstance. Each SQL
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
	"github.com/cockroachdb/errors"
)

var (
	// DefaultTTL specifies the time to expiration when a session is created.
	DefaultTTL = settings.RegisterNonNegativeDurationSetting(
		"server.sqlliveness.ttl",
		"default sqlliveness session ttl",
		40*time.Second,
	)
	// DefaultHeartBeat specifies the period between attempts to extend a session.
	DefaultHeartBeat = settings.RegisterNonNegativeDurationSetting(
		"server.sqlliveness.heartbeat",
		"duration heart beats to push session expiration further out in time",
		time.Second,
	)
)

type session struct {
	id  sqlliveness.SessionID
	exp hlc.Timestamp
}

// ID implements the Session interface method ID.
func (s *session) ID() sqlliveness.SessionID { return s.id }

// Expiration implements the Session interface method Expiration.
func (s *session) Expiration() hlc.Timestamp { return s.exp }

// SQLInstance implements the sqlliveness.SQLInstance interface by storing the
// liveness sessions in table system.sqlliveness and relying on a heart beat
// loop to extend the existing sessions' expirations or creating a new session
// to replace a session that has expired and deleted from the table.
type SQLInstance struct {
	clock   *hlc.Clock
	stopper *stop.Stopper
	storage sqlliveness.Storage
	ttl     func() time.Duration
	hb      func() time.Duration
	mu      struct {
		started bool
		syncutil.Mutex
		blockCh chan struct{}
		s       *session
	}
}

func (l *SQLInstance) getSessionOrBlockCh() (*session, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.s != nil {
		return l.mu.s, nil
	}
	return nil, l.mu.blockCh
}

func (l *SQLInstance) setSession(s *session) {
	l.mu.Lock()
	l.mu.s = s
	// Notify all calls to Session that a non-nil session is available.
	close(l.mu.blockCh)
	l.mu.Unlock()
}

func (l *SQLInstance) clearSession() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.s = nil
	l.mu.blockCh = make(chan struct{})
}

// createSession tries until it can create a new session and returns an error
// only if the heart beat loop should exit.
func (l *SQLInstance) createSession(ctx context.Context) (*session, error) {
	id := uuid.MakeV4().GetBytes()
	exp := l.clock.Now().Add(l.ttl().Nanoseconds(), 0)
	s := &session{id: id, exp: exp}

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
		Closer:         l.stopper.ShouldStop(),
	}
	everySecond := log.Every(time.Second)
	var err error
	for i, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
		i++
		if err = l.storage.Insert(ctx, s); err != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
				break
			}
			if everySecond.ShouldLog() {
				log.Errorf(ctx, "Failed to create a session at %d-th attempt: %v", i, err.Error())
			}
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "Created new SQL liveness session %s", s.ID())
	return s, nil
}

func (l *SQLInstance) extendSession(ctx context.Context, s sqlliveness.Session) (bool, error) {
	exp := l.clock.Now().Add(l.ttl().Nanoseconds(), 0)

	opts := retry.Options{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     1.5,
		Closer:         l.stopper.ShouldStop(),
	}
	var err error
	var found bool
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		if found, err = l.storage.Update(ctx, &session{id: s.ID(), exp: exp}); err != nil {
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

func (l *SQLInstance) heartbeatLoop(ctx context.Context) {
	defer func() {
		log.Warning(ctx, "exiting heartbeat loop")
	}()
	t := timeutil.NewTimer()
	t.Reset(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stopper.ShouldStop():
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
				log.Infof(ctx, "Extended SQL liveness session %s", s.ID())
			}
			t.Reset(l.hb())
		}
	}
}

// NewSQLInstance returns a new SQLInstance struct and starts its heartbeating
// loop.
func NewSQLInstance(
	stopper *stop.Stopper, clock *hlc.Clock, storage sqlliveness.Storage, settings *cluster.Settings,
) sqlliveness.SQLInstance {
	l := &SQLInstance{
		clock: clock, storage: storage, stopper: stopper,
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
func (l *SQLInstance) Start(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.started {
		return
	}
	log.Infof(ctx, "starting SQL liveness instance")
	if err := l.stopper.RunAsyncTask(ctx, "SQL-livenes-storage", func(ctx context.Context) {
		l.storage.Start(ctx)
	}); err != nil {
		log.Fatal(ctx, err.Error())
	}
	l.stopper.RunWorker(ctx, l.heartbeatLoop)
	l.mu.started = true
}

// Session returns a live session id. For each Sqlliveness instance the
// invariant is that there exists at most one live session at any point in time.
// If the current one has expired then a new one is created.
func (l *SQLInstance) Session(ctx context.Context) (sqlliveness.Session, error) {
	l.mu.Lock()
	if !l.mu.started {
		l.mu.Unlock()
		return nil, errors.New("the SQLInstance has not been started yet")
	}
	l.mu.Unlock()

	for {
		s, ch := l.getSessionOrBlockCh()
		if s != nil {
			return s, nil
		}

		select {
		case <-l.stopper.ShouldStop():
			return nil, errors.New("the SQLInstance has been stopped")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}
