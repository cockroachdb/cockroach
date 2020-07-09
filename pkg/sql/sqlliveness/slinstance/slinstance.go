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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

type session struct {
	sid sqlliveness.SessionID
	exp hlc.Timestamp
}

// ID implements the Session interface method ID.
func (s *session) ID() sqlliveness.SessionID { return s.sid }

// Expiration implements the Session interface method Expiration.
func (s *session) Expiration() hlc.Timestamp { return s.exp }

// SQLInstance implements the sqlliveness.SQLInstance interface by storing the
// liveness sessions in table system.sqlliveness and relying on a heart beat
// loop to extend the existing sessions' expirations or creating a new session
// to replace a session that has expired and deleted from the table.
type SQLInstance struct {
	clock   *hlc.Clock
	db      *kv.DB
	ex      sqlutil.InternalExecutor
	ttl     time.Duration
	hb      time.Duration
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		blockCh chan struct{}
		s       *session
	}
}

func (l *SQLInstance) getSessionOrBlockCh() (sqlliveness.Session, chan struct{}) {
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

func (l *SQLInstance) createSession(ctx context.Context) (*session, error) {
	id := uuid.MakeV4().GetBytes()
	exp := l.clock.Now().Add(l.ttl.Nanoseconds(), 0)
	if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := l.ex.QueryRowEx(
			ctx, "create-session", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`INSERT INTO system.sqlliveness VALUES ($1, $2)`,
			id, tree.TimestampToDecimal(exp),
		)
		return err
	}); err != nil {
		return nil, errors.Wrapf(err, "Could not create new liveness session")
	}
	return &session{sid: id, exp: exp}, nil
}

func (l *SQLInstance) extendSession(ctx context.Context) error {
	s, _ := l.getSessionOrBlockCh()
	exp := l.clock.Now().Add(l.ttl.Nanoseconds(), 1)
	if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		data, err := l.ex.QueryRowEx(
			ctx, "extend-session", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
UPDATE system.sqlliveness SET expiration = $1
WHERE session_id = $2 RETURNING session_id`,
			tree.TimestampToDecimal(exp), s.ID(),
		)
		if err != nil {
			return errors.Wrapf(err, "Could not extend session: %s with expiration: %+v",
				s.ID(), s.Expiration())
		}
		if sessionExpired := data == nil; sessionExpired {
			return errors.Errorf("Session %s expired", s.ID())
		}
		return nil
	}); err != nil {
		return err
	}

	l.mu.Lock()
	l.mu.s.exp = exp
	l.mu.Unlock()
	return nil
}

func (l *SQLInstance) heartbeatLoop(ctx context.Context) {
	for {
		t := timeutil.NewTimer()
		t.Reset(0)
		select {
		case <-ctx.Done():
			return
		case <-l.stopper.ShouldStop():
			return
		case <-t.C:
			t.Read = true
			t.Reset(l.hb)
			opts := retry.Options{
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     2 * time.Second,
				Multiplier:     1.5,
			}
			everySecond := log.Every(time.Second)
			if s, _ := l.getSessionOrBlockCh(); s == nil {
				for r := retry.StartWithCtx(ctx, opts); r.Next(); {
					select {
					case <-l.stopper.ShouldStop():
						return
					default:
					}
					newSession, err := l.createSession(ctx)
					if err != nil {
						if everySecond.ShouldLog() {
							log.Error(ctx, err.Error())
						}
						continue
					}
					l.setSession(newSession)
					break
				}
			} else {
				opts.MaxRetries = 5
				var err error
				for r := retry.StartWithCtx(ctx, opts); r.Next(); {
					select {
					// Finish immediately in tests instead of waiting for the retry.;w
					case <-l.stopper.ShouldStop():
						return
					default:
					}
					if err = l.extendSession(ctx); err != nil {
						continue
					}
					break
				}
				if err != nil {
					log.Error(ctx, err.Error())
					l.clearSession()
					// Start next loop iteration immediately to insert a new session.
					t.Reset(0)
				}
			}
		}
	}
}

type Options struct {
	Deadline  time.Duration
	Heartbeat time.Duration
}

// NewSqlInstance returns a new SQLInstance struct and starts its heartbeating
// loop.
func NewSqlInstance(
	stopper *stop.Stopper, clock *hlc.Clock, db *kv.DB, ex sqlutil.InternalExecutor, opts *Options,
) sqlliveness.SQLInstance {
	l := &SQLInstance{clock: clock, db: db, ex: ex, ttl: opts.Deadline, hb: opts.Heartbeat, stopper: stopper}
	l.mu.blockCh = make(chan struct{})
	return l
}

// Start starts the hearbeat loop.
func (l *SQLInstance) Start(ctx context.Context) {
	l.stopper.RunWorker(ctx, l.heartbeatLoop)
}

// Session returns a live session id. For each Sqlliveness instance the
// invariant is that there exists at most one live session at any point in time.
// If the current one has expired then a new one is created.
func (l *SQLInstance) Session(ctx context.Context) (sqlliveness.Session, error) {
	for {
		s, ch := l.getSessionOrBlockCh()
		if s != nil {
			return s, nil
		}

		select {
		case <-l.stopper.ShouldStop():
			return nil, errors.New("SQLInstance has been stopped.")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}
