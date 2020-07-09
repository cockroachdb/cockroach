// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

type session struct {
	Sid sqlliveness.SessionID
	Exp hlc.Timestamp
}

// ID implements the Session interface method ID.
func (s *session) ID() sqlliveness.SessionID { return s.Sid }

// Expiration implements the Session interface method Expiration.
func (s *session) Expiration() hlc.Timestamp { return s.Exp }

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
	exp := l.clock.Now().Add(l.ttl.Nanoseconds(), 1)
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
	return &session{Sid: id, Exp: exp}, nil
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
	l.mu.s.Exp = exp
	l.mu.Unlock()
	return nil
}

func (l *SQLInstance) heartbeatLoop(ctx context.Context) {
	for {
		t := timeutil.NewTimer()
		t.Reset(0)
		select {
		case <-l.stopper.ShouldStop():
			return
		case <-t.C:
			t.Read = true
			t.Reset(l.hb)

			if s, _ := l.getSessionOrBlockCh(); s == nil {
				newSession, err := l.createSession(ctx)
				if err != nil {
					// TODO(spaskob): should we poison the SQL instance so that calls
					// to Session are not blocked indefinitely? Should we retry?
					log.Errorf(ctx, err.Error())
					continue
				}
				l.setSession(newSession)
			} else {
				if err := l.extendSession(ctx); err != nil {
					l.clearSession()
					t.Reset(0)
				}
			}
		}
	}
}

// NewSqlInstance returns a new SQLInstance struct and starts its heartbeating
// loop.
func NewSqlInstance(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
	deadline time.Duration,
	heartbeat time.Duration,
) sqlliveness.SQLInstance {
	l := &SQLInstance{clock: clock, db: db, ex: ex, ttl: deadline, hb: heartbeat, stopper: stopper}
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
		case <-ch:
		}
	}
}
