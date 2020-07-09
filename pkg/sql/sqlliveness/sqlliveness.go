// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlliveness

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// SessionID represents an opaque identifier for a session. This ID should be
// globally unique.
type SessionID []byte

// Equals returns true if ids are equal.
func (s SessionID) Equals(other SessionID) bool {
	return bytes.Equal(s, other)
}

// SQLInstance represents a SQL tenant server instance and is responsible for
// maintaining at most once session for this instance and heart beating the
// current live one if it exists and otherwise creating a new  live one.
type SQLInstance interface {
	Session(context.Context) (Session, error)
}

type Session interface {
	ID() SessionID

	// Expiration is the current expiration value for this Session. If the Session
	// expires, this function will return a zero-value timestamp.
	// Transactions run by this Instance which ensure that they commit before
	// this time will be assured that any resources claimed under this session
	// are known to be valid.
	//
	// See discussion in Open Questions in
	// http://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200615_sql_liveness.md
	Expiration() hlc.Timestamp
}

// Storage abstracts over the set of sessions in the cluster.
type Storage interface {
	// IsAlive is used to query the liveness of a Session typically by another
	// SQLInstance that is attempting to claim expired resources.
	IsAlive(context.Context, SessionID) (alive bool, err error)
}

type session struct {
	sid SessionID
	exp hlc.Timestamp
}

// ID implements the Session interface method ID.
func (s *session) ID() SessionID { return s.sid }

// Expiration implements the Session interface method Expiration.
func (s *session) Expiration() hlc.Timestamp { return s.exp }

type SqlLiveness struct {
	db      *kv.DB
	ex      sqlutil.InternalExecutor
	d       time.Duration
	hb      time.Duration
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		s *session
	}
}

func (l *SqlLiveness) startHeartbeatLoop(ctx context.Context) {
	for {
		select {
		case <-l.stopper.ShouldStop():
			return
		case <-time.After(l.hb):
			l.mu.Lock()
			if l.mu.s == nil {
				continue
				l.mu.Unlock()
			}
			// Extend the current live session.
			if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				now := l.db.Clock().Now()
				exp := now.Add(l.d.Nanoseconds(), 1)

				data, err := l.ex.QueryRowEx(
					ctx, "extend-session", txn,
					sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
UPDATE system.sqlliveness SET expiration = $1
WHERE expiration > $2 AND session_id = $3 RETURNING session_id`,
					tree.TimestampToDecimal(exp), tree.TimestampToDecimal(now), l.mu.s.ID(),
				)
				if err != nil {
					return errors.Wrapf(err, "Could not heart beat session: %d with expiration: %+v",
						l.mu.s.ID(), l.mu.s.Expiration())
				}
				if data == nil {
					l.mu.s.exp = hlc.Timestamp{}
					log.Warningf(ctx, "sid: %s has expired\n", hex.EncodeToString(l.mu.s.ID()))
					_, err = l.ex.QueryRowEx(
						ctx, "delete-session", txn,
						sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
DELETE FROM system.sqlliveness WHERE session_id = $1`, l.mu.s.ID(),
					)
					if err != nil {
						return errors.Wrapf(err, "Could not delete session: %d", l.mu.s.ID())
					}
				} else {
					l.mu.s.exp = exp
				}
				return nil
			}); err != nil {
				log.Error(ctx, err.Error())
			}
			l.mu.Unlock()
		}
	}
}

func NewSqlLiveness(
	ctx context.Context,
	stopper *stop.Stopper,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
	deadline time.Duration,
	heartbeat time.Duration,
) SQLInstance {
	l := &SqlLiveness{db: db, ex: ex, d: deadline, hb: heartbeat, stopper: stopper}
	stopper.RunWorker(ctx, l.startHeartbeatLoop)
	return l
}

// Get returns a live session id. For each Sqlliveness instance the invariant is
// that there exists at most one live session at any point in time. If the
// current one has expired then a new one is created.
func (l *SqlLiveness) Session(ctx context.Context) (Session, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.db.Clock().Now()
	if l.mu.s != nil && l.mu.s.Expiration().WallTime > now.WallTime {
		return l.mu.s, nil
	}

	id := uuid.MakeV4().GetBytes()
	exp := l.db.Clock().Now().Add(l.d.Nanoseconds(), 1)
	if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := l.ex.QueryRowEx(
			ctx, "insert-session", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`INSERT INTO system.sqlliveness VALUES ($1, $2)`,
			id, tree.TimestampToDecimal(exp),
		)
		if err != nil {
			return errors.Wrap(err, "Could not create new liveness session")
		}
		if l.mu.s != nil {
			_, err = l.ex.QueryRowEx(
				ctx, "delete-session", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
DELETE FROM system.sqlliveness WHERE session_id = $1`, l.mu.s.ID(),
			)
		}
		if err != nil {
			log.Warningf(ctx, "Could not delete session: %d with err: %+v", l.mu.s.ID(), err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	log.Infof(ctx, "Created new sid: %s).\n", hex.EncodeToString(id))
	l.mu.s = &session{sid: id, exp: exp}
	return l.mu.s, nil
}
