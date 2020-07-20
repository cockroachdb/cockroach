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
	"encoding/hex"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slsession"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

type SQLInstance struct {
	db      *kv.DB
	ex      sqlutil.InternalExecutor
	d       time.Duration
	hb      time.Duration
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		s *slsession.Session
	}
}

func (l *SQLInstance) startHeartbeatLoop(ctx context.Context) {
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
					l.mu.s.Exp = hlc.Timestamp{}
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
					l.mu.s.Exp = exp
				}
				return nil
			}); err != nil {
				log.Error(ctx, err.Error())
			}
			l.mu.Unlock()
		}
	}
}

// NewSqlInstance returns a new SQLInstance struct and starts its heartbeating
// loop.
func NewSqlInstance(
	ctx context.Context,
	stopper *stop.Stopper,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
	deadline time.Duration,
	heartbeat time.Duration,
) sqlliveness.SQLInstance {
	l := &SQLInstance{db: db, ex: ex, d: deadline, hb: heartbeat, stopper: stopper}
	stopper.RunWorker(ctx, l.startHeartbeatLoop)
	return l
}

// Session returns a live session id. For each Sqlliveness instance the
// invariant is that there exists at most one live session at any point in time.
// If the current one has expired then a new one is created.
func (l *SQLInstance) Session(ctx context.Context) (sqlliveness.Session, error) {
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
	l.mu.s = &slsession.Session{Sid: id, Exp: exp}
	return l.mu.s, nil
}
