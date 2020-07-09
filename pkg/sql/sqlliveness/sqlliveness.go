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
	"context"
	"encoding/hex"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// SessionID represents an opaque identifier for a session. This ID should be
// globally unique.
type SessionID []byte

// Session represents a session that was at some point believed to be alive.
// TODO(spaskob): Session is not the right name, session will abstract away
// a session id + management of the expriation. This should be higher order
// primitive.
type Session interface {
	// TODO(spaskob): Get should take a txn. We should probably return session
	// instead of id.
	Get(ctx context.Context) (SessionID, error)
	Equals(sid1, sid2 SessionID) bool
}

type SqlLiveness struct {
	db *kv.DB
	ex sqlutil.InternalExecutor
	d  time.Duration
	mu struct {
		syncutil.Mutex
		sid SessionID
	}
}

func NewSqlLiveness(db *kv.DB, ex sqlutil.InternalExecutor, d time.Duration) Session {
	return &SqlLiveness{db: db, ex: ex, d: d}
}

// Get returns a live session id. For each Sqlliveness instance the invariant is
// that there exists at most one live session at any point in time. If the
// current one has expired then a new one is created.
func (l *SqlLiveness) Get(ctx context.Context) (SessionID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.sid != nil {
		if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			now := l.db.Clock().Now()
			exp := now.Add(l.d.Nanoseconds(), 1)

			data, err := l.ex.QueryRowEx(
				ctx, "extend-session", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser}, `
UPDATE system.sqlliveness SET expiration = $1
WHERE expiration > $2 AND session_id = $3 RETURNING session_id`,
				tree.TimestampToDecimal(exp), tree.TimestampToDecimal(now), l.mu.sid,
			)
			if err != nil {
				return err
			}
			if data == nil {
				// TODO also delete the row, since the session has expired.
				log.Infof(ctx, "sid: %s has expired\n", hex.EncodeToString(l.mu.sid))
				l.mu.sid = nil
			}
			return nil
		}); err != nil {
			return nil, errors.Wrapf(err, "extend session")
		}
	}

	if l.mu.sid != nil {
		return l.mu.sid, nil
	}

	id := uuid.MakeV4().GetBytes()
	if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		exp := l.db.Clock().Now().Add(l.d.Nanoseconds(), 1)
		_, err := l.ex.QueryRowEx(
			ctx, "insert-session", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`INSERT INTO system.sqlliveness VALUES ($1, $2)`,
			id, tree.TimestampToDecimal(exp),
		)
		return err
	}); err != nil {
		return nil, errors.Wrapf(err, "create session id")
	}
	log.Infof(ctx, "Created new sid: %s).\n", hex.EncodeToString(id))
	l.mu.sid = id
	return l.mu.sid, nil
}

func (l *SqlLiveness) Equals(a, b SessionID) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
