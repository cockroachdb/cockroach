// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DefaultGCInterval specifies duration between attempts to delete extant
// sessions that have expired.
var DefaultGCInterval = settings.RegisterNonNegativeDurationSetting(
	"server.sqlliveness.gc_interval",
	"duration between attempts to delete extant sessions that have expired",
	time.Second,
)

// Storage implements sqlliveness.Storage.
type Storage struct {
	stopper    *stop.Stopper
	clock      *hlc.Clock
	db         *kv.DB
	ex         tree.InternalExecutor
	gcInterval func() time.Duration
	settings   *cluster.Settings
}

// Options are used to configure a new Storage.
type Options struct {
	gcInterval time.Duration
}

// NewStorage creates a new storage struct.
func NewStorage(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	ie tree.InternalExecutor,
	settings *cluster.Settings,
) sqlliveness.Storage {
	s := &Storage{
		stopper: stopper, clock: clock, db: db, ex: ie,
		gcInterval: func() time.Duration {
			return DefaultGCInterval.Get(&settings.SV)
		},
	}
	return s
}

// Start runs the delete sessions loop.
func (s *Storage) Start(ctx context.Context) {
	log.Infof(ctx, "starting SQL liveness storage")
	s.stopper.RunWorker(ctx, s.deleteSessions)
}

// IsAlive returns whether the query session is currently alive. It may return
// true for a session which is no longer alive but will never return false for
// a session which is alive.
func (s *Storage) IsAlive(
	ctx context.Context, txn *kv.Txn, sid sqlliveness.SessionID,
) (alive bool, err error) {
	row, err := s.ex.QueryRow(
		ctx, "expire-single-session", txn, `
SELECT session_id FROM system.sqlliveness WHERE session_id = $1`, sid,
	)
	if err != nil {
		return true, errors.Wrapf(err, "Could not query session id: %s", sid)
	}
	return row != nil, nil
}

func (s *Storage) deleteSessions(ctx context.Context) {
	t := timeutil.NewTimer()
	t.Reset(0)
	for {
		if t.Read {
			t.Reset(s.gcInterval())
		}
		select {
		case <-s.stopper.ShouldStop():
			return
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
			now := s.clock.Now()
			var n int
			err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				rows, err := s.ex.QueryRow(
					ctx, "delete-sessions", txn,
					`DELETE FROM system.sqlliveness WHERE expiration < $1 RETURNING session_id`,
					tree.TimestampToDecimalDatum(now),
				)
				n = len(rows)
				return err
			})
			if err != nil {
				log.Errorf(ctx, "Could not delete expired sessions: %+v", err)
				continue
			}
			if log.V(2) {
				log.Infof(ctx, "Deleted %d expired SQL liveness sessions", n)
			}
		}
	}
}

// Insert inserts the input Session in table `system.sqlliveness`.
func (s *Storage) Insert(ctx context.Context, session sqlliveness.Session) error {
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := s.ex.QueryRow(
			ctx, "insert-session", txn,
			`INSERT INTO system.sqlliveness VALUES ($1, $2)`,
			session.ID(), tree.TimestampToDecimalDatum(session.Expiration()),
		)
		return err
	}); err != nil {
		return errors.Wrapf(err, "Could not insert session %s", session.ID())
	}
	return nil
}

// Update updates the row in table `system.sqlliveness` with the given input if
// if the row exists and in that case returns true. Otherwise it returns false.
func (s *Storage) Update(ctx context.Context, session sqlliveness.Session) (bool, error) {
	var sessionExists bool
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		data, err := s.ex.QueryRow(
			ctx, "update-session", txn, `
UPDATE system.sqlliveness SET expiration = $1 WHERE session_id = $2 RETURNING session_id`,
			tree.TimestampToDecimalDatum(session.Expiration()), session.ID(),
		)
		if err != nil {
			return err
		}
		sessionExists = data != nil
		return nil
	}); err != nil {
		return false, errors.Wrapf(err, "Could not update session %s", session.ID())
	}
	return sessionExists, nil
}
