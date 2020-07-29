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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DefaultGCInterval is the duration between attempts to delete extant sessions
// that have expired.
const DefaultGCInterval = 1 * time.Second

// Storage implements sqlliveness.Storage.
type Storage struct {
	stopper    *stop.Stopper
	clock      *hlc.Clock
	db         *kv.DB
	ex         tree.InternalExecutor
	gcInterval time.Duration
}

// Options are used to configure a new Storage.
type Options struct {
	gcInterval time.Duration
}

// NewStorage creates a new storage struct.
func NewStorage(
	ctx context.Context,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	ie tree.InternalExecutor,
	options Options,
) sqlliveness.Storage {
	if options.gcInterval <= 0 {
		options.gcInterval = DefaultGCInterval
	}
	s := &Storage{stopper: stopper, clock: clock, db: db, ex: ie, gcInterval: options.gcInterval}
	//s.stopper.RunWorker(ctx, s.deleteSessions)
	go s.deleteSessions(ctx)
	return s
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
	for {
		t := timeutil.NewTimer()
		t.Reset(0)
		select {
		//case <-s.stopper.ShouldStop():
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
			t.Reset(s.gcInterval)
			now := s.clock.Now()
			var n int
			if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				rows, err := s.ex.QueryRow(
					ctx, "delete-sessions", txn,
					`DELETE FROM system.sqlliveness WHERE exp < $1 RETURNING session_id`,
					tree.TimestampToDecimal(now),
				)
				n = len(rows)
				return err
			}); err != nil {
				log.Errorf(ctx, "Could not delete expired sessions: %+v", err)
				continue
			}
			if log.V(2) {
				log.Infof(ctx, "Deleted %d expired SQL liveness sessions", n)
			}
		}
	}
}
