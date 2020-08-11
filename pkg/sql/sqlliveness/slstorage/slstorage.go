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
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
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

// CacheSize is the size of the entries to store in the cache.
// In general this should be larger than the number of nodes in the cluster.
//
// TODO(ajwerner): thread memory monitoring to this level and consider
// increasing the cache size dynamically. The entries are just bytes each so
// this should not be a big deal.
var CacheSize = settings.RegisterIntSetting(
	"server.sqlliveness.storage_session_cache_size",
	"number of session entries to store in the LRU",
	1024)

// TODO(ajwerner): Add metrics.

// Storage implements sqlliveness.Storage.
type Storage struct {
	stopper    *stop.Stopper
	clock      *hlc.Clock
	db         *kv.DB
	ex         tree.InternalExecutor
	gcInterval func() time.Duration
	g          singleflight.Group

	mu struct {
		syncutil.RWMutex
		started bool
		// liveSessions caches the current view of expirations of live sessions.
		liveSessions *cache.UnorderedCache
		// deadSessions caches the IDs of sessions which have not been found. This
		// package makes an assumption that a session which is queried at some
		// point was alive (otherwise, how would one know the ID to query?).
		// Furthermore, this package assumes that once a sessions no longer exists,
		// it will never exist again in the future.
		deadSessions *cache.UnorderedCache
	}
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
	cacheConfig := cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > int(CacheSize.Get(&settings.SV))
		},
	}
	s.mu.liveSessions = cache.NewUnorderedCache(cacheConfig)
	s.mu.deadSessions = cache.NewUnorderedCache(cacheConfig)
	return s
}

// Start runs the delete sessions loop.
func (s *Storage) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.started {
		return
	}
	log.Infof(ctx, "starting SQL liveness storage")
	s.stopper.RunWorker(ctx, s.deleteSessions)
	s.mu.started = true
}

func (s *Storage) IsAlive(
	ctx context.Context, _ *kv.Txn, sid sqlliveness.SessionID,
) (alive bool, err error) {
	// TODO(ajwerner): consider creating a zero-allocation cache key by converting
	// the bytes to a string using unsafe.
	sidKey := string(sid)
	s.mu.RLock()
	if _, ok := s.mu.deadSessions.Get(sidKey); ok {
		s.mu.RUnlock()
		return false, nil
	}
	if expiration, ok := s.mu.liveSessions.Get(sidKey); ok {
		expiration := expiration.(hlc.Timestamp)
		// The record exists but is expired. If we returned that the session was
		// alive regardless of the expiration then we'd never update the cache.
		//
		// TODO(ajwerner): Utilize a rangefeed for the session state to update
		// cache entries and always rely on the currently cached value. This
		// approach may lead to lots of request in the period of time when a
		// session is expired but has not yet been removed. Alternatively, this
		// code could trigger deleteSessions or could use some other mechanism
		// to wait for deleteSessions.
		if s.clock.Now().Less(expiration) {
			s.mu.RUnlock()
			return true, nil
		}
	}

	// Launch singleflight to go read from the database. If it is found, we
	// can add it and its expiration to the liveSessions cache. If it isn't
	// found, we know it's dead and we can add that to the deadSessions cache.
	resChan, _ := s.g.DoChan(sidKey, func() (interface{}, error) {
		// store the result underneath the singleflight to avoid the need
		// for additional synchronization.
		live, expiration, err := s.fetchSession(ctx, sid)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if live {
			s.mu.liveSessions.Add(sidKey, expiration)
		} else {
			s.mu.deadSessions.Add(sidKey, nil)
		}
		return live, nil
	})
	s.mu.RUnlock()
	res := <-resChan
	if res.Err != nil {
		return false, err
	}
	return res.Val.(bool), nil
}

// fetchSessions returns whether the query session currently exists by reading
// from the database. If the record exists, the associated expiration will be
// returned.
func (s *Storage) fetchSession(
	ctx context.Context, sid sqlliveness.SessionID,
) (alive bool, expiration hlc.Timestamp, err error) {
	var row tree.Datums
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		row, err = s.ex.QueryRow(
			ctx, "expire-single-session", txn, `
SELECT expiration FROM system.sqlliveness WHERE session_id = $1`, sid,
		)
		return errors.Wrapf(err, "Could not query session id: %s", sid)
	}); err != nil {
		return false, hlc.Timestamp{}, err
	}
	if row == nil {
		return false, hlc.Timestamp{}, nil
	}
	ts := row[0].(*tree.DDecimal)
	exp, err := tree.DecimalToHLC(&ts.Decimal)
	if err != nil {
		return false, hlc.Timestamp{}, errors.Wrapf(err, "failed to parse expiration for session")
	}
	return true, exp, nil
}

func (s *Storage) deleteSessions(ctx context.Context) {
	defer func() {
		log.Warning(ctx, "exiting delete sessions loop")
	}()
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
// A client must never call this method with a session which was previously
// used! The contract of IsAlive is that once a session becomes not alive, it
// must never become alive again.
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
