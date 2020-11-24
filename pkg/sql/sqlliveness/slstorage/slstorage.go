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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// GCInterval specifies duration between attempts to delete extant
// sessions that have expired.
var GCInterval = settings.RegisterNonNegativeDurationSetting(
	"server.sqlliveness.gc_interval",
	"duration between attempts to delete extant sessions that have expired",
	20*time.Second,
)

// GCJitter specifies the jitter fraction on the interval between attempts to
// delete extant sessions that have expired.
//
// [(1-GCJitter) * GCInterval, (1+GCJitter) * GCInterval]
var GCJitter = settings.RegisterValidatedFloatSetting(
	"server.sqlliveness.gc_jitter",
	"jitter fraction on the duration between attempts to delete extant sessions that have expired",
	.15,
	func(f float64) error {
		if f < 0 || f > 1 {
			return errors.Errorf("%f is not in [0, 1]", f)
		}
		return nil
	},
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

// Storage implements sqlliveness.Storage.
type Storage struct {
	settings   *cluster.Settings
	stopper    *stop.Stopper
	clock      *hlc.Clock
	db         *kv.DB
	ex         sqlutil.InternalExecutor
	metrics    Metrics
	gcInterval func() time.Duration
	g          singleflight.Group
	sd         sessiondata.InternalExecutorOverride
	newTimer   func() timeutil.TimerI

	mu struct {
		syncutil.Mutex
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

// NewTestingStorage constructs a new storage with control for the database
// in which the `sqlliveness` table should exist.
func NewTestingStorage(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	settings *cluster.Settings,
	database string,
	newTimer func() timeutil.TimerI,
) *Storage {
	s := &Storage{
		settings: settings,
		stopper:  stopper,
		clock:    clock,
		db:       db,
		ex:       ie,
		sd: sessiondata.InternalExecutorOverride{
			User:     security.NodeUserName(),
			Database: database,
		},
		newTimer: newTimer,
		gcInterval: func() time.Duration {
			baseInterval := GCInterval.Get(&settings.SV)
			jitter := GCJitter.Get(&settings.SV)
			frac := 1 + (2*rand.Float64()-1)*jitter
			return time.Duration(frac * float64(baseInterval.Nanoseconds()))
		},
		metrics: makeMetrics(),
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

// NewStorage creates a new storage struct.
func NewStorage(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	settings *cluster.Settings,
) *Storage {
	return NewTestingStorage(stopper, clock, db, ie, settings, "system",
		timeutil.DefaultTimeSource{}.NewTimer)
}

// Metrics returns the associated metrics struct.
func (s *Storage) Metrics() *Metrics {
	return &s.metrics
}

// Start runs the delete sessions loop.
func (s *Storage) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.started {
		return
	}
	_ = s.stopper.RunAsyncTask(ctx, "slstorage", s.deleteSessionsLoop)
	s.mu.started = true
}

// IsAlive determines whether a given session is alive. If this method returns
// true, the session may no longer be alive, but if it returns false, the
// session definitely is not alive.
func (s *Storage) IsAlive(ctx context.Context, sid sqlliveness.SessionID) (alive bool, err error) {
	s.mu.Lock()
	if !s.mu.started {
		s.mu.Unlock()
		return false, sqlliveness.NotStartedError
	}
	if _, ok := s.mu.deadSessions.Get(sid); ok {
		s.mu.Unlock()
		s.metrics.IsAliveCacheHits.Inc(1)
		return false, nil
	}
	var prevExpiration hlc.Timestamp
	if expiration, ok := s.mu.liveSessions.Get(sid); ok {
		expiration := expiration.(hlc.Timestamp)
		// The record exists and is valid.
		if s.clock.Now().Less(expiration) {
			s.mu.Unlock()
			s.metrics.IsAliveCacheHits.Inc(1)
			return true, nil
		}
		// The record exists in the cache but seems expired according to our clock.
		// If we returned that the session was alive regardless of the expiration
		// then we'd never update the cache. Go fetch the session and pass in the
		// current view of the expiration. If the expiration has not changed, then
		// the session is expired and should be deleted. If it has, get the new
		// expiration for the cache.
		prevExpiration = expiration
	}

	// Launch singleflight to go read from the database and maybe delete the
	// entry. If it is found, we can add it and its expiration to the liveSessions
	// cache. If it isn't found, we know it's dead and we can add that to the
	// deadSessions cache.
	resChan, _ := s.g.DoChan(string(sid), func() (interface{}, error) {
		// store the result underneath the singleflight to avoid the need
		// for additional synchronization.
		live, expiration, err := s.deleteOrFetchSession(ctx, sid, prevExpiration)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if live {
			s.mu.liveSessions.Add(sid, expiration)
		} else {
			s.mu.deadSessions.Del(sid)
			s.mu.deadSessions.Add(sid, nil)
		}
		return live, nil
	})
	s.mu.Unlock()
	res := <-resChan
	if res.Err != nil {
		return false, err
	}
	s.metrics.IsAliveCacheMisses.Inc(1)
	return res.Val.(bool), nil
}

// deleteOrFetchSession returns whether the query session currently exists by
// reading from the database. If passed expiration is non-zero and the existing
// record has the same expiration, the record will be deleted and false will
// be returning, indicating that it no longer exists. If the record exists and
// has a differring expiration timestamp, true and the associated expiration
// will be returned.
func (s *Storage) deleteOrFetchSession(
	ctx context.Context, sid sqlliveness.SessionID, prevExpiration hlc.Timestamp,
) (alive bool, expiration hlc.Timestamp, err error) {
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {

		row, err := s.ex.QueryRowEx(ctx, "fetch-single-session", txn, s.sd, `
SELECT expiration FROM sqlliveness WHERE session_id = $1
`, sid.UnsafeBytes())
		if err != nil {
			return err
		}

		// The session is not alive.
		if row == nil {
			return nil
		}

		// The session is alive if the read expiration differs from prevExpiration.
		expiration, err = tree.DecimalToHLC(&row[0].(*tree.DDecimal).Decimal)
		if err != nil {
			return errors.Wrapf(err, "failed to parse expiration for session")
		}
		if !expiration.Equal(prevExpiration) {
			alive = true
			return nil
		}

		// The session is expired and needs to be deleted.
		expiration = hlc.Timestamp{}
		_, err = s.ex.ExecEx(ctx, "delete-expired-session", txn, s.sd, `
DELETE FROM sqlliveness WHERE session_id = $1
`, sid.UnsafeBytes())
		return err
	}); err != nil {
		return false, hlc.Timestamp{}, errors.Wrapf(err,
			"could not query session id: %s", sid)
	}
	return alive, expiration, nil
}

// deleteSessionsLoop is launched in start and periodically deletes sessions.
func (s *Storage) deleteSessionsLoop(ctx context.Context) {
	ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	t := s.newTimer()
	t.Reset(s.gcInterval())
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.Ch():
			t.MarkRead()
			s.deleteExpiredSessions(ctx)
			t.Reset(s.gcInterval())
		}
	}
}

// TODO(ajwerner): find a way to utilize this table scan to update the
// expirations stored in the in-memory cache or remove it altogether. As it
// stand, this scan will run more frequently than sessions expire but it won't
// propagate that fact to IsAlive. It seems like the lazy session deletion
// which has been added should be sufficient to delete expired sessions which
// matter. This would closer align with the behavior in node-liveness.
func (s *Storage) deleteExpiredSessions(ctx context.Context) {
	now := s.clock.Now()
	row, err := s.ex.QueryRowEx(ctx, "delete-sessions", nil /* txn */, s.sd,
		`
  WITH deleted_sessions AS (
                            DELETE FROM sqlliveness
                                  WHERE expiration < $1
                              RETURNING session_id
                        )
	SELECT count(*)
  FROM deleted_sessions;`,
		tree.TimestampToDecimalDatum(now),
	)
	if err != nil {
		if ctx.Err() == nil {
			log.Errorf(ctx, "could not delete expired sessions: %+v", err)
		}
		return
	}
	deleted := int64(*row[0].(*tree.DInt))

	s.metrics.SessionDeletionsRuns.Inc(1)
	s.metrics.SessionsDeleted.Inc(deleted)
	if log.V(2) || deleted > 0 {
		log.Infof(ctx, "deleted %d expired SQL liveness sessions", deleted)
	}
}

// Insert inserts the input Session in table `system.sqlliveness`.
// A client must never call this method with a session which was previously
// used! The contract of IsAlive is that once a session becomes not alive, it
// must never become alive again.
func (s *Storage) Insert(
	ctx context.Context, sid sqlliveness.SessionID, expiration hlc.Timestamp,
) (err error) {
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := s.ex.QueryRowEx(
			ctx, "insert-session", txn, s.sd,
			`INSERT INTO sqlliveness VALUES ($1, $2)`,
			sid.UnsafeBytes(), tree.TimestampToDecimalDatum(expiration),
		)
		return err
	}); err != nil {
		s.metrics.WriteFailures.Inc(1)
		return errors.Wrapf(err, "could not insert session %s", sid)
	}
	log.Infof(ctx, "inserted sqlliveness session %s", sid)
	s.metrics.WriteSuccesses.Inc(1)
	return nil
}

// Update updates the row in table `system.sqlliveness` with the given input if
// if the row exists and in that case returns true. Otherwise it returns false.
func (s *Storage) Update(
	ctx context.Context, sid sqlliveness.SessionID, expiration hlc.Timestamp,
) (sessionExists bool, err error) {
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		data, err := s.ex.QueryRowEx(
			ctx, "update-session", txn, s.sd, `
UPDATE sqlliveness SET expiration = $1 WHERE session_id = $2 RETURNING session_id`,
			tree.TimestampToDecimalDatum(expiration), sid.UnsafeBytes(),
		)
		if err != nil {
			return err
		}
		sessionExists = data != nil
		return nil
	})
	if err != nil || !sessionExists {
		s.metrics.WriteFailures.Inc(1)
	}
	if err != nil {
		return false, errors.Wrapf(err, "could not update session %s", sid)
	}
	s.metrics.WriteSuccesses.Inc(1)
	return sessionExists, nil
}
