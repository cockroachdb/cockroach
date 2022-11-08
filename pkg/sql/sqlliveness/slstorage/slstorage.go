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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// GCInterval specifies duration between attempts to delete extant
// sessions that have expired.
var GCInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.sqlliveness.gc_interval",
	"duration between attempts to delete extant sessions that have expired",
	time.Hour,
	settings.NonNegativeDuration,
)

// GCJitter specifies the jitter fraction on the interval between attempts to
// delete extant sessions that have expired.
//
// [(1-GCJitter) * GCInterval, (1+GCJitter) * GCInterval]
var GCJitter = settings.RegisterFloatSetting(
	settings.TenantWritable,
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
	settings.TenantWritable,
	"server.sqlliveness.storage_session_cache_size",
	"number of session entries to store in the LRU",
	1024)

// Storage deals with reading and writing session records. It implements the
// sqlliveness.Reader interface, and the slinstace.Writer interface.
type Storage struct {
	log.AmbientContext

	settings   *cluster.Settings
	stopper    *stop.Stopper
	clock      *hlc.Clock
	db         *kv.DB
	codec      keys.SQLCodec
	metrics    Metrics
	gcInterval func() time.Duration
	newTimer   func() timeutil.TimerI
	keyCodec   keyCodec

	mu struct {
		syncutil.Mutex

		g singleflight.Group

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

var _ sqlliveness.Reader = &Storage{}

// NewTestingStorage constructs a new storage with control for the database
// in which the `sqlliveness` table should exist.
func NewTestingStorage(
	ambientCtx log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	sqllivenessTableID catid.DescID,
	rbrIndexID catid.IndexID,
	newTimer func() timeutil.TimerI,
) *Storage {
	s := &Storage{
		AmbientContext: ambientCtx,

		settings: settings,
		stopper:  stopper,
		clock:    clock,
		db:       db,
		codec:    codec,
		keyCodec: makeKeyCodec(codec, sqllivenessTableID, rbrIndexID),
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
	ambientCtx log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
) *Storage {
	const rbrIndexID = 2
	return NewTestingStorage(ambientCtx, stopper, clock, db, codec, settings, keys.SqllivenessID, rbrIndexID,
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
	return s.isAlive(ctx, sid, sync)
}

type readType byte

const (
	_ readType = iota
	sync
	async
)

func (s *Storage) isAlive(
	ctx context.Context, sid sqlliveness.SessionID, syncOrAsync readType,
) (alive bool, _ error) {
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
	if expiration, ok := s.mu.liveSessions.Get(sid); ok {
		expiration := expiration.(hlc.Timestamp)
		// The record exists and is valid.
		if s.clock.Now().Less(expiration) {
			s.mu.Unlock()
			s.metrics.IsAliveCacheHits.Inc(1)
			return true, nil
		}
	}

	// We think that the session is expired; check, and maybe delete it.
	resChan := s.deleteOrFetchSessionSingleFlightLocked(ctx, sid)

	// At this point, we know that the singleflight goroutine has been launched.
	// Releasing the lock here ensures that callers will either join the single-
	// flight or see the result.
	s.mu.Unlock()
	s.metrics.IsAliveCacheMisses.Inc(1)

	// If we do not want to wait for the result, assume that the session is
	// indeed alive.
	if syncOrAsync == async {
		return true, nil
	}
	select {
	case res := <-resChan:
		if res.Err != nil {
			return false, res.Err
		}
		return res.Val.(bool), nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// This function will launch a singleflight goroutine for the session which
// will populate its result into the caches underneath the mutex. The result
// value will be a bool. The singleflight goroutine does not cancel its work
// in the face of cancellation of ctx.
//
// This method assumes that s.mu is held.
func (s *Storage) deleteOrFetchSessionSingleFlightLocked(
	ctx context.Context, sid sqlliveness.SessionID,
) <-chan singleflight.Result {
	s.mu.AssertHeld()

	// If it is found, we can add it and its expiration to the liveSessions
	// cache. If it isn't found, we know it's dead, and we can add that to the
	// deadSessions cache.
	resChan, _ := s.mu.g.DoChan(string(sid), func() (interface{}, error) {

		// Note that we use a new `context` here to avoid a situation where a cancellation
		// of the first context cancels other callers to the `acquireNodeLease()` method,
		// because of its use of `singleflight.Group`. See issue #41780 for how this has
		// happened.
		bgCtx := s.AnnotateCtx(context.Background())
		bgCtx = logtags.AddTags(bgCtx, logtags.FromContext(ctx))
		newCtx, cancel := s.stopper.WithCancelOnQuiesce(bgCtx)
		defer cancel()

		// store the result underneath the singleflight to avoid the need
		// for additional synchronization. Also, use a stopper task to ensure
		// the goroutine is tracked during shutdown.
		var live bool
		const taskName = "sqlliveness-fetch-or-delete-session"
		if err := s.stopper.RunTaskWithErr(newCtx, taskName, func(
			ctx context.Context,
		) (err error) {
			var expiration hlc.Timestamp
			live, expiration, err = s.deleteOrFetchSession(newCtx, sid)
			if err != nil {
				return err
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if live {
				s.mu.liveSessions.Add(sid, expiration)
			} else {
				s.mu.deadSessions.Add(sid, nil)
			}
			return nil
		}); err != nil {
			return false, err
		}
		return live, nil
	})
	return resChan
}

// deleteOrFetchSession returns whether the query session currently exists by
// reading from the database. If the record exists but is expired, this method
// will delete the record transactionally, moving it to from alive to dead. The
// returned expiration will be non-zero only if the session is alive.
func (s *Storage) deleteOrFetchSession(
	ctx context.Context, sid sqlliveness.SessionID,
) (alive bool, expiration hlc.Timestamp, err error) {
	var deleted bool
	var prevExpiration hlc.Timestamp
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Reset captured variable in case of retry.
		deleted, expiration, prevExpiration = false, hlc.Timestamp{}, hlc.Timestamp{}

		k, err := s.keyCodec.encode(sid)
		if err != nil {
			return err
		}
		kv, err := txn.Get(ctx, k)
		if err != nil {
			return err
		}
		// The session is not alive.
		if kv.Value == nil {
			return nil
		}
		expiration, err = decodeValue(kv)
		if err != nil {
			return errors.Wrapf(err, "failed to decode expiration for %s",
				redact.SafeString(sid.String()))
		}
		prevExpiration = expiration
		if !expiration.Less(s.clock.Now()) {
			alive = true
			return nil
		}

		// The session is expired and needs to be deleted.
		deleted, expiration = true, hlc.Timestamp{}
		ba := txn.NewBatch()
		ba.Del(k)
		return txn.CommitInBatch(ctx, ba)
	}); err != nil {
		return false, hlc.Timestamp{}, errors.Wrapf(err,
			"could not query session id: %s", sid)
	}
	if deleted {
		s.metrics.SessionsDeleted.Inc(1)
		log.Infof(ctx, "deleted session %s which expired at %s", sid, prevExpiration)
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
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	toCheck, err := s.fetchExpiredSessionIDs(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Errorf(ctx, "could not delete expired sessions: %v", err)
		}
		return
	}
	launchSessionCheck := func(id sqlliveness.SessionID) <-chan singleflight.Result {
		s.mu.Lock()
		defer s.mu.Unlock()
		// We have evidence that the session is expired, so remove any cached
		// fact that it might be alive and launch the goroutine to determine its
		// true state.
		s.mu.liveSessions.Del(id)
		return s.deleteOrFetchSessionSingleFlightLocked(ctx, id)
	}
	checkSession := func(id sqlliveness.SessionID) error {
		select {
		case r := <-launchSessionCheck(id):
			return r.Err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, id := range toCheck {
		if err := checkSession(id); err != nil {
			log.Warningf(ctx, "failed to check on expired session %v: %v", id, err)
		}
	}
	s.metrics.SessionDeletionsRuns.Inc(1)
}

func (s *Storage) fetchExpiredSessionIDs(ctx context.Context) ([]sqlliveness.SessionID, error) {
	var toCheck []sqlliveness.SessionID
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		toCheck = nil // reset for restarts
		start := s.keyCodec.indexPrefix()
		end := start.PrefixEnd()
		now := s.clock.Now()
		const maxRows = 1024 // arbitrary but plenty
		for {
			rows, err := txn.Scan(ctx, start, end, maxRows)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				return nil
			}
			for i := range rows {
				exp, err := decodeValue(rows[i])
				if err != nil {
					log.Warningf(ctx, "failed to decode row %s expiration: %v", rows[i].Key.String(), err)
					continue
				}
				if exp.Less(now) {
					id, err := s.keyCodec.decode(rows[i].Key)
					if err != nil {
						log.Warningf(ctx, "failed to decode row %s session: %v", rows[i].Key.String(), err)
					}
					toCheck = append(toCheck, id)
				}
			}
			if len(rows) < maxRows {
				return nil
			}
			start = rows[len(rows)-1].Key.Next()
		}
	}); err != nil {
		return nil, err
	}
	return toCheck, nil
}

// Insert inserts the input Session in table `system.sqlliveness`.
// A client must never call this method with a session which was previously
// used! The contract of IsAlive is that once a session becomes not alive, it
// must never become alive again.
func (s *Storage) Insert(
	ctx context.Context, sid sqlliveness.SessionID, expiration hlc.Timestamp,
) (err error) {
	k, err := s.keyCodec.encode(sid)
	if err != nil {
		return err
	}
	v := encodeValue(expiration)
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	if err := s.db.InitPut(ctx, k, &v, true); err != nil {
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
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		k, err := s.keyCodec.encode(sid)
		if err != nil {
			return err
		}
		kv, err := txn.Get(ctx, k)
		if err != nil {
			return err
		}
		if sessionExists = kv.Value != nil; !sessionExists {
			return nil
		}
		v := encodeValue(expiration)
		ba := txn.NewBatch()
		ba.Put(k, &v)
		return txn.CommitInBatch(ctx, ba)
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

// CachedReader returns an implementation of sqlliveness.Reader which does
// not synchronously read from the store. Calls to IsAlive will return the
// currently known state of the session, but will trigger an asynchronous
// refresh of the state of the session if it is not known.
func (s *Storage) CachedReader() sqlliveness.Reader {
	return (*cachedStorage)(s)
}

// cachedStorage implements the sqlliveness.Reader interface, and the
// slinstace.Writer interface, but does not read from the underlying store
// synchronously during IsAlive.
type cachedStorage Storage

func (s *cachedStorage) IsAlive(
	ctx context.Context, sid sqlliveness.SessionID,
) (alive bool, err error) {
	return (*Storage)(s).isAlive(ctx, sid, async)
}

func decodeValue(kv kv.KeyValue) (hlc.Timestamp, error) {
	tup, err := kv.Value.GetTuple()
	if err != nil {
		return hlc.Timestamp{},
			errors.Wrapf(err, "failed to decode tuple from key %v", kv.Key)
	}
	_, dec, err := encoding.DecodeDecimalValue(tup)
	if err != nil {
		return hlc.Timestamp{},
			errors.Wrapf(err, "failed to decode decimal from key %v", kv.Key)
	}
	return hlc.DecimalToHLC(&dec)
}

func encodeValue(expiration hlc.Timestamp) roachpb.Value {
	var v roachpb.Value
	dec := eval.TimestampToDecimal(expiration)
	v.SetTuple(encoding.EncodeDecimalValue(nil, 2, &dec))
	return v
}
