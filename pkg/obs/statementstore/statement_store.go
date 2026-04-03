// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statementstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var cacheSize = settings.RegisterIntSetting(settings.ApplicationLevel,
	"obs.statements.store.cache_size",
	"the maximum number of statement fingerprints to cache in memory",
	20_000,
	settings.PositiveInt)

// StatementStoreEnabled controls whether the statement store persists
// fingerprints. Exported for use in tests.
var StatementStoreEnabled = settings.RegisterBoolSetting(settings.ApplicationLevel,
	"obs.statements.store.enabled",
	"if true, the statement store is enabled and will persist statement fingerprints to system.statements",
	true,
)

// BatchSize controls the number of entries per INSERT batch.
// Exported for use in tests.
var BatchSize = settings.RegisterIntSetting(settings.ApplicationLevel,
	"obs.statements.store.batch_size",
	"the maximum number of statement fingerprints to include in a single batch INSERT into system.statements",
	50,
	settings.IntInRange(1, 1000))

var flushInterval = settings.RegisterDurationSetting(settings.ApplicationLevel,
	"obs.statements.store.flush_interval",
	"how often the pending buffer is flushed to system.statements",
	time.Second,
	settings.PositiveDuration)

const (
	metadataImplicitTrue  = `{"implicit_txn": true}`
	metadataImplicitFalse = `{"implicit_txn": false}`
)

// TestingKnobs provides hooks for tests to inject behavior into the
// StatementStore.
type TestingKnobs struct {
	// BatchInsertOverride, if set, is called instead of the real batch
	// INSERT into system.statements. This allows tests to simulate
	// flush failures without side effects like dropping system tables.
	BatchInsertOverride func(ctx context.Context, entries []StatementInfo) error
}

// StatementStore deduplicates statement fingerprints using an in-memory
// FIFO cache and asynchronously persists new entries into
// system.statements. Callers append fingerprints via PutStatement,
// which is non-blocking: it checks the cache and appends to a pending
// slice under a mutex. A background goroutine periodically drains the
// slice and batch-inserts the fingerprints.
type StatementStore struct {
	cacheMu struct {
		syncutil.Mutex
		cache *cache.UnorderedCache
	}

	settings     *cluster.Settings
	db           isql.DB
	mon          *mon.BytesMonitor
	testingKnobs *TestingKnobs

	flushMu struct {
		syncutil.Mutex
		pending []StatementInfo
		spare   []StatementInfo // recycled backing array from previous flush
	}
}

// NewStatementStore returns a new StatementStore. SetInternalExecutor
// must be called before Start.
func NewStatementStore(s *cluster.Settings, knob *TestingKnobs) *StatementStore {
	ss := &StatementStore{
		settings: s,
	}

	ss.testingKnobs = knob
	ss.cacheMu.cache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheFIFO,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return int64(size) > cacheSize.Get(&s.SV)
		},
	})
	return ss
}

// SetInternalExecutor sets the internal executor and its memory monitor.
// It must be called before Start. The executor is not passed through the
// constructor to break a dependency cycle during server initialization.
// The StatementStore takes ownership of the monitor and stops it on
// shutdown.
func (ss *StatementStore) SetInternalExecutor(db isql.DB, mon *mon.BytesMonitor) {
	ss.db = db
	ss.mon = mon
}

// maxPendingSize caps the number of entries in the pending buffer to
// prevent unbounded growth if the background writer is slow.
const maxPendingSize = 5000

// PutStatement buffers a statement fingerprint for asynchronous
// persistence. If the fingerprint ID is already cached, the call is a
// no-op. Otherwise the fingerprint is appended to a pending buffer
// that a background goroutine drains in batches. On persistence
// failure the cache entry is evicted so the next occurrence retries
// the insert.
func (ss *StatementStore) PutStatement(ctx context.Context, info StatementInfo) {
	if !StatementStoreEnabled.Get(&ss.settings.SV) {
		return
	}
	if !ss.addToCacheIfAbsent(info.FingerprintID) {
		return
	}
	ss.appendPending(info)
}

func (ss *StatementStore) addToCacheIfAbsent(id appstatspb.StmtFingerprintID) bool {
	ss.cacheMu.Lock()
	defer ss.cacheMu.Unlock()
	if _, ok := ss.cacheMu.cache.Get(id); ok {
		return false
	}
	ss.cacheMu.cache.Add(id, struct{}{})
	return true
}

func (ss *StatementStore) evictFromCacheByID(id appstatspb.StmtFingerprintID) {
	ss.cacheMu.Lock()
	defer ss.cacheMu.Unlock()
	ss.cacheMu.cache.Del(id)
}

func (ss *StatementStore) appendPending(info StatementInfo) {
	ss.flushMu.Lock()
	defer ss.flushMu.Unlock()
	if len(ss.flushMu.pending) >= maxPendingSize {
		// Buffer full — evict from cache so we retry on the next
		// occurrence rather than silently dropping the fingerprint.
		ss.evictFromCacheByID(info.FingerprintID)
		return
	}
	ss.flushMu.pending = append(ss.flushMu.pending, info)
}

var logEvery = log.Every(10 * time.Second)

// Start launches the background writer and cache prewarm goroutines.
// Any fingerprints that arrive before prewarm finishes will produce
// redundant inserts (which are harmless due to the idempotent ON
// CONFLICT DO NOTHING).
func (ss *StatementStore) Start(ctx context.Context, stopper *stop.Stopper) {
	if ss.db == nil || ss.mon == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf(
				"StatementStore.Start called before SetInternalExecutor"))
		}
		log.Dev.Error(ctx,
			"StatementStore.Start called before SetInternalExecutor")
		return
	}
	stopper.AddCloser(stop.CloserFn(func() {
		ss.mon.Stop(ctx)
	}))

	resetTickerCh := make(chan time.Duration, 1)
	flushInterval.SetOnChange(&ss.settings.SV, func(ctx context.Context) {
		select {
		case resetTickerCh <- flushInterval.Get(&ss.settings.SV):
		default:
		}
	})

	if err := stopper.RunAsyncTask(ctx,
		"statement-store-writer", func(ctx context.Context) {
			ticker := time.NewTicker(flushInterval.Get(&ss.settings.SV))
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					ss.drainAndFlush(ctx)
				case d := <-resetTickerCh:
					ticker.Reset(d)
				case <-stopper.ShouldQuiesce():
					return
				}
			}
		}); err != nil {
		log.Ops.Warningf(ctx,
			"failed to start statement store writer: %v", err)
		return
	}

	if err := stopper.RunAsyncTask(ctx,
		"statement-store-prewarm", func(ctx context.Context) {
			ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
			defer cancel()
			ss.prewarmCache(ctx)
		}); err != nil {
		log.Ops.Warningf(ctx,
			"failed to start statement store cache prewarm: %v", err)
	}
}

// drainAndFlush swaps the pending buffer with a spare, flushes the
// drained entries, then recycles the backing array. Two slice backing
// arrays rotate between three roles to avoid allocating a new slice
// each cycle:
//
//	pending (receiving appends) → toFlush (being written) → spare (idle)
//
// On each tick the current pending becomes toFlush, the spare becomes
// the new pending, and after the flush completes the old toFlush
// backing array is saved as the spare for the next cycle.
func (ss *StatementStore) drainAndFlush(ctx context.Context) {
	var toFlush []StatementInfo
	func() {
		ss.flushMu.Lock()
		defer ss.flushMu.Unlock()
		if len(ss.flushMu.pending) == 0 {
			return
		}
		toFlush = ss.flushMu.pending
		ss.flushMu.pending = ss.flushMu.spare[:0]
		ss.flushMu.spare = nil
	}()
	if len(toFlush) == 0 {
		return
	}
	ss.flushEntries(ctx, toFlush)
	func() {
		ss.flushMu.Lock()
		defer ss.flushMu.Unlock()
		ss.flushMu.spare = toFlush[:0]
	}()
}

// flushEntries persists entries to system.statements in chunks of
// batch_size per INSERT.
func (ss *StatementStore) flushEntries(ctx context.Context, entries []StatementInfo) {
	log.Ops.VInfof(ctx, 2,
		"statement store: flushing %d fingerprints", len(entries))

	chunk := int(BatchSize.Get(&ss.settings.SV))
	for start := 0; start < len(entries); start += chunk {
		end := start + chunk
		if end > len(entries) {
			end = len(entries)
		}
		if err := ss.batchInsertStatements(
			ctx, entries[start:end],
		); err != nil {
			if logEvery.ShouldLog() {
				log.Ops.Warningf(ctx,
					"batch inserting statement fingerprints: %v", err)
			}
			for i := start; i < end; i++ {
				ss.evictFromCacheByID(entries[i].FingerprintID)
			}
		}
	}
}

// prewarmCache loads existing fingerprint IDs from system.statements
// into the in-memory cache. This prevents redundant inserts on server
// restart. Errors are logged and the store continues with a cold
// cache.
func (ss *StatementStore) prewarmCache(ctx context.Context) {
	it, err := ss.db.Executor().QueryIteratorEx(ctx,
		"prewarm-statement-cache",
		nil, /* txn */
		sessiondata.NoSessionDataOverride,
		"SELECT fingerprint_id FROM system.statements "+
			"ORDER BY last_upserted DESC LIMIT 1000",
	)
	if err != nil {
		log.Ops.Warningf(ctx,
			"failed to prewarm statement cache: %v", err)
		return
	}
	defer func() { _ = it.Close() }()

	var count int
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		id, decErr := sqlstatsutil.DatumToUint64(row[0])
		if decErr != nil {
			log.Ops.Warningf(ctx,
				"failed to decode fingerprint ID during cache prewarm: %v",
				decErr)
			continue
		}
		ss.addToCacheIfAbsent(appstatspb.StmtFingerprintID(id))
		count++
	}
	if err != nil {
		log.Ops.Warningf(ctx,
			"failed to prewarm statement cache: %v", err)
	}
	log.Ops.Infof(ctx,
		"prewarmed statement cache with %d fingerprints", count)
}

// batchInsertStatements inserts multiple statement fingerprints into
// system.statements in a single INSERT ... ON CONFLICT DO NOTHING.
func (ss *StatementStore) batchInsertStatements(
	ctx context.Context, entries []StatementInfo,
) error {
	if ss.testingKnobs != nil && ss.testingKnobs.BatchInsertOverride != nil {
		return ss.testingKnobs.BatchInsertOverride(ctx, entries)
	}
	const colsPerRow = 5
	var sb strings.Builder
	// Pre-size: ~80 bytes for the INSERT prefix, ~40 bytes for the
	// ON CONFLICT suffix, and ~25 bytes per row for placeholders
	// like "($1, $2, $3, $4, $5),".
	sb.Grow(120 + len(entries)*25)
	sb.WriteString(
		"INSERT INTO system.statements " +
			"(fingerprint_id, fingerprint, summary, db, metadata) VALUES ")
	args := make([]interface{}, 0, len(entries)*colsPerRow)
	for i, info := range entries {
		if i > 0 {
			sb.WriteByte(',')
		}
		p := i*colsPerRow + 1
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d)",
			p, p+1, p+2, p+3, p+4)
		metadataStr := metadataImplicitFalse
		if info.ImplicitTxn {
			metadataStr = metadataImplicitTrue
		}
		args = append(args,
			sqlstatsutil.EncodeUint64ToBytes(uint64(info.FingerprintID)),
			info.Fingerprint,
			info.Summary,
			info.Database,
			metadataStr,
		)
	}
	sb.WriteString(" ON CONFLICT (fingerprint_id) DO NOTHING")

	return ss.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx,
			"batch-insert-statements", txn.KV(), sb.String(), args...)
		return err
	})
}

// TestingIsCached returns whether the given fingerprint ID is present
// in the in-memory cache. Intended for use in tests only.
func (ss *StatementStore) TestingIsCached(id appstatspb.StmtFingerprintID) bool {
	ss.cacheMu.Lock()
	defer ss.cacheMu.Unlock()
	_, ok := ss.cacheMu.cache.Get(id)
	return ok
}

// TestingFlushPending synchronously drains the pending buffer and
// persists entries. Intended for use in tests only.
func (ss *StatementStore) TestingFlushPending(ctx context.Context) {
	ss.drainAndFlush(ctx)
}

// TestingPendingCount returns the number of entries in the pending
// buffer. Intended for use in tests only.
func (ss *StatementStore) TestingPendingCount() int {
	ss.flushMu.Lock()
	defer ss.flushMu.Unlock()
	return len(ss.flushMu.pending)
}

// StatementInfo contains the metadata for a statement fingerprint to
// be persisted in the statement store.
type StatementInfo struct {
	FingerprintID appstatspb.StmtFingerprintID
	Fingerprint   string
	Database      string
	Summary       string
	ImplicitTxn   bool
}
