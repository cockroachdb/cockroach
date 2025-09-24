// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import (
	"context"
	"hash/fnv"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// StatementHintsCache allows statements to access their external hints stored
// in the system.statement_hints table (and determine whether they have hints)
// without having to perform DB reads. Because it is kept up to date via a
// rangefeed, there can be a delay between when hints are added/removed and when
// they become visible to statements.
type StatementHintsCache struct {
	mu struct {
		// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
		// manipulates an internal LRU list.
		syncutil.Mutex

		// hintedHashes contains all fingerprint hashes with associated hints. If a
		// fingerprint hash is not stored in this map, there are no hints associated
		// with any fingerprint with that hash. This map allows StatementHintsCache
		// to quickly determine whether there are any hints for a given statement,
		// ensuring that overhead is minimized for non-hinted statements.
		hintedHashes map[int64]int

		// hintCache is an LRU cache that stores statements hints from the
		// system.statement_hints table. Cached hints are never modified after being
		// added to the cache, and so can be returned directly without copying.
		hintCache *cache.UnorderedCache
	}

	// Used to start the rangefeed.
	clock   *hlc.Clock
	f       *rangefeed.Factory
	stopper *stop.Stopper

	// Used when decoding KVs from the range feed.
	datumAlloc tree.DatumAlloc

	// Used to read hints from the database when they are not in the cache.
	db descs.DB
}

// cacheSize is the size of the entries to store in the cache.
// In general this should be larger than the number of active statement
// fingerprints with hints.
var cacheSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.hints.statement_hints_cache_size",
	"number of hint entries to store in the LRU",
	1024,
)

// NewStatementHintsCache creates a new StatementHintsCache that can hold
// external hints for cacheSize statements (and records the existence of hints
// for all statements).
func NewStatementHintsCache(
	clock *hlc.Clock, f *rangefeed.Factory, stopper *stop.Stopper, db descs.DB, st *cluster.Settings,
) *StatementHintsCache {
	hintsCache := &StatementHintsCache{clock: clock, f: f, stopper: stopper, db: db}
	hintsCache.mu.hintCache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool {
			return s > int(cacheSize.Get(&st.SV))
		},
	})
	return hintsCache
}

// ============================================================================
// Populating the cache.
// ============================================================================

// Start begins watching for updates in the statement_hints table.
func (c *StatementHintsCache) Start(
	ctx context.Context, sysTableResolver catalog.SystemTableIDResolver,
) error {
	// TODO(drewk): in a follow-up commit, block until the initial scan is done
	// so that statements without hints don't have to check the LRU cache.
	return c.startRangefeedInternal(ctx, sysTableResolver)
}

// startRangefeedInternal starts a rangefeed on the statement_hints table to
// keep the hintedHashes map up to date and invalidate entries in the hintCache.
// The initial scan of the table is asynchronous.
func (c *StatementHintsCache) startRangefeedInternal(
	ctx context.Context, sysTableResolver catalog.SystemTableIDResolver,
) error {
	// We need to retry unavailable replicas here. This is only meant to be called
	// at server startup.
	tableID, err := startup.RunIdempotentWithRetryEx(ctx,
		c.stopper.ShouldQuiesce(),
		"get-statement-hints-table-ID",
		func(ctx context.Context) (descpb.ID, error) {
			return sysTableResolver.LookupSystemTableID(ctx, systemschema.StatementHintsTable.GetName())
		})
	if err != nil {
		return err
	}
	statementHintsHashIndexPrefix := keys.SystemSQLCodec.IndexPrefix(
		uint32(tableID), uint32(systemschema.StatementHintsHashIndexID),
	)
	statementHintsHashIndexSpan := roachpb.Span{
		Key:    statementHintsHashIndexPrefix,
		EndKey: statementHintsHashIndexPrefix.PrefixEnd(),
	}

	watcher := rangefeedcache.NewWatcher(
		"statement-hints-watcher",
		c.clock, c.f,
		0, /* bufferSize */
		[]roachpb.Span{statementHintsHashIndexSpan},
		false, /* withPrevValue */
		true,  /* withRowTSInInitialScan */
		c.translateEvent,
		c.onUpdate,
		nil, /* knobs */
	)

	// Kick off the rangefeedcache which will retry until the stopper stops. This
	// function only returns an error if we're shutting down.
	return rangefeedcache.Start(ctx, c.stopper, watcher, nil /* onError */)
}

func (c *StatementHintsCache) translateEvent(
	ctx context.Context, kv *kvpb.RangeFeedValue,
) (*bufferEvent, bool) {
	hash, err := decodeHashFromStatementHintsKey(keys.SystemSQLCodec, kv.Key, &c.datumAlloc)
	if err != nil {
		log.Dev.Warningf(ctx, "failed to decode statement_hints row %v: %v", kv.Key, err)
		return nil, false
	}
	return &bufferEvent{ts: kv.Timestamp(), hash: hash, del: !kv.Value.IsPresent()}, true
}

func (c *StatementHintsCache) onUpdate(
	ctx context.Context, update rangefeedcache.Update[*bufferEvent],
) {
	if update.Type == rangefeedcache.CompleteUpdate {
		// The rangefeed has completed the initial scan of the table. Build the
		// hintedHashes map, and then add it to StatementHintsCache.
		log.Dev.Info(ctx, "statement_hints rangefeed completed initial scan")
		hintedHashes := make(map[int64]int)
		applyUpdate(hintedHashes, update.Events)
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.hintedHashes = hintedHashes

		// Invalidate all cache entries.
		c.mu.hintCache.Clear()
	} else {
		log.Dev.Info(ctx, "statement_hints rangefeed applying incremental update")
		c.mu.Lock()
		defer c.mu.Unlock()
		applyUpdate(c.mu.hintedHashes, update.Events)

		// Invalidate any cache entries for the affected hashes.
		for _, ev := range update.Events {
			c.mu.hintCache.Del(ev.hash)
		}
	}
}

// decodeHashFromStatementHintsKey decodes the query hash from a range feed
// event on system.statement_hints.
func decodeHashFromStatementHintsKey(
	codec keys.SQLCodec, key roachpb.Key, da *tree.DatumAlloc,
) (statementHash int64, err error) {
	// The rangefeed is set up on the secondary index on the hash column.
	dirs := []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}
	keyVals := make([]rowenc.EncDatum, 1)
	if _, err = rowenc.DecodeIndexKey(codec, keyVals, dirs, key); err != nil {
		return 0, err
	}
	if err = keyVals[0].EnsureDecoded(types.Int, da); err != nil {
		return 0, err
	}
	statementHash = int64(tree.MustBeDInt(keyVals[0].Datum))
	return statementHash, nil
}

type bufferEvent struct {
	ts   hlc.Timestamp
	hash int64
	del  bool
}

// Timestamp implements the rangefeedbuffer.Event interface.
func (e *bufferEvent) Timestamp() hlc.Timestamp {
	return e.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}

func applyUpdate(hintedHashes map[int64]int, events []*bufferEvent) {
	for _, ev := range events {
		if ev.del {
			// NOTE: depending on the ordering of events, the count could become
			// negative temporarily, but will be corrected once all events are
			// processed.
			hintedHashes[ev.hash]--
		} else {
			hintedHashes[ev.hash]++
		}
		if hintedHashes[ev.hash] == 0 {
			delete(hintedHashes, ev.hash)
		}
	}
}

// ============================================================================
// Reading from the cache.
// ============================================================================

// MaybeGetStatementHints attempts to retrieve the hints for the given statement
// fingerprint. It returns nil if the statement has no hints, or there was an
// error retrieving them.
func (c *StatementHintsCache) MaybeGetStatementHints(
	ctx context.Context, statementFingerprint string,
) (hints []StatementHint) {
	hash := fnv.New64()
	_, err := hash.Write([]byte(statementFingerprint))
	if err != nil {
		// This should never happen for 64-bit FNV-1.
		log.Dev.Errorf(ctx, "failed to compute hash for statement fingerprint: %v", err)
		return nil
	}
	statementHash := int64(hash.Sum64())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.hintedHashes != nil {
		// If the hintedHashes map has not yet been initialized, statements must
		// unconditionally check hintCache.
		if _, ok := c.mu.hintedHashes[statementHash]; !ok {
			// There are no plan hints for this query.
			return nil
		}
	}
	var e interface{}
	e, ok := c.mu.hintCache.Get(statementHash)
	if !ok {
		// The plan hints were evicted from the cache. Retrieve them from the
		// database and add them to the cache.
		return c.addCacheEntryLocked(ctx, statementHash, statementFingerprint)
	}
	entry := e.(*cacheEntry)
	c.maybeWaitForRefreshLocked(ctx, entry, statementHash)
	return entry.getMatchingHints(statementFingerprint)
}

// maybeWaitForRefreshLocked checks if the given cache entry is being refreshed
// by a concurrent query, and waits for the refresh to finish if necessary.
// While maybeWaitForRefreshLocked is waiting, the lock on the cache is
// released.
func (c *StatementHintsCache) maybeWaitForRefreshLocked(
	ctx context.Context, entry *cacheEntry, statementHash int64,
) {
	if entry.mustWait {
		// We are in the process of grabbing plan hints for this query. Wait until
		// that is complete, at which point e.hints will be populated.
		log.VEventf(ctx, 1, "waiting for hints for query with hash %v", statementHash)
		entry.waitCond.Wait()
		log.VEventf(ctx, 1, "finished waiting for hints for query with hash %v", statementHash)
	} else {
		// This is the expected "fast" path; don't emit an event.
		if log.V(2) {
			log.Dev.Infof(ctx, "hints for query with hash %v found in cache", statementHash)
		}
	}
}

// addCacheEntryLocked creates a new cache entry and retrieves the plan hints
// for the given query fingerprint from the database. addCacheEntryLocked allows
// other queries to wait for the result via sync.Cond. Note that the lock is
// released while reading from the db, and then reacquired.
func (c *StatementHintsCache) addCacheEntryLocked(
	ctx context.Context, statementHash int64, statementFingerprint string,
) []StatementHint {
	// Add a cache entry that other queries can find and wait on until we have the
	// plan hints.
	entry := &cacheEntry{
		mustWait: true,
		waitCond: sync.Cond{L: &c.mu},
	}
	c.mu.hintCache.Add(statementHash, entry)

	var err error
	func() {
		c.mu.Unlock()
		defer c.mu.Lock()
		log.VEventf(ctx, 1, "reading hints for query %s", statementFingerprint)
		err = c.getStatementHintsFromDB(ctx, statementHash, entry)
		log.VEventf(ctx, 1, "finished reading hints for query %s", statementFingerprint)
	}()

	// Allow any concurrent queries to proceed.
	entry.mustWait = false
	entry.waitCond.Broadcast()

	if err != nil {
		// Remove the cache entry so that the next caller retries the query.
		c.mu.hintCache.Del(statementHash)
		log.VEventf(ctx, 1, "encountered error while reading hints for query %s", statementFingerprint)
		return nil
	}
	return entry.getMatchingHints(statementFingerprint)
}

// getStatementHintsFromDB queries the system.statement_hints table for hints matching the
// given fingerprint hash. It is able to handle the case when multiple
// fingerprints match the hash, as well as the case when there are no hints for
// the fingerprint.
func (c *StatementHintsCache) getStatementHintsFromDB(
	ctx context.Context, statementHash int64, entry *cacheEntry,
) (retErr error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `SELECT "fingerprint", "hint" FROM system.statement_hints WHERE "hash" = $1`
	it, err := c.db.Executor().QueryIteratorEx(
		ctx, opName, nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		getHintsStmt, statementHash,
	)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()
	for {
		ok, err := it.Next(ctx)
		if !ok || err != nil {
			return err
		}
		datums := it.Cur()
		fingerprint := string(tree.MustBeDString(datums[0]))
		hint, err := NewStatementHint([]byte(tree.MustBeDBytes(datums[1])))
		if err != nil {
			return err
		}
		entry.fingerprints = append(entry.fingerprints, fingerprint)
		entry.hints = append(entry.hints, hint)
	}
}

type cacheEntry struct {
	// If mustWait is true, we do not have any hints for this hash, and we are in
	// the process of fetching them from the database. Other callers can wait on
	// the waitCond until this is false.
	mustWait bool
	waitCond sync.Cond

	// fingerprints and hints have the same length. fingerprints[i] is the
	// statement fingerprint to which hints[i] applies.
	fingerprints []string
	hints        []StatementHint
}

// getMatchingHints returns the plan hints for the given fingerprint, or nil if
// they don't exist.
func (entry *cacheEntry) getMatchingHints(statementFingerprint string) []StatementHint {
	var res []StatementHint
	for i := range entry.hints {
		if entry.fingerprints[i] == statementFingerprint {
			// TODO(drewk): we could do something smarter here to avoid allocations.
			res = append(res, entry.hints[i])
		}
	}
	return res
}
