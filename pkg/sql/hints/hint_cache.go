// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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
// TODO(drewk): add memory monitoring for the hintedHashes and hintCache maps.
type StatementHintsCache struct {
	mu struct {
		// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
		// manipulates an internal LRU list.
		syncutil.Mutex

		// hintedHashes contains every 64-bit fingerprint hash in the system table.
		// This map allows StatementHintsCache to quickly determine whether there
		// are any hints for a given statement, minimizing the overhead for
		// non-hinted statements.
		//
		// Most of the time, each hash maps to the empty timestamp. After a
		// rangefeed event kicks off a refresh goroutine, the timestamp is set to
		// the refresh timestamp. Further rangefeed events during the refresh simply
		// forward the refresh timestamp. See handleIncrementalUpdate for more
		// details.
		hintedHashes map[int64]hlc.Timestamp

		// hintCache is an LRU cache that stores statements hints from the
		// system.statement_hints table. Cached hints are never modified after being
		// added to the cache, and so can be returned directly without copying.
		hintCache *cache.UnorderedCache

		// Used for testing; keeps track of how many times we actually read hints
		// from the system table. Note that this count only includes reads used for
		// hintCache, not for hintedHashes.
		// TODO(drewk): consider making this a metric.
		numInternalQueries int
	}

	// Used to start/coordinate the rangefeed.
	clock   *hlc.Clock
	f       *rangefeed.Factory
	stopper *stop.Stopper
	codec   keys.SQLCodec

	// Used when decoding KVs from the range feed.
	datumAlloc tree.DatumAlloc

	// Used to read hints from the database when they are not in the cache.
	db descs.DB

	// generation is incremented any time the hint cache is updated by the
	// rangefeed.
	generation atomic.Int64
}

// cacheSize is the size of the entries to store in the cache.
// In general this should be larger than the number of active statement
// fingerprints with hints.
var cacheSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.hints.statement_hints_cache_size",
	"number of hint entries to store in the LRU",
	metamorphic.ConstantWithTestChoice[int64](
		"sql.hints.statement_hints_cache_size",
		1024,                  /* defaultValue */
		1, 2, 3, 8, 128, 4096, /* otherValues */
	),
	settings.NonNegativeInt,
)

// rangefeedBufferSize is the size of the internal buffer used by the rangefeed
// watcher to ensure emitted events constitute a consistent snapshot.
var rangefeedBufferSize = metamorphic.ConstantWithTestChoice[int](
	"hint-cache-rangefeed-buffer-size",
	10000,              /* defaultValue */
	0, 1, 2, 3, 8, 128, /* otherValues */
)

// NewStatementHintsCache creates a new StatementHintsCache that can hold
// external hints for cacheSize statements (and records the existence of hints
// for all statements).
func NewStatementHintsCache(
	clock *hlc.Clock,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	codec keys.SQLCodec,
	db descs.DB,
	st *cluster.Settings,
) *StatementHintsCache {
	hintsCache := &StatementHintsCache{clock: clock, f: f, stopper: stopper, codec: codec, db: db}
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
	statementHintsHashIndexPrefix := c.codec.IndexPrefix(
		uint32(tableID), uint32(systemschema.StatementHintsHashIndexID),
	)
	statementHintsHashIndexSpan := roachpb.Span{
		Key:    statementHintsHashIndexPrefix,
		EndKey: statementHintsHashIndexPrefix.PrefixEnd(),
	}

	watcher := rangefeedcache.NewWatcher(
		"statement-hints-watcher",
		c.clock, c.f,
		rangefeedBufferSize,
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
	hash, err := decodeHashFromStatementHintsKey(c.codec, kv.Key, &c.datumAlloc)
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
		log.Dev.Info(ctx, "statement_hints rangefeed completed initial scan")
		c.handleInitialScan(update)
	} else if len(update.Events) > 0 {
		// Ignore empty updates that only bump the resolved timestamp.
		log.Dev.Info(ctx, "statement_hints rangefeed applying incremental update")
		c.handleIncrementalUpdate(ctx, update)
	}
}

// handleInitialScan builds the hintedHashes map and adds it to
// StatementHintsCache after the initial scan completes.
func (c *StatementHintsCache) handleInitialScan(update rangefeedcache.Update[*bufferEvent]) {
	defer c.generation.Add(1)
	hintedHashes := make(map[int64]hlc.Timestamp)
	for _, ev := range update.Events {
		if ev.del {
			// Deletions should never happen during an initial scan.
			if buildutil.CrdbTestBuild {
				panic(errors.AssertionFailedf("unexpected deletion during rangefeed initial scan"))
			}
			continue
		}
		hintedHashes[ev.hash] = hlc.Timestamp{}
	}
	func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.hintedHashes = hintedHashes

		// Invalidate all cache entries. In most cases the cache will already be
		// empty, but if the first attempted initial scan failed, server startup
		// will have proceeded and some statements may have populated the cache.
		c.mu.hintCache.Clear()
	}()
}

// handleIncrementalUpdate intercepts an incremental update from the rangefeed
// and applies it to the cache.
func (c *StatementHintsCache) handleIncrementalUpdate(
	ctx context.Context, update rangefeedcache.Update[*bufferEvent],
) {
	defer c.generation.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range update.Events {
		// Drop outdated entries from hintCache.
		c.mu.hintCache.Del(ev.hash)
		if pendingTS := c.mu.hintedHashes[ev.hash]; pendingTS.IsSet() {
			// The hintedHashes entry for this hash is already being refreshed.
			if pendingTS.Less(ev.ts) {
				// Forward the refresh timestamp. The worker will perform another read
				// at or after this timestamp after the first read returns.
				c.mu.hintedHashes[ev.hash] = ev.ts
			}
			continue
		}
		// Launch a goroutine to check the system table for hints on the hash. The
		// refresh will observe the effect of this rangefeed event as well as all
		// others that happened before the current timestamp.
		// TODO(drewk): an insertion event implies on its own that there is at least
		// one hint at that timestamp, so we could skip the DB read for insertions.
		refreshTS := c.clock.Now()
		c.mu.hintedHashes[ev.hash] = refreshTS
		c.checkHashHasHintsAsync(ctx, ev.hash, refreshTS)
	}
}

// checkHashHasHintsAsync launches an async goroutine to check if there are any
// hints for the given fingerprint hash by reading the system table. It is able
// to retry the table read if there is an error or a more recent rangefeed event
// occurs.
func (c *StatementHintsCache) checkHashHasHintsAsync(
	ctx context.Context, hash int64, refreshTS hlc.Timestamp,
) {
	// It's OK to ignore the error here, since it only happens on server shutdown,
	// in which case we don't need the cache anyway.
	const opName = "check-statement-hints"
	_ = c.stopper.RunAsyncTask(ctx, opName, func(ctx context.Context) {
		retryOnError := retry.StartWithCtx(ctx, retry.Options{
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     10 * time.Second,
		})
		for {
			log.VEventf(ctx, 1, "checking hints for fingerprint hash %d @ %v", hash, refreshTS)
			hasHints, err := c.checkForStatementHintsInDB(ctx, hash)
			if err != nil {
				log.Dev.Warningf(ctx, "failed to check hints for hash %d: %v", hash, err)
				if retryOnError.Next() {
					continue
				}
				return
			}
			log.VEventf(ctx, 1, "done checking hints for fingerprint hash %d @ %v", hash, refreshTS)
			done := func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				if refreshTS.Forward(c.mu.hintedHashes[hash]) {
					// The refresh timestamp was bumped by a rangefeed event. Retry at the
					// new timestamp.
					return false
				}
				if hasHints {
					// Unset the timestamp to indicate that the refresh is done.
					c.mu.hintedHashes[hash] = hlc.Timestamp{}
				} else {
					delete(c.mu.hintedHashes, hash)
				}
				return true
			}()
			if done {
				return
			}
		}
	})
}

// checkForStatementHintsInDB queries the system.statement_hints table to
// determine if there are any hints for the given fingerprint hash. The caller
// must be able to retry if an error is returned.
func (c *StatementHintsCache) checkForStatementHintsInDB(
	ctx context.Context, statementHash int64,
) (hasHints bool, retErr error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `SELECT hash FROM system.statement_hints WHERE "hash" = $1 LIMIT 1`
	it, err := c.db.Executor().QueryIteratorEx(
		ctx, opName, nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		getHintsStmt, statementHash,
	)
	if err != nil {
		return false, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()
	return it.Next(ctx)
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

// ============================================================================
// Reading from the cache.
// ============================================================================

// GetGeneration returns the current generation, which will change if any
// modifications happen to the cache.
func (c *StatementHintsCache) GetGeneration() int64 {
	return c.generation.Load()
}

// MaybeGetStatementHints attempts to retrieve the hints for the given statement
// fingerprint, along with the unique ID of each hint (for invalidating cached
// plans). It returns nil if the statement has no hints, or there was an error
// retrieving them.
func (c *StatementHintsCache) MaybeGetStatementHints(
	ctx context.Context, statementFingerprint string,
) (hints []StatementHint, ids []int64) {
	hash := fnv.New64()
	_, err := hash.Write([]byte(statementFingerprint))
	if err != nil {
		// This should never happen for 64-bit FNV-1.
		log.Dev.Errorf(ctx, "failed to compute hash for statement fingerprint: %v", err)
		return nil, nil
	}
	statementHash := int64(hash.Sum64())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.hintedHashes != nil {
		// If the hintedHashes map has not yet been initialized, statements must
		// unconditionally check hintCache.
		if _, ok := c.mu.hintedHashes[statementHash]; !ok {
			// There are no plan hints for this query.
			return nil, nil
		}
	}
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
	c.mu.AssertHeld()
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
) (hints []StatementHint, ids []int64) {
	c.mu.AssertHeld()

	// Add a cache entry that other queries can find and wait on until we have the
	// plan hints.
	entry := &cacheEntry{
		mustWait: true,
		waitCond: sync.Cond{L: &c.mu},
	}
	c.mu.hintCache.Add(statementHash, entry)
	c.mu.numInternalQueries++

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
		return nil, nil
	}
	return entry.getMatchingHints(statementFingerprint)
}

// getStatementHintsFromDB queries the system.statement_hints table for hints
// matching the given fingerprint hash. It is able to handle the case when
// multiple fingerprints match the hash, as well as the case when there are no
// hints for the fingerprint. Results are ordered by row ID.
func (c *StatementHintsCache) getStatementHintsFromDB(
	ctx context.Context, statementHash int64, entry *cacheEntry,
) (retErr error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `
    SELECT "row_id", "fingerprint", "hint"
    FROM system.statement_hints
    WHERE "hash" = $1
    ORDER BY "row_id" ASC`
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
		rowID := int64(tree.MustBeDInt(datums[0]))
		fingerprint := string(tree.MustBeDString(datums[1]))
		hint, err := NewStatementHint([]byte(tree.MustBeDBytes(datums[2])))
		if err != nil {
			return err
		}
		entry.ids = append(entry.ids, rowID)
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

	// hints, fingerprints, and ids have the same length. fingerprints[i] is the
	// statement fingerprint to which hints[i] applies, while ids[i] uniquely
	// identifies a hint in the system table. They are kept in order of id.
	//
	// We track the hint ID for invalidating cached query plans after a hint is
	// added or removed. We track the fingerprint to resolve hash collisions. Note
	// that a single fingerprint can have multiple hints, in which case there will
	// be duplicate entries in the fingerprints slice.
	// TODO(drewk): consider de-duplicating the fingerprint strings to reduce
	// memory usage.
	hints        []StatementHint
	fingerprints []string
	ids          []int64
}

// getMatchingHints returns the plan hints and row IDs for the given
// fingerprint, or nil if they don't exist. The results are in order of row ID.
func (entry *cacheEntry) getMatchingHints(
	statementFingerprint string,
) (hints []StatementHint, ids []int64) {
	for i := range entry.hints {
		if entry.fingerprints[i] == statementFingerprint {
			hints = append(hints, entry.hints[i])
			ids = append(ids, entry.ids[i])
		}
	}
	return hints, ids
}

// ============================================================================
// Test helpers.
// ============================================================================

// TestingHashCount returns the number of hashes with hints.
func (c *StatementHintsCache) TestingHashCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.mu.hintedHashes)
}

// TestingHashHasHints returns true if the given hash has any hints, and false
// otherwise.
func (c *StatementHintsCache) TestingHashHasHints(hash int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, hasHints := c.mu.hintedHashes[hash]
	return hasHints
}

// TestingNumTableReads returns the number of times hints have been read from
// the system.statement_hints table.
func (c *StatementHintsCache) TestingNumTableReads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.numInternalQueries
}
