// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hints

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// PlanHintsCache allows queries to access their plan hints stored in the
// system.plan_hints table (and determine whether they have hints) without
// having to perform DB reads.
type PlanHintsCache struct {
	mu struct {
		// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
		// manipulates an internal LRU list.
		syncutil.Mutex

		// queryHashes maintains an entry for *every* query that has plan hints.
		// This allows queries without hints to return immediately without
		// consulting the cache and system.plan_hints table. It is updated only
		// by the system.plan_hints rangefeed.
		//
		// In order to handle hash collisions, queryHashes maintains a count for the
		// number of fingerprints associated with each hash.
		queryHashes map[int64]int

		// cache stores query plan hints from the system.plan_hints table. It has a
		// maximum capacity, and uses an LRU eviction policy.
		cache *cache.UnorderedCache
	}

	db descs.DB

	// Used when decoding KVs from the range feed.
	datumAlloc tree.DatumAlloc
}

type cacheEntry struct {
	// If mustWait is true, we do not have any statistics for this table and we
	// are in the process of fetching the stats from the database. Other callers
	// can wait on the waitCond until this is false.
	mustWait bool
	waitCond sync.Cond

	// A cache entry can store multiple sets of hints in case of hash collisions.
	hints []planHints
}

// planHints annotates PlanHints with a query fingerprint in order to resolve
// hash collisions.
type planHints struct {
	// queryFingerprint is the fingerprint common to all queries that match this
	// hint. It is used in addition to the fingerprint hash to determine whether
	// the plan hints apply to a specific query.
	queryFingerprint string

	// hints is the set of query plan hints for queries that match the
	// fingerprint.
	hints *PlanHints
}

func NewPlanHintsCache(cacheSize int, db descs.DB) *PlanHintsCache {
	planHintsCache := &PlanHintsCache{db: db}
	planHintsCache.mu.queryHashes = make(map[int64]int)
	planHintsCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > cacheSize },
	})
	return planHintsCache
}

// Start begins watching for updates in the plan_hints table.
func (c *PlanHintsCache) Start(
	ctx context.Context,
	codec keys.SQLCodec,
	rangeFeedFactory *rangefeed.Factory,
	tableResolver catalog.SystemTableIDResolver,
) error {
	tableID, err := tableResolver.LookupSystemTableID(ctx, systemschema.PlanHintsTable.GetName())
	if err != nil {
		return err
	}
	planHintsTablePrefix := codec.TablePrefix(uint32(tableID))
	planHintsTableSpan := roachpb.Span{
		Key:    planHintsTablePrefix,
		EndKey: planHintsTablePrefix.PrefixEnd(),
	}

	// Set up a range feed to watch for updates to system.plan_hints.
	handleEvent := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		queryHash, err := decodePlanHintsIDFromKV(codec, kv, &c.datumAlloc)
		if err != nil {
			log.Warningf(ctx, "failed to decode system.plan_hints key %v: %v", kv.Key, err)
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if kv.Value.IsPresent() {
			// Ensure an entry in the queryHashes map.
			c.mu.queryHashes[queryHash] = c.mu.queryHashes[queryHash] + 1
		} else {
			// This is a deletion. Remove the entry from queryHashes if there are no
			// collisions.
			if c.mu.queryHashes[queryHash] > 1 {
				c.mu.queryHashes[queryHash] = c.mu.queryHashes[queryHash] - 1
			} else {
				delete(c.mu.queryHashes, queryHash)
			}
		}
		// Unconditionally remove the cache entry to ensure a refresh on the next
		// matching query. Note that this may cause unnecessary overhead in the case
		// of hash collisions, but these are expected to be rare.
		c.mu.cache.Del(queryHash)
	}
	_, err = rangeFeedFactory.RangeFeed(
		ctx,
		"plan-hints-cache",
		[]roachpb.Span{planHintsTableSpan},
		c.db.KV().Clock().Now(),
		handleEvent,
		rangefeed.WithSystemTablePriority(),
		rangefeed.WithInitialScan(func(ctx context.Context) {}),
	)
	return err
}

// decodePlanHintsKV decodes the query hash and row ID from a range feed event
// on system.plan_hints.
func decodePlanHintsIDFromKV(
	codec keys.SQLCodec, kv *kvpb.RangeFeedValue, da *tree.DatumAlloc,
) (queryHash int64, err error) {
	// The primary key of plan_hints is (id INT8).
	dirs := []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC}
	keyVals := make([]rowenc.EncDatum, 1)
	if _, err = rowenc.DecodeIndexKey(codec, keyVals, dirs, kv.Key); err != nil {
		return 0, err
	}
	if err = keyVals[0].EnsureDecoded(types.Int, da); err != nil {
		return 0, err
	}
	queryHash = int64(tree.MustBeDInt(keyVals[0].Datum))
	return queryHash, nil
}

// MaybeGetPlanHints attempts to retrieve the plan hints for the given query
// fingerprint. It returns nil if the plan has no hints, or there was an error
// retrieving them.
func (c *PlanHintsCache) MaybeGetPlanHints(
	ctx context.Context, queryFingerprint string,
) (hints *PlanHints) {
	// TODO(drewk): consider reusing the hash calculated here and for query
	// logging.
	fnv := util.MakeFNV64()
	for _, c := range queryFingerprint {
		fnv.Add(uint64(c))
	}
	queryHash := int64(fnv.Sum())
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.mu.queryHashes[queryHash]; !ok {
		// There are no plan hints for this query.
		return nil
	}
	var e interface{}
	e, ok := c.mu.cache.Get(queryHash)
	if !ok {
		// The plan hints were evicted from the cache. Retrieve them from the
		// database and add them to the cache.
		return c.addCacheEntryLocked(ctx, queryHash, queryFingerprint)
	}
	entry := e.(*cacheEntry)
	c.maybeWaitForRefreshLocked(ctx, entry, queryHash)
	for i := range entry.hints {
		if entry.hints[i].queryFingerprint == queryFingerprint {
			return entry.hints[i].hints
		}
	}
	return nil
}

// maybeWaitForRefreshLocked checks if the given cache entry is being refreshed
// by a concurrent query, and waits for the refresh to finish if necessary.
// While maybeWaitForRefreshLocked is waiting, the lock on the cache is
// released.
func (c *PlanHintsCache) maybeWaitForRefreshLocked(
	ctx context.Context, entry *cacheEntry, queryHash int64,
) {
	if entry.mustWait {
		// We are in the process of grabbing plan hints for this query. Wait until
		// that is complete, at which point e.hints will be populated.
		log.VEventf(ctx, 1, "waiting for hints for query with hash %v", queryHash)
		entry.waitCond.Wait()
		log.VEventf(ctx, 1, "finished waiting for hints for query with hash %v", queryHash)
	} else {
		// This is the expected "fast" path; don't emit an event.
		if log.V(2) {
			log.Infof(ctx, "hints for query with hash %v found in cache", queryHash)
		}
	}
}

// addCacheEntryLocked creates a new cache entry and retrieves the plan hints
// for the given query fingerprint from the database. addCacheEntryLocked allows
// other queries to wait for the result via sync.Cond. Note that the lock is
// released while reading from the db, and then reacquired.
func (c *PlanHintsCache) addCacheEntryLocked(
	ctx context.Context, queryHash int64, queryFingerprint string,
) (hints *PlanHints) {
	// Add a cache entry that other queries can find and wait on until we have the
	// plan hints.
	entry := &cacheEntry{
		mustWait: true,
		waitCond: sync.Cond{L: &c.mu},
	}
	c.mu.cache.Add(queryHash, entry)

	var err error
	func() {
		c.mu.Unlock()
		defer c.mu.Lock()
		log.VEventf(ctx, 1, "reading hints for query %s", queryFingerprint)
		hints, err = c.getPlanHintsFromDB(ctx, queryHash, queryFingerprint)
		log.VEventf(ctx, 1, "finished reading hints for query %s", queryFingerprint)
	}()

	// Allow any concurrent queries to proceed.
	entry.mustWait = false
	entry.waitCond.Broadcast()

	if err != nil {
		// Remove the cache entry so that the next caller retries the query.
		c.mu.cache.Del(queryHash)
		log.VEventf(ctx, 1, "encountered error while reading hints for query %s", queryFingerprint)
		return nil
	}
	return hints
}

// getPlanHintsFromDB queries the system.plan_hints table for hints matching the
// given fingerprint. It is able to handle the case when multiple fingerprints
// match the hash, as well as the case when there are no hints for the
// fingerprint.
func (c *PlanHintsCache) getPlanHintsFromDB(
	ctx context.Context, queryHash int64, queryFingerprint string,
) (hints *PlanHints, err error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `SELECT "fingerprint", "plan_hints" FROM system.plan_hints WHERE "query_hash" = $1`
	it, err := c.db.Executor().QueryIteratorEx(
		ctx, opName, nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		getHintsStmt, queryHash,
	)
	if err != nil {
		return nil, err
	}
	var datums tree.Datums
	for {
		ok, err := it.Next(ctx)
		if !ok || err != nil {
			return nil, err
		}
		datums = it.Cur()
		fingerprint := string(tree.MustBeDString(datums[0]))
		if fingerprint == queryFingerprint {
			// It is possible for the query hash to collide, so check the actual
			// fingerprint strings.
			break
		}
	}
	return NewPlanHints([]byte(tree.MustBeDBytes(datums[1])))
}
