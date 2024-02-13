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
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type PlanHintsCache struct {
	mu struct {
		// NB: This can't be a RWMutex for lookup because UnorderedCache.Get
		// manipulates an internal LRU list.
		syncutil.Mutex

		// queryIDs contains the ID of *every* query that has plan hints. It is used
		// to allow queries without hints to return immediately without consulting
		// the cache (and potentially the system.plan_hints table).
		queryIDs map[int64]struct{}

		// cache stores query plan hints from the system.plan_hints table.
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

	// MVCC timestamp of the last update to this set of plan hints.
	timestamp hlc.Timestamp

	hints *PlanHints
}

func NewPlanHintsCache(cacheSize int, db descs.DB) *PlanHintsCache {
	planHintsCache := &PlanHintsCache{db: db}
	planHintsCache.mu.queryIDs = make(map[int64]struct{})
	planHintsCache.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(s int, key, value interface{}) bool { return s > cacheSize },
	})
	return planHintsCache
}

func (c *PlanHintsCache) MaybeGetPlanHints(
	ctx context.Context, id int64,
) (hints *PlanHints, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok = c.mu.queryIDs[id]; !ok {
		// There are no plan hints for this query.
		return nil, false
	}
	var e interface{}
	e, ok = c.mu.cache.Get(id)
	if ok {
		entry := e.(*cacheEntry)
		if entry.mustWait {
			// We are in the process of grabbing plan hints for this query. Wait until
			// that is complete, at which point e.hints will be populated.
			log.VEventf(ctx, 1, "waiting for plan hints for query with ID %d", id)
			entry.waitCond.Wait()
			log.VEventf(ctx, 1, "finished waiting for plan hints for query with ID %d", id)
		} else {
			// This is the expected "fast" path; don't emit an event.
			if log.V(2) {
				log.Infof(ctx, "plan hints for query with ID %d found in cache", id)
			}
		}
		return entry.hints, true
	}
	// The plan hints were evicted from the cache. Retrieve them from the database
	// and add them to the cache.
	return c.addCacheEntryLocked(ctx, id)
}

func (c *PlanHintsCache) addCacheEntryLocked(
	ctx context.Context, id int64,
) (hints *PlanHints, ok bool) {
	// Add a cache entry that other queries can find and wait on until we have the
	// plan hints.
	e := &cacheEntry{
		mustWait: true,
		waitCond: sync.Cond{L: &c.mu},
	}
	c.mu.cache.Add(id, e)

	var err error
	var ts hlc.Timestamp
	func() {
		c.mu.Unlock()
		defer c.mu.Lock()

		log.VEventf(ctx, 1, "reading plan hints for query with ID %d", id)
		hints, ts, err = c.getPlanHintsFromDB(ctx, id)
		log.VEventf(ctx, 1, "finished reading plan hints for query with ID %d", id)
	}()

	// It is possible for the entry to be updated by the rangefeed during the call
	// to getPlanHintsFromDB. Only update the entry if this set of hints has a
	// later timestamp.
	if e.timestamp.Less(ts) {
		e.hints = hints
	}
	e.mustWait = false

	// Wake up any other callers that are waiting on these stats.
	e.waitCond.Broadcast()

	if err != nil {
		// Remove the cache entry so that the next caller retries the query.
		c.mu.cache.Del(id)
		log.VEventf(ctx, 1, "encountered error while reading plan hints for query with ID %d", id)
		return nil, false
	}
	return hints, true
}

func (c *PlanHintsCache) getPlanHintsFromDB(
	ctx context.Context, id int64,
) (hints *PlanHints, ts hlc.Timestamp, err error) {
	getPlanHintsStmt := `SELECT "plan_hints", "crdb_internal_mvcc_timestamp" FROM system.plan_hints WHERE "query_id" = $1`
	opName := "get-plan-hints"
	it, err := c.db.Executor().QueryIteratorEx(
		ctx, opName, nil /* txn */, sessiondata.NodeUserSessionDataOverride, getPlanHintsStmt, id,
	)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	ok, err := it.Next(ctx)
	if !ok || err != nil {
		return nil, hlc.Timestamp{}, err
	}
	datums := it.Cur()
	hintRawBytes := []byte(tree.MustBeDBytes(datums[0]))
	hints, err = NewPlanHints(hintRawBytes)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	tsDec := tree.MustBeDDecimal(datums[1])
	ts, err = hlc.DecimalToHLC(&tsDec.Decimal)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	return hints, ts, nil
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
		id, err := decodePlanHintsIDFromKV(codec, kv, &c.datumAlloc)
		if err != nil {
			log.Warningf(ctx, "failed to decode system.plan_hints ID %v: %v", kv.Key, err)
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if !kv.Value.IsPresent() {
			// The plan hints for this query were deleted.
			delete(c.mu.queryIDs, id)
			c.mu.cache.Del(id)
			return
		}
		c.mu.queryIDs[id] = struct{}{}
		e, ok := c.mu.cache.StealthyGet(id)
		if !ok {
			// There is no entry in the LRU cache for this query, so don't bother
			// updating it.
			return
		}
		entry := e.(*cacheEntry)
		if kv.Timestamp().LessEq(entry.timestamp) {
			// The cache has already been updated at a later timestamp. This could
			// happen if the entry was previously evicted, and then a query refreshed
			// it.
			return
		}
		// Update the cache entry with the hints.
		hints, err := c.decodePlanHintsFromKV(kv)
		if err != nil {
			log.Warningf(ctx, "failed to decode system.plan_hints values for ID %v: %v", id, err)
			return
		}
		entry.hints = hints
		entry.timestamp = kv.Timestamp()
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

func (c *PlanHintsCache) decodePlanHintsFromKV(
	kv *kvpb.RangeFeedValue,
) (hints *PlanHints, err error) {
	var rawBytes []byte
	rawBytes, err = kv.Value.GetTuple()
	if err != nil {
		return nil, err
	}
	var hintBytes rowenc.EncDatum
	hintBytes, _, err = rowenc.EncDatumFromBuffer(catenumpb.DatumEncoding_VALUE, rawBytes)
	if err != nil {
		return nil, err
	}
	if err = hintBytes.EnsureDecoded(types.Bytes, &c.datumAlloc); err != nil {
		return nil, err
	}
	return NewPlanHints([]byte(tree.MustBeDBytes(hintBytes.Datum)))
}

// decodePlanHintsKV decodes the query ID from a range feed event on
// system.plan_hints.
func decodePlanHintsIDFromKV(
	codec keys.SQLCodec, kv *kvpb.RangeFeedValue, da *tree.DatumAlloc,
) (int64, error) {
	// The primary key of plan_hints is (id INT8).
	idxTypes := []*types.T{types.Int}
	dirs := []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}
	keyVals := make([]rowenc.EncDatum, 1)
	if _, err := rowenc.DecodeIndexKey(codec, keyVals, dirs, kv.Key); err != nil {
		return 0, err
	}
	if err := keyVals[0].EnsureDecoded(idxTypes[0], da); err != nil {
		return 0, err
	}
	return int64(tree.MustBeDInt(keyVals[0].Datum)), nil
}
