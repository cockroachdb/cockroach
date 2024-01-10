// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangecache

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

//go:generate mockgen -package=rangecachemock -destination=rangecachemock/mocks_generated.go . RangeDescriptorDB

// rangeCacheKey is the key type used to store and sort values in the
// RangeCache.
type rangeCacheKey roachpb.RKey

var minCacheKey = newRangeCacheKey(roachpb.RKeyMin)

var rangeCacheKeyPool = sync.Pool{
	New: func() interface{} { return &rangeCacheKey{} },
}

// newRangeCacheKey allocates a new rangeCacheKey using the supplied key. The
// objects escape to the heap because they are passed through an interface{}
// when handed to an OrderedCache, so the sync.Pool avoids a heap allocation.
func newRangeCacheKey(key roachpb.RKey) *rangeCacheKey {
	k := rangeCacheKeyPool.Get().(*rangeCacheKey)
	*k = rangeCacheKey(key)
	return k
}

func (k *rangeCacheKey) release() {
	*k = rangeCacheKey{}
	rangeCacheKeyPool.Put(k)
}

func (k *rangeCacheKey) String() string {
	return roachpb.Key(*k).String()
}

// Compare implements the llrb.Comparable interface for *rangeCacheKey, so that
// it can be used as a key for util.OrderedCache.
func (k *rangeCacheKey) Compare(o llrb.Comparable) int {
	return bytes.Compare(*k, *o.(*rangeCacheKey))
}

// RangeLookupConsistency is an alias for ReadConsistencyType. In the
// cases it is referenced, the only acceptable values are READ_UNCOMMITTED
// and INCONSISTENT. The hope with this alias and the consts below
// it increase code clarity and lead readers of the code here.
type RangeLookupConsistency = kvpb.ReadConsistencyType

const (
	// ReadFromFollower is the RangeLookupConsistency used to read from a follower.
	ReadFromFollower = kvpb.INCONSISTENT
	// ReadFromLeaseholder is the RangeLookupConsistency used to read from the
	// leaseholder.
	// TODO(baptist): This may be incorrect. An uncommitted read may not see the
	// updated value. Revisit if this should be a CONSISTENT read against the
	// leaseholder. READ_UNCOMMITTED does not guarantee a more up-to-date view
	// than INCONSISTENT, it only guarantees that the read is on the leaseholder,
	// but may not include writes that have been appended to Raft, but not yet
	// applied. In the case of certain disk issues, the leaseholders disk may be
	// significantly behind. An alternative would be to change READ_UNCOMMITTED to
	// acquire latches. See #98862.
	ReadFromLeaseholder = kvpb.READ_UNCOMMITTED
)

// UnknownClosedTimestampPolicy is used to mark on a CacheEntry that the closed
// timestamp policy is not known. This value is never serialized into
// RangeInfo or any other message which uses the type.
const UnknownClosedTimestampPolicy roachpb.RangeClosedTimestampPolicy = -1

// RangeDescriptorDB is a type which can query range descriptors from an
// underlying datastore. This interface is used by RangeCache to
// initially retrieve information which will be cached.
type RangeDescriptorDB interface {
	// RangeLookup takes a key to look up descriptors for. Two slices of range
	// descriptors are returned. The first of these slices holds descriptors
	// whose [startKey,endKey) spans contain the given key (possibly from
	// intents), and the second holds prefetched adjacent descriptors.
	//
	// Note that the acceptable consistency values are the constants defined
	// in this package: ReadFromFollower and ReadFromLeaseholder. The
	// RangeLookupConsistency type is aliased to kvpb.ReadConsistencyType
	// in order to permit implementations of this interface to import this
	// package.
	RangeLookup(
		ctx context.Context,
		key roachpb.RKey,
		consistency RangeLookupConsistency,
		useReverseScan bool,
	) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error)
}

// RangeCache is used to retrieve range descriptors for
// arbitrary keys. Descriptors are initially queried from storage
// using a RangeDescriptorDB, but are cached for subsequent lookups.
type RangeCache struct {
	st      *cluster.Settings
	stopper *stop.Stopper
	// RangeDescriptorDB is used to retrieve range descriptors from the
	// database, which will be cached by this structure.
	db RangeDescriptorDB
	// rangeCache caches replica metadata for key ranges. The cache is
	// filled while servicing read and write requests to the key value
	// store.
	rangeCache struct {
		syncutil.RWMutex
		cache *cache.OrderedCache
	}
	// lookupRequests stores all inflight requests retrieving range
	// descriptors from the database. It allows multiple RangeDescriptorDB
	// lookup requests for the same inferred range descriptor to be
	// multiplexed onto the same database lookup. See makeLookupRequestKey
	// for details on this inference.
	lookupRequests *singleflight.Group

	// coalesced, if not nil, is sent on every time a request is coalesced onto
	// another in-flight one. Used by tests to block until a lookup request is
	// blocked on the single-flight querying the db.
	coalesced chan struct{}
}

// makeLookupRequestKey constructs a key for the lookupRequest group with the
// goal of mapping all requests which are inferred to be looking for the same
// descriptor onto the same request key to establish request coalescing.
//
// If the key is part of a descriptor that we previously had cached (but the
// cache entry is stale), we use that previous descriptor to coalesce all
// requests for keys within it into a single request. Namely, there are three
// possible events that may have happened causing our cache to be stale. For
// each of these, we try to coalesce all requests that will end up on the same
// range post-event together.
//   - Split:  for a split, only the right half of the split will attempt to evict
//     the stale descriptor because only the right half will be sending to
//     the wrong range. Once this stale descriptor is evicted, keys from
//     both halves of the split will miss the cache. Because both sides of
//     the split will now map to the same EvictionToken, it is important to
//     use EvictAndReplace if possible to insert one of the two new descriptors.
//     This way, no requests to that descriptor will ever miss the cache and
//     risk being coalesced into the other request. If this is not possible,
//     the lookup will still work, but it will require multiple lookups, which
//     will be launched in series when requests find that their desired key
//     is outside of the returned descriptor.
//   - Merges: for a merge, the left half of the merge will never notice. The right
//     half of the merge will suddenly find its descriptor to be stale, so
//     it will evict and lookup the new descriptor. We set the key to hash
//     to the start of the stale descriptor for lookup requests to the right
//     half of the merge so that all requests will be coalesced to the same
//     lookupRequest.
//   - Rebal:  for a rebalance, the entire descriptor will suddenly go stale and
//     requests to it will evict the descriptor. We set the key to hash to
//     the start of the stale descriptor for lookup requests to the rebalanced
//     descriptor so that all requests will be coalesced to the same lookupRequest.
//
// Note that the above description assumes that useReverseScan is false for simplicity.
// If useReverseScan is true, we need to use the end key of the stale descriptor instead.
func makeLookupRequestKey(
	key roachpb.RKey, prevDesc *roachpb.RangeDescriptor, useReverseScan bool,
) string {
	var ret strings.Builder
	// We only want meta1, meta2, user range lookups to be coalesced with other
	// meta1, meta2, user range lookups, respectively. Otherwise, deadlocks could
	// happen due to singleflight. If the range lookup is in a meta range, we
	// prefix the request key with the corresponding meta prefix to disambiguate
	// the different lookups.
	if key.AsRawKey().Compare(keys.Meta1KeyMax) < 0 {
		ret.Write(keys.Meta1Prefix)
	} else if key.AsRawKey().Compare(keys.Meta2KeyMax) < 0 {
		ret.Write(keys.Meta2Prefix)
	}
	if prevDesc != nil {
		if useReverseScan {
			key = prevDesc.EndKey
		} else {
			key = prevDesc.StartKey
		}
	}
	ret.Write(key)
	ret.WriteString(":")
	ret.WriteString(strconv.FormatBool(useReverseScan))
	// Add the generation of the previous descriptor to the lookup request key to
	// decrease the number of lookups in the rare double split case. Suppose we
	// have a range [a, e) that gets split into [a, c) and [c, e). The requests
	// on [c, e) will fail and will have to retry the lookup. If [a, c) gets
	// split again into [a, b) and [b, c), we don't want to the requests on [a,
	// b) to be coalesced with the retried requests on [c, e). To distinguish the
	// two cases, we can use the generation of the previous descriptor.
	if prevDesc != nil {
		ret.WriteString(":")
		ret.WriteString(prevDesc.Generation.String())
	}
	return ret.String()
}

// NewRangeCache returns a new RangeCache which uses the given RangeDescriptorDB
// as the underlying source of range descriptors.
func NewRangeCache(
	st *cluster.Settings, db RangeDescriptorDB, size func() int64, stopper *stop.Stopper,
) *RangeCache {
	rdc := &RangeCache{
		st: st, db: db, stopper: stopper,
		lookupRequests: singleflight.NewGroup("range lookup", "lookup"),
	}
	rdc.rangeCache.cache = cache.NewOrderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(n int, _, _ interface{}) bool {
			return int64(n) > size()
		},
	})
	return rdc
}

func (rc *RangeCache) String() string {
	rc.rangeCache.RLock()
	defer rc.rangeCache.RUnlock()
	return rc.stringLocked()
}

func (rc *RangeCache) stringLocked() string {
	var buf strings.Builder
	rc.rangeCache.cache.Do(func(k, v interface{}) bool {
		fmt.Fprintf(&buf, "key=%s desc=%+v\n", roachpb.Key(*k.(*rangeCacheKey)), v)
		return false
	})
	return buf.String()
}

// EvictionToken holds eviction state between calls to Lookup.
type EvictionToken struct {
	// rdc is the cache that produced this token - and that will be modified by
	// Evict().
	rdc *RangeCache

	// desc, lease, and closedts represent the information retrieved from the
	// cache. This can advance throughout the life of the token, as various
	// methods re-synchronize with the cache. However, if it changes, the
	// descriptor only changes to other "compatible" descriptors (same range id
	// and key bounds).
	desc *roachpb.RangeDescriptor

	// The lease can be speculative, in which case it will have an unset sequence
	// number. This happens when a follower replica knows Raft leader was, but did
	// not have a valid lease record and did not try to become the Leaseholder.
	// The leader is the most likely replica to be the next Leaseholder for this
	// range.
	lease *roachpb.Lease

	closedts roachpb.RangeClosedTimestampPolicy

	// speculativeDesc, if not nil, is the descriptor that should replace desc if
	// desc proves to be stale - i.e. speculativeDesc is inserted in the cache
	// automatically by Evict(). This is used when the range descriptor lookup
	// that populated the cache returned an intent in addition to the current
	// descriptor value. The idea is that, if the range lookup was performed in
	// the middle of a split or a merge and it's seen an intent, it's likely that
	// the intent will get committed soon and so the client should use it if the
	// previous version proves stale. This mechanism also has a role for resolving
	// intents for the split transactions itself where, immediately after the
	// split's txn record is committed, an intent is the only correct copy of the
	// LHS' descriptor.
	//
	// TODO(andrei): It's weird that speculativeDesc hangs from an EvictionToken,
	// instead of from a cache entry. Hanging from a particular token, only one
	// actor has the opportunity to use this speculativeDesc; if another actor
	// races to evict the respective cache entry and wins, speculativeDesc becomes
	// useless.
	speculativeDesc *roachpb.RangeDescriptor
}

func (rc *RangeCache) makeEvictionToken(
	entry *CacheEntry, speculativeDesc *roachpb.RangeDescriptor,
) EvictionToken {
	if speculativeDesc != nil {
		// speculativeDesc comes from intents. Being uncommitted, it is speculative.
		// We reset its generation to indicate this fact and allow it to be easily
		// overwritten. Putting a speculative descriptor in the cache with a
		// generation might make it hard for the real descriptor with the same
		// generation to overwrite it, in case the speculation fails.
		nextCpy := *speculativeDesc
		nextCpy.Generation = 0
		speculativeDesc = &nextCpy
	}
	return EvictionToken{
		rdc:             rc,
		desc:            entry.Desc(),
		lease:           entry.leaseEvenIfSpeculative(),
		closedts:        entry.closedts,
		speculativeDesc: speculativeDesc,
	}
}

// MakeEvictionToken is the exported ctor. For tests only.
func (rc *RangeCache) MakeEvictionToken(entry *CacheEntry) EvictionToken {
	return rc.makeEvictionToken(entry, nil /* speculativeDesc */)
}

func (et EvictionToken) String() string {
	if !et.Valid() {
		return "<empty>"
	}
	return fmt.Sprintf("desc:%s lease:%s spec desc: %v", et.desc, et.lease, et.speculativeDesc)
}

// Valid returns false if the token does not contain any replicas.
func (et EvictionToken) Valid() bool {
	return et.rdc != nil
}

// clear wipes the token. Valid() will return false.
func (et *EvictionToken) clear() {
	*et = EvictionToken{}
}

// Desc returns the RangeDescriptor that was retrieved from the cache. The
// result is to be considered immutable.
//
// Note that the returned descriptor might have Generation = 0. This means that
// the descriptor is speculative; it is not know to have committed.
//
//gcassert:noescape
func (et EvictionToken) Desc() *roachpb.RangeDescriptor {
	if !et.Valid() {
		return nil
	}
	return et.desc
}

// Leaseholder returns the cached leaseholder. If the cache didn't have any
// lease information, returns nil. The result is to be considered immutable.
//
// If a leaseholder is returned, it will correspond to one of the replicas in
// et.Desc().
func (et EvictionToken) Leaseholder() *roachpb.ReplicaDescriptor {
	if !et.Valid() || et.lease == nil {
		return nil
	}
	return &et.lease.Replica
}

// Lease returns the cached lease. If the cache didn't have any lease
// information, returns nil. The result is considered immutable.
func (et EvictionToken) Lease() *roachpb.Lease {
	if !et.Valid() {
		return nil
	}
	return et.lease
}

// LeaseSeq returns the sequence of the cached lease. If no lease is cached, or
// the cached lease is speculative, 0 is returned.
func (et EvictionToken) LeaseSeq() roachpb.LeaseSequence {
	if !et.Valid() {
		panic("invalid LeaseSeq() call on empty EvictionToken")
	}
	if et.lease == nil {
		return 0
	}
	return et.lease.Sequence
}

// ClosedTimestampPolicy returns the cache's current understanding of the
// range's closed timestamp policy. If no policy is known, the _default
// is returned.
func (et EvictionToken) ClosedTimestampPolicy(
	_default roachpb.RangeClosedTimestampPolicy,
) roachpb.RangeClosedTimestampPolicy {
	if !et.Valid() {
		panic("invalid ClosedTimestampPolicy() call on empty EvictionToken")
	}
	if et.closedts == UnknownClosedTimestampPolicy {
		return _default
	}
	return et.closedts
}

// syncRLocked syncs the token with the cache. If the cache has a newer, but
// compatible, descriptor and lease, the token is updated. If not, the token is
// invalidated. The token is also invalidated if the cache doesn't contain an
// entry for the start key any more.
func (et *EvictionToken) syncRLocked(
	ctx context.Context,
) (stillValid bool, cachedEntry *CacheEntry, rawEntry *cache.Entry) {
	cachedEntry, rawEntry = et.rdc.getCachedRLocked(ctx, et.desc.StartKey, false /* inverted */)
	if cachedEntry == nil || !descsCompatible(cachedEntry.Desc(), et.Desc()) {
		et.clear()
		return false, nil, nil
	}
	et.desc = cachedEntry.Desc()
	et.lease = cachedEntry.leaseEvenIfSpeculative()
	return true, cachedEntry, rawEntry
}

// SyncTokenAndMaybeUpdateCache acts as a synchronization point between the
// caller and the RangeCache. It updates the EvictionToken with fresher
// information in case the EvictionToken was no longer up to date with the
// cache entry from whence it came. The leaseholder and range descriptor for the
// token's cache entry are updated to the specified lease/range descriptor if
// they are fresher than what the cache contains (which is reflected in the
// EvictionToken itself).
//
// The returned bool `updatedLeaseholder` is true if the leaseholder was updated
// in the cache (i.e the passed-in lease was more recent than the cached lease).
//
// The updated token is guaranteed to contain a descriptor compatible with the
// original one (same range id and key span). If the cache no longer contains
// an entry for the start key or the range boundaries have changed, the token
// will be invalidated. The caller should take an invalidated token to mean that
// the information it was working with is too stale to be useful, and it should
// use a range iterator again to get an updated cache entry.
//
// It's legal to pass in a lease with a zero Sequence; it will be treated as a
// speculative lease and considered newer[1] than any existing lease (and then
// in-turn will be overwritten by any subsequent update).
//
// [1]: As long as the associated range descriptor is not older than what's
// cached.
//
// TODO(arul): Instead of returning updatedLeaseholder all the way back to the
// DistSender and then using its value to determine whether we need to backoff,
// we should instead check if we're retrying the same replica. This will allow
// us to eschew plumbing this state back up to the caller.
func (et *EvictionToken) SyncTokenAndMaybeUpdateCache(
	ctx context.Context, l *roachpb.Lease, rangeDesc *roachpb.RangeDescriptor,
) (updatedLeaseholder bool) {
	rdc := et.rdc
	rdc.rangeCache.Lock()
	defer rdc.rangeCache.Unlock()

	stillValid, cachedEntry, rawEntry := et.syncRLocked(ctx)
	if !stillValid {
		return false
	}

	// Check if the supplied range descriptor is compatible with the one in the
	// cache. If it isn't, and the supplied range descriptor is newer than what's
	// in the cache, we simply evict the old descriptor and add the new
	// descriptor/lease pair. On the other hand, if the supplied range descriptor
	// is older, we can simply return early.
	if !descsCompatible(rangeDesc, et.Desc()) {
		if rangeDesc.Generation < et.desc.Generation {
			return false
		}
		// Newer descriptor.
		ri := roachpb.RangeInfo{
			Desc:                  *rangeDesc,
			Lease:                 *l,
			ClosedTimestampPolicy: et.closedts,
		}
		et.evictAndReplaceLocked(ctx, ri)
		return false
	}

	updated, updatedLeaseholder, newEntry := cachedEntry.maybeUpdate(ctx, l, rangeDesc)
	if !updated {
		// The cachedEntry wasn't updated; no need to replace it with newEntry in
		// the RangeCache.
		return false
	}
	rdc.swapEntryLocked(ctx, rawEntry, newEntry)

	// Finish syncing the eviction token by updating its fields using the freshest
	// range descriptor/lease information available in the RangeCache.
	et.desc = newEntry.Desc()
	et.lease = newEntry.leaseEvenIfSpeculative()
	return updatedLeaseholder
}

// EvictLease evicts information about the current lease from the cache, if the
// cache entry referenced by the token is still in the cache and the leaseholder
// is the one indicated by the token. Note that we look at the lease's replica,
// not sequence; the idea is that this clearing of a lease comes in response to
// trying the known leaseholder and failing - so it's a particular node that we
// have a problem with, not a particular lease (i.e. we want to evict even a
// newer lease, but with the same leaseholder).
//
// Similarly to SyncTokenAndMaybeUpdateCache(), EvictLease() acts as a
// synchronization point between the caller and the RangeCache. The caller might
// get an updated token (besides the lease). Note that the updated token might
// have a newer descriptor than before and/or still have a lease in it - in case
// the cache already had a more recent entry. The updated descriptor is
// compatible (same range id and key span) to the original one. The token is
// invalidated if the cache has a more recent entry, but the current descriptor
// is incompatible. Callers should interpret such an update as a signal that
// they should use a range iterator again to get updated ranges.
func (et *EvictionToken) EvictLease(ctx context.Context) {
	et.rdc.rangeCache.Lock()
	defer et.rdc.rangeCache.Unlock()

	if et.lease == nil {
		log.Fatalf(ctx, "attempting to clear lease from cache entry without lease")
	}

	lh := et.lease.Replica
	stillValid, cachedEntry, rawEntry := et.syncRLocked(ctx)
	if !stillValid {
		return
	}
	ok, newEntry := cachedEntry.evictLeaseholder(lh)
	if !ok {
		return
	}
	et.desc = newEntry.Desc()
	et.lease = newEntry.leaseEvenIfSpeculative()
	et.rdc.swapEntryLocked(ctx, rawEntry, newEntry)
}

func descsCompatible(a, b *roachpb.RangeDescriptor) bool {
	return (a.RangeID == b.RangeID) && (a.RSpan().Equal(b.RSpan()))
}

// Evict instructs the EvictionToken to evict the RangeDescriptor it was created
// with from the RangeCache. The token is invalidated.
func (et *EvictionToken) Evict(ctx context.Context) {
	et.EvictAndReplace(ctx)
}

// EvictAndReplace instructs the EvictionToken to evict the RangeDescriptor it was
// created with from the RangeCache. It also allows the user to provide
// new RangeDescriptors to insert into the cache, all atomically. When called without
// arguments, EvictAndReplace will behave the same as Evict.
//
// The token is invalidated.
func (et *EvictionToken) EvictAndReplace(ctx context.Context, newDescs ...roachpb.RangeInfo) {
	if !et.Valid() {
		panic("trying to evict an invalid token")
	}

	et.rdc.rangeCache.Lock()
	defer et.rdc.rangeCache.Unlock()

	et.evictAndReplaceLocked(ctx, newDescs...)
}

// evictAndReplaceLocked is like EvictAndReplace except it assumes that the
// caller holds a write lock on rdc.rangeCache.
func (et *EvictionToken) evictAndReplaceLocked(ctx context.Context, newDescs ...roachpb.RangeInfo) {
	if !et.Valid() {
		panic("trying to evict an invalid token")
	}

	// Evict unless the cache has something newer. Regardless of what the cache
	// has, we'll still attempt to insert newDescs (if any).
	evicted := et.rdc.evictLocked(ctx, et.Desc(), et.Lease())

	if len(newDescs) > 0 {
		log.Eventf(ctx, "evicting cached range descriptor with %d replacements", len(newDescs))
		et.rdc.insertLocked(ctx, newDescs...)
	} else if et.speculativeDesc != nil {
		log.Eventf(ctx, "evicting cached range descriptor with replacement from token")
		et.rdc.insertLocked(ctx, roachpb.RangeInfo{
			Desc: *et.speculativeDesc,
			// We don't know anything about the new lease.
			Lease: roachpb.Lease{},
			// The closed timestamp policy likely hasn't changed.
			ClosedTimestampPolicy: et.closedts,
		})
	} else if evicted {
		log.Eventf(ctx, "evicting cached range descriptor")
	}
	et.clear()
}

// LookupWithEvictionToken attempts to locate a descriptor, and possibly also a
// lease) for the range containing the given key. This is done by first trying
// the cache, and then querying the two-level lookup table of range descriptors
// which cockroach maintains. The function should be provided with an
// EvictionToken if one was acquired from this function on a previous lookup. If
// not, a zero-valued EvictionToken can be provided. Note that if a non-zero
// EvictionToken is provided, it must contain the key being queried.
//
// This method first looks up the specified key in the first level of
// range metadata, which returns the location of the key within the
// second level of range metadata. This second level location is then
// queried to retrieve a descriptor for the range where the key's
// value resides. Range descriptors retrieved during each search are
// cached for subsequent lookups.
//
// The returned EvictionToken contains the descriptor and, possibly, the lease.
// It can also be used to evict information from the cache if it's found to be
// stale.
//
// In the common cache miss case, the lookup will be routed to a
// follower replica. In cases where a stale view of the range descriptor
// no longer corresponds to any replicas in the range, relying on a stale
// view of the range addressing as might be served from a follower does not
// ensure that the client will be able to make progress. Fortunately, this
// scenario can be detected through the use of evictToken.
//
// This method is called with a non-zero evictToken after a sendError is
// returned from (*DistSender).sendToReplicas. In this case, the eviction
// token from the previous iteration of the sendPartialBatch loop (the one
// that failed) is provided. In this case, the method will ensure that the
// optimistic read which may have been evaluated on follower returns a
// result with a generation number greater than the generation stored in
// evictToken. If the initial read which maybe routed to followers returns
// a result that is not newer, the request will be re-issued to the
// leaseholder.
func (rc *RangeCache) LookupWithEvictionToken(
	ctx context.Context, key roachpb.RKey, evictToken EvictionToken, useReverseScan bool,
) (EvictionToken, error) {
	tok, err := rc.lookupInternal(ctx, key, evictToken, useReverseScan)
	if err != nil {
		return EvictionToken{}, err
	}
	return tok, nil
}

// Lookup presents a simpler interface for looking up a RangeDescriptor for a
// key without the eviction tokens or scan direction control of
// LookupWithEvictionToken.
func (rc *RangeCache) Lookup(ctx context.Context, key roachpb.RKey) (CacheEntry, error) {
	tok, err := rc.lookupInternal(
		ctx, key, EvictionToken{}, false /* useReverseScan */)
	if err != nil {
		return CacheEntry{}, err
	}
	var e CacheEntry
	if tok.desc != nil {
		e.desc = *tok.desc
	}
	if tok.lease != nil {
		e.lease = *tok.lease
	}
	e.closedts = tok.closedts
	return e, nil
}

// GetCachedOverlapping returns all the cached entries which overlap a given
// span [Key, EndKey). The results are sorted ascendingly.
func (rc *RangeCache) GetCachedOverlapping(ctx context.Context, span roachpb.RSpan) []*CacheEntry {
	rc.rangeCache.RLock()
	defer rc.rangeCache.RUnlock()
	rawEntries := rc.getCachedOverlappingRLocked(ctx, span)
	entries := make([]*CacheEntry, len(rawEntries))
	for i, e := range rawEntries {
		entries[i] = rc.getValue(e)
	}
	return entries
}

func (rc *RangeCache) getCachedOverlappingRLocked(
	ctx context.Context, span roachpb.RSpan,
) []*cache.Entry {
	from := newRangeCacheKey(span.EndKey)
	defer from.release()
	var res []*cache.Entry
	rc.rangeCache.cache.DoRangeReverseEntry(func(e *cache.Entry) (exit bool) {
		desc := rc.getValue(e).Desc()
		if desc.StartKey.Equal(span.EndKey) {
			// Skip over descriptor starting at the end key, who'd supposed to be exclusive.
			return false
		}
		// Stop when we get to a lower range.
		if desc.EndKey.Compare(span.Key) <= 0 {
			return true
		}
		res = append(res, e)
		return false // continue iterating
	}, from, minCacheKey)
	// Invert the results so the get sorted ascendingly.
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

// lookupInternal is called from Lookup or from tests.
//
// If a WaitGroup is supplied, it is signaled when the request is
// added to the inflight request map (with or without merging) or the
// function finishes. Used for testing.
func (rc *RangeCache) lookupInternal(
	ctx context.Context, key roachpb.RKey, evictToken EvictionToken, useReverseScan bool,
) (EvictionToken, error) {
	// Retry while we're hitting lookupCoalescingErrors.
	for {
		newToken, err := rc.tryLookup(ctx, key, evictToken, useReverseScan)
		if errors.HasType(err, lookupCoalescingError{}) {
			log.VEventf(ctx, 2, "bad lookup coalescing; retrying: %s", err)
			continue
		}
		if err != nil {
			return EvictionToken{}, err
		}
		return newToken, nil
	}
}

// lookupCoalescingError is returned by tryLookup() when the
// descriptor database lookup failed because this request was grouped with
// another request for another key, and the grouping proved bad since that other
// request returned a descriptor that doesn't cover our request. The lookup
// should be retried.
type lookupCoalescingError struct {
	// key is the key whose range was being looked-up.
	key       roachpb.RKey
	wrongDesc *roachpb.RangeDescriptor
}

func (e lookupCoalescingError) Error() string {
	return fmt.Sprintf("key %q not contained in range lookup's "+
		"resulting descriptor %v", e.key, e.wrongDesc)
}

func newLookupCoalescingError(key roachpb.RKey, wrongDesc *roachpb.RangeDescriptor) error {
	return lookupCoalescingError{
		key:       key,
		wrongDesc: wrongDesc,
	}
}

// tryLookup can return a lookupCoalescingError.
func (rc *RangeCache) tryLookup(
	ctx context.Context, key roachpb.RKey, evictToken EvictionToken, useReverseScan bool,
) (EvictionToken, error) {
	rc.rangeCache.RLock()
	if entry, _ := rc.getCachedRLocked(ctx, key, useReverseScan); entry != nil {
		rc.rangeCache.RUnlock()
		returnToken := rc.makeEvictionToken(entry, nil /* nextDesc */)
		return returnToken, nil
	}

	log.VEventf(ctx, 2, "looking up range descriptor: key=%s", key)

	var prevDesc *roachpb.RangeDescriptor
	if evictToken.Valid() {
		prevDesc = evictToken.Desc()
	}
	requestKey := makeLookupRequestKey(key, prevDesc, useReverseScan)

	// lookupResult wraps the EvictionToken to report to the callers the
	// consistency level ultimately used for this lookup.
	type lookupResult struct {
		EvictionToken
		consistency RangeLookupConsistency
	}

	// lookupResultIsStale is used to determine if the result of a lookup which
	// was performed on a follower is known to be stale given the current
	// eviction token. In many cases, there is no previous descriptor. The case
	// of interest is when we're doing a lookup
	lookupResultIsStale := func(lookupResult) bool { return false }
	if evictToken.Valid() {
		// Enforce that the causality token actually applies to the key we're
		// looking up.
		if (useReverseScan && !evictToken.desc.ContainsKeyInverted(key)) ||
			(!useReverseScan && !evictToken.desc.ContainsKey(key)) {
			return EvictionToken{}, errors.AssertionFailedf(
				"invalid eviction token for lookup %v (reverse=%v) does not contain %v",
				evictToken.desc.RSpan(), useReverseScan, key,
			)
		}
		lookupResultIsStale = func(res lookupResult) bool {
			return res.Desc().Generation <= evictToken.desc.Generation
		}
	}

	future, leader := rc.lookupRequests.DoChan(ctx,
		requestKey,
		singleflight.DoOpts{
			Stop:               rc.stopper,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			var lookupRes lookupResult
			// Attempt to perform the lookup by reading from a follower. If the
			// result is too old for the leader of this group, then we'll fall back
			// to reading from the leaseholder. Note that it's possible that the
			// leader will have no freshness constraints on its view of the range
			// addressing but one of the shared members of the group will see the
			// result as too old. That case will be detected below and will result
			// in that goroutine re-fetching.
			lookupRes.consistency = ReadFromFollower
			{
				var err error
				lookupRes.EvictionToken, err = tryLookupImpl(ctx, rc, key, lookupRes.consistency, useReverseScan)
				if err != nil && !errors.Is(err, errFailedToFindNewerDescriptor) {
					return nil, err
				}
				gotFreshResult := err == nil && !lookupResultIsStale(lookupRes)
				if gotFreshResult {
					return lookupRes, nil
				}
			}
			var err error
			lookupRes.consistency = ReadFromLeaseholder
			lookupRes.EvictionToken, err = tryLookupImpl(ctx, rc, key, lookupRes.consistency, useReverseScan)
			if err != nil {
				return nil, err
			}
			return lookupRes, nil
		})

	// We must use DoChan above so that we can always unlock this mutex. This must
	// be done *after* the request has been added to the lookupRequests group, or
	// we risk it racing with an inflight request.
	rc.rangeCache.RUnlock()

	if !leader {
		log.VEvent(ctx, 2, "coalesced range lookup request onto in-flight one")
		if rc.coalesced != nil {
			rc.coalesced <- struct{}{}
		}
	}

	// Wait for the inflight request.
	res := future.WaitForResult(ctx)
	var s string
	if res.Err != nil {
		s = res.Err.Error()
	} else {
		s = res.Val.(lookupResult).String()
	}
	if res.Shared {
		log.VEventf(ctx, 2, "looked up range descriptor with shared request: %s", s)
	} else {
		log.VEventf(ctx, 2, "looked up range descriptor: %s", s)
	}
	if res.Err != nil {
		return EvictionToken{}, res.Err
	}

	// We might get a descriptor that doesn't contain the key we're looking for
	// because of bad grouping of requests. For example, say we had a stale
	// [a-z) in the cache who's info is passed into this function as evictToken.
	// In the meantime the range has been split to [a-m),[m-z). A request for "a"
	// will be coalesced with a request for "m" in the singleflight, above, but
	// one of them will get a wrong results. We return an error that will trigger
	// a retry at a higher level inside the cache. Note that the retry might find
	// the descriptor it's looking for in the cache if it was pre-fetched by the
	// original lookup.
	lookupRes := res.Val.(lookupResult)
	desc := lookupRes.Desc()
	containsFn := (*roachpb.RangeDescriptor).ContainsKey
	if useReverseScan {
		containsFn = (*roachpb.RangeDescriptor).ContainsKeyInverted
	}
	if !containsFn(desc, key) ||
		// The result may be stale relative to the requirements implied by the
		// eviction token, but our goroutine joined a single-flight which was
		// content to accept a result we know was stale. If the read was
		// performed against a follower and is stale, then we should try again.
		(lookupRes.consistency == ReadFromFollower && lookupResultIsStale(lookupRes)) {

		// The singleflight leader should never get this error: the
		// kv.RangeLookup code will ensure that the returned descriptor contains
		// the key that this goroutine was looking for. The lookup logic in the
		// singleflight should ensure that if we get back a stale response, it's
		// only after going to the leaseholder.
		if !res.Shared {
			return EvictionToken{}, errors.AssertionFailedf(
				"singleflight leader in range lookup received a stale response",
			)
		}

		return EvictionToken{}, newLookupCoalescingError(key, desc)
	}
	return lookupRes.EvictionToken, nil
}

var errFailedToFindNewerDescriptor = errors.New("failed to find descriptor")

// tryLookupImpl is the implementation of one attempt of rc.tryLookupImpl at a
// specified consistency. Note that if the consistency is ReadFromFollower,
// this call may return errFailedToFindNewerDescriptor, which the caller
// should handle by performing a fresh lookup at ReadFromLeaseholder. If
// the consistency is ReadFromLeaseholder, that error will not be returned.
func tryLookupImpl(
	ctx context.Context,
	rc *RangeCache,
	key roachpb.RKey,
	consistency RangeLookupConsistency,
	useReverseScan bool,
) (lookupRes EvictionToken, _ error) {
	// Since we don't inherit any other cancelation, let's put in a generous
	// timeout as some protection against unavailable meta ranges.
	var rs, preRs []roachpb.RangeDescriptor
	if err := timeutil.RunWithTimeout(ctx, "range lookup", 10*time.Second,
		func(ctx context.Context) error {
			var err error
			rs, preRs, err = rc.performRangeLookup(ctx, key, consistency, useReverseScan)
			return err
		}); err != nil {
		return EvictionToken{}, err
	}

	switch {
	case len(rs) == 0 && consistency == ReadFromFollower:
		// If we don't find any matching descriptors, but we read from a follower,
		// return the sentinel error to retry on the leaseholder.
		return EvictionToken{}, errFailedToFindNewerDescriptor
	case len(rs) == 0:
		// The lookup code, when routed to a leaseholder, ought to retry
		// internally, so this case is not expected.
		return EvictionToken{}, errors.AssertionFailedf(
			"no range descriptors returned for %s", key,
		)
	case len(rs) > 2:
		// Only one intent is allowed to exist on a key at a time. The results
		// should, at most, be one committed value and one intent. Anything else
		// is unexpected.
		return EvictionToken{}, errors.AssertionFailedf(
			"more than 2 matching range descriptors returned for %s: %v", key, rs,
		)
	}

	// We want to be assured that all goroutines which experienced a cache miss
	// have joined our in-flight request, and all others will experience a
	// cache hit. This requires atomicity across cache population and
	// notification, hence this exclusive lock.
	rc.rangeCache.Lock()
	defer rc.rangeCache.Unlock()

	// Insert the descriptor and the prefetched ones. We don't insert rs[1]
	// (if any), since it overlaps with rs[0]; rs[1] will be handled by
	// rs[0]'s eviction token. Note that ranges for which the cache has more
	// up-to-date information will not be clobbered - for example ranges for
	// which the cache has the prefetched descriptor already plus a lease.
	newEntries := make([]*CacheEntry, len(preRs)+1)
	newEntries[0] = &CacheEntry{
		desc: rs[0],
		// We don't have any lease information.
		lease: roachpb.Lease{},
		// We don't know the closed timestamp policy.
		closedts: UnknownClosedTimestampPolicy,
	}
	for i, preR := range preRs {
		newEntries[i+1] = &CacheEntry{desc: preR, closedts: UnknownClosedTimestampPolicy}
	}
	insertedEntries := rc.insertLockedInner(ctx, newEntries)
	// entry corresponds to rs[0], which is the descriptor covering the key
	// we're interested in.
	entry := insertedEntries[0]
	// There's 4 cases here:
	//
	//  1. We succeeded in inserting rs[0].
	//  2. We didn't succeed in inserting rs[0], but insertedEntries[0] still
	//     was non-nil. This means that the cache had a newer version of the
	//     descriptor. In that case it's all good, we just pretend that
	//     that's the version we were inserting; we put it in our token and
	//     continue.
	//  3. insertedEntries[0] is nil and we did a consistent read. The cache
	//     has newer entries in them and they're not compatible with the
	//     descriptor we were trying to insert. This case should be rare, since
	//     very recently (before starting the singleflight), the cache didn't
	//     have any entry for the requested key. We'll continue with the stale
	//     rs[0]; we'll pretend that we did by putting a dummy entry in the
	//     eviction token. This will make eviction no-ops (which makes sense -
	//     there'll be nothing to evict since we didn't insert anything).
	//  4. insertedEntries[0] is nil and we did a potentially stale read. The
	//     caller should retry by routing a request to the leaseholder for the
	//     meta range.
	//
	// TODO(andrei): It'd be better to retry the cache/database lookup in
	// case 3.
	if entry == nil {
		if consistency == ReadFromFollower {
			return EvictionToken{}, errFailedToFindNewerDescriptor
		}
		entry = &CacheEntry{
			desc:     rs[0],
			lease:    roachpb.Lease{},
			closedts: UnknownClosedTimestampPolicy,
		}
	}
	if len(rs) == 1 {
		lookupRes = rc.makeEvictionToken(entry, nil /* nextDesc */)
	} else {
		lookupRes = rc.makeEvictionToken(entry, &rs[1] /* nextDesc */)
	}
	return lookupRes, nil
}

// performRangeLookup handles delegating the range lookup to the cache's
// RangeDescriptorDB.
func (rc *RangeCache) performRangeLookup(
	ctx context.Context, key roachpb.RKey, consistency RangeLookupConsistency, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// Tag inner operations.
	ctx = logtags.AddTag(ctx, "range-lookup", key)

	return rc.db.RangeLookup(ctx, key, consistency, useReverseScan)
}

// Clear clears all RangeDescriptors from the RangeCache.
func (rc *RangeCache) Clear() {
	rc.rangeCache.Lock()
	defer rc.rangeCache.Unlock()
	rc.rangeCache.cache.Clear()
}

// EvictByKey evicts the descriptor containing the given key, if any.
//
// Returns true if a descriptor was evicted.
func (rc *RangeCache) EvictByKey(ctx context.Context, descKey roachpb.RKey) bool {
	rc.rangeCache.Lock()
	defer rc.rangeCache.Unlock()

	cachedDesc, entry := rc.getCachedRLocked(ctx, descKey, false /* inverted */)
	if cachedDesc == nil {
		return false
	}
	log.VEventf(ctx, 2, "evict cached descriptor: %s", cachedDesc)
	rc.delEntryLocked(entry)
	return true
}

// evictLocked evicts a cache entry unless its newer than the passed in
// descriptor and lease.
func (rc *RangeCache) evictLocked(
	ctx context.Context, desc *roachpb.RangeDescriptor, lease *roachpb.Lease,
) bool {
	cachedEntry, rawEntry := rc.getCachedRLocked(ctx, desc.StartKey, false /* inverted */)
	if cachedEntry == nil {
		// Cache is empty; nothing to do.
		return false
	}
	// The cached descriptor is newer, so we don't want to evict.
	if cachedEntry.desc.Generation > desc.Generation {
		return false
	}
	var etSequence roachpb.LeaseSequence
	if lease != nil {
		etSequence = lease.Sequence
	}
	// The cached lease is newer and the descriptors are the same, so don't evict.
	if cachedEntry.lease.Sequence > etSequence {
		// if the cached entry has the same descriptor but the cached lease is newer, don't evict.
		return false
	}

	// The cache has a descriptor that's older or equal to desc (it should be
	// equal because the desc that the caller supplied also came from the cache
	// and the cache is not expected to go backwards). Evict it.
	log.VEventf(ctx, 2, "evict cached descriptor: desc=%s", cachedEntry)
	rc.delEntryLocked(rawEntry)
	return true
}

// GetCached retrieves the descriptor of the range which contains
// the given key. It returns nil if the descriptor is not found in the cache.
//
// `inverted` determines the behavior at the range boundary: If set to true
// and `key` is the EndKey and StartKey of two adjacent ranges, the first range
// is returned instead of the second (which technically contains the given key).
func (rc *RangeCache) GetCached(ctx context.Context, key roachpb.RKey, inverted bool) *CacheEntry {
	rc.rangeCache.RLock()
	defer rc.rangeCache.RUnlock()
	entry, _ := rc.getCachedRLocked(ctx, key, inverted)
	return entry
}

// getCachedRLocked is like GetCached, but it assumes that the caller holds a
// read lock on rdc.rangeCache.
//
// In addition to GetCached, it also returns an internal cache Entry that can be
// used for descriptor eviction.
func (rc *RangeCache) getCachedRLocked(
	ctx context.Context, key roachpb.RKey, inverted bool,
) (*CacheEntry, *cache.Entry) {
	// rawEntry will be the range containing key, or the first cached entry around
	// key, in the direction indicated by inverted.
	var rawEntry *cache.Entry
	if !inverted {
		k := newRangeCacheKey(key)
		defer k.release()
		var ok bool
		rawEntry, ok = rc.rangeCache.cache.FloorEntry(k)
		if !ok {
			return nil, nil
		}
	} else {
		from := newRangeCacheKey(key)
		defer from.release()
		rc.rangeCache.cache.DoRangeReverseEntry(func(e *cache.Entry) bool {
			startKey := roachpb.RKey(*e.Key.(*rangeCacheKey))
			if key.Equal(startKey) {
				// DoRangeReverseEntry is inclusive on the higher key. We're iterating
				// backwards and we got a range that starts at key. We're not interested
				// in this range; we're interested in the range before it that ends at
				// key.
				return false // continue iterating
			}
			rawEntry = e
			return true
		}, from, minCacheKey)
		// DoRangeReverseEntry is exclusive on the "to" part, so we need to check
		// manually if there's an entry for RKeyMin.
		if rawEntry == nil {
			rawEntry, _ = rc.rangeCache.cache.FloorEntry(minCacheKey)
		}
	}

	if rawEntry == nil {
		return nil, nil
	}
	entry := rc.getValue(rawEntry)

	containsFn := (*roachpb.RangeDescriptor).ContainsKey
	if inverted {
		containsFn = (*roachpb.RangeDescriptor).ContainsKeyInverted
	}

	// Return nil if the key does not belong to the range.
	if !containsFn(entry.Desc(), key) {
		return nil, nil
	}
	return entry, rawEntry
}

// Insert inserts range info into the cache.
//
// This is a no-op for the ranges that already have the same, or newer, info in
// the cache.
func (rc *RangeCache) Insert(ctx context.Context, rs ...roachpb.RangeInfo) {
	rc.rangeCache.Lock()
	defer rc.rangeCache.Unlock()
	rc.insertLocked(ctx, rs...)
}

// insertLocked is like Insert, but it assumes that the caller holds a write
// lock on rdc.rangeCache. It also returns the inserted cache values, suitable
// for putting in eviction tokens. Any element in the returned array can be nil
// if inserting the respective RangeInfo failed because it was found to be
// stale.
func (rc *RangeCache) insertLocked(ctx context.Context, rs ...roachpb.RangeInfo) []*CacheEntry {
	entries := make([]*CacheEntry, len(rs))
	for i, r := range rs {
		entries[i] = &CacheEntry{
			desc:     r.Desc,
			lease:    r.Lease,
			closedts: r.ClosedTimestampPolicy,
		}
	}
	return rc.insertLockedInner(ctx, entries)
}

func (rc *RangeCache) insertLockedInner(ctx context.Context, rs []*CacheEntry) []*CacheEntry {
	// entries will have the same element as rs, except the ones that couldn't be
	// inserted for which the slots will remain nil.
	entries := make([]*CacheEntry, len(rs))
	for i, ent := range rs {
		if !ent.desc.IsInitialized() {
			log.Fatalf(ctx, "inserting uninitialized desc: %s", ent)
		}
		if !ent.lease.Empty() {
			replID := ent.lease.Replica.ReplicaID
			_, ok := ent.desc.GetReplicaDescriptorByID(replID)
			if !ok {
				log.Fatalf(ctx, "leaseholder replicaID: %d not part of descriptor: %s. lease: %s",
					replID, ent.Desc(), ent.Lease())
			}
		}
		// Note: we append the end key of each range to meta records
		// so that calls to rdc.rangeCache.cache.Ceil() for a key will return
		// the correct range.

		// Before adding a new entry, make sure we clear out any
		// pre-existing, overlapping entries which might have been
		// re-inserted due to concurrent range lookups.
		ok, newerEntry := rc.clearOlderOverlappingLocked(ctx, ent)
		if !ok {
			// The descriptor we tried to insert is already in the cache, or is stale.
			// We might have gotten a newer cache entry, if the descriptor in the
			// cache is similar enough. If that's the case, we'll use it.
			// NB: entries[i] stays nil if newerEntry is nil.
			entries[i] = newerEntry
			continue
		}
		if log.V(2) {
			log.Infof(ctx, "adding cache entry: value=%s", ent)
		}
		rc.addEntryLocked(ent)
		entries[i] = ent
	}
	return entries
}

func (rc *RangeCache) getValue(entry *cache.Entry) *CacheEntry {
	return entry.Value.(*CacheEntry)
}

func (rc *RangeCache) clearOlderOverlapping(
	ctx context.Context, newEntry *CacheEntry,
) (ok bool, newerEntry *CacheEntry) {
	rc.rangeCache.Lock()
	defer rc.rangeCache.Unlock()
	return rc.clearOlderOverlappingLocked(ctx, newEntry)
}

// clearOlderOverlappingLocked clears any stale cache entries which overlap the
// specified descriptor. Returns true if the clearing succeeds, and false if any
// overlapping newer descriptor is found (or if the descriptor we're trying to
// insert is already in the cache). If false is returned, a cache entry might
// also be returned - a cache entry that's similar (same descriptor range id and
// key span) to the one we were trying to insert, but newer (or identical). Such
// an entry might not exist, in which case false, nil will be returned.
//
// Note that even if false is returned, older descriptors are still cleared from
// the cache.
func (rc *RangeCache) clearOlderOverlappingLocked(
	ctx context.Context, newEntry *CacheEntry,
) (ok bool, newerEntry *CacheEntry) {
	log.VEventf(ctx, 2, "clearing entries overlapping %s", newEntry.Desc())
	newest := true
	var newerFound *CacheEntry
	overlapping := rc.getCachedOverlappingRLocked(ctx, newEntry.Desc().RSpan())
	for _, e := range overlapping {
		entry := rc.getValue(e)
		if newEntry.overrides(entry) {
			if log.V(2) {
				log.Infof(ctx, "clearing overlapping descriptor: key=%s entry=%s", e.Key, rc.getValue(e))
			}
			rc.delEntryLocked(e)
		} else {
			newest = false
			if descsCompatible(entry.Desc(), newEntry.Desc()) {
				newerFound = entry
				// We've found a similar descriptor in the cache; there can't be any
				// other overlapping ones so let's stop the iteration.
				if len(overlapping) != 1 {
					log.Errorf(ctx, "%s", errors.AssertionFailedf(
						"found compatible descriptor but also got multiple overlapping results. newEntry: %s. overlapping: %s",
						newEntry, overlapping).Error())
				}
			}
		}
	}
	return newest, newerFound
}

// swapEntryLocked swaps oldEntry for newEntry. If newEntry is nil, oldEntry is
// simply removed.
func (rc *RangeCache) swapEntryLocked(
	ctx context.Context, oldEntry *cache.Entry, newEntry *CacheEntry,
) {
	if newEntry != nil {
		old := rc.getValue(oldEntry)
		if !descsCompatible(old.Desc(), newEntry.Desc()) {
			log.Fatalf(ctx, "attempting to swap non-compatible descs: %s vs %s",
				old, newEntry)
		}
	}

	rc.delEntryLocked(oldEntry)
	if newEntry != nil {
		log.VEventf(ctx, 2, "caching new entry: %s", newEntry)
		rc.addEntryLocked(newEntry)
	}
}

func (rc *RangeCache) addEntryLocked(entry *CacheEntry) {
	key := newRangeCacheKey(entry.Desc().StartKey)
	rc.rangeCache.cache.Add(key, entry)
}

func (rc *RangeCache) delEntryLocked(entry *cache.Entry) {
	rc.rangeCache.cache.DelEntry(entry)
	entry.Key.(*rangeCacheKey).release()
}

// DB returns the descriptor database, for tests.
func (rc *RangeCache) DB() RangeDescriptorDB {
	return rc.db
}

// TestingSetDB allows tests to override the database.
func (rc *RangeCache) TestingSetDB(db RangeDescriptorDB) {
	rc.db = db
}

// NumInFlight allows tests to check the number of in-flight calls under the
// given name.
func (rc *RangeCache) NumInFlight(name string) int {
	return rc.lookupRequests.NumCalls(name)
}

// CacheEntry represents one cache entry.
//
// The cache stores *CacheEntry. Entries are immutable: cache lookups
// returns the same *CacheEntry to multiple queriers for efficiency, but
// nobody should modify the lookup result.
type CacheEntry struct {
	// desc is always populated.
	desc roachpb.RangeDescriptor
	// Lease has info on the range's lease. It can be Empty() if no lease
	// information is known. When a lease is known, it is guaranteed that the
	// lease comes from Desc's range id (i.e. we'll never put a lease from another
	// range in here). This allows SyncTokenAndMaybeUpdateCache() to use
	// Lease.Sequence to compare leases. Moreover, the lease will correspond to
	// one of the replicas in Desc.
	lease roachpb.Lease
	// closedts indicates the range's closed timestamp policy.
	closedts roachpb.RangeClosedTimestampPolicy
}

func (e CacheEntry) String() string {
	return fmt.Sprintf("desc:%s, lease:%s", e.Desc(), e.lease)
}

// Desc returns the cached descriptor. Note that, besides being possibly stale,
// this descriptor also might not represent a descriptor that was ever
// committed. See DescSpeculative().
func (e *CacheEntry) Desc() *roachpb.RangeDescriptor {
	return &e.desc
}

// Leaseholder returns the cached leaseholder replica, if known. Returns nil if
// the leaseholder is not known.
func (e *CacheEntry) Leaseholder() *roachpb.ReplicaDescriptor {
	if e.lease.Empty() {
		return nil
	}
	return &e.lease.Replica
}

// Lease returns the cached lease, if known. Returns nil if no lease is known.
// It's possible for a leaseholder to be known, but not a full lease, in which
// case Leaseholder() returns non-nil but Lease() returns nil.
func (e *CacheEntry) Lease() *roachpb.Lease {
	if e.lease.Empty() {
		return nil
	}
	if e.LeaseSpeculative() {
		return nil
	}
	return &e.lease
}

// leaseEvenIfSpeculative is like Lease, except it returns a Lease object even
// if that lease is speculative. Returns nil if no speculative or non-speculative
// lease is known.
func (e *CacheEntry) leaseEvenIfSpeculative() *roachpb.Lease {
	if e.lease.Empty() {
		return nil
	}
	return &e.lease
}

// ClosedTimestampPolicy returns the cached understanding of the range's closed
// timestamp policy. If no policy is known, LAG_BY_CLUSTER_SETTING is returned.
func (e *CacheEntry) ClosedTimestampPolicy() roachpb.RangeClosedTimestampPolicy {
	return e.closedts
}

// DescSpeculative returns true if the descriptor in the entry is "speculative"
// - i.e. it doesn't correspond to a committed value. Such descriptors have been
// inserted in the cache with Generation=0.
//
// Speculative descriptors come from (not-yet-committed) intents.
func (e *CacheEntry) DescSpeculative() bool {
	return e.desc.Generation == 0
}

// LeaseSpeculative returns true if the lease in the entry is "speculative"
// - i.e. it doesn't correspond to a committed lease. Such leases have been
// inserted in the cache with Sequence=0.
func (e *CacheEntry) LeaseSpeculative() bool {
	if e.lease.Empty() {
		panic(fmt.Sprintf("LeaseSpeculative called on entry with empty lease: %s", e))
	}
	return e.lease.Speculative()
}

// overrides returns true if o should replace e in the cache. It is assumed that
// e's and o'd descriptors overlap (and so they can't co-exist in the cache). A
// newer entry overrides an older entry. What entry is newer is decided based
// the descriptor's generation and, for equal generations, by the lease's
// sequence and, for equal lease sequences, by the closed timestamp policy.
//
// In situations where it can't be determined which entry represents newer
// information, e wins - the assumption is that o is already in the cache and we
// have some reason to believe e should get in the cache instead (generally
// because a server gave us this information recently). Situations where it
// can't be determined what information is newer is when at least one of the
// descriptors is "speculative" (generation=0), or when the lease information is
// "speculative" (sequence=0).
func (e *CacheEntry) overrides(o *CacheEntry) bool {
	if util.RaceEnabled {
		if _, err := e.Desc().RSpan().Intersect(o.Desc().RSpan()); err != nil {
			panic(fmt.Sprintf("descriptors don't intersect: %s vs %s", e.Desc(), o.Desc()))
		}
	}

	if res := compareEntryDescs(o, e); res != 0 {
		return res < 0
	}

	// Equal descriptor generations. Let's look at the lease sequence.

	// If two RangeDescriptors overlap and have the same Generation, they must
	// be referencing the same range, in which case their lease sequences are
	// comparable.
	if e.Desc().RangeID != o.Desc().RangeID {
		panic(fmt.Sprintf("overlapping descriptors with same gen but different IDs: %s vs %s",
			e.Desc(), o.Desc()))
	}

	if res := compareEntryLeases(o, e); res != 0 {
		return res < 0
	}

	// Equal lease sequences. Let's look at the closed timestamp policy. We
	// don't assign sequence numbers to closed timestamp policy changes, so in
	// cases where the two policies differ, we can't tell which one is old and
	// which is new. So we conservatively say that if the policy changes, we
	// will replace e with o. Closed timestamp policy changes are very rare, so
	// minor thrashing in the cache is ok, as long as it eventually converges
	// and we are left with a correct understanding of the policy.
	return o.closedts != e.closedts
}

// compareEntryDescs returns -1, 0 or 1 depending on whether a's descriptor is
// considered older, equal to, or newer than b's.
//
// In case at least one of the descriptors is "speculative", a is considered
// older; this matches the semantics of b.overrides(a).
func compareEntryDescs(a, b *CacheEntry) int {
	if util.RaceEnabled {
		if _, err := a.Desc().RSpan().Intersect(b.Desc().RSpan()); err != nil {
			panic(fmt.Sprintf("descriptors don't intersect: %s vs %s", a.Desc(), b.Desc()))
		}
	}

	if a.desc.Equal(&b.desc) {
		return 0
	}

	if a.DescSpeculative() || b.DescSpeculative() {
		return -1
	}

	if a.Desc().Generation < b.Desc().Generation {
		return -1
	}
	if a.Desc().Generation > b.Desc().Generation {
		return 1
	}
	return 0
}

// compareEntryLeases returns -1, 0 or 1 depending on whether a's lease is
// considered older, equal to, or newer than b's. The descriptors in a and b are
// assumed to be the same.
//
// An empty lease is considered older than any other. In case at least one of
// the leases is "speculative", a is considered older; this matches the
// semantics of b.overrides(a).
func compareEntryLeases(a, b *CacheEntry) int {
	if aEmpty, bEmpty := a.lease.Empty(), b.lease.Empty(); aEmpty || bEmpty {
		if aEmpty && !bEmpty {
			return -1
		}
		if !aEmpty && bEmpty {
			return 1
		}
		return 0
	}

	// A speculative lease always loses; we don't know the sequence number of a
	// speculative lease.
	if a.LeaseSpeculative() || b.LeaseSpeculative() {
		return -1
	}

	if a.Lease().Sequence < b.Lease().Sequence {
		return -1
	}
	if a.Lease().Sequence > b.Lease().Sequence {
		return 1
	}
	return 0
}

// maybeUpdate returns a new CacheEntry which contains the freshest lease/range
// descriptor by comparing the receiver's fields to the passed-in parameters.
//
// The updated retval indicates if either the passed-in lease or the range
// descriptor or both appear to be newer than what the entry had before. If
// updated is returned true, the caller should evict the existing entry
// (the receiver) and replace it with the newEntry.
//
// If updated=false is returned, then newEntry will be the same as the receiver
// This means that both the passed-in lease and the range descriptor are the
// same or older than the lease and range descriptor already in the entry.
//
// The updatedLease retval indicates whether the passed-in lease appears to be
// newer than the lease the entry had before. If updatedLease is returned true,
// updated will be true as well, so the caller should perform the same eviction
// and replacement described above.
//
// If the freshest combination of lease/range descriptor pair is incompatible
// (i.e the range descriptor doesn't contain the leaseholder replica), the
// returned cache entry will have an empty lease field.
//
// It's expected that the supplied rangeDesc is compatible with the descriptor
// on the cache entry.
func (e *CacheEntry) maybeUpdate(
	ctx context.Context, l *roachpb.Lease, rangeDesc *roachpb.RangeDescriptor,
) (updated, updatedLease bool, newEntry *CacheEntry) {
	if !descsCompatible(e.Desc(), rangeDesc) {
		log.Fatalf(ctx, "attempting to update by comparing non-compatible descs: %s vs %s",
			e.Desc(), rangeDesc)
	}

	newEntry = &CacheEntry{
		lease:    e.lease,
		desc:     e.desc,
		closedts: e.closedts,
	}

	updatedLease = false
	updatedDesc := false

	// First, we handle the lease. If l is older than what the entry has (or the
	// same), there's nothing to update.
	//
	// This method handles speculative leases: a new lease with a sequence of 0 is
	// presumed to be newer than anything if the replica it's coming from doesn't
	// have an outdated view of the world (i.e does not have an older range
	// descriptor than the one cached on the client); an existing lease with a
	// sequence number of 0 is presumed to be older than anything and will be
	// replaced by the supplied lease if the associated range descriptor is
	// non-stale.
	//
	// Lastly, if the cached lease is empty, it will be updated with the supplied
	// lease regardless of the associated range descriptor's generation.
	if (l.Sequence != 0 && e.lease.Sequence != 0 && l.Sequence > e.lease.Sequence) ||
		((l.Sequence == 0 || e.lease.Sequence == 0) && rangeDesc.Generation >= e.desc.Generation) ||
		e.lease.Empty() {
		newEntry.lease = *l
		updatedLease = true
	}

	// We only want to use the supplied rangeDesc if its generation indicates it's
	// strictly newer than what the was in the cache entry.
	if e.desc.Generation < rangeDesc.Generation {
		newEntry.desc = *rangeDesc
		updatedDesc = true
	}

	// Lastly, we want to check if the leaseholder is indeed a replica
	// (in some form) on the descriptor. The range cache doesn't like when this
	// isn't the case and doesn't allow us to insert an incompatible
	// leaseholder/range descriptor pair. Given we only ever insert incrementally
	// fresher range descriptors in the cache, we choose to empty out the lease
	// field and insert the range descriptor. Going through each of the replicas
	// on the range descriptor further up the stack should provide us with a
	// compatible and actionable state of the world.
	//
	// TODO(arul): While having this safeguard, logging, and not fatal-ing further
	// down seems like the right thing to do, do we expect this to happen in the
	// wild?
	_, ok := newEntry.desc.GetReplicaDescriptorByID(newEntry.lease.Replica.ReplicaID)
	if !ok {
		log.VEventf(
			ctx,
			2,
			"incompatible leaseholder id: %d/descriptor %v pair; eliding lease update to the cache",
			newEntry.lease.Replica.ReplicaID,
			newEntry.Desc(),
		)
		newEntry.lease = roachpb.Lease{}
		updatedLease = false
	}

	return updatedLease || updatedDesc, updatedLease, newEntry
}

func (e *CacheEntry) evictLeaseholder(
	lh roachpb.ReplicaDescriptor,
) (updated bool, newEntry *CacheEntry) {
	if e.lease.Replica != lh {
		return false, e
	}
	return true, &CacheEntry{
		desc:     e.desc,
		closedts: e.closedts,
	}
}

// IsRangeLookupErrorRetryable returns whether the provided range lookup error
// can be retried or whether it should be propagated immediately.
func IsRangeLookupErrorRetryable(err error) bool {
	// Auth errors are not retryable. These imply that the local node has been
	// decommissioned or is otherwise not part of the cluster.
	return !grpcutil.IsAuthError(err)
}
