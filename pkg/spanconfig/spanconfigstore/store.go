// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"bytes"
	"context"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Store is an in-memory data structure to store and retrieve span configs.
// Internally it makes use of a left-leaning red-black tree to store
// non-overlapping span configs, and ensures that adjacent spans with the same
// config are coalesced.
//
// TODO(zcfgs-pod): Switch over to using util/interval.Tree instead, which does
// most of what we want. We'd still have to over-write existing spans, and
// coalesce adjacent ones, but seems more appropriate than the OrderedCache.
type Store struct {
	mu struct {
		syncutil.RWMutex
		cache *cache.OrderedCache
	}
}

// NeedsSplit is part of the spanconfig.QueueReader interface.
func (s *Store) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	res := s.ComputeSplitKey(ctx, start, end) != nil
	return res
}

// ComputeSplitKey is part of the spanconfig.QueueReader interface.
func (s *Store) ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey {
	sp := roachpb.Span{Key: start.AsRawKey(), EndKey: end.AsRawKey()}
	cfgs := s.GetConfigsForSpan(sp)
	if len(cfgs) == 0 || len(cfgs) == 1 {
		return nil
	}

	return roachpb.RKey(cfgs[1].Span.Key)
}

// GetSpanConfigForKey is part of the spanconfig.QueueReader interface.
func (s *Store) GetSpanConfigForKey(key roachpb.RKey) (roachpb.SpanConfig, error) {
	sp := roachpb.Span{Key: key.AsRawKey(), EndKey: key.Next().AsRawKey()}
	cfgs := s.GetConfigsForSpan(sp)
	if len(cfgs) == 0 {
		// return zonepb.DefaultZoneConfigRef().AsSpanConfig(), nil // XXX: should return default? We should be expecting a fully covering datastructure.
		return roachpb.SpanConfig{}, errors.New("span config not found")
	}
	return cfgs[0].Config, nil
}

var _ spanconfig.Store = &Store{}
var _ spanconfig.QueueReader = &Store{}

// New instantiates a span config store.
func New() *Store {
	s := &Store{}
	s.mu.cache = cache.NewOrderedCache(cache.Config{
		Policy: cache.CacheNone,
	})
	return s
}

// storeKey is the key type used to store and sort values in the span config
// storage.
type storeKey roachpb.Key

var _ llrb.Comparable = storeKey{}

var minCacheKey interface{} = storeKey(roachpb.KeyMin)
var maxCacheKey interface{} = storeKey(roachpb.KeyMax)

func (a storeKey) String() string {
	return roachpb.Key(a).String()
}

// Compare implements the llrb.Comparable interface for storeKey, so that
// it can be used as a key for util.OrderedCache.
func (a storeKey) Compare(b llrb.Comparable) int {
	return bytes.Compare(a, b.(storeKey))
}

// SetSpanConfig is part of the spanconfig.Store interface.
func (s *Store) SetSpanConfig(sp roachpb.Span, conf roachpb.SpanConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !sp.Valid() {
		panic("invalid span")
	}

	// Clear out all existing span configs overlapping with our own.
	s.clearLocked(sp)

	if conf.IsEmpty() {
		// Nothing to do; empty span configs are delete operations.
		return
	}

	// Check to see if the adjacent span configs can be coalesced into.
	startKey, endKey := sp.Key, sp.EndKey

	next, found := s.getCeilEntryLocked(sp.EndKey) // check the span config to our right
	if found && next.Span.Key.Equal(sp.EndKey) && next.Config.Equal(conf) {
		s.clearLocked(next.Span)
		endKey = next.Span.EndKey
	}

	prev, found := s.getFloorEntryLocked(sp.Key) // check the span config to our left
	if found && prev.Span.EndKey.Equal(sp.Key) && prev.Config.Equal(conf) {
		s.clearLocked(prev.Span)
		startKey = prev.Span.Key
	}

	// Finally, write the span config entry.
	s.mu.cache.AddEntry(&cache.Entry{
		Key: storeKey(startKey),
		Value: roachpb.SpanConfigEntry{
			Span: roachpb.Span{
				Key:    startKey,
				EndKey: endKey,
			},
			Config: conf,
		},
	})
}

// GetConfigsForSpan is part of the spanconfig.Store interface.
func (s *Store) GetConfigsForSpan(sp roachpb.Span) []roachpb.SpanConfigEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !sp.Valid() {
		panic("invalid span")
	}

	// Iterate over all overlapping ranges, return corresponding span
	// config entries.
	var res []roachpb.SpanConfigEntry
	searchStartKey, searchEndKey := s.getSearchBoundsLocked(sp)
	s.mu.cache.DoRangeEntry(func(e *cache.Entry) bool {
		entry, _ := e.Value.(roachpb.SpanConfigEntry)
		if !entry.Span.Overlaps(sp) {
			// Skip non-overlapping entries.
			return false
		}

		res = append(res, roachpb.SpanConfigEntry{
			Span:   entry.Span,
			Config: entry.Config,
		})

		return false
	}, searchStartKey, searchEndKey)
	return res
}

// GetSplitsBetween is part of the spanconfig.Store interface.
func (s *Store) GetSplitsBetween(start, end roachpb.Key) []roachpb.Key {
	// TODO(zcfgs-pod): Need to make sure we populate static splits/span configs as startup
	// migration. All splits in KV done through the system config span needs to
	// be done here as well.
	panic("unimplemented")

}

// clearLocked is used to clear out a given span from the underlying storage. At
// a high-level, we'll find all overlapping spans and delete the intersections.
// We do this by deleting each overlapping span in its entirety and re-adding
// the non-overlapping components (if any). Pseudo-code:
//
// 	for each entry in storage.overlapping(sp):
// 		union, intersection = union(sp, entry), intersection(sp, entry)
// 		pre, post = span{union.start_key, intersection.start_key}, span{intersection.end_key, union.end_key}
//
// 		delete entry
// 		if entry.contains(sp.start_key):
// 			add pre=entry.conf
// 		if entry.contains(sp.end_key):
// 			add post=entry.conf
//
func (s *Store) clearLocked(sp roachpb.Span) {
	searchStartKey, searchEndKey := s.getSearchBoundsLocked(sp)
	var toDelete, toAdd []cache.Entry
	s.mu.cache.DoRangeEntry(func(e *cache.Entry) bool { // keyed by span start key, storing value span config entry
		entry, _ := e.Value.(roachpb.SpanConfigEntry)
		oldSpan, newSpan := entry.Span, sp
		if !oldSpan.Overlaps(newSpan) { // skip non-overlapping ranges
			return false
		}

		// A:		[----------------------)
		// B:	            [--------------------)
		//
		// union:	[----------------------------)
		// inter:	        [--------------)
		// pre: 	[-------)
		// post:	                       [-----)
		union, intersection := oldSpan.Combine(newSpan), oldSpan.Intersect(newSpan)
		pre := roachpb.Span{Key: union.Key, EndKey: intersection.Key}
		post := roachpb.Span{Key: intersection.EndKey, EndKey: union.EndKey}

		// Delete the overlapping span in its entirety. Below we'll re-add the
		// non-intersecting parts of the span.
		toDelete = append(toDelete, cache.Entry{
			Key:   storeKey(entry.Span.Key),
			Value: entry,
		})

		if entry.Span.ContainsKey(sp.Key) { // entry contains the given span's start key
			// entry:  [-----------------)
			//
			// sp:         [-------)
			// sp:         [-------------)
			// sp:         [--------------
			// sp:     [-------)
			// sp:     [-----------------)
			// sp:     [------------------

			// Re-add the non-intersecting spans, if any.
			if pre.Valid() {
				toAdd = append(toAdd, cache.Entry{
					Key:   storeKey(pre.Key),
					Value: roachpb.SpanConfigEntry{Span: pre, Config: entry.Config},
				})
			}
		}

		if entry.Span.ContainsKey(sp.EndKey) { // entry contains the given span's end key
			// entry:  [-----------------)
			//
			// sp:     ------------------)
			// sp:     [-----------------)
			// sp:               [-------)
			// sp:     -------------)
			// sp:     [------------)
			// sp:        [---------)

			// Re-add the non-intersecting spans, if any.
			if post.Valid() {
				toAdd = append(toAdd, cache.Entry{
					Key:   storeKey(post.Key),
					Value: roachpb.SpanConfigEntry{Span: post, Config: entry.Config},
				})
			}
		}

		return false
	}, searchStartKey, searchEndKey)

	// Execute the accumulated operations.
	for _, entry := range toDelete {
		entry := entry // copy out of loop variable
		s.mu.cache.DelEntry(&entry)
	}

	for _, entry := range toAdd {
		entry := entry // copy out of loop variable
		s.mu.cache.AddEntry(&entry)
	}
}

// getCeilEntryLocked returns the span config entry keyed using a key greater than or
// equal to the one provided.
func (s *Store) getCeilEntryLocked(k roachpb.Key) (_ roachpb.SpanConfigEntry, found bool) {
	var cacheEntry *cache.Entry
	cacheEntry, found = s.mu.cache.CeilEntry(storeKey(k))
	if !found {
		return roachpb.SpanConfigEntry{}, false
	}

	entry, _ := cacheEntry.Value.(roachpb.SpanConfigEntry)
	return entry, true
}

// getFloorEntryLocked returns the span config entry keyed using a key less than or
// equal to the one provided.
func (s *Store) getFloorEntryLocked(k roachpb.Key) (_ roachpb.SpanConfigEntry, found bool) {
	var cacheEntry *cache.Entry
	cacheEntry, found = s.mu.cache.FloorEntry(storeKey(k))
	if !found {
		return roachpb.SpanConfigEntry{}, false
	}

	entry, _ := cacheEntry.Value.(roachpb.SpanConfigEntry)
	return entry, true
}

// getSearchBoundsLocked constructs the search bounds to find all spans that possibly
// overlap with the given one. It's possible that spans retrieved using these
// search bounds don't overlap with the requested span. Since spans are keyed
// using their start key, it's possible the search is started at a span whose
// end key precedes our own.
func (s *Store) getSearchBoundsLocked(sp roachpb.Span) (searchStartKey, searchEndKey interface{}) {
	floorEntry, found := s.mu.cache.FloorEntry(storeKey(sp.Key))
	if found {
		searchStartKey = floorEntry.Key
	} else {
		searchStartKey = minCacheKey
	}

	ceilEntry, found := s.mu.cache.CeilEntry(storeKey(sp.EndKey))
	if found {
		searchEndKey = ceilEntry.Key
	} else {
		searchEndKey = maxCacheKey
	}

	return searchStartKey, searchEndKey
}
