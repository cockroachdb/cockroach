// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstorage

import (
	"bytes"
	"sync"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
)

type Storage struct {
	sync.RWMutex
	cache *cache.OrderedCache
}

var _ spanconfig.Storage = &Storage{}

// New instantiates a new spanconfig storage.
func New() *Storage {
	return &Storage{
		cache: cache.NewOrderedCache(cache.Config{
			Policy: cache.CacheNone,
		}),
	}
}

// storageKey is the key type used to store and sort values in the span config
// storage.
type storageKey roachpb.Key

var minCacheKey interface{} = storageKey(roachpb.KeyMin)
var maxCacheKey interface{} = storageKey(roachpb.KeyMax)

func (a storageKey) String() string {
	return roachpb.Key(a).String()
}

// Compare implements the llrb.Comparable interface for storageKey, so that
// it can be used as a key for util.OrderedCache.
func (a storageKey) Compare(b llrb.Comparable) int {
	return bytes.Compare(a, b.(storageKey))
}

// Set is part of the spanconfig.Storage interface.
func (s *Storage) Set(sp roachpb.Span, conf roachpb.SpanConfig) {
	s.Lock()
	defer s.Unlock()

	// Clear out all existing span configs overlapping with our own.
	s.clearLocked(sp)

	// Check to see if the adjacent span configs can be coalesced into.
	startKey, endKey := sp.Key, sp.EndKey

	next, found := s.getCeilEntry(sp.EndKey) // check the span config to our right
	if found && next.Span.Key.Equal(sp.EndKey) && next.Config.Equal(conf) {
		s.clearLocked(next.Span)
		endKey = next.Span.EndKey
	}

	prev, found := s.getFloorEntry(sp.Key) // check the span config to our left
	if found && prev.Span.EndKey.Equal(sp.Key) && prev.Config.Equal(conf) {
		s.clearLocked(prev.Span)
		startKey = prev.Span.Key
	}

	// Finally, write the span config entry.
	s.cache.AddEntry(&cache.Entry{
		Key: storageKey(startKey),
		Value: roachpb.SpanConfigEntry{
			Span: roachpb.Span{
				Key:    startKey,
				EndKey: endKey,
			},
			Config: conf,
		},
	})
}

// Get is part of the spanconfig.Storage interface.
func (s *Storage) Get(sp roachpb.Span) []roachpb.SpanConfigEntry {
	s.RLock()
	defer s.RUnlock()

	// Iterate over all overlapping ranges, return corresponding span
	// config entries.
	var res []roachpb.SpanConfigEntry
	searchStartKey, searchEndKey := s.getSearchBounds(sp)
	s.cache.DoRangeEntry(func(e *cache.Entry) bool {
		entry, _ := e.Value.(roachpb.SpanConfigEntry)
		if !entry.Span.Overlaps(sp) {
			// Skip non-overlapping entries.
			return false
		}

		res = append(res, roachpb.SpanConfigEntry{
			Span:   entry.Span.Intersect(sp),
			Config: entry.Config,
		})

		return false
	}, searchStartKey, searchEndKey)
	return res
}

// Delete is part of the spanconfig.Storage interface.
func (s *Storage) Delete(sp roachpb.Span) {
	s.Lock()
	defer s.Unlock()

	s.clearLocked(sp)
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
func (s *Storage) clearLocked(sp roachpb.Span) {
	searchStartKey, searchEndKey := s.getSearchBounds(sp)
	var toDelete, toAdd []cache.Entry
	s.cache.DoRangeEntry(func(e *cache.Entry) bool { // keyed by span start key, storing value span config entry
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
			Key:   storageKey(entry.Span.Key),
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
					Key:   storageKey(pre.Key),
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
					Key:   storageKey(post.Key),
					Value: roachpb.SpanConfigEntry{Span: post, Config: entry.Config},
				})
			}
		}

		return false
	}, searchStartKey, searchEndKey)

	// Execute the accumulated operations.
	for _, entry := range toDelete {
		entry := entry // copy out of loop variable
		s.cache.DelEntry(&entry)
	}

	for _, entry := range toAdd {
		entry := entry // copy out of loop variable
		s.cache.AddEntry(&entry)
	}
}

// getCeilEntry returns the span config entry keyed using a key greater than or
// equal to the one provided.
func (s *Storage) getCeilEntry(k roachpb.Key) (_ roachpb.SpanConfigEntry, found bool) {
	var cacheEntry *cache.Entry
	cacheEntry, found = s.cache.CeilEntry(storageKey(k))
	if !found {
		return roachpb.SpanConfigEntry{}, false
	}

	entry, _ := cacheEntry.Value.(roachpb.SpanConfigEntry)
	return entry, true
}

// getFloorEntry returns the span config entry keyed using a key less than or
// equal to the one provided.
func (s *Storage) getFloorEntry(k roachpb.Key) (_ roachpb.SpanConfigEntry, found bool) {
	var cacheEntry *cache.Entry
	cacheEntry, found = s.cache.FloorEntry(storageKey(k))
	if !found {
		return roachpb.SpanConfigEntry{}, false
	}

	entry, _ := cacheEntry.Value.(roachpb.SpanConfigEntry)
	return entry, true
}

// getSearchBounds constructs the search bounds to find all spans that possibly
// overlap with the given one. It's possible that spans retrieved using these
// search bounds don't overlap with the requested span. Since spans are keyed
// using their start key, it's possible the search is started at a span whose
// end key precedes our own.
func (s *Storage) getSearchBounds(sp roachpb.Span) (searchStartKey, searchEndKey interface{}) {
	floorEntry, found := s.cache.FloorEntry(storageKey(sp.Key))
	if found {
		searchStartKey = floorEntry.Key
	} else {
		searchStartKey = minCacheKey
	}

	ceilEntry, found := s.cache.CeilEntry(storageKey(sp.EndKey))
	if found {
		searchEndKey = ceilEntry.Key
	} else {
		searchEndKey = maxCacheKey
	}

	return searchStartKey, searchEndKey
}
