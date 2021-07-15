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

// XXX: Think about merging -- should this happen in sql or in KV?

// Set is part of the spanconfig.Storage interface.
func (s *Storage) Set(sp roachpb.Span, conf roachpb.SpanConfig) {
	s.Lock()
	defer s.Unlock()

	s.clearLocked(sp)
	s.cache.Add(storageKey(sp.Key), roachpb.SpanConfigEntry{
		Span:   sp,
		Config: conf,
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
// 		if first overlapping entry:
// 			if entry.contains(sp):
// 				add pre=entry.conf
// 				add post=entry.conf
//				break
// 			else if entry.contains(sp.start_key):
// 				add pre=entry.conf
// 		else:
// 			if entry.contains(sp.end_key):
// 				add post=entry.conf
//
func (s *Storage) clearLocked(sp roachpb.Span) {
	first := true
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

		if first {
			first = false

			// First overlapping entry.
			if entry.Span.Contains(sp) { // entry fully contains the given span
				// entry:  [-----------------)
				//
				// sp:          [-----)
				// sp:     [-----------------)
				// sp:     [-----)
				// sp:                 [-----)

				// Re-add the non-intersecting spans, if any.
				if pre.Valid() {
					toAdd = append(toAdd, cache.Entry{
						Key:   storageKey(pre.Key),
						Value: roachpb.SpanConfigEntry{Span: pre, Config: entry.Config},
					})
				}

				if post.Valid() {
					toAdd = append(toAdd, cache.Entry{
						Key:   storageKey(post.Key),
						Value: roachpb.SpanConfigEntry{Span: post, Config: entry.Config},
					})
				}
				return true // we're done iterating
			}

			if entry.Span.ContainsKey(sp.Key) { // entry contains the given span's start key
				// entry:  [-----------------)
				//
				// sp:                 [------
				if !pre.Valid() {
					panic("expected non-intersecting span to be valid")
				}
				toAdd = append(toAdd, cache.Entry{
					Key:   storageKey(pre.Key),
					Value: roachpb.SpanConfigEntry{Span: pre, Config: entry.Config},
				})
				return false
			}

			panic("unreachable")
		}

		// Subsequent overlapping entry.
		if entry.Span.ContainsKey(sp.EndKey) { // entry contains the given span's end key
			// entry:  [----------------)
			//
			// sp:     ----------)
			// sp:     -----------------)
			if post.Valid() {
				toAdd = append(toAdd, cache.Entry{
					Key:   storageKey(post.Key),
					Value: roachpb.SpanConfigEntry{Span: post, Config: entry.Config},
				})
			}
			return true // we're done iterating
		}

		if sp.Contains(entry.Span) { // entry is fully contained by the given span
			// sp:     -------------------)
			//
			// entry:     [---------)
			// entry:         [---------)

			// There's nothing to do, we've already deleted the overlapping
			// entry and there are no remaining non-intersections to re-add.
			return false
		}

		panic("unreachable")
	}, searchStartKey, searchEndKey)

	// Execute the accumulated operations.
	for _, entry := range toDelete {
		s.cache.Del(entry.Key)
	}

	for _, entry := range toAdd {
		s.cache.Add(entry.Key, entry.Value)
	}
}

// getSearchBounds constructs the search bounds to find all spans that possibly
// overlap with the given one. It's possible that spans retrieved using these
// search bounds don't overlap with the requested span. Since spans are keyed
// using their start key, it's possible the search is started at a span whose
// end key precedes our own.
func (s *Storage) getSearchBounds(sp roachpb.Span) (searchStartKey, searchEndKey interface{}) {
	var found bool
	searchStartKey, _, found = s.cache.Floor(storageKey(sp.Key))
	if !found {
		searchStartKey = minCacheKey
	}
	searchEndKey, _, found = s.cache.Ceil(storageKey(sp.EndKey))
	if !found {
		searchEndKey = maxCacheKey
	}

	return searchStartKey, searchEndKey
}
