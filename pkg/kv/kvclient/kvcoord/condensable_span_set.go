// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// condensableSpanSet is a set of key spans that is condensable in order to
// stay below some maximum byte limit. Condensing of the set happens in two
// ways. Initially, overlapping spans are merged together to deduplicate
// redundant keys. If that alone isn't sufficient to stay below the byte limit,
// spans within the same Range will be merged together. This can cause the
// "footprint" of the set to grow, so the set should be thought of as on
// overestimate.
type condensableSpanSet struct {
	s     []roachpb.Span
	bytes int64

	// condensed is set if we ever condensed the spans. Meaning, if the set of
	// spans currently tracked has lost fidelity compared to the spans inserted.
	// Note that we might have otherwise mucked with the inserted spans to save
	// memory without losing fidelity, in which case this flag would not be set
	// (e.g. merging overlapping or adjacent spans).
	condensed bool
}

// insert adds new spans to the condensable span set. No attempt to condense the
// set or deduplicate the new span with existing spans is made.
func (s *condensableSpanSet) insert(spans ...roachpb.Span) {
	s.s = append(s.s, spans...)
	for _, sp := range spans {
		s.bytes += spanSize(sp)
	}
}

// mergeAndSort merges all overlapping spans. Calling this method will not
// increase the overall bounds of the span set, but will eliminate duplicated
// spans and combine overlapping spans.
//
// The method has the side effect of sorting the stable write set.
func (s *condensableSpanSet) mergeAndSort() {
	oldLen := len(s.s)
	s.s, _ = roachpb.MergeSpans(&s.s)
	// Recompute the size if anything has changed.
	if oldLen != len(s.s) {
		s.bytes = 0
		for _, sp := range s.s {
			s.bytes += spanSize(sp)
		}
	}
}

// maybeCondense is similar in spirit to mergeAndSort, but it only adjusts the
// span set when the maximum byte limit is exceeded. However, when this limit is
// exceeded, the method is more aggressive in its attempt to reduce the memory
// footprint of the span set. Not only will it merge overlapping spans, but
// spans within the same range boundaries are also condensed.
//
// Returns true if condensing was done. Note that, even if condensing was
// performed, this doesn't guarantee that the size was reduced below the byte
// limit. Condensing is only performed at the level of individual ranges, not
// across ranges, so it's possible to not be able to condense as much as
// desired.
//
// maxBytes <= 0 means that each range will be maximally condensed.
func (s *condensableSpanSet) maybeCondense(
	ctx context.Context, riGen rangeIteratorFactory, maxBytes int64,
) bool {
	if s.bytes < maxBytes {
		return false
	}

	// Start by attempting to simply merge the spans within the set. This alone
	// may bring us under the byte limit. Even if it doesn't, this step has the
	// nice property that it sorts the spans by start key, which we rely on
	// lower in this method.
	s.mergeAndSort()
	if s.bytes < maxBytes {
		return false
	}

	ri := riGen.newRangeIterator()

	// Divide spans by range boundaries and condense. Iterate over spans
	// using a range iterator and add each to a bucket keyed by range
	// ID. Local keys are kept in a new slice and not added to buckets.
	type spanBucket struct {
		rangeID roachpb.RangeID
		bytes   int64
		spans   []roachpb.Span
	}
	var buckets []spanBucket
	var localSpans []roachpb.Span
	for _, sp := range s.s {
		if keys.IsLocal(sp.Key) {
			localSpans = append(localSpans, sp)
			continue
		}
		ri.Seek(ctx, roachpb.RKey(sp.Key), Ascending)
		if !ri.Valid() {
			// We haven't modified s.s yet, so it is safe to return.
			log.Warningf(ctx, "failed to condense lock spans: %v", ri.Error())
			return false
		}
		rangeID := ri.Desc().RangeID
		if l := len(buckets); l > 0 && buckets[l-1].rangeID == rangeID {
			buckets[l-1].spans = append(buckets[l-1].spans, sp)
		} else {
			buckets = append(buckets, spanBucket{
				rangeID: rangeID, spans: []roachpb.Span{sp},
			})
		}
		buckets[len(buckets)-1].bytes += spanSize(sp)
	}

	// Sort the buckets by size and collapse from largest to smallest
	// until total size of uncondensed spans no longer exceeds threshold.
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].bytes > buckets[j].bytes })
	s.s = localSpans // reset to hold just the local spans; will add newly condensed and remainder
	for _, bucket := range buckets {
		// Condense until we get to half the threshold.
		if s.bytes <= maxBytes/2 {
			// Collect remaining spans from each bucket into uncondensed slice.
			s.s = append(s.s, bucket.spans...)
			continue
		}
		s.bytes -= bucket.bytes
		// TODO(spencer): consider further optimizations here to create
		// more than one span out of a bucket to avoid overly broad span
		// combinations.
		cs := bucket.spans[0]
		for _, s := range bucket.spans[1:] {
			cs = cs.Combine(s)
			if !cs.Valid() {
				// If we didn't fatal here then we would need to ensure that the
				// spans were restored or a transaction could lose part of its
				// lock footprint.
				log.Fatalf(ctx, "failed to condense lock spans: "+
					"combining span %s yielded invalid result", s)
			}
		}
		s.bytes += spanSize(cs)
		s.s = append(s.s, cs)
	}
	s.condensed = true
	return true
}

// asSlice returns the set as a slice of spans.
func (s *condensableSpanSet) asSlice() []roachpb.Span {
	l := len(s.s)
	return s.s[:l:l] // immutable on append
}

// empty returns whether the set is empty or whether it contains spans.
func (s *condensableSpanSet) empty() bool {
	return len(s.s) == 0
}

func (s *condensableSpanSet) clear() {
	*s = condensableSpanSet{}
}

func spanSize(sp roachpb.Span) int64 {
	return int64(len(sp.Key) + len(sp.EndKey))
}

func keySize(k roachpb.Key) int64 {
	return int64(len(k))
}
