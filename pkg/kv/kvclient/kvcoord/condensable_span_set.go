// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"cmp"
	"context"
	"slices"

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
	// TODO(nvanbenschoten): It feels like there is a lot that we could do with
	// this data structure to 1) reduce the per span overhead, and 2) avoid the
	// retention of many small keys for the duration of a transaction. For
	// instance, we could allocate a single large block of memory and copy keys
	// into it. We could also store key lengths inline to minimize the per-span
	// overhead. Recognizing that many spans are actually point keys would also
	// help.
	s     []roachpb.Span
	bytes int64

	// condensed is set if we ever condensed the spans. Meaning, if the set of
	// spans currently tracked has lost fidelity compared to the spans inserted.
	// Note that we might have otherwise mucked with the inserted spans to save
	// memory without losing fidelity, in which case this flag would not be set
	// (e.g. merging overlapping or adjacent spans).
	condensed bool

	// Avoid heap allocations for transactions with a small number of spans.
	sAlloc [2]roachpb.Span
}

// insert adds new spans to the condensable span set. No attempt to condense the
// set or deduplicate the new span with existing spans is made.
func (s *condensableSpanSet) insert(spans ...roachpb.Span) {
	if cap(s.s) == 0 {
		s.s = s.sAlloc[:0]
	}
	s.s = append(s.s, spans...)
	for _, sp := range spans {
		s.bytes += spanSize(sp)
	}
}

// mergeAndSort merges all overlapping spans. Calling this method will not
// increase the overall bounds of the span set, but will eliminate duplicated
// spans and combine overlapping spans.
//
// The method has the side effect of sorting the set.
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
	if s.bytes <= maxBytes {
		return false
	}

	// Start by attempting to simply merge the spans within the set. This alone
	// may bring us under the byte limit. Even if it doesn't, this step has the
	// nice property that it sorts the spans by start key, which we rely on
	// lower in this method.
	s.mergeAndSort()
	if s.bytes <= maxBytes {
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
	slices.SortFunc(buckets, func(a, b spanBucket) int { return -cmp.Compare(a.bytes, b.bytes) })
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

// estimateSize returns the size that the spanSet would grow to if spans were
// added to the set. As a side-effect, the receiver might get its spans merged.
//
// The result doesn't take into consideration the effect of condensing the
// spans, but might take into consideration the effects of merging the spans
// (which is not a lossy operation): mergeThresholdBytes instructs the
// simulation to perform merging and de-duping if the size grows over this
// threshold.
func (s *condensableSpanSet) estimateSize(spans []roachpb.Span, mergeThresholdBytes int64) int64 {
	var bytes int64
	for _, sp := range spans {
		bytes += spanSize(sp)
	}
	{
		estimate := s.bytes + bytes
		if estimate <= mergeThresholdBytes {
			return estimate
		}
	}

	// Merge and de-dupe in the hope of saving some space.

	// First, merge the existing spans in-place. Doing it in-place instead of
	// operating on a copy avoids the risk of quadratic behavior over a series of
	// estimateSize() calls, where each call has to repeatedly merge a copy in
	// order to discover that the merge saves enough space to stay under the
	// threshold.
	s.mergeAndSort()

	// See if merging s was enough.
	estimate := s.bytes + bytes
	if estimate <= mergeThresholdBytes {
		return estimate
	}

	// Try harder - merge (a copy of) the existing spans with the new spans.
	spans = append(spans, s.s...)
	lenBeforeMerge := len(spans)
	spans, _ = roachpb.MergeSpans(&spans)
	if len(spans) == lenBeforeMerge {
		// Nothing changed -i.e. we failed to merge any spans.
		return estimate
	}
	// Recompute the size.
	bytes = 0
	for _, sp := range spans {
		bytes += spanSize(sp)
	}
	return bytes
}

// bytesSize returns the size of the tracked spans.
func (s *condensableSpanSet) bytesSize() int64 {
	return s.bytes
}

func spanSize(sp roachpb.Span) int64 {
	// Since the span is included into a []roachpb.Span, we also need to account
	// for the overhead of storing it in that slice.
	return roachpb.SpanOverhead + int64(cap(sp.Key)+cap(sp.EndKey))
}
