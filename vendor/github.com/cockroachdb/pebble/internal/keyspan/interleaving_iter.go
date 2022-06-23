// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// A SpanMask may be used to configure an interleaving iterator to skip point
// keys that fall within the bounds of some spans.
type SpanMask interface {
	// SpanChanged is invoked by an interleaving iterator whenever the current
	// span changes. As the iterator passes into or out of a Span, it invokes
	// SpanChanged, passing the new Span. When the iterator passes out of a
	// span's boundaries and is no longer covered by any span, SpanChanged is
	// invoked with a nil span.
	//
	// SpanChanged is invoked before SkipPoint, and callers may use SpanChanged
	// to recalculate state used by SkipPoint for masking.
	//
	// SpanChanged may be invoked consecutively with identical spans under some
	// circumstances, such as repeatedly absolutely positioning an iterator to
	// positions covered by the same span, or while changing directions.
	SpanChanged(*Span)
	// SkipPoint is invoked by the interleaving iterator whenever the iterator
	// encounters a point key covered by a Span. If SkipPoint returns true, the
	// interleaving iterator skips the point key without returning it. This is
	// used during range key iteration to skip over point keys 'masked' by range
	// keys.
	SkipPoint(userKey []byte) bool
}

// InterleavingIter combines an iterator over point keys with an iterator over
// key spans.
//
// Throughout Pebble, some keys apply at single discrete points within the user
// keyspace. Other keys apply over continuous spans of the user key space.
// Internally, iterators over point keys adhere to the base.InternalIterator
// interface, and iterators over spans adhere to the keyspan.FragmentIterator
// interface. The InterleavingIterator wraps a point iterator and span iterator,
// providing access to all the elements of both iterators.
//
// The InterleavingIterator implements the point base.InternalIterator
// interface. After any of the iterator's methods return a key, a caller may
// call Span to retrieve the span covering the returned key, if any.  A span is
// considered to 'cover' a returned key if the span's [start, end) bounds
// include the key's user key.
//
// In addition to tracking the current covering span, InterleavingIter returns a
// special InternalKey at span start boundaries. Start boundaries are surfaced
// as a synthetic span marker: an InternalKey with the boundary as the user key,
// the infinite sequence number and a key kind selected from an arbitrary key
// the infinite sequence number and an arbitrary contained key's kind. Since
// which of the Span's key's kind is surfaced is undefined, the caller should
// not use the InternalKey's kind. The caller should only rely on the `Span`
// method for retrieving information about spanning keys. The interleaved
// synthetic keys have the infinite sequence number so that they're interleaved
// before any point keys with the same user key when iterating forward and after
// when iterating backward.
//
// Interleaving the synthetic start key boundaries at the maximum sequence
// number provides an opportunity for the higher-level, public Iterator to
// observe the Span, even if no live points keys exist within the boudns of the
// Span.
//
// When returning a synthetic marker key for a start boundary, InterleavingIter
// will truncate the span's start bound to the SeekGE or SeekPrefixGE search
// key. For example, a SeekGE("d") that finds a span [a, z) may return a
// synthetic span marker key `d#72057594037927935,21`.
//
// If bounds have been applied to the iterator through SetBounds,
// InterleavingIter will truncate the bounds of spans returned through Span to
// the set bounds. The bounds returned through Span are not truncated by a
// SeekGE or SeekPrefixGE search key. Consider, for example SetBounds('c', 'e'),
// with an iterator containing the Span [a,z):
//
//     First()     = `c#72057594037927935,21`        Span() = [c,e)
//     SeekGE('d') = `d#72057594037927935,21`        Span() = [c,e)
//
// InterleavedIter does not interleave synthetic markers for spans that do not
// contain any keys.
//
// SpanMask
//
// InterelavingIter takes a SpanMask parameter that may be used to configure the
// behavior of the iterator. See the documentation on the SpanMask type.
//
// All spans containing keys are exposed during iteration.
type InterleavingIter struct {
	cmp         base.Compare
	comparer    *base.Comparer
	pointIter   base.InternalIterator
	keyspanIter FragmentIterator
	mask        SpanMask

	// lower and upper hold the iteration bounds set through SetBounds.
	lower, upper []byte
	// keyBuf is used to copy SeekGE or SeekPrefixGE arguments when they're used
	// to truncate a span. The byte slices backing a SeekGE/SeekPrefixGE search
	// keys can come directly from the end user, so they're copied into keyBuf
	// to ensure key stability.
	keyBuf []byte
	// nextPrefixBuf is used during SeekPrefixGE calls to store the truncated
	// upper bound of the returned spans. SeekPrefixGE truncates the returned
	// spans to an upper bound of the seeked prefix's immediate successor.
	nextPrefixBuf []byte
	pointKey      *base.InternalKey
	pointVal      []byte
	// span holds the span at the keyspanIter's current position. If the span is
	// wholly contained within the iterator bounds, this span is directly
	// returned to the iterator consumer through Span(). If either bound needed
	// to be truncated to the iterator bounds, then truncated is set to true and
	// Span() must return a pointer to truncatedSpan.
	span *Span
	// spanMarker holds the synthetic key that is returned when the iterator
	// passes over a key span's start bound.
	spanMarker base.InternalKey
	// truncated indicates whether or not the span at the current position
	// needed to be truncated. If it did, truncatedSpan holds the truncated
	// span that should be returned.
	truncatedSpan Span
	truncated     bool

	// Keeping all of the bools together reduces the sizeof the struct.

	// spanCoversKey indicates whether the current span covers the last-returned
	// key.
	spanCoversKey bool
	// pointKeyInterleaved indicates whether the current point key has been
	// interleaved in the current direction.
	pointKeyInterleaved bool
	// keyspanInterleaved indicates whether or not the current span has been
	// interleaved at its start key in the current direction. A span marker is
	// interleaved when first passing over the start key.
	//
	// When iterating in the forward direction, the span start key is
	// interleaved when the span first begins to cover the current iterator
	// position. The keyspan iterator isn't advanced until the
	// InterleavingIterator moves beyond the current span's end key. This field
	// is used to remember that the span has already been interleaved and
	// shouldn't be interleaved again.
	//
	// When iterating in the reverse direction, the span start key is
	// interleaved immediately before the iterator will move to a key no longer
	// be covered by the span. This field behaves analagously to
	// pointKeyInterleaved and if true signals that we must Prev the keyspan
	// iterator on the next Prev call.
	keyspanInterleaved bool
	// spanMarkerTruncated is set by SeekGE/SeekPrefixGE calls that truncate a
	// span's start bound marker to the search key. It's returned to false on
	// the next repositioning of the keyspan iterator.
	spanMarkerTruncated bool
	// maskSpanChangedCalled records whether or not the last call to
	// SpanMask.SpanChanged provided the current span (i.span) or not.
	maskSpanChangedCalled bool
	// prefix records whether the iteator is in prefix mode. During prefix mode,
	// Pebble will truncate spans to the next prefix. If the iterator
	// subsequently leaves prefix mode, the existing span cached in i.span must
	// be invalidated because its bounds do not reflect the original span's true
	// bounds.
	prefix bool
	// dir indicates the direction of iteration: forward (+1) or backward (-1)
	dir int8
}

// Assert that *InterleavingIter implements the InternalIterator interface.
var _ base.InternalIterator = &InterleavingIter{}

// Init initializes the InterleavingIter to interleave point keys from pointIter
// with key spans from keyspanIter.
//
// The point iterator must already have the provided bounds. Init does not
// propagate the bounds down the iterator stack.
func (i *InterleavingIter) Init(
	comparer *base.Comparer,
	pointIter base.InternalIterator,
	keyspanIter FragmentIterator,
	mask SpanMask,
	lowerBound, upperBound []byte,
) {
	*i = InterleavingIter{
		cmp:         comparer.Compare,
		comparer:    comparer,
		pointIter:   pointIter,
		keyspanIter: keyspanIter,
		mask:        mask,
		lower:       lowerBound,
		upper:       upperBound,
	}
}

// InitSeekGE may be called after Init but before any positioning method.
// InitSeekGE initializes the current position of the point iterator and then
// performs a SeekGE on the keyspan iterator using the provided key. InitSeekGE
// returns whichever point or keyspan key is smaller. After InitSeekGE, the
// iterator is positioned and may be repositioned using relative positioning
// methods.
//
// This method is used specifically for lazily constructing combined iterators.
// It allows for seeding the iterator with the current position of the point
// iterator.
func (i *InterleavingIter) InitSeekGE(
	prefix, key []byte, pointKey *base.InternalKey, pointValue []byte,
) (*base.InternalKey, []byte) {
	i.dir = +1
	i.clearMask()
	i.prefix = prefix != nil
	i.pointKey, i.pointVal = pointKey, pointValue
	i.pointKeyInterleaved = false
	// NB: This keyspanSeekGE call will truncate the span to the seek key if
	// necessary. This truncation is important for cases where a switch to
	// combined iteration is made during a user-initiated SeekGE.
	i.keyspanSeekGE(key, prefix)
	return i.interleaveForward(key, prefix)
}

// InitSeekLT may be called after Init but before any positioning method.
// InitSeekLT initializes the current position of the point iterator and then
// performs a SeekLT on the keyspan iterator using the provided key. InitSeekLT
// returns whichever point or keyspan key is larger. After InitSeekLT, the
// iterator is positioned and may be repositioned using relative positioning
// methods.
//
// This method is used specifically for lazily constructing combined iterators.
// It allows for seeding the iterator with the current position of the point
// iterator.
func (i *InterleavingIter) InitSeekLT(
	key []byte, pointKey *base.InternalKey, pointValue []byte,
) (*base.InternalKey, []byte) {
	i.dir = -1
	i.clearMask()
	i.pointKey, i.pointVal = pointKey, pointValue
	i.pointKeyInterleaved = false
	i.keyspanSeekLT(key)
	return i.interleaveBackward()
}

// SeekGE implements (base.InternalIterator).SeekGE.
//
// If there exists a span with a start key ≤ the first matching point key,
// SeekGE will return a synthetic span marker key for the span. If this span's
// start key is less than key, the returned marker will be truncated to key.
// Note that this search-key truncation of the marker's key is not applied to
// the span returned by Span.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekGE(key []byte, flags base.SeekGEFlags) (*base.InternalKey, []byte) {
	i.clearMask()
	i.disablePrefixMode()
	i.pointKey, i.pointVal = i.pointIter.SeekGE(key, flags)
	i.pointKeyInterleaved = false

	// We need to seek the keyspan iterator too. If the keyspan iterator was
	// already positioned at a span, we might be able to avoid the seek if the
	// seek key falls within the existing span's bounds.
	if i.span != nil && i.cmp(key, i.span.End) < 0 && i.cmp(key, i.span.Start) >= 0 {
		// We're seeking within the existing span's bounds. We still might need
		// truncate the span to the iterator's bounds.
		i.checkForwardBound(nil /* prefix */)
		i.savedKeyspan()
	} else {
		i.keyspanSeekGE(key, nil /* prefix */)
	}

	i.dir = +1
	return i.interleaveForward(key, nil /* prefix */)
}

// SeekPrefixGE implements (base.InternalIterator).SeekPrefixGE.
//
// If there exists a span with a start key ≤ the first matching point key,
// SeekPrefixGE will return a synthetic span marker key for the span. If this
// span's start key is less than key, the returned marker will be truncated to
// key. Note that this search-key truncation of the marker's key is not applied
// to the span returned by Span.
//
// NB: In accordance with the base.InternalIterator contract:
//   i.lower ≤ key
func (i *InterleavingIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	i.clearMask()
	i.pointKey, i.pointVal = i.pointIter.SeekPrefixGE(prefix, key, flags)
	i.pointKeyInterleaved = false
	i.prefix = true

	// We need to seek the keyspan iterator too. If the keyspan iterator was
	// already positioned at a span, we might be able to avoid the seek if the
	// entire seek prefix key falls within the existing span's bounds.
	//
	// During a SeekPrefixGE, Pebble defragments range keys within the bounds of
	// the prefix. For example, a SeekPrefixGE('c', 'c@8') must defragment the
	// any overlapping range keys within the bounds of [c,c\00).
	//
	// If range keys are fragmented within a prefix (eg, because a version
	// within a prefix was chosen as an sstable boundary), then it's possible
	// the seek key falls into the current i.span, but the current i.span does
	// not wholly cover the seek prefix.
	//
	// For example, a SeekPrefixGE('d@5') may only defragment a range key to
	// the bounds of [c@2,e). A subsequent SeekPrefixGE('c@0') must re-seek the
	// keyspan iterator, because although 'c@0' is contained within [c@2,e), the
	// full span of the prefix is not.
	//
	// Similarly, a SeekPrefixGE('a@3') may only defragment a range key to the
	// bounds [a,c@8). A subsequent SeekPrefixGE('c@10') must re-seek the
	// keyspan iterator, because although 'c@10' is contained within [a,c@8),
	// the full span of the prefix is not.
	seekKeyspanIter := true
	if i.span != nil && i.cmp(prefix, i.span.Start) >= 0 {
		if ei := i.comparer.Split(i.span.End); i.cmp(prefix, i.span.End[:ei]) < 0 {
			// We're seeking within the existing span's bounds. We still might need
			// truncate the span to the iterator's bounds.
			i.checkForwardBound(prefix)
			i.savedKeyspan()
			seekKeyspanIter = false
		}
	}
	if seekKeyspanIter {
		i.keyspanSeekGE(key, prefix)
	}

	i.dir = +1
	return i.interleaveForward(key, prefix)
}

// SeekLT implements (base.InternalIterator).SeekLT.
func (i *InterleavingIter) SeekLT(key []byte, flags base.SeekLTFlags) (*base.InternalKey, []byte) {
	i.clearMask()
	i.disablePrefixMode()
	i.pointKey, i.pointVal = i.pointIter.SeekLT(key, flags)
	i.pointKeyInterleaved = false

	// We need to seek the keyspan iterator too. If the keyspan iterator was
	// already positioned at a span, we might be able to avoid the seek if the
	// seek key falls within the existing span's bounds.
	if i.span != nil && i.cmp(key, i.span.Start) > 0 && i.cmp(key, i.span.End) < 0 {
		// We're seeking within the existing span's bounds. We still might need
		// truncate the span to the iterator's bounds.
		i.checkBackwardBound()
		// The span's start key is still not guaranteed to be less than key,
		// because of the bounds enforcement. Consider the following example:
		//
		// Bounds are set to [d,e). The user performs a SeekLT(d). The
		// FragmentIterator.SeekLT lands on a span [b,f). This span has a start
		// key less than d, as expected. Above, checkBackwardBound truncates the
		// span to match the iterator's current bounds, modifying the span to
		// [d,e), which does not overlap the search space of [-∞, d).
		//
		// This problem is a consequence of the SeekLT's exclusive search key
		// and the fact that we don't perform bounds truncation at every leaf
		// iterator.
		if i.span != nil && i.truncated && i.cmp(i.truncatedSpan.Start, key) >= 0 {
			i.span = nil
		}
		i.savedKeyspan()
	} else {
		i.keyspanSeekLT(key)
	}

	i.dir = -1
	return i.interleaveBackward()
}

// First implements (base.InternalIterator).First.
func (i *InterleavingIter) First() (*base.InternalKey, []byte) {
	i.clearMask()
	i.disablePrefixMode()
	i.pointKey, i.pointVal = i.pointIter.First()
	i.pointKeyInterleaved = false
	i.span = i.keyspanIter.First()
	i.checkForwardBound(nil /* prefix */)
	i.savedKeyspan()
	i.dir = +1
	return i.interleaveForward(i.lower, nil /* prefix */)
}

// Last implements (base.InternalIterator).Last.
func (i *InterleavingIter) Last() (*base.InternalKey, []byte) {
	i.clearMask()
	i.disablePrefixMode()
	i.pointKey, i.pointVal = i.pointIter.Last()
	i.pointKeyInterleaved = false
	i.span = i.keyspanIter.Last()
	i.checkBackwardBound()
	i.savedKeyspan()
	i.dir = -1
	return i.interleaveBackward()
}

// Next implements (base.InternalIterator).Next.
func (i *InterleavingIter) Next() (*base.InternalKey, []byte) {
	if i.dir == -1 {
		// Switching directions.
		i.dir = +1

		if i.mask != nil {
			// Clear the mask while we reposition the point iterator. While
			// switching directions, we may move the point iterator outside of
			// i.span's bounds.
			i.clearMask()
		}

		// The existing point key (denoted below with *) is either the last
		// key we returned (the current iterator position):
		//   points:    x     (y*)    z
		// or the upcoming point key in the backward direction if we just
		// returned a span start boundary key:
		//   points:    x*            z
		//    spans:        ([y-?))
		// direction. Either way, we must move to the next point key.
		switch {
		case i.pointKey == nil && i.lower == nil:
			i.pointKey, i.pointVal = i.pointIter.First()
		case i.pointKey == nil && i.lower != nil:
			i.pointKey, i.pointVal = i.pointIter.SeekGE(i.lower, base.SeekGEFlagsNone)
		default:
			i.pointKey, i.pointVal = i.pointIter.Next()
		}
		i.pointKeyInterleaved = false

		if i.span == nil {
			// There was no span in the reverse direction, but there may be
			// a span in the forward direction.
			i.span = i.keyspanIter.Next()
			i.checkForwardBound(nil /* prefix */)
			i.savedKeyspan()
		} else {
			// Regardless of the current iterator state, we mark any existing
			// span as interleaved when switching to forward iteration,
			// justified below.
			//
			// If the point key is the last key returned:
			//   pointIter   :         ... (y)   z ...
			//   keyspanIter : ... ([x -               )) ...
			//                              ^
			// The span's start key must be ≤ the point key, otherwise we'd have
			// interleaved the span's start key. From a forward-iteration
			// perspective, the span's start key is in the past and should be
			// considered already-interleaved.
			//
			// If the span start boundary key is the last key returned:
			//   pointIter   : ... (x)       z ...
			//   keyspanIter :     ... ([y -        )) ...
			//                           ^
			// i.span.Start is the key we last returned during reverse
			// iteration. From the perspective of forward-iteration, its start
			// key was just visited.
			i.keyspanInterleaved = true
		}
	}

	// Refresh the point key if the current point key has already been
	// interleaved.
	if i.pointKeyInterleaved {
		i.pointKey, i.pointVal = i.pointIter.Next()
		i.pointKeyInterleaved = false
	}
	// If we already interleaved the current span start key, and the point key
	// is ≥ the span's end key, move to the next span.
	if i.keyspanInterleaved && i.pointKey != nil && i.span != nil &&
		i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
		i.span = i.keyspanIter.Next()
		i.checkForwardBound(nil /* prefix */)
		i.savedKeyspan()
	}
	return i.interleaveForward(i.lower, nil /* prefix */)
}

// Prev implements (base.InternalIterator).Prev.
func (i *InterleavingIter) Prev() (*base.InternalKey, []byte) {
	if i.dir == +1 {
		// Switching directions.
		i.dir = -1

		if i.mask != nil {
			// Clear the mask while we reposition the point iterator. While
			// switching directions, we may move the point iterator outside of
			// i.span's bounds.
			i.clearMask()
		}

		if i.keyspanInterleaved {
			// The current span's start key has already been interleaved in the
			// forward direction. The start key may have been interleaved a
			// while ago, or it might've been interleaved at the current
			// iterator position. If it was interleaved a while ago, the current
			// span is still relevant and we should not move the keyspan
			// iterator.
			//
			// If it was just interleaved at the current iterator position, the
			// span start was the last key returned to the user. We should
			// prev past it so we don't return it again, with an exception.
			// Consider span [a, z) and this sequence of iterator calls:
			//
			//   SeekGE('c') = c.RANGEKEYSET#72057594037927935
			//   Prev()      = a.RANGEKEYSET#72057594037927935
			//
			// If the current span's start key was last surfaced truncated due
			// to a SeekGE or SeekPrefixGE call, then it's still relevant in the
			// reverse direction with an untruncated start key.
			//
			// We can determine whether the last key returned was a point key by
			// checking i.pointKeyInterleaved, because every Next/Prev will
			// advance the point iterator and reset pointKeyInterleaved if it
			// was.
			if i.pointKeyInterleaved || i.spanMarkerTruncated {
				// The last returned key was a point key, OR a truncated span
				// marker key. Don't move, but re-save the span because it
				// should no longer be considered truncated or interleaved.
				i.savedKeyspan()
			} else {
				// The last returned key is this key's start boundary, so Prev
				// past it so we don't return it again.
				i.span = i.keyspanIter.Prev()
				i.checkBackwardBound()
				i.savedKeyspan()
			}
		} else {
			// If the current span's start key has not been interleaved, then
			// the span's start key is greater than the current iterator
			// position (denoted in parenthesis), and the current span's start
			// key is ahead of our iterator position. Move it to the previous
			// span:
			//  points:    (x*)
			//    span:          [y-z)*
			i.span = i.keyspanIter.Prev()
			i.checkBackwardBound()
			i.savedKeyspan()
		}

		// The existing point key (denoted below with *) is either the last
		// key we returned (the current iterator position):
		//   points:    x     (y*)    z
		// or the upcoming point key in the forward direction if we just
		// returned a span start boundary key :
		//   points:    x             z*
		//    spans:        ([y-?))
		// direction. Either way, we must move the point iterator backwards.
		switch {
		case i.pointKey == nil && i.upper == nil:
			i.pointKey, i.pointVal = i.pointIter.Last()
		case i.pointKey == nil && i.upper != nil:
			i.pointKey, i.pointVal = i.pointIter.SeekLT(i.upper, base.SeekLTFlagsNone)
		default:
			i.pointKey, i.pointVal = i.pointIter.Prev()
		}
		i.pointKeyInterleaved = false
	}

	// Refresh the point key if we just returned the current point key.
	if i.pointKeyInterleaved {
		i.pointKey, i.pointVal = i.pointIter.Prev()
		i.pointKeyInterleaved = false
	}
	// Refresh the span if we just returned the span's start boundary key.
	if i.keyspanInterleaved {
		i.span = i.keyspanIter.Prev()
		i.checkBackwardBound()
		i.savedKeyspan()
	}
	return i.interleaveBackward()
}

func (i *InterleavingIter) interleaveForward(
	lowerBound []byte, prefix []byte,
) (*base.InternalKey, []byte) {
	// This loop determines whether a point key or a span marker key should be
	// interleaved on each iteration. If masking is disabled and the span is
	// nonempty, this loop executes for exactly one iteration. If masking is
	// enabled and a masked key is determined to be interleaved next, this loop
	// continues until the interleaved key is unmasked. If a span's start key
	// should be interleaved next, but the span is empty, the loop continues to
	// the next key.
	for {
		// Check invariants.
		if invariants.Enabled {
			// INVARIANT: !pointKeyInterleaved
			if i.pointKeyInterleaved {
				panic("pebble: invariant violation: point key interleaved")
			}
			switch {
			case i.span == nil:
			case i.pointKey == nil:
			default:
				// INVARIANT: !keyspanInterleaved || pointKey < span.End
				// The caller is responsible for advancing this span if it's already
				// been interleaved and the span ends before the point key.
				// Absolute positioning methods will never have already interleaved
				// the span's start key, so only Next needs to handle the case where
				// pointKey >= span.End.
				if i.keyspanInterleaved && i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
					panic("pebble: invariant violation: span interleaved, but point key >= span end")
				}
			}
		}

		// Interleave.
		switch {
		case i.span == nil:
			// If we're out of spans, just return the point key.
			return i.yieldPointKey(false /* covered */)
		case i.pointKey == nil:
			if i.pointKeyInterleaved {
				panic("pebble: invariant violation: point key already interleaved")
			}
			// If we're out of point keys, we need to return a span marker. If
			// the current span has already been interleaved, advance it. Since
			// there are no more point keys, we don't need to worry about
			// advancing past the current point key.
			if i.keyspanInterleaved {
				i.span = i.keyspanIter.Next()
				i.checkForwardBound(prefix)
				i.savedKeyspan()
				if i.span == nil {
					return i.yieldNil()
				}
			}
			if i.span.Empty() {
				i.keyspanInterleaved = true
				continue
			}
			return i.yieldSyntheticSpanMarker(lowerBound)
		default:
			if i.cmp(i.pointKey.UserKey, i.startKey()) >= 0 {
				// The span start key lies before the point key. If we haven't
				// interleaved it, we should.
				if !i.keyspanInterleaved {
					if i.span.Empty() {
						if i.pointKey != nil && i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
							// Advance the keyspan iterator, as just flipping
							// keyspanInterleaved would likely trip up the invariant check
							// above.
							i.span = i.keyspanIter.Next()
							i.checkForwardBound(prefix)
							i.savedKeyspan()
						} else {
							i.keyspanInterleaved = true
						}
						continue
					}
					return i.yieldSyntheticSpanMarker(lowerBound)
				}

				// Otherwise, the span's start key is already interleaved and we
				// need to return the point key. The current span necessarily
				// must cover the point key:
				//
				// Since the span's start is less than or equal to the point
				// key, the only way for this span to not cover the point would
				// be if the span's end is less than or equal to the point.
				// (For example span = [a, b), point key = c).
				//
				// However, the invariant at the beginning of the function
				// guarantees that if:
				//  * we have both a point key and a span
				//  * and the span has already been interleaved
				// => then the point key must be less than the span's end, and
				//    the point key must be covered by the current span.

				// The span covers the point key. If a SkipPoint hook is
				// configured, ask it if we should skip this point key.
				//
				// But first, we may need to update the mask to the current span
				// if we have stepped outside of the span last saved as a mask,
				// so that the decision to skip is made with the correct
				// knowledge of the covering span.
				i.maybeUpdateMask(true /*covered */)

				if i.mask != nil && i.mask.SkipPoint(i.pointKey.UserKey) {
					i.pointKey, i.pointVal = i.pointIter.Next()
					// We may have just invalidated the invariant that
					// ensures the span's End is > the point key, so
					// reestablish it before the next iteration.
					if i.pointKey != nil && i.cmp(i.pointKey.UserKey, i.span.End) >= 0 {
						i.span = i.keyspanIter.Next()
						i.checkForwardBound(prefix)
						i.savedKeyspan()
					}
					continue
				}

				// Point key is unmasked but covered.
				return i.yieldPointKey(true /* covered */)
			}
			return i.yieldPointKey(false /* covered */)
		}
	}
}

func (i *InterleavingIter) interleaveBackward() (*base.InternalKey, []byte) {
	// This loop determines whether a point key or a span's start key should be
	// interleaved on each iteration. If masking is disabled and the span is
	// nonempty, this loop executes for exactly one iteration. If masking is
	// enabled and a masked key is determined to be interleaved next, this loop
	// continues until the interleaved key is unmasked. If a span's start key
	// should be interleaved next, but the span is empty, the loop continues to
	// the next key.
	for {
		// Check invariants.
		if invariants.Enabled {
			// INVARIANT: !pointKeyInterleaved
			if i.pointKeyInterleaved {
				panic("pebble: invariant violation: point key interleaved")
			}
		}

		// Interleave.
		switch {
		case i.span == nil:
			// If we're out of spans, just return the point key.
			return i.yieldPointKey(false /* covered */)
		case i.pointKey == nil:
			// If we're out of point keys, we need to return a span marker.
			if i.span.Empty() {
				i.span = i.keyspanIter.Prev()
				i.checkBackwardBound()
				i.savedKeyspan()
				continue
			}
			return i.yieldSyntheticSpanMarker(i.lower)
		default:
			// If the span's start key is greater than the point key, return a
			// marker for the span.
			if i.cmp(i.startKey(), i.pointKey.UserKey) > 0 {
				if i.span.Empty() {
					i.span = i.keyspanIter.Prev()
					i.checkBackwardBound()
					i.savedKeyspan()
					continue
				}
				return i.yieldSyntheticSpanMarker(i.lower)
			}
			// We have a span but it has not been interleaved and begins at a
			// key equal to or before the current point key. The point key
			// should be interleaved next, if it's not masked.
			if i.cmp(i.pointKey.UserKey, i.span.End) < 0 {
				// The span covers the point key. The point key might be masked
				// too if masking is enabled.
				//
				// The span may have changed since the last time we updated the
				// mask. Consider the following range-key masking scenario:
				//
				//     |--------------) [b,d)@5
				//            . c@4          . e@9
				//
				// During reverse iteration when we step from e@9 to c@4, we
				// enter the span [b,d)@5. Since end boundaries are not
				// interleaved, the mask hasn't been updated with the span
				// [b,d)@5 yet.  We must update the mask before calling
				// SkipPoint(c@4) to maintain the SpanMask contract and give the
				// mask implementation an opportunity to build the state
				// necessary to be able to determine whether [b,d)@5 masks c@4.
				i.maybeUpdateMask(true /* covered */)

				// The span covers the point key. If a SkipPoint hook is
				// configured, ask it if we should skip this point key.
				if i.mask != nil && i.mask.SkipPoint(i.pointKey.UserKey) {
					i.pointKey, i.pointVal = i.pointIter.Prev()
					continue
				}

				// Point key is unmasked but covered.
				return i.yieldPointKey(true /* covered */)
			}
			return i.yieldPointKey(false /* covered */)
		}
	}
}

// keyspanSeekGE seeks the keyspan iterator to the first span covering k ≥ key.
// Note that this differs from the FragmentIterator.SeekGE semantics, which
// seek to the first span with a start key ≥ key.
func (i *InterleavingIter) keyspanSeekGE(key []byte, prefix []byte) {
	// Seek using SeekLT to look for a span that starts before key, with an end
	// boundary extending beyond key.
	i.span = i.keyspanIter.SeekLT(key)
	if i.span == nil || i.cmp(i.span.End, key) <= 0 {
		// The iterator is exhausted in the reverse direction, or the span we
		// found ends before key. Next to the first key with a start ≥ key.
		i.span = i.keyspanIter.Next()
	}
	i.checkForwardBound(prefix)
	i.savedKeyspan()
}

// keyspanSeekLT seeks the keyspan iterator to the last span covering k < key.
// Note that this differs from the FragmentIterator.SeekLT semantics, which
// seek to the last span with a start key < key.
func (i *InterleavingIter) keyspanSeekLT(key []byte) {
	i.span = i.keyspanIter.SeekLT(key)
	i.checkBackwardBound()
	// The current span's start key is not guaranteed to be less than key,
	// because of the bounds enforcement. Consider the following example:
	//
	// Bounds are set to [d,e). The user performs a SeekLT(d). The
	// FragmentIterator.SeekLT lands on a span [b,f). This span has a start key
	// less than d, as expected. Above, checkBackwardBound truncates the span to
	// match the iterator's current bounds, modifying the span to [d,e), which
	// does not overlap the search space of [-∞, d).
	//
	// This problem is a consequence of the SeekLT's exclusive search key and
	// the fact that we don't perform bounds truncation at every leaf iterator.
	if i.span != nil && i.truncated && i.cmp(i.truncatedSpan.Start, key) >= 0 {
		i.span = nil
	}
	i.savedKeyspan()
}

func (i *InterleavingIter) checkForwardBound(prefix []byte) {
	i.truncated = false
	i.truncatedSpan = Span{}
	if i.span == nil {
		return
	}
	// Check the upper bound if we have one.
	if i.upper != nil && i.cmp(i.span.Start, i.upper) >= 0 {
		i.span = nil
		return
	}

	// TODO(jackson): The key comparisons below truncate bounds whenever the
	// keyspan iterator is repositioned. We could perform this lazily, and do it
	// the first time the user actually asks for this span's bounds in
	// SpanBounds. This would reduce work in the case where there's no span
	// covering the point and the keyspan iterator is non-empty.

	// NB: These truncations don't require setting `keyspanMarkerTruncated`:
	// That flag only applies to truncated span marker keys.
	if i.lower != nil && i.cmp(i.span.Start, i.lower) < 0 {
		i.truncated = true
		i.truncatedSpan = *i.span
		i.truncatedSpan.Start = i.lower
	}
	if i.upper != nil && i.cmp(i.upper, i.span.End) < 0 {
		if !i.truncated {
			i.truncated = true
			i.truncatedSpan = *i.span
		}
		i.truncatedSpan.End = i.upper
	}
	// If this is a part of a SeekPrefixGE call, we may also need to truncate to
	// the prefix's bounds.
	if prefix != nil {
		if !i.truncated {
			i.truncated = true
			i.truncatedSpan = *i.span
		}
		if i.cmp(prefix, i.truncatedSpan.Start) > 0 {
			i.truncatedSpan.Start = prefix
		}
		i.nextPrefixBuf = i.comparer.ImmediateSuccessor(i.nextPrefixBuf[:0], prefix)
		if i.truncated && i.cmp(i.nextPrefixBuf, i.truncatedSpan.End) < 0 {
			i.truncatedSpan.End = i.nextPrefixBuf
		}
	}

	if i.truncated && i.comparer.Equal(i.truncatedSpan.Start, i.truncatedSpan.End) {
		i.span = nil
	}
}

func (i *InterleavingIter) checkBackwardBound() {
	i.truncated = false
	i.truncatedSpan = Span{}
	if i.span == nil {
		return
	}
	// Check the lower bound if we have one.
	if i.lower != nil && i.cmp(i.span.End, i.lower) <= 0 {
		i.span = nil
		return
	}

	// TODO(jackson): The key comparisons below truncate bounds whenever the
	// keyspan iterator is repositioned. We could perform this lazily, and do it
	// the first time the user actually asks for this span's bounds in
	// SpanBounds. This would reduce work in the case where there's no span
	// covering the point and the keyspan iterator is non-empty.

	// NB: These truncations don't require setting `keyspanMarkerTruncated`:
	// That flag only applies to truncated span marker keys.
	if i.lower != nil && i.cmp(i.span.Start, i.lower) < 0 {
		i.truncated = true
		i.truncatedSpan = *i.span
		i.truncatedSpan.Start = i.lower
	}
	if i.upper != nil && i.cmp(i.upper, i.span.End) < 0 {
		if !i.truncated {
			i.truncated = true
			i.truncatedSpan = *i.span
		}
		i.truncatedSpan.End = i.upper
	}
	if i.truncated && i.comparer.Equal(i.truncatedSpan.Start, i.truncatedSpan.End) {
		i.span = nil
	}
}

func (i *InterleavingIter) yieldNil() (*base.InternalKey, []byte) {
	i.spanCoversKey = false
	i.clearMask()
	return i.verify(nil, nil)
}

func (i *InterleavingIter) yieldPointKey(covered bool) (*base.InternalKey, []byte) {
	i.pointKeyInterleaved = true
	i.spanCoversKey = covered
	i.maybeUpdateMask(covered)
	return i.verify(i.pointKey, i.pointVal)
}

func (i *InterleavingIter) yieldSyntheticSpanMarker(lowerBound []byte) (*base.InternalKey, []byte) {
	i.spanMarker.UserKey = i.startKey()
	i.spanMarker.Trailer = base.MakeTrailer(base.InternalKeySeqNumMax, i.span.Keys[0].Kind())
	i.keyspanInterleaved = true
	i.spanCoversKey = true

	// Truncate the key we return to our lower bound if we have one. Note that
	// we use the lowerBound function parameter, not i.lower. The lowerBound
	// argument is guaranteed to be ≥ i.lower. It may be equal to the SetBounds
	// lower bound, or it could come from a SeekGE or SeekPrefixGE search key.
	if lowerBound != nil && i.cmp(lowerBound, i.startKey()) > 0 {
		// Truncating to the lower bound may violate the upper bound if
		// lowerBound == i.upper. For example, a SeekGE(k) uses k as a lower
		// bound for truncating a span. The span a-z will be truncated to [k,
		// z). If i.upper == k, we'd mistakenly try to return a span [k, k), an
		// invariant violation.
		if i.comparer.Equal(lowerBound, i.upper) {
			return i.yieldNil()
		}

		// If the lowerBound argument came from a SeekGE or SeekPrefixGE
		// call, and it may be backed by a user-provided byte slice that is not
		// guaranteed to be stable.
		//
		// If the lowerBound argument is the lower bound set by SetBounds,
		// Pebble owns the slice's memory. However, consider two successive
		// calls to SetBounds(). The second may overwrite the lower bound.
		// Although the external contract requires a seek after a SetBounds,
		// Pebble's tests don't always. For this reason and to simplify
		// reasoning around lifetimes, always copy the bound into keyBuf when
		// truncating.
		i.keyBuf = append(i.keyBuf[:0], lowerBound...)
		i.spanMarker.UserKey = i.keyBuf
		i.spanMarkerTruncated = true
	}
	i.maybeUpdateMask(true /* covered */)
	return i.verify(&i.spanMarker, nil)
}

func (i *InterleavingIter) disablePrefixMode() {
	if i.prefix {
		i.prefix = false
		// Clear the existing span. It may not hold the true end bound of the
		// underlying span.
		i.span = nil
	}
}

func (i *InterleavingIter) verify(k *base.InternalKey, v []byte) (*base.InternalKey, []byte) {
	// Wrap the entire function body in the invariants build tag, so that
	// production builds elide this entire function.
	if invariants.Enabled {
		switch {
		case k != nil && !i.keyspanInterleaved && !i.pointKeyInterleaved:
			panic("pebble: invariant violation: both keys marked as noninterleaved")
		case i.dir == -1 && k != nil && i.keyspanInterleaved == i.pointKeyInterleaved:
			// During reverse iteration, if we're returning a key, either the span's
			// start key must have been interleaved OR the current point key value
			// is being returned, not both.
			//
			// This invariant holds because in reverse iteration the start key of the
			// span behaves like a point. Once the start key is interleaved, we move
			// the keyspan iterator to the previous span.
			panic(fmt.Sprintf("pebble: invariant violation: interleaving (point %t, span %t)",
				i.pointKeyInterleaved, i.keyspanInterleaved))
		case i.dir == -1 && i.spanMarkerTruncated:
			panic("pebble: invariant violation: truncated span key in reverse iteration")
		case k != nil && i.lower != nil && i.cmp(k.UserKey, i.lower) < 0:
			panic("pebble: invariant violation: key < lower bound")
		case k != nil && i.upper != nil && i.cmp(k.UserKey, i.upper) >= 0:
			panic("pebble: invariant violation: key ≥ upper bound")
		case i.span != nil && k != nil && i.mask != nil && i.pointKeyInterleaved &&
			i.cmp(k.UserKey, i.span.Start) >= 0 && i.cmp(k.UserKey, i.span.End) < 0 && i.mask.SkipPoint(k.UserKey):
			panic("pebble: invariant violation: point key eligible for skipping returned")
		}
	}
	return k, v
}

func (i *InterleavingIter) savedKeyspan() {
	i.keyspanInterleaved = false
	i.spanMarkerTruncated = false
	i.maskSpanChangedCalled = false
}

// maybeUpdateMask updates the current mask, if a mask is configured and
// the mask hasn't been updated with the current keyspan yet.
func (i *InterleavingIter) maybeUpdateMask(covered bool) {
	if i.mask != nil {
		if !covered || i.span.Empty() {
			i.clearMask()
		} else if !i.maskSpanChangedCalled {
			if i.truncated {
				i.mask.SpanChanged(&i.truncatedSpan)
			} else {
				i.mask.SpanChanged(i.span)
			}
			i.maskSpanChangedCalled = true
		}
	}
}

// clearMask clears the current mask, if a mask is configured and no mask should
// be active.
func (i *InterleavingIter) clearMask() {
	if i.mask != nil {
		i.maskSpanChangedCalled = false
		i.mask.SpanChanged(nil)
	}
}

func (i *InterleavingIter) startKey() []byte {
	if i.truncated {
		return i.truncatedSpan.Start
	}
	return i.span.Start
}

// Span returns the span covering the last key returned, if any. A span key is
// considered to 'cover' a key if the key falls within the span's user key
// bounds. The returned span is owned by the InterleavingIter. The caller is
// responsible for copying if stability is required.
//
// Span will never return an invalid or empty span.
func (i *InterleavingIter) Span() *Span {
	if !i.spanCoversKey || i.span.Empty() {
		return nil
	} else if i.truncated {
		return &i.truncatedSpan
	} else {
		return i.span
	}
}

// SetBounds implements (base.InternalIterator).SetBounds.
func (i *InterleavingIter) SetBounds(lower, upper []byte) {
	i.lower, i.upper = lower, upper
	i.pointIter.SetBounds(lower, upper)
	i.Invalidate()
}

// Invalidate invalidates the interleaving iterator's current position, clearing
// its state. This prevents optimizations such as reusing the current span on
// seek.
func (i *InterleavingIter) Invalidate() {
	i.span = nil
	i.pointKey = nil
	i.pointVal = nil
}

// Error implements (base.InternalIterator).Error.
func (i *InterleavingIter) Error() error {
	return firstError(i.pointIter.Error(), i.keyspanIter.Error())
}

// Close implements (base.InternalIterator).Close.
func (i *InterleavingIter) Close() error {
	perr := i.pointIter.Close()
	rerr := i.keyspanIter.Close()
	return firstError(perr, rerr)
}

// String implements (base.InternalIterator).String.
func (i *InterleavingIter) String() string {
	return fmt.Sprintf("keyspan-interleaving(%q)", i.pointIter.String())
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
