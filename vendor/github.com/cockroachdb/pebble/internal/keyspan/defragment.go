// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// bufferReuseMaxCapacity is the maximum capacity of a DefragmentingIter buffer
// that DefragmentingIter will reuse. Buffers greater than this will be
// discarded and reallocated as necessary.
const bufferReuseMaxCapacity = 10 << 10 // 10 KB

// DefragmentMethod configures the defragmentation performed by the
// DefragmentingIter.
type DefragmentMethod interface {
	// ShouldDefragment takes two abutting spans and returns whether the two
	// spans should be combined into a single, defragmented Span.
	ShouldDefragment(equal base.Equal, left, right *Span) bool
}

// The DefragmentMethodFunc type is an adapter to allow the use of ordinary
// functions as DefragmentMethods. If f is a function with the appropriate
// signature, DefragmentMethodFunc(f) is a DefragmentMethod that calls f.
type DefragmentMethodFunc func(equal base.Equal, left, right *Span) bool

// ShouldDefragment calls f(equal, left, right).
func (f DefragmentMethodFunc) ShouldDefragment(equal base.Equal, left, right *Span) bool {
	return f(equal, left, right)
}

// DefragmentInternal configures a DefragmentingIter to defragment spans
// only if they have identical keys. It requires spans' keys to be sorted in
// trailer descending order.
//
// This defragmenting method is intended for use in compactions that may see
// internal range keys fragments that may now be joined, because the state that
// required their fragmentation has been dropped.
var DefragmentInternal DefragmentMethod = DefragmentMethodFunc(func(equal base.Equal, a, b *Span) bool {
	if a.KeysOrder != ByTrailerDesc || b.KeysOrder != ByTrailerDesc {
		panic("pebble: span keys unexpectedly not in trailer descending order")
	}
	if len(a.Keys) != len(b.Keys) {
		return false
	}
	for i := range a.Keys {
		if a.Keys[i].Trailer != b.Keys[i].Trailer {
			return false
		}
		if !equal(a.Keys[i].Suffix, b.Keys[i].Suffix) {
			return false
		}
		if !bytes.Equal(a.Keys[i].Value, b.Keys[i].Value) {
			return false
		}
	}
	return true
})

// DefragmentReducer merges the current and next Key slices, returning a new Key
// slice.
//
// Implementations should modify and return `cur` to save on allocations, or
// consider allocating a new slice, as the `cur` slice may be retained by the
// DefragmentingIter and mutated. The `next` slice must not be mutated.
//
// The incoming slices are sorted by (SeqNum, Kind) descending. The output slice
// must also have this sort order.
type DefragmentReducer func(cur, next []Key) []Key

// StaticDefragmentReducer is a no-op DefragmentReducer that simply returns the
// current key slice, effectively retaining the first set of keys encountered
// for a defragmented span.
//
// This reducer can be used, for example, when the set of Keys for each Span
// being reduced is not expected to change, and therefore the keys from the
// first span encountered can be used without considering keys in subsequent
// spans.
var StaticDefragmentReducer DefragmentReducer = func(cur, _ []Key) []Key {
	return cur
}

// iterPos is an enum indicating the position of the defragmenting iter's
// wrapped iter. The defragmenting iter must look ahead or behind when
// defragmenting forward or backwards respectively, and this enum records that
// current position.
type iterPos int8

const (
	iterPosPrev iterPos = -1
	iterPosCurr iterPos = 0
	iterPosNext iterPos = +1
)

// DefragmentingIter wraps a key span iterator, defragmenting physical
// fragmentation during iteration.
//
// During flushes and compactions, keys applied over a span may be split at
// sstable boundaries. This fragmentation can produce internal key bounds that
// do not match any of the bounds ever supplied to a user operation. This
// physical fragmentation is necessary to avoid excessively wide sstables.
//
// The defragmenting iterator undoes this physical fragmentation, joining spans
// with abutting bounds and equal state. The defragmenting iterator takes a
// DefragmentMethod to determine what is "equal state" for a span. The
// DefragmentMethod is a function type, allowing arbitrary comparisons between
// Span keys.
//
// Seeking (SeekGE, SeekLT) poses an obstacle to defragmentation. A seek may
// land on a physical fragment in the middle of several fragments that must be
// defragmented. A seek first degfragments in the opposite direction of
// iteration to find the beginning of the defragmented span, and then
// defragments in the iteration direction, ensuring it's found a whole
// defragmented span.
type DefragmentingIter struct {
	comparer *base.Comparer
	equal    base.Equal
	iter     FragmentIterator
	iterSpan *Span
	iterPos  iterPos

	// curr holds the span at the current iterator position. currBuf is a buffer
	// for use when copying user keys for curr. keysBuf is a buffer for use when
	// copying Keys for curr. currBuf is cleared between positioning methods.
	//
	// keyBuf is a buffer specifically for the defragmented start key when
	// defragmenting backwards or the defragmented end key when defragmenting
	// forwards. These bounds are overwritten repeatedly during defragmentation,
	// and the defragmentation routines overwrite keyBuf repeatedly to store
	// these extended bounds.
	curr    Span
	currBuf []byte
	keysBuf []Key
	keyBuf  []byte

	// method is a comparison function for two spans. method is called when two
	// spans are abutting to determine whether they may be defragmented.
	// method does not itself check for adjacency for the two spans.
	method DefragmentMethod

	// reduce is the reducer function used to collect Keys across all spans that
	// constitute a defragmented span.
	reduce DefragmentReducer
}

// Assert that *DefragmentingIter implements the FragmentIterator interface.
var _ FragmentIterator = (*DefragmentingIter)(nil)

// Init initializes the defragmenting iter using the provided defragment
// method.
func (i *DefragmentingIter) Init(
	comparer *base.Comparer, iter FragmentIterator, equal DefragmentMethod, reducer DefragmentReducer,
) {
	*i = DefragmentingIter{
		comparer: comparer,
		equal:    comparer.Equal,
		iter:     iter,
		method:   equal,
		reduce:   reducer,
	}
}

// Error returns any accumulated error.
func (i *DefragmentingIter) Error() error {
	return i.iter.Error()
}

// Close closes the underlying iterators.
func (i *DefragmentingIter) Close() error {
	return i.iter.Close()
}

// SeekGE seeks the iterator to the first span with a start key greater than or
// equal to key and returns it.
func (i *DefragmentingIter) SeekGE(key []byte) *Span {
	i.iterSpan = i.iter.SeekGE(key)
	if i.iterSpan == nil {
		i.iterPos = iterPosCurr
		return nil
	} else if i.iterSpan.Empty() {
		i.iterPos = iterPosCurr
		return i.iterSpan
	}
	// Save the current span and peek backwards.
	i.saveCurrent()
	i.iterSpan = i.iter.Prev()
	if i.iterSpan != nil && i.equal(i.curr.Start, i.iterSpan.End) && i.checkEqual(i.iterSpan, &i.curr) {
		// A continuation. The span we originally landed on and defragmented
		// backwards has a true Start key < key. To obey the FragmentIterator
		// contract, we must not return this defragmented span. Defragment
		// forward to finish defragmenting the span in the forward direction.
		i.defragmentForward()

		// Now we must be on a span that truly has a defragmented Start key >
		// key.
		return i.defragmentForward()
	}

	// The span previous to i.curr does not defragment, so we should return it.
	// Next the underlying iterator back onto the span we previously saved to
	// i.curr and then defragment forward.
	i.iterSpan = i.iter.Next()
	return i.defragmentForward()
}

// SeekLT seeks the iterator to the last span with a start key less than
// key and returns it.
func (i *DefragmentingIter) SeekLT(key []byte) *Span {
	i.iterSpan = i.iter.SeekLT(key)
	if i.iterSpan == nil {
		i.iterPos = iterPosCurr
		return nil
	} else if i.iterSpan.Empty() {
		i.iterPos = iterPosCurr
		return i.iterSpan
	}
	// Defragment forward to find the end of the defragmented span.
	i.defragmentForward()
	if i.iterPos == iterPosNext {
		// Prev once back onto the span.
		i.iterSpan = i.iter.Prev()
	}
	// Defragment the full span from its end.
	return i.defragmentBackward()
}

// First seeks the iterator to the first span and returns it.
func (i *DefragmentingIter) First() *Span {
	i.iterSpan = i.iter.First()
	if i.iterSpan == nil {
		i.iterPos = iterPosCurr
		return nil
	}
	return i.defragmentForward()
}

// Last seeks the iterator to the last span and returns it.
func (i *DefragmentingIter) Last() *Span {
	i.iterSpan = i.iter.Last()
	if i.iterSpan == nil {
		i.iterPos = iterPosCurr
		return nil
	}
	return i.defragmentBackward()
}

// Next advances to the next span and returns it.
func (i *DefragmentingIter) Next() *Span {
	switch i.iterPos {
	case iterPosPrev:
		// Switching directions; The iterator is currently positioned over the
		// last span of the previous set of fragments. In the below diagram,
		// the iterator is positioned over the last span that contributes to
		// the defragmented x position. We want to be positioned over the first
		// span that contributes to the z position.
		//
		//   x x x y y y z z z
		//       ^       ^
		//      old     new
		//
		// Next once to move onto y, defragment forward to land on the first z
		// position.
		i.iterSpan = i.iter.Next()
		if invariants.Enabled && i.iterSpan == nil {
			panic("pebble: invariant violation: no next span while switching directions")
		}
		// We're now positioned on the first span that was defragmented into the
		// current iterator position. Skip over the rest of the current iterator
		// position's constitutent fragments. In the above example, this would
		// land on the first 'z'.
		i.defragmentForward()
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil
		}

		// Now that we're positioned over the first of the next set of
		// fragments, defragment forward.
		return i.defragmentForward()
	case iterPosCurr:
		// iterPosCurr is only used when the iter is exhausted or when the iterator
		// is at an empty span.
		if invariants.Enabled && i.iterSpan != nil && !i.iterSpan.Empty() {
			panic("pebble: invariant violation: iterPosCurr with valid iterSpan")
		}

		i.iterSpan = i.iter.Next()
		if i.iterSpan == nil {
			return nil
		}
		return i.defragmentForward()
	case iterPosNext:
		// Already at the next span.
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil
		}
		return i.defragmentForward()
	default:
		panic("unreachable")
	}
}

// Prev steps back to the previous span and returns it.
func (i *DefragmentingIter) Prev() *Span {
	switch i.iterPos {
	case iterPosPrev:
		// Already at the previous span.
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil
		}
		return i.defragmentBackward()
	case iterPosCurr:
		// iterPosCurr is only used when the iter is exhausted or when the iterator
		// is at an empty span.
		if invariants.Enabled && i.iterSpan != nil && !i.iterSpan.Empty() {
			panic("pebble: invariant violation: iterPosCurr with valid iterSpan")
		}

		i.iterSpan = i.iter.Prev()
		if i.iterSpan == nil {
			return nil
		}
		return i.defragmentBackward()
	case iterPosNext:
		// Switching directions; The iterator is currently positioned over the
		// first fragment of the next set of fragments. In the below diagram,
		// the iterator is positioned over the first span that contributes to
		// the defragmented z position. We want to be positioned over the last
		// span that contributes to the x position.
		//
		//   x x x y y y z z z
		//       ^       ^
		//      new     old
		//
		// Prev once to move onto y, defragment backward to land on the last x
		// position.
		i.iterSpan = i.iter.Prev()
		if invariants.Enabled && i.iterSpan == nil {
			panic("pebble: invariant violation: no previous span while switching directions")
		}
		// We're now positioned on the last span that was defragmented into the
		// current iterator position. Skip over the rest of the current iterator
		// position's constitutent fragments. In the above example, this would
		// land on the last 'x'.
		i.defragmentBackward()

		// Now that we're positioned over the last of the prev set of
		// fragments, defragment backward.
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil
		}
		return i.defragmentBackward()
	default:
		panic("unreachable")
	}
}

// checkEqual checks the two spans for logical equivalence. It uses the passed-in
// DefragmentMethod and ensures both spans are NOT empty; not defragmenting empty
// spans is an optimization that lets us load fewer sstable blocks.
func (i *DefragmentingIter) checkEqual(left, right *Span) bool {
	return (!left.Empty() && !right.Empty()) && i.method.ShouldDefragment(i.equal, i.iterSpan, &i.curr)
}

// defragmentForward defragments spans in the forward direction, starting from
// i.iter's current position. The span at the current position must be non-nil,
// but may be Empty().
func (i *DefragmentingIter) defragmentForward() *Span {
	if i.iterSpan.Empty() {
		// An empty span will never be equal to another span; see checkEqual for
		// why. To avoid loading non-empty range keys further ahead by calling Next,
		// return early.
		i.iterPos = iterPosCurr
		return i.iterSpan
	}
	i.saveCurrent()

	i.iterPos = iterPosNext
	i.iterSpan = i.iter.Next()
	for i.iterSpan != nil {
		if !i.equal(i.curr.End, i.iterSpan.Start) {
			// Not a continuation.
			break
		}
		if !i.checkEqual(i.iterSpan, &i.curr) {
			// Not a continuation.
			break
		}
		i.keyBuf = append(i.keyBuf[:0], i.iterSpan.End...)
		i.curr.End = i.keyBuf
		i.keysBuf = i.reduce(i.keysBuf, i.iterSpan.Keys)
		i.iterSpan = i.iter.Next()
	}
	i.curr.Keys = i.keysBuf
	return &i.curr
}

// defragmentBackward defragments spans in the backward direction, starting from
// i.iter's current position. The span at the current position must be non-nil,
// but may be Empty().
func (i *DefragmentingIter) defragmentBackward() *Span {
	if i.iterSpan.Empty() {
		// An empty span will never be equal to another span; see checkEqual for
		// why. To avoid loading non-empty range keys further ahead by calling Next,
		// return early.
		i.iterPos = iterPosCurr
		return i.iterSpan
	}
	i.saveCurrent()

	i.iterPos = iterPosPrev
	i.iterSpan = i.iter.Prev()
	for i.iterSpan != nil {
		if !i.equal(i.curr.Start, i.iterSpan.End) {
			// Not a continuation.
			break
		}
		if !i.checkEqual(i.iterSpan, &i.curr) {
			// Not a continuation.
			break
		}
		i.keyBuf = append(i.keyBuf[:0], i.iterSpan.Start...)
		i.curr.Start = i.keyBuf
		i.keysBuf = i.reduce(i.keysBuf, i.iterSpan.Keys)
		i.iterSpan = i.iter.Prev()
	}
	i.curr.Keys = i.keysBuf
	return &i.curr
}

func (i *DefragmentingIter) saveCurrent() {
	i.currBuf = i.currBuf[:0]
	i.keysBuf = i.keysBuf[:0]
	i.keyBuf = i.keyBuf[:0]
	if cap(i.currBuf) > bufferReuseMaxCapacity {
		i.currBuf = nil
	}
	if cap(i.keyBuf) > bufferReuseMaxCapacity {
		i.keyBuf = nil
	}
	if i.iterSpan == nil {
		return
	}
	i.curr = Span{
		Start:     i.saveBytes(i.iterSpan.Start),
		End:       i.saveBytes(i.iterSpan.End),
		KeysOrder: i.iterSpan.KeysOrder,
	}
	for j := range i.iterSpan.Keys {
		i.keysBuf = append(i.keysBuf, Key{
			Trailer: i.iterSpan.Keys[j].Trailer,
			Suffix:  i.saveBytes(i.iterSpan.Keys[j].Suffix),
			Value:   i.saveBytes(i.iterSpan.Keys[j].Value),
		})
	}
	i.curr.Keys = i.keysBuf
}

func (i *DefragmentingIter) saveBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	ret := append(i.currBuf, b...)
	i.currBuf = ret[len(ret):]
	return ret
}
