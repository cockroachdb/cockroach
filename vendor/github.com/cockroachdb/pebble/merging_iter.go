// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"runtime/debug"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

type mergingIterLevel struct {
	iter internalIterator
	// rangeDelIter is set to the range-deletion iterator for the level. When
	// configured with a levelIter, this pointer changes as sstable boundaries
	// are crossed. See levelIter.initRangeDel and the Range Deletions comment
	// below.
	rangeDelIter keyspan.FragmentIterator
	// iterKey and iterValue cache the current key and value iter are pointed at.
	iterKey   *InternalKey
	iterValue []byte

	// levelIterBoundaryContext's fields are set when using levelIter, in order
	// to surface sstable boundary keys and file-level context. See levelIter
	// comment and the Range Deletions comment below.
	levelIterBoundaryContext

	// tombstone caches the tombstone rangeDelIter is currently pointed at. If
	// tombstone is nil, there are no further tombstones within the
	// current sstable in the current iterator direction. The cached tombstone is
	// only valid for the levels in the range [0,heap[0].index]. This avoids
	// positioning tombstones at lower levels which cannot possibly shadow the
	// current key.
	tombstone *keyspan.Span
}

type levelIterBoundaryContext struct {
	// smallestUserKey and largestUserKey are populated with the smallest and
	// largest boundaries of the current file.
	smallestUserKey, largestUserKey []byte
	// isLargestUserKeyRangeDelSentinel is set to true when a file's largest
	// boundary is an exclusive range deletion sentinel. If true, the file does
	// not contain any keys with the provided user key, and the largestUserKey
	// bound is exclusive.
	isLargestUserKeyRangeDelSentinel bool
	// isSyntheticIterBoundsKey is set to true iff the key returned by the level
	// iterator is a synthetic key derived from the iterator bounds. This is
	// used to prevent the mergingIter from being stuck at such a synthetic key
	// if it becomes the top element of the heap.
	isSyntheticIterBoundsKey bool
	// isIgnorableBoundaryKey is set to true iff the key returned by the level
	// iterator is a file boundary key that should be ignored. This is used to
	// keep a levelIter file's range deletion iterator open as long as other
	// levels within the merging iterator require it.
	isIgnorableBoundaryKey bool
}

// mergingIter provides a merged view of multiple iterators from different
// levels of the LSM.
//
// The core of a mergingIter is a heap of internalIterators (see
// mergingIterHeap). The heap can operate as either a min-heap, used during
// forward iteration (First, SeekGE, Next) or a max-heap, used during reverse
// iteration (Last, SeekLT, Prev). The heap is initialized in calls to First,
// Last, SeekGE, and SeekLT. A call to Next or Prev takes the current top
// element on the heap, advances its iterator, and then "fixes" the heap
// property. When one of the child iterators is exhausted during Next/Prev
// iteration, it is removed from the heap.
//
// Range Deletions
//
// A mergingIter can optionally be configured with a slice of range deletion
// iterators. The range deletion iterator slice must exactly parallel the point
// iterators and the range deletion iterator must correspond to the same level
// in the LSM as the point iterator. Note that each memtable and each table in
// L0 is a different "level" from the mergingIter perspective. So level 0 below
// does not correspond to L0 in the LSM.
//
// A range deletion iterator iterates over fragmented range tombstones. Range
// tombstones are fragmented by splitting them at any overlapping points. This
// fragmentation guarantees that within an sstable tombstones will either be
// distinct or will have identical start and end user keys. While range
// tombstones are fragmented within an sstable, the start and end keys are not truncated
// to sstable boundaries. This is necessary because the tombstone end key is
// exclusive and does not have a sequence number. Consider an sstable
// containing the range tombstone [a,c)#9 and the key "b#8". The tombstone must
// delete "b#8", yet older versions of "b" might spill over to the next
// sstable. So the boundary key for this sstable must be "b#8". Adjusting the
// end key of tombstones to be optionally inclusive or contain a sequence
// number would be possible solutions (such solutions have potentially serious
// issues: tombstones have exclusive end keys since an inclusive deletion end can
// be converted to an exclusive one while the reverse transformation is not possible;
// the semantics of a sequence number for the end key of a range tombstone are murky).
//
// The approach taken here performs an
// implicit truncation of the tombstone to the sstable boundaries.
//
// During initialization of a mergingIter, the range deletion iterators for
// batches, memtables, and L0 tables are populated up front. Note that Batches
// and memtables index unfragmented tombstones.  Batch.newRangeDelIter() and
// memTable.newRangeDelIter() fragment and cache the tombstones on demand. The
// L1-L6 range deletion iterators are populated by levelIter. When configured
// to load range deletion iterators, whenever a levelIter loads a table it
// loads both the point iterator and the range deletion
// iterator. levelIter.rangeDelIter is configured to point to the right entry
// in mergingIter.levels. The effect of this setup is that
// mergingIter.levels[i].rangeDelIter always contains the fragmented range
// tombstone for the current table in level i that the levelIter has open.
//
// Another crucial mechanism of levelIter is that it materializes fake point
// entries for the table boundaries if the boundary is range deletion
// key. Consider a table that contains only a range tombstone [a-e)#10. The
// sstable boundaries for this table will be a#10,15 and
// e#72057594037927935,15. During forward iteration levelIter will return
// e#72057594037927935,15 as a key. During reverse iteration levelIter will
// return a#10,15 as a key. These sentinel keys act as bookends to point
// iteration and allow mergingIter to keep a table and its associated range
// tombstones loaded as long as there are keys at lower levels that are within
// the bounds of the table.
//
// The final piece to the range deletion puzzle is the LSM invariant that for a
// given key K newer versions of K can only exist earlier in the level, or at
// higher levels of the tree. For example, if K#4 exists in L3, k#5 can only
// exist earlier in the L3 or in L0, L1, L2 or a memtable. Get very explicitly
// uses this invariant to find the value for a key by walking the LSM level by
// level. For range deletions, this invariant means that a range deletion at
// level N will necessarily shadow any keys within its bounds in level Y where
// Y > N. One wrinkle to this statement is that it only applies to keys that
// lie within the sstable bounds as well, but we get that guarantee due to the
// way the range deletion iterator and point iterator are bound together by a
// levelIter.
//
// Tying the above all together, we get a picture where each level (index in
// mergingIter.levels) is composed of both point operations (pX) and range
// deletions (rX). The range deletions for level X shadow both the point
// operations and range deletions for level Y where Y > X allowing mergingIter
// to skip processing entries in that shadow. For example, consider the
// scenario:
//
//   r0: a---e
//   r1:    d---h
//   r2:       g---k
//   r3:          j---n
//   r4:             m---q
//
// This is showing 5 levels of range deletions. Consider what happens upon
// SeekGE("b"). We first seek the point iterator for level 0 (the point values
// are not shown above) and we then seek the range deletion iterator. That
// returns the tombstone [a,e). This tombstone tells us that all keys in the
// range [a,e) in lower levels are deleted so we can skip them. So we can
// adjust the seek key to "e", the tombstone end key. For level 1 we seek to
// "e" and find the range tombstone [d,h) and similar logic holds. By the time
// we get to level 4 we're seeking to "n".
//
// One consequence of not truncating tombstone end keys to sstable boundaries
// is the seeking process described above cannot always seek to the tombstone
// end key in the older level. For example, imagine in the above example r3 is
// a partitioned level (i.e., L1+ in our LSM), and the sstable containing [j,
// n) has "k" as its upper boundary. In this situation, compactions involving
// keys at or after "k" can output those keys to r4+, even if they're newer
// than our tombstone [j, n). So instead of seeking to "n" in r4 we can only
// seek to "k".  To achieve this, the instance variable `largestUserKey.`
// maintains the upper bounds of the current sstables in the partitioned
// levels. In this example, `levels[3].largestUserKey` holds "k", telling us to
// limit the seek triggered by a tombstone in r3 to "k".
//
// During actual iteration levels can contain both point operations and range
// deletions. Within a level, when a range deletion contains a point operation
// the sequence numbers must be checked to determine if the point operation is
// newer or older than the range deletion tombstone. The mergingIter maintains
// the invariant that the range deletion iterators for all levels newer that
// the current iteration key (L < m.heap.items[0].index) are positioned at the
// next (or previous during reverse iteration) range deletion tombstone. We
// know those levels don't contain a range deletion tombstone that covers the
// current key because if they did the current key would be deleted. The range
// deletion iterator for the current key's level is positioned at a range
// tombstone covering or past the current key. The position of all of other
// range deletion iterators is unspecified. Whenever a key from those levels
// becomes the current key, their range deletion iterators need to be
// positioned. This lazy positioning avoids seeking the range deletion
// iterators for keys that are never considered. (A similar bit of lazy
// evaluation can be done for the point iterators, but is still TBD).
//
// For a full example, consider the following setup:
//
//   p0:               o
//   r0:             m---q
//
//   p1:              n p
//   r1:       g---k
//
//   p2:  b d    i
//   r2: a---e           q----v
//
//   p3:     e
//   r3:
//
// If we start iterating from the beginning, the first key we encounter is "b"
// in p2. When the mergingIter is pointing at a valid entry, the range deletion
// iterators for all of the levels < m.heap.items[0].index are positioned at
// the next range tombstone past the current key. So r0 will point at [m,q) and
// r1 at [g,k). When the key "b" is encountered, we check to see if the current
// tombstone for r0 or r1 contains it, and whether the tombstone for r2, [a,e),
// contains and is newer than "b".
//
// Advancing the iterator finds the next key at "d". This is in the same level
// as the previous key "b" so we don't have to reposition any of the range
// deletion iterators, but merely check whether "d" is now contained by any of
// the range tombstones at higher levels or has stepped past the range
// tombstone in its own level or higher levels. In this case, there is nothing to be done.
//
// Advancing the iterator again finds "e". Since "e" comes from p3, we have to
// position the r3 range deletion iterator, which is empty. "e" is past the r2
// tombstone of [a,e) so we need to advance the r2 range deletion iterator to
// [q,v).
//
// The next key is "i". Because this key is in p2, a level above "e", we don't
// have to reposition any range deletion iterators and instead see that "i" is
// covered by the range tombstone [g,k). The iterator is immediately advanced
// to "n" which is covered by the range tombstone [m,q) causing the iterator to
// advance to "o" which is visible.
//
// TODO(peter,rangedel): For testing, advance the iterator through various
// scenarios and have each step display the current state (i.e. the current
// heap and range-del iterator positioning).
type mergingIter struct {
	logger   Logger
	split    Split
	dir      int
	snapshot uint64
	levels   []mergingIterLevel
	heap     mergingIterHeap
	err      error
	prefix   []byte
	lower    []byte
	upper    []byte
	stats    *InternalIteratorStats

	combinedIterState *combinedIterState

	// Elide range tombstones from being returned during iteration. Set to true
	// when mergingIter is a child of Iterator and the mergingIter is processing
	// range tombstones.
	elideRangeTombstones bool
}

// mergingIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*mergingIter)(nil)

// newMergingIter returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp. It is permissible to
// pass a nil split parameter if the caller is never going to call
// SeekPrefixGE.
//
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
//
// None of the iters may be nil.
func newMergingIter(
	logger Logger,
	stats *base.InternalIteratorStats,
	cmp Compare,
	split Split,
	iters ...internalIterator,
) *mergingIter {
	m := &mergingIter{}
	levels := make([]mergingIterLevel, len(iters))
	for i := range levels {
		levels[i].iter = iters[i]
	}
	m.init(&IterOptions{logger: logger}, stats, cmp, split, levels...)
	return m
}

func (m *mergingIter) init(
	opts *IterOptions,
	stats *base.InternalIteratorStats,
	cmp Compare,
	split Split,
	levels ...mergingIterLevel,
) {
	m.err = nil // clear cached iteration error
	m.logger = opts.getLogger()
	if opts != nil {
		m.lower = opts.LowerBound
		m.upper = opts.UpperBound
	}
	m.snapshot = InternalKeySeqNumMax
	m.levels = levels
	m.heap.cmp = cmp
	m.split = split
	m.stats = stats
	if cap(m.heap.items) < len(levels) {
		m.heap.items = make([]mergingIterItem, 0, len(levels))
	} else {
		m.heap.items = m.heap.items[:0]
	}
}

func (m *mergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		if l := &m.levels[i]; l.iterKey != nil {
			m.heap.items = append(m.heap.items, mergingIterItem{
				index: i,
				key:   *l.iterKey,
				value: l.iterValue,
			})
		} else {
			m.err = firstError(m.err, l.iter.Error())
			if m.err != nil {
				return
			}
		}
	}
	m.heap.init()
}

func (m *mergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
	m.initMinRangeDelIters(-1)
}

// The level of the previous top element was oldTopLevel. Note that all range delete
// iterators < oldTopLevel are positioned past the key of the previous top element and
// the range delete iterator == oldTopLevel is positioned at or past the key of the
// previous top element. We need to position the range delete iterators from oldTopLevel + 1
// to the level of the current top element.
func (m *mergingIter) initMinRangeDelIters(oldTopLevel int) {
	if m.heap.len() == 0 {
		return
	}

	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := &m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil {
			continue
		}
		l.tombstone = keyspan.SeekGE(m.heap.cmp, l.rangeDelIter, item.key.UserKey)
	}
}

func (m *mergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
	m.initMaxRangeDelIters(-1)
}

// The level of the previous top element was oldTopLevel. Note that all range delete
// iterators < oldTopLevel are positioned before the key of the previous top element and
// the range delete iterator == oldTopLevel is positioned at or before the key of the
// previous top element. We need to position the range delete iterators from oldTopLevel + 1
// to the level of the current top element.
func (m *mergingIter) initMaxRangeDelIters(oldTopLevel int) {
	if m.heap.len() == 0 {
		return
	}
	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := &m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil {
			continue
		}
		l.tombstone = keyspan.SeekLE(m.heap.cmp, l.rangeDelIter, item.key.UserKey)
	}
}

func (m *mergingIter) switchToMinHeap() {
	if m.heap.len() == 0 {
		if m.lower != nil {
			m.SeekGE(m.lower, base.SeekGEFlagsNone)
		} else {
			m.First()
		}
		return
	}

	// We're switching from using a max heap to a min heap. We need to advance
	// any iterator that is less than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
	//
	// i1:     *a:2     b:2
	// i2: a:1      b:1
	//
	// The current key is a:2 and i2 is pointed at a:1. When we switch to forward
	// iteration, we want to return a key that is greater than a:2.

	key := m.heap.items[0].key
	cur := &m.levels[m.heap.items[0].index]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}

		// If the iterator is exhausted, it may be out of bounds if range
		// deletions modified our search key as we descended. we need to
		// reposition it within the search bounds. If the current key is a
		// range tombstone, the iterator might still be exhausted but at a
		// sstable boundary sentinel. It would be okay to reposition an
		// interator like this only through successive Next calls, except that
		// it would violate the levelIter's invariants by causing it to return
		// a key before the lower bound.
		//
		//           bounds = [ f, _ )
		// L0:   [ b ]          [ f*                   z ]
		// L1: [ a           |----|        k        y ]
		// L2:    [  c  (d) ] [ e      g     m ]
		// L3:             [                    x ]
		//
		// * - current key   [] - table bounds () - heap item
		//
		// In the above diagram, the L2 iterator is positioned at a sstable
		// boundary (d) outside the lower bound (f). It arrived here from a
		// seek whose seek-key was modified by a range tombstone. If we called
		// Next on the L2 iterator, it would return e, violating its lower
		// bound.  Instead, we seek it to >= f and Next from there.

		if l.iterKey == nil || (m.lower != nil && l.isSyntheticIterBoundsKey &&
			l.iterKey.IsExclusiveSentinel() &&
			m.heap.cmp(l.iterKey.UserKey, m.lower) <= 0) {
			if m.lower != nil {
				l.iterKey, l.iterValue = l.iter.SeekGE(m.lower, base.SeekGEFlagsNone)
			} else {
				l.iterKey, l.iterValue = l.iter.First()
			}
		}
		for ; l.iterKey != nil; l.iterKey, l.iterValue = l.iter.Next() {
			if base.InternalCompare(m.heap.cmp, key, *l.iterKey) < 0 {
				// key < iter-key
				break
			}
			// key >= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above. The iterator cur.iter may still be exhausted at a sstable boundary
	// sentinel. Similar to the logic applied to the other levels, in these
	// cases we seek the iterator to the first key in order to avoid violating
	// levelIter's invariants. See the example in the for loop above.
	if m.lower != nil && cur.isSyntheticIterBoundsKey && cur.iterKey.IsExclusiveSentinel() &&
		m.heap.cmp(cur.iterKey.UserKey, m.lower) <= 0 {
		cur.iterKey, cur.iterValue = cur.iter.SeekGE(m.lower, base.SeekGEFlagsNone)
	} else {
		cur.iterKey, cur.iterValue = cur.iter.Next()
	}
	m.initMinHeap()
}

func (m *mergingIter) switchToMaxHeap() {
	if m.heap.len() == 0 {
		if m.upper != nil {
			m.SeekLT(m.upper, base.SeekLTFlagsNone)
		} else {
			m.Last()
		}
		return
	}

	// We're switching from using a min heap to a max heap. We need to backup any
	// iterator that is greater than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
	//
	// i1: a:2     *b:2
	// i2:     a:1      b:1
	//
	// The current key is b:2 and i2 is pointing at b:1. When we switch to
	// reverse iteration, we want to return a key that is less than b:2.
	key := m.heap.items[0].key
	cur := &m.levels[m.heap.items[0].index]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}

		// If the iterator is exhausted, it may be out of bounds if range
		// deletions modified our search key as we descended. we need to
		// reposition it within the search bounds. If the current key is a
		// range tombstone, the iterator might still be exhausted but at a
		// sstable boundary sentinel. It would be okay to reposition an
		// interator like this only through successive Prev calls, except that
		// it would violate the levelIter's invariants by causing it to return
		// a key beyond the upper bound.
		//
		//           bounds = [ _, g )
		// L0:   [ b ]          [ f*                   z ]
		// L1: [ a                |-------| k       y ]
		// L2:    [  c   d  ]        h [(i)    m ]
		// L3:             [  e                  x ]
		//
		// * - current key   [] - table bounds () - heap item
		//
		// In the above diagram, the L2 iterator is positioned at a sstable
		// boundary (i) outside the upper bound (g). It arrived here from a
		// seek whose seek-key was modified by a range tombstone. If we called
		// Prev on the L2 iterator, it would return h, violating its upper
		// bound.  Instead, we seek it to < g, and Prev from there.

		if l.iterKey == nil || (m.upper != nil && l.isSyntheticIterBoundsKey &&
			l.iterKey.IsExclusiveSentinel() && m.heap.cmp(l.iterKey.UserKey, m.upper) >= 0) {
			if m.upper != nil {
				l.iterKey, l.iterValue = l.iter.SeekLT(m.upper, base.SeekLTFlagsNone)
			} else {
				l.iterKey, l.iterValue = l.iter.Last()
			}
		}
		for ; l.iterKey != nil; l.iterKey, l.iterValue = l.iter.Prev() {
			if base.InternalCompare(m.heap.cmp, key, *l.iterKey) > 0 {
				// key > iter-key
				break
			}
			// key <= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above. The iterator cur.iter may still be exhausted at a sstable boundary
	// sentinel. Similar to the logic applied to the other levels, in these
	// cases we seek the iterator to  in order to avoid violating levelIter's
	// invariants by Prev-ing through files.  See the example in the for loop
	// above.
	if m.upper != nil && cur.isSyntheticIterBoundsKey && cur.iterKey.IsExclusiveSentinel() &&
		m.heap.cmp(cur.iterKey.UserKey, m.upper) >= 0 {
		cur.iterKey, cur.iterValue = cur.iter.SeekLT(m.upper, base.SeekLTFlagsNone)
	} else {
		cur.iterKey, cur.iterValue = cur.iter.Prev()
	}
	m.initMaxHeap()
}

// Steps to the next entry. item is the current top item in the heap.
func (m *mergingIter) nextEntry(item *mergingIterItem) {
	l := &m.levels[item.index]
	oldTopLevel := item.index
	oldRangeDelIter := l.rangeDelIter
	if l.iterKey, l.iterValue = l.iter.Next(); l.iterKey != nil {
		item.key, item.value = *l.iterKey, l.iterValue
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
		if l.rangeDelIter != oldRangeDelIter {
			// The rangeDelIter changed which indicates that the l.iter moved to the
			// next sstable. We have to update the tombstone for oldTopLevel as well.
			oldTopLevel--
		}
	} else {
		m.err = l.iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}

	// The cached tombstones are only valid for the levels
	// [0,oldTopLevel]. Updated the cached tombstones for any levels in the range
	// [oldTopLevel+1,heap[0].index].
	m.initMinRangeDelIters(oldTopLevel)
}

// isNextEntryDeleted() starts from the current entry (as the next entry) and if it is deleted,
// moves the iterators forward as needed and returns true, else it returns false. item is the top
// item in the heap.
func (m *mergingIter) isNextEntryDeleted(item *mergingIterItem) bool {
	// Look for a range deletion tombstone containing item.key at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil || l.tombstone == nil {
			// If l.tombstone is nil, there are no further tombstones
			// in the current sstable in the current (forward) iteration
			// direction.
			continue
		}
		if m.heap.cmp(l.tombstone.End, item.key.UserKey) <= 0 {
			// The current key is at or past the tombstone end key.
			//
			// NB: for the case that this l.rangeDelIter is provided by a levelIter we know that
			// the levelIter must be positioned at a key >= item.key. So it is sufficient to seek the
			// current l.rangeDelIter (since any range del iterators that will be provided by the
			// levelIter in the future cannot contain item.key). Also, it is possible that we
			// will encounter parts of the range delete that should be ignored -- we handle that
			// below.
			l.tombstone = keyspan.SeekGE(m.heap.cmp, l.rangeDelIter, item.key.UserKey)
		}
		if l.tombstone == nil {
			continue
		}

		// Reasoning for correctness of untruncated tombstone handling when the untruncated
		// tombstone is at a higher level:
		// The iterator corresponding to this tombstone is still in the heap so it must be
		// positioned >= item.key. Which means the Largest key bound of the sstable containing this
		// tombstone is >= item.key. So the upper limit of this tombstone cannot be file-bounds-constrained
		// to < item.key. But it is possible that item.key < smallestUserKey, in which
		// case this tombstone should be ignored.
		//
		// Example 1:
		// sstable bounds [c#8, g#12] containing a tombstone [b, i)#7, and key is c#6. The
		// smallestUserKey is c, so we know the key is within the file bounds and the tombstone
		// [b, i) covers it.
		//
		// Example 2:
		// Same sstable bounds but key is b#10. The smallestUserKey is c, so the tombstone [b, i)
		// does not cover this key.
		//
		// For a tombstone at the same level as the key, the file bounds are trivially satisfied.
		if (l.smallestUserKey == nil || m.heap.cmp(l.smallestUserKey, item.key.UserKey) <= 0) &&
			l.tombstone.VisibleAt(m.snapshot) && l.tombstone.Contains(m.heap.cmp, item.key.UserKey) {
			if level < item.index {
				// We could also do m.seekGE(..., level + 1). The levels from
				// [level + 1, item.index) are already after item.key so seeking them may be
				// wasteful.

				// We can seek up to the min of largestUserKey and tombstone.End.
				//
				// Using example 1 above, we can seek to the smaller of g and i, which is g.
				//
				// Another example, where the sstable bounds are [c#8, i#InternalRangeDelSentinel],
				// and the tombstone is [b, i)#8. Seeking to i is correct since it is seeking up to
				// the exclusive bound of the tombstone. We do not need to look at
				// isLargestKeyRangeDelSentinel.
				//
				// Progress argument: Since this file is at a higher level than item.key we know
				// that the iterator in this file must be positioned within its bounds and at a key
				// X > item.key (otherwise it would be the min of the heap). It is not
				// possible for X.UserKey == item.key.UserKey, since it is incompatible with
				// X > item.key (a lower version cannot be in a higher sstable), so it must be that
				// X.UserKey > item.key.UserKey. Which means l.largestUserKey > item.key.UserKey.
				// We also know that l.tombstone.End > item.key.UserKey. So the min of these,
				// seekKey, computed below, is > item.key.UserKey, so the call to seekGE() will
				// make forward progress.
				seekKey := l.tombstone.End
				if l.largestUserKey != nil && m.heap.cmp(l.largestUserKey, seekKey) < 0 {
					seekKey = l.largestUserKey
				}
				// This seek is not directly due to a SeekGE call, so we don't
				// know enough about the underlying iterator positions, and so
				// we keep the try-seek-using-next optimization disabled.
				//
				// Additionally, we set the relative-seek flag. This is
				// important when iterating with lazy combined iteration. If
				// there's a range key between this level's current file and the
				// file the seek will land on, we need to detect it in order to
				// trigger construction of the combined iterator.
				m.seekGE(seekKey, item.index, base.SeekGEFlagsNone.EnableRelativeSeek())
				return true
			}
			if l.tombstone.CoversAt(m.snapshot, item.key.SeqNum()) {
				m.nextEntry(item)
				return true
			}
		}
	}
	return false
}

// Starting from the current entry, finds the first (next) entry that can be returned.
func (m *mergingIter) findNextEntry() (*InternalKey, []byte) {
	var reseeked bool
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		if m.levels[item.index].isSyntheticIterBoundsKey {
			break
		}
		// For prefix iteration, stop if we already seeked the iterator due to a
		// range tombstone and are now past the prefix. We could amortize the
		// cost of this comparison, by doing it only after we have iterated in
		// this for loop a few times. But unless we find a performance benefit
		// to that, we do the simple thing and compare each time. Note that
		// isNextEntryDeleted already did at least 4 key comparisons in order to
		// return true, and additionally at least one heap comparison to step to
		// the next entry.
		//
		// Note that we cannot move this comparison into the isNextEntryDeleted
		// branch. Once isNextEntryDeleted determines a key is deleted and seeks
		// the level's iterator, item.key's memory is potentially invalid. If
		// the iterator is now exhausted, item.key may be garbage.
		if m.prefix != nil && reseeked {
			if n := m.split(item.key.UserKey); !bytes.Equal(m.prefix, item.key.UserKey[:n]) {
				return nil, nil
			}
		}

		m.addItemStats(item)
		if m.isNextEntryDeleted(item) {
			m.stats.PointsCoveredByRangeTombstones++
			reseeked = true
			continue
		}
		if item.key.Visible(m.snapshot) &&
			(!m.levels[item.index].isIgnorableBoundaryKey) &&
			(item.key.Kind() != InternalKeyKindRangeDelete || !m.elideRangeTombstones) {
			return &item.key, item.value
		}
		m.nextEntry(item)
	}
	return nil, nil
}

// Steps to the prev entry. item is the current top item in the heap.
func (m *mergingIter) prevEntry(item *mergingIterItem) {
	l := &m.levels[item.index]
	oldTopLevel := item.index
	oldRangeDelIter := l.rangeDelIter
	if l.iterKey, l.iterValue = l.iter.Prev(); l.iterKey != nil {
		item.key, item.value = *l.iterKey, l.iterValue
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
		if l.rangeDelIter != oldRangeDelIter && l.rangeDelIter != nil {
			// The rangeDelIter changed which indicates that the l.iter moved to the
			// previous sstable. We have to update the tombstone for oldTopLevel as
			// well.
			oldTopLevel--
		}
	} else {
		m.err = l.iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}

	// The cached tombstones are only valid for the levels
	// [0,oldTopLevel]. Updated the cached tombstones for any levels in the range
	// [oldTopLevel+1,heap[0].index].
	m.initMaxRangeDelIters(oldTopLevel)
}

// isPrevEntryDeleted() starts from the current entry (as the prev entry) and if it is deleted,
// moves the iterators backward as needed and returns true, else it returns false. item is the top
// item in the heap.
func (m *mergingIter) isPrevEntryDeleted(item *mergingIterItem) bool {
	// Look for a range deletion tombstone containing item.key at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil || l.tombstone == nil {
			// If l.tombstone is nil, there are no further tombstones
			// in the current sstable in the current (reverse) iteration
			// direction.
			continue
		}
		if m.heap.cmp(item.key.UserKey, l.tombstone.Start) < 0 {
			// The current key is before the tombstone start key.
			//
			// NB: for the case that this l.rangeDelIter is provided by a levelIter we know that
			// the levelIter must be positioned at a key < item.key. So it is sufficient to seek the
			// current l.rangeDelIter (since any range del iterators that will be provided by the
			// levelIter in the future cannot contain item.key). Also, it is it is possible that we
			// will encounter parts of the range delete that should be ignored -- we handle that
			// below.
			l.tombstone = keyspan.SeekLE(m.heap.cmp, l.rangeDelIter, item.key.UserKey)
		}
		if l.tombstone == nil {
			continue
		}

		// Reasoning for correctness of untruncated tombstone handling when the untruncated
		// tombstone is at a higher level:
		//
		// The iterator corresponding to this tombstone is still in the heap so it must be
		// positioned <= item.key. Which means the Smallest key bound of the sstable containing this
		// tombstone is <= item.key. So the lower limit of this tombstone cannot have been
		// file-bounds-constrained to > item.key. But it is possible that item.key >= Largest
		// key bound of this sstable, in which case this tombstone should be ignored.
		//
		// Example 1:
		// sstable bounds [c#8, g#12] containing a tombstone [b, i)#7, and key is f#6. The
		// largestUserKey is g, so we know the key is within the file bounds and the tombstone
		// [b, i) covers it.
		//
		// Example 2:
		// Same sstable but the key is g#6. This cannot happen since the [b, i)#7 untruncated
		// tombstone was involved in a compaction which must have had a file to the right of this
		// sstable that is part of the same atomic compaction group for future compactions. That
		// file must have bounds that cover g#6 and this levelIter must be at that file.
		//
		// Example 3:
		// sstable bounds [c#8, g#RangeDelSentinel] containing [b, i)#7 and the key is g#10.
		// This key is not deleted by this tombstone. We need to look at
		// isLargestUserKeyRangeDelSentinel.
		//
		// For a tombstone at the same level as the key, the file bounds are trivially satisfied.

		// Default to within bounds.
		withinLargestSSTableBound := true
		if l.largestUserKey != nil {
			cmpResult := m.heap.cmp(l.largestUserKey, item.key.UserKey)
			withinLargestSSTableBound = cmpResult > 0 || (cmpResult == 0 && !l.isLargestUserKeyRangeDelSentinel)
		}
		if withinLargestSSTableBound && l.tombstone.Contains(m.heap.cmp, item.key.UserKey) && l.tombstone.VisibleAt(m.snapshot) {
			if level < item.index {
				// We could also do m.seekLT(..., level + 1). The levels from
				// [level + 1, item.index) are already before item.key so seeking them may be
				// wasteful.

				// We can seek up to the max of smallestUserKey and tombstone.Start.UserKey.
				//
				// Using example 1 above, we can seek to the larger of c and b, which is c.
				//
				// Progress argument: We know that the iterator in this file is positioned within
				// its bounds and at a key X < item.key (otherwise it would be the max of the heap).
				// So smallestUserKey <= item.key.UserKey and we already know that
				// l.tombstone.Start.UserKey <= item.key.UserKey. So the seekKey computed below
				// is <= item.key.UserKey, and since we do a seekLT() we will make backwards
				// progress.
				seekKey := l.tombstone.Start
				if l.smallestUserKey != nil && m.heap.cmp(l.smallestUserKey, seekKey) > 0 {
					seekKey = l.smallestUserKey
				}
				// We set the relative-seek flag. This is important when
				// iterating with lazy combined iteration. If there's a range
				// key between this level's current file and the file the seek
				// will land on, we need to detect it in order to trigger
				// construction of the combined iterator.
				m.seekLT(seekKey, item.index, base.SeekLTFlagsNone.EnableRelativeSeek())
				return true
			}
			if l.tombstone.CoversAt(m.snapshot, item.key.SeqNum()) {
				m.prevEntry(item)
				return true
			}
		}
	}
	return false
}

// Starting from the current entry, finds the first (prev) entry that can be returned.
func (m *mergingIter) findPrevEntry() (*InternalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		if m.levels[item.index].isSyntheticIterBoundsKey {
			break
		}
		m.addItemStats(item)
		if m.isPrevEntryDeleted(item) {
			m.stats.PointsCoveredByRangeTombstones++
			continue
		}
		if item.key.Visible(m.snapshot) &&
			(!m.levels[item.index].isIgnorableBoundaryKey) &&
			(item.key.Kind() != InternalKeyKindRangeDelete || !m.elideRangeTombstones) {
			return &item.key, item.value
		}
		m.prevEntry(item)
	}
	return nil, nil
}

// Seeks levels >= level to >= key. Additionally uses range tombstones to extend the seeks.
func (m *mergingIter) seekGE(key []byte, level int, flags base.SeekGEFlags) {
	// When seeking, we can use tombstones to adjust the key we seek to on each
	// level. Consider the series of range tombstones:
	//
	//   1: a---e
	//   2:    d---h
	//   3:       g---k
	//   4:          j---n
	//   5:             m---q
	//
	// If we SeekGE("b") we also find the tombstone "b" resides within in the
	// first level which is [a,e). Regardless of whether this tombstone deletes
	// "b" in that level, we know it deletes "b" in all lower levels, so we
	// adjust the search key in the next level to the tombstone end key "e". We
	// then SeekGE("e") in the second level and find the corresponding tombstone
	// [d,h). This process continues and we end up seeking for "h" in the 3rd
	// level, "k" in the 4th level and "n" in the last level.
	//
	// TODO(peter,rangedel): In addition to the above we can delay seeking a
	// level (and any lower levels) when the current iterator position is
	// contained within a range tombstone at a higher level.

	for ; level < len(m.levels); level++ {
		if invariants.Enabled && m.lower != nil && m.heap.cmp(key, m.lower) < 0 {
			m.logger.Fatalf("mergingIter: lower bound violation: %s < %s\n%s", key, m.lower, debug.Stack())
		}

		l := &m.levels[level]
		if m.prefix != nil {
			l.iterKey, l.iterValue = l.iter.SeekPrefixGE(m.prefix, key, flags)
		} else {
			l.iterKey, l.iterValue = l.iter.SeekGE(key, flags)
		}

		// If this level contains overlapping range tombstones, alter the seek
		// key accordingly. Caveat: If we're performing lazy-combined iteration,
		// we cannot alter the seek key: Range tombstones don't delete range
		// keys, and there might exist live range keys within the range
		// tombstone's span that need to be observed to trigger a switch to
		// combined iteration.
		if rangeDelIter := l.rangeDelIter; rangeDelIter != nil &&
			(m.combinedIterState == nil || m.combinedIterState.initialized) {
			// The level has a range-del iterator. Find the tombstone containing
			// the search key.
			//
			// For untruncated tombstones that are possibly file-bounds-constrained, we are using a
			// levelIter which will set smallestUserKey and largestUserKey. Since the levelIter
			// is at this file we know that largestUserKey >= key, so we know that the
			// tombstone we find cannot be file-bounds-constrained in its upper bound to something < key.
			// We do need to  compare with smallestUserKey to ensure that the tombstone is not
			// file-bounds-constrained in its lower bound.
			//
			// See the detailed comments in isNextEntryDeleted() on why similar containment and
			// seeking logic is correct. The subtle difference here is that key is a user key,
			// so we can have a sstable with bounds [c#8, i#InternalRangeDelSentinel], and the
			// tombstone is [b, k)#8 and the seek key is i: levelIter.SeekGE(i) will move past
			// this sstable since it realizes the largest key is a InternalRangeDelSentinel.
			l.tombstone = keyspan.SeekGE(m.heap.cmp, rangeDelIter, key)
			if l.tombstone != nil && l.tombstone.VisibleAt(m.snapshot) && l.tombstone.Contains(m.heap.cmp, key) &&
				(l.smallestUserKey == nil || m.heap.cmp(l.smallestUserKey, key) <= 0) {
				// NB: Based on the comment above l.largestUserKey >= key, and based on the
				// containment condition tombstone.End > key, so the assignment to key results
				// in a monotonically non-decreasing key across iterations of this loop.
				//
				// The adjustment of key here can only move it to a larger key. Since
				// the caller of seekGE guaranteed that the original key was greater
				// than or equal to m.lower, the new key will continue to be greater
				// than or equal to m.lower.
				if l.largestUserKey != nil &&
					m.heap.cmp(l.largestUserKey, l.tombstone.End) < 0 {
					// Truncate the tombstone for seeking purposes. Note that this can over-truncate
					// but that is harmless for this seek optimization.
					key = l.largestUserKey
				} else {
					key = l.tombstone.End
				}
			}
		}
	}

	m.initMinHeap()
}

func (m *mergingIter) String() string {
	return "merging"
}

// SeekGE implements base.InternalIterator.SeekGE. Note that SeekGE only checks
// the upper bound. It is up to the caller to ensure that key is greater than
// or equal to the lower bound.
func (m *mergingIter) SeekGE(key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	m.err = nil // clear cached iteration error
	m.prefix = nil
	m.seekGE(key, 0 /* start level */, flags)
	return m.findNextEntry()
}

// SeekPrefixGE implements base.InternalIterator.SeekPrefixGE. Note that
// SeekPrefixGE only checks the upper bound. It is up to the caller to ensure
// that key is greater than or equal to the lower bound.
func (m *mergingIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	m.err = nil // clear cached iteration error
	m.prefix = prefix
	m.seekGE(key, 0 /* start level */, flags)
	return m.findNextEntry()
}

// Seeks levels >= level to < key. Additionally uses range tombstones to extend the seeks.
func (m *mergingIter) seekLT(key []byte, level int, flags base.SeekLTFlags) {
	// See the comment in seekGE regarding using tombstones to adjust the seek
	// target per level.
	m.prefix = nil
	for ; level < len(m.levels); level++ {
		if invariants.Enabled && m.upper != nil && m.heap.cmp(key, m.upper) > 0 {
			m.logger.Fatalf("mergingIter: upper bound violation: %s > %s\n%s", key, m.upper, debug.Stack())
		}

		l := &m.levels[level]
		l.iterKey, l.iterValue = l.iter.SeekLT(key, flags)

		// If this level contains overlapping range tombstones, alter the seek
		// key accordingly. Caveat: If we're performing lazy-combined iteration,
		// we cannot alter the seek key: Range tombstones don't delete range
		// keys, and there might exist live range keys within the range
		// tombstone's span that need to be observed to trigger a switch to
		// combined iteration.
		if rangeDelIter := l.rangeDelIter; rangeDelIter != nil &&
			(m.combinedIterState == nil || m.combinedIterState.initialized) {
			// The level has a range-del iterator. Find the tombstone containing
			// the search key.
			//
			// For untruncated tombstones that are possibly file-bounds-constrained we are using a
			// levelIter which will set smallestUserKey and largestUserKey. Since the levelIter
			// is at this file we know that smallestUserKey <= key, so we know that the
			// tombstone we find cannot be file-bounds-constrained in its lower bound to something > key.
			// We do need to  compare with largestUserKey to ensure that the tombstone is not
			// file-bounds-constrained in its upper bound.
			//
			// See the detailed comments in isPrevEntryDeleted() on why similar containment and
			// seeking logic is correct.

			// Default to within bounds.
			withinLargestSSTableBound := true
			if l.largestUserKey != nil {
				cmpResult := m.heap.cmp(l.largestUserKey, key)
				withinLargestSSTableBound = cmpResult > 0 || (cmpResult == 0 && !l.isLargestUserKeyRangeDelSentinel)
			}

			l.tombstone = keyspan.SeekLE(m.heap.cmp, rangeDelIter, key)
			if l.tombstone != nil && l.tombstone.VisibleAt(m.snapshot) &&
				l.tombstone.Contains(m.heap.cmp, key) && withinLargestSSTableBound {
				// NB: Based on the comment above l.smallestUserKey <= key, and based
				// on the containment condition tombstone.Start.UserKey <= key, so the
				// assignment to key results in a monotonically non-increasing key
				// across iterations of this loop.
				//
				// The adjustment of key here can only move it to a smaller key. Since
				// the caller of seekLT guaranteed that the original key was less than
				// or equal to m.upper, the new key will continue to be less than or
				// equal to m.upper.
				if l.smallestUserKey != nil &&
					m.heap.cmp(l.smallestUserKey, l.tombstone.Start) >= 0 {
					// Truncate the tombstone for seeking purposes. Note that this can over-truncate
					// but that is harmless for this seek optimization.
					key = l.smallestUserKey
				} else {
					key = l.tombstone.Start
				}
			}
		}
	}

	m.initMaxHeap()
}

// SeekLT implements base.InternalIterator.SeekLT. Note that SeekLT only checks
// the lower bound. It is up to the caller to ensure that key is less than the
// upper bound.
func (m *mergingIter) SeekLT(key []byte, flags base.SeekLTFlags) (*InternalKey, []byte) {
	m.err = nil // clear cached iteration error
	m.prefix = nil
	m.seekLT(key, 0 /* start level */, flags)
	return m.findPrevEntry()
}

// First implements base.InternalIterator.First. Note that First only checks
// the upper bound. It is up to the caller to ensure that key is greater than
// or equal to the lower bound (e.g. via a call to SeekGE(lower)).
func (m *mergingIter) First() (*InternalKey, []byte) {
	m.err = nil // clear cached iteration error
	m.prefix = nil
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.First()
	}
	m.initMinHeap()
	return m.findNextEntry()
}

// Last implements base.InternalIterator.Last. Note that Last only checks the
// lower bound. It is up to the caller to ensure that key is less than the
// upper bound (e.g. via a call to SeekLT(upper))
func (m *mergingIter) Last() (*InternalKey, []byte) {
	m.err = nil // clear cached iteration error
	m.prefix = nil
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.Last()
	}
	m.initMaxHeap()
	return m.findPrevEntry()
}

func (m *mergingIter) Next() (*InternalKey, []byte) {
	if m.err != nil {
		return nil, nil
	}

	if m.dir != 1 {
		m.switchToMinHeap()
		return m.findNextEntry()
	}

	if m.heap.len() == 0 {
		return nil, nil
	}

	m.nextEntry(&m.heap.items[0])
	return m.findNextEntry()
}

func (m *mergingIter) Prev() (*InternalKey, []byte) {
	if m.err != nil {
		return nil, nil
	}

	if m.dir != -1 {
		if m.prefix != nil {
			m.err = errors.New("pebble: unsupported reverse prefix iteration")
			return nil, nil
		}
		m.switchToMaxHeap()
		return m.findPrevEntry()
	}

	if m.heap.len() == 0 {
		return nil, nil
	}

	m.prevEntry(&m.heap.items[0])
	return m.findPrevEntry()
}

func (m *mergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
		return m.err
	}
	return m.levels[m.heap.items[0].index].iter.Error()
}

func (m *mergingIter) Close() error {
	for i := range m.levels {
		iter := m.levels[i].iter
		if err := iter.Close(); err != nil && m.err == nil {
			m.err = err
		}
		if rangeDelIter := m.levels[i].rangeDelIter; rangeDelIter != nil {
			if err := rangeDelIter.Close(); err != nil && m.err == nil {
				m.err = err
			}
		}
	}
	m.levels = nil
	m.heap.items = m.heap.items[:0]
	return m.err
}

func (m *mergingIter) SetBounds(lower, upper []byte) {
	m.prefix = nil
	m.lower = lower
	m.upper = upper
	for i := range m.levels {
		m.levels[i].iter.SetBounds(lower, upper)
	}
	m.heap.clear()
}

func (m *mergingIter) DebugString() string {
	var buf bytes.Buffer
	sep := ""
	for m.heap.len() > 0 {
		item := m.heap.pop()
		fmt.Fprintf(&buf, "%s%s", sep, item.key)
		sep = " "
	}
	if m.dir == 1 {
		m.initMinHeap()
	} else {
		m.initMaxHeap()
	}
	return buf.String()
}

func (m *mergingIter) ForEachLevelIter(fn func(li *levelIter) bool) {
	for _, iter := range m.levels {
		if li, ok := iter.iter.(*levelIter); ok {
			if done := fn(li); done {
				break
			}
		}
	}
}

func (m *mergingIter) addItemStats(item *mergingIterItem) {
	m.stats.PointCount++
	m.stats.KeyBytes += uint64(len(item.key.UserKey))
	m.stats.ValueBytes += uint64(len(item.value))
}

var _ internalIterator = &mergingIter{}
