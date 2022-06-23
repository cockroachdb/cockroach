/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

/*
Further adapted from Badger: https://github.com/dgraph-io/badger.

Key differences:
- Support for previous pointers - doubly linked lists. Note that it's up to higher
  level code to deal with the intermediate state that occurs during insertion,
  where node A is linked to node B, but node B is not yet linked back to node A.
- Iterator includes mutator functions.
*/

package arenaskl

import (
	"bytes"
	"errors"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/andy-kimball/arenaskl/internal/fastrand"
)

const (
	maxHeight  = 20
	pValue     = 1 / math.E
	linksSize  = int(unsafe.Sizeof(links{}))
	deletedVal = 0
)

const MaxNodeSize = int(unsafe.Sizeof(node{}))

var ErrRecordExists = errors.New("record with this key already exists")
var ErrRecordUpdated = errors.New("record was updated by another caller")
var ErrRecordDeleted = errors.New("record was deleted by another caller")

type Skiplist struct {
	arena  *Arena
	head   *node
	tail   *node
	height uint32 // Current height. 1 <= height <= maxHeight. CAS.

	// If set to true by tests, then extra delays are added to make it easier to
	// detect unusual race conditions.
	testing bool
}

var (
	probabilities [maxHeight]uint32
)

func init() {
	// Precompute the skiplist probabilities so that only a single random number
	// needs to be generated and so that the optimal pvalue can be used (inverse
	// of Euler's number).
	p := float64(1.0)
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

// NewSkiplist constructs and initializes a new, empty skiplist. All nodes, keys,
// and values in the skiplist will be allocated from the given arena.
func NewSkiplist(arena *Arena) *Skiplist {
	// Allocate head and tail nodes.
	head, err := newNode(arena, maxHeight)
	if err != nil {
		panic("arenaSize is not large enough to hold the head node")
	}

	tail, err := newNode(arena, maxHeight)
	if err != nil {
		panic("arenaSize is not large enough to hold the tail node")
	}

	// Link all head/tail levels together.
	headOffset := arena.GetPointerOffset(unsafe.Pointer(head))
	tailOffset := arena.GetPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset = tailOffset
		tail.tower[i].prevOffset = headOffset
	}

	skl := &Skiplist{
		arena:  arena,
		head:   head,
		tail:   tail,
		height: 1,
	}

	return skl
}

// Height returns the height of the highest tower within any of the nodes that
// have ever been allocated as part of this skiplist.
func (s *Skiplist) Height() uint32 { return atomic.LoadUint32(&s.height) }

// Arena returns the arena backing this skiplist.
func (s *Skiplist) Arena() *Arena { return s.arena }

// Size returns the number of bytes that have allocated from the arena.
func (s *Skiplist) Size() uint32 { return s.arena.Size() }

func (s *Skiplist) newNode(key, val []byte, meta uint16) (nd *node, height uint32, err error) {
	height = s.randomHeight()
	nd, err = newNode(s.arena, height)
	if err != nil {
		return
	}

	// Try to increase s.height via CAS.
	listHeight := s.Height()
	for height > listHeight {
		if atomic.CompareAndSwapUint32(&s.height, listHeight, height) {
			// Successfully increased skiplist.height.
			break
		}

		listHeight = s.Height()
	}

	// Allocate node's key and value.
	nd.keyOffset, nd.keySize, err = s.allocKey(key)
	if err != nil {
		return
	}

	nd.value, err = s.allocVal(val, meta)
	return
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := fastrand.Uint32()
	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}

	return h
}

func (s *Skiplist) allocKey(key []byte) (keyOffset uint32, keySize uint32, err error) {
	keySize = uint32(len(key))
	if keySize > math.MaxUint32 {
		panic("key is too large")
	}

	keyOffset, err = s.arena.Alloc(keySize, 0 /* overflow */, Align1)
	if err == nil {
		copy(s.arena.GetBytes(keyOffset, keySize), key)
	}

	return
}

func (s *Skiplist) allocVal(val []byte, meta uint16) (uint64, error) {
	if len(val) > math.MaxUint16 {
		panic("value is too large")
	}

	valSize := uint16(len(val))
	valOffset, err := s.arena.Alloc(uint32(valSize), 0 /* overflow */, Align1)
	if err != nil {
		return 0, err
	}

	copy(s.arena.GetBytes(valOffset, uint32(valSize)), val)
	return encodeValue(valOffset, valSize, meta), nil
}

func (s *Skiplist) findSpliceForLevel(key []byte, level int, start *node) (prev, next *node, found bool) {
	prev = start

	for {
		// Assume prev.key < key.
		next = s.getNext(prev, level)
		nextKey := next.getKey(s.arena)
		if nextKey == nil {
			// Tail node key, so done.
			break
		}

		cmp := bytes.Compare(key, nextKey)
		if cmp == 0 {
			// Equality case.
			found = true
			break
		}

		if cmp < 0 {
			// We are done for this level, since prev.key < key < next.key.
			break
		}

		// Keep moving right on this level.
		prev = next
	}

	return
}

func (s *Skiplist) getNext(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].nextOffset)
	return (*node)(s.arena.GetPointer(offset))
}

func (s *Skiplist) getPrev(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].prevOffset)
	return (*node)(s.arena.GetPointer(offset))
}

func encodeValue(valOffset uint32, valSize, meta uint16) uint64 {
	return uint64(meta)<<48 | uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint16) {
	valOffset = uint32(value)
	valSize = uint16(value >> 32)
	return
}

func decodeMeta(value uint64) uint16 {
	return uint16(value >> 48)
}
