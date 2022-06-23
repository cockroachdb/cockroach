// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

func uvarintLen(v uint32) int {
	i := 0
	for v >= 0x80 {
		v >>= 7
		i++
	}
	return i + 1
}

type blockWriter struct {
	restartInterval int
	nEntries        int
	nextRestart     int
	buf             []byte
	restarts        []uint32
	curKey          []byte
	curValue        []byte
	prevKey         []byte
	tmp             [4]byte
}

func (w *blockWriter) clear() {
	*w = blockWriter{
		buf:      w.buf[:0],
		restarts: w.restarts[:0],
		curKey:   w.curKey[:0],
		curValue: w.curValue[:0],
		prevKey:  w.prevKey[:0],
	}
}

func (w *blockWriter) store(keySize int, value []byte) {
	shared := 0
	if w.nEntries == w.nextRestart {
		w.nextRestart = w.nEntries + w.restartInterval
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	} else {
		// TODO(peter): Manually inlined version of base.SharedPrefixLen(). This
		// is 3% faster on BenchmarkWriter on go1.16. Remove if future versions
		// show this to not be a performance win. For now, functions that use of
		// unsafe cannot be inlined.
		n := len(w.curKey)
		if n > len(w.prevKey) {
			n = len(w.prevKey)
		}
		asUint64 := func(b []byte, i int) uint64 {
			return binary.LittleEndian.Uint64(b[i:])
		}
		for shared < n-7 && asUint64(w.curKey, shared) == asUint64(w.prevKey, shared) {
			shared += 8
		}
		for shared < n && w.curKey[shared] == w.prevKey[shared] {
			shared++
		}
	}

	needed := 3*binary.MaxVarintLen32 + len(w.curKey[shared:]) + len(value)
	n := len(w.buf)
	if cap(w.buf) < n+needed {
		newCap := 2 * cap(w.buf)
		if newCap == 0 {
			newCap = 1024
		}
		for newCap < n+needed {
			newCap *= 2
		}
		newBuf := make([]byte, n, newCap)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
	w.buf = w.buf[:n+needed]

	// TODO(peter): Manually inlined versions of binary.PutUvarint(). This is 15%
	// faster on BenchmarkWriter on go1.13. Remove if go1.14 or future versions
	// show this to not be a performance win.
	{
		x := uint32(shared)
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	{
		x := uint32(keySize - shared)
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	{
		x := uint32(len(value))
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	n += copy(w.buf[n:], w.curKey[shared:])
	n += copy(w.buf[n:], value)
	w.buf = w.buf[:n]

	w.curValue = w.buf[n-len(value):]

	w.nEntries++
}

func (w *blockWriter) add(key InternalKey, value []byte) {
	w.curKey, w.prevKey = w.prevKey, w.curKey

	size := key.Size()
	if cap(w.curKey) < size {
		w.curKey = make([]byte, 0, size*2)
	}
	w.curKey = w.curKey[:size]
	key.Encode(w.curKey)

	w.store(size, value)
}

func (w *blockWriter) finish() []byte {
	// Write the restart points to the buffer.
	if w.nEntries == 0 {
		// Every block must have at least one restart point.
		if cap(w.restarts) > 0 {
			w.restarts = w.restarts[:1]
			w.restarts[0] = 0
		} else {
			w.restarts = append(w.restarts, 0)
		}
	}
	tmp4 := w.tmp[:4]
	for _, x := range w.restarts {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf = append(w.buf, tmp4...)
	}
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp4...)
	result := w.buf

	// Reset the block state.
	w.nEntries = 0
	w.nextRestart = 0
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	return result
}

// emptyBlockSize holds the size of an empty block. Every block ends
// in a uint32 trailer encoding the number of restart points within the
// block.
const emptyBlockSize = 4

func (w *blockWriter) estimatedSize() int {
	return len(w.buf) + 4*len(w.restarts) + emptyBlockSize
}

type blockEntry struct {
	offset   int32
	keyStart int32
	keyEnd   int32
	valStart int32
	valSize  int32
}

// blockIter is an iterator over a single block of data.
//
// A blockIter provides an additional guarantee around key stability when a
// block has a restart interval of 1 (i.e. when there is no prefix
// compression). Key stability refers to whether the InternalKey.UserKey bytes
// returned by a positioning call will remain stable after a subsequent
// positioning call. The normal case is that a positioning call will invalidate
// any previously returned InternalKey.UserKey. If a block has a restart
// interval of 1 (no prefix compression), blockIter guarantees that
// InternalKey.UserKey will point to the key as stored in the block itself
// which will remain valid until the blockIter is closed. The key stability
// guarantee is used by the range tombstone and range key code, which knows that
// the respective blocks are always encoded with a restart interval of 1. This
// per-block key stability guarantee is sufficient for range tombstones and
// range deletes as they are always encoded in a single block.
//
// A blockIter also provides a value stability guarantee for range deletions and
// range keys since there is only a single range deletion and range key block
// per sstable and the blockIter will not release the bytes for the block until
// it is closed.
type blockIter struct {
	cmp Compare
	// offset is the byte index that marks where the current key/value is
	// encoded in the block.
	offset int32
	// nextOffset is the byte index where the next key/value is encoded in the
	// block.
	nextOffset int32
	// A "restart point" in a block is a point where the full key is encoded,
	// instead of just having a suffix of the key encoded. See readEntry() for
	// how prefix compression of keys works. Keys in between two restart points
	// only have a suffix encoded in the block. When restart interval is 1, no
	// prefix compression of keys happens. This is the case with range tombstone
	// blocks.
	//
	// All restart offsets are listed in increasing order in
	// i.ptr[i.restarts:len(block)-4], while numRestarts is encoded in the last
	// 4 bytes of the block as a uint32 (i.ptr[len(block)-4:]). i.restarts can
	// therefore be seen as the point where data in the block ends, and a list
	// of offsets of all restart points begins.
	restarts int32
	// Number of restart points in this block. Encoded at the end of the block
	// as a uint32.
	numRestarts  int32
	globalSeqNum uint64
	ptr          unsafe.Pointer
	data         []byte
	// key contains the raw key the iterator is currently pointed at. This may
	// point directly to data stored in the block (for a key which has no prefix
	// compression), to fullKey (for a prefix compressed key), or to a slice of
	// data stored in cachedBuf (during reverse iteration).
	key []byte
	// fullKey is a buffer used for key prefix decompression.
	fullKey []byte
	// val contains the value the iterator is currently pointed at. If non-nil,
	// this points to a slice of the block data.
	val []byte
	// ikey contains the decoded InternalKey the iterator is currently pointed
	// at. Note that the memory backing ikey.UserKey is either data stored
	// directly in the block, fullKey, or cachedBuf. The key stability guarantee
	// for blocks built with a restart interval of 1 is achieved by having
	// ikey.UserKey always point to data stored directly in the block.
	ikey InternalKey
	// cached and cachedBuf are used during reverse iteration. They are needed
	// because we can't perform prefix decoding in reverse, only in the forward
	// direction. In order to iterate in reverse, we decode and cache the entries
	// between two restart points.
	//
	// Note that cached[len(cached)-1] contains the previous entry to the one the
	// blockIter is currently pointed at. As usual, nextOffset will contain the
	// offset of the next entry. During reverse iteration, nextOffset will be
	// updated to point to offset, and we'll set the blockIter to point at the
	// entry cached[len(cached)-1]. See Prev() for more details.
	//
	// For a block encoded with a restart interval of 1, cached and cachedBuf
	// will not be used as there are no prefix compressed entries between the
	// restart points.
	cached      []blockEntry
	cachedBuf   []byte
	cacheHandle cache.Handle
	// The first key in the block. This is used by the caller to set bounds
	// for block iteration for already loaded blocks.
	firstKey InternalKey
}

// blockIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*blockIter)(nil)

func newBlockIter(cmp Compare, block block) (*blockIter, error) {
	i := &blockIter{}
	return i, i.init(cmp, block, 0)
}

func (i *blockIter) String() string {
	return "block"
}

func (i *blockIter) init(cmp Compare, block block, globalSeqNum uint64) error {
	numRestarts := int32(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if numRestarts == 0 {
		return base.CorruptionErrorf("pebble/table: invalid table (block has no restart points)")
	}
	i.cmp = cmp
	i.restarts = int32(len(block)) - 4*(1+numRestarts)
	i.numRestarts = numRestarts
	i.globalSeqNum = globalSeqNum
	i.ptr = unsafe.Pointer(&block[0])
	i.data = block
	i.fullKey = i.fullKey[:0]
	i.val = nil
	i.clearCache()
	if i.restarts > 0 {
		if err := i.readFirstKey(); err != nil {
			return err
		}
	} else {
		// Block is empty.
		i.firstKey = InternalKey{}
	}
	return nil
}

func (i *blockIter) initHandle(cmp Compare, block cache.Handle, globalSeqNum uint64) error {
	i.cacheHandle.Release()
	i.cacheHandle = block
	return i.init(cmp, block.Get(), globalSeqNum)
}

func (i *blockIter) invalidate() {
	i.clearCache()
	i.offset = 0
	i.nextOffset = 0
	i.restarts = 0
	i.numRestarts = 0
	i.data = nil
}

// isDataInvalidated returns true when the blockIter has been invalidated
// using an invalidate call. NB: this is different from blockIter.Valid
// which is part of the InternalIterator implementation.
func (i *blockIter) isDataInvalidated() bool {
	return i.data == nil
}

func (i *blockIter) resetForReuse() blockIter {
	return blockIter{
		fullKey:   i.fullKey[:0],
		cached:    i.cached[:0],
		cachedBuf: i.cachedBuf[:0],
		data:      nil,
	}
}

func (i *blockIter) readEntry() {
	ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(i.offset))

	// This is an ugly performance hack. Reading entries from blocks is one of
	// the inner-most routines and decoding the 3 varints per-entry takes
	// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
	// us, so we do it manually. This provides a 10-15% performance improvement
	// on blockIter benchmarks on both go1.11 and go1.12.
	//
	// TODO(peter): remove this hack if go:inline is ever supported.

	var shared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		shared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		shared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		shared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		shared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		shared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var value uint32
	if a := *((*uint8)(ptr)); a < 128 {
		value = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		value = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	unsharedKey := getBytes(ptr, int(unshared))
	i.fullKey = append(i.fullKey[:shared], unsharedKey...)
	if shared == 0 {
		// Provide stability for the key across positioning calls if the key
		// doesn't share a prefix with the previous key. This removes requiring the
		// key to be copied if the caller knows the block has a restart interval of
		// 1. An important example of this is range-del blocks.
		i.key = unsharedKey
	} else {
		i.key = i.fullKey
	}
	ptr = unsafe.Pointer(uintptr(ptr) + uintptr(unshared))
	i.val = getBytes(ptr, int(value))
	i.nextOffset = int32(uintptr(ptr)-uintptr(i.ptr)) + int32(value)
}

func (i *blockIter) readFirstKey() error {
	ptr := i.ptr

	// This is an ugly performance hack. Reading entries from blocks is one of
	// the inner-most routines and decoding the 3 varints per-entry takes
	// significant time. Neither go1.11 or go1.12 will inline decodeVarint for
	// us, so we do it manually. This provides a 10-15% performance improvement
	// on blockIter benchmarks on both go1.11 and go1.12.
	//
	// TODO(peter): remove this hack if go:inline is ever supported.

	if shared := *((*uint8)(ptr)); shared == 0 {
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else {
		// The shared length is != 0, which is invalid.
		panic("first key in block must have zero shared length")
	}

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	// Skip the value length.
	if a := *((*uint8)(ptr)); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a := *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if a := *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if a := *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); a < 128 {
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	firstKey := getBytes(ptr, int(unshared))
	// Manually inlining base.DecodeInternalKey provides a 5-10% speedup on
	// BlockIter benchmarks.
	if n := len(firstKey) - 8; n >= 0 {
		i.firstKey.Trailer = binary.LittleEndian.Uint64(firstKey[n:])
		i.firstKey.UserKey = firstKey[:n:n]
		if i.globalSeqNum != 0 {
			i.firstKey.SetSeqNum(i.globalSeqNum)
		}
	} else {
		i.firstKey.Trailer = uint64(InternalKeyKindInvalid)
		i.firstKey.UserKey = nil
		return base.CorruptionErrorf("pebble/table: invalid firstKey in block")
	}
	return nil
}

func (i *blockIter) decodeInternalKey(key []byte) {
	// Manually inlining base.DecodeInternalKey provides a 5-10% speedup on
	// BlockIter benchmarks.
	if n := len(key) - 8; n >= 0 {
		i.ikey.Trailer = binary.LittleEndian.Uint64(key[n:])
		i.ikey.UserKey = key[:n:n]
		if i.globalSeqNum != 0 {
			i.ikey.SetSeqNum(i.globalSeqNum)
		}
	} else {
		i.ikey.Trailer = uint64(InternalKeyKindInvalid)
		i.ikey.UserKey = nil
	}
}

func (i *blockIter) clearCache() {
	i.cached = i.cached[:0]
	i.cachedBuf = i.cachedBuf[:0]
}

func (i *blockIter) cacheEntry() {
	var valStart int32
	valSize := int32(len(i.val))
	if valSize > 0 {
		valStart = int32(uintptr(unsafe.Pointer(&i.val[0])) - uintptr(i.ptr))
	}

	i.cached = append(i.cached, blockEntry{
		offset:   i.offset,
		keyStart: int32(len(i.cachedBuf)),
		keyEnd:   int32(len(i.cachedBuf) + len(i.key)),
		valStart: valStart,
		valSize:  valSize,
	})
	i.cachedBuf = append(i.cachedBuf, i.key...)
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *blockIter) SeekGE(key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	i.clearCache()

	ikey := base.MakeSearchKey(key)

	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	var index int32

	{
		// NB: manually inlined sort.Seach is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			// For a restart point, there are 0 bytes shared with the previous key.
			// The varint encoding of 0 occupies 1 byte.
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if *((*uint8)(ptr)) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			// Manually inlining base.DecodeInternalKey provides a 5-10% speedup on
			// BlockIter benchmarks.
			s := getBytes(ptr, int(v1))
			var k InternalKey
			if n := len(s) - 8; n >= 0 {
				k.Trailer = binary.LittleEndian.Uint64(s[n:])
				k.UserKey = s[:n:n]
				// NB: We can't have duplicate keys if the globalSeqNum != 0, so we
				// leave the seqnum on this key as 0 as it won't affect our search
				// since ikey has the maximum seqnum.
			} else {
				k.Trailer = uint64(InternalKeyKindInvalid)
			}

			if base.InternalCompare(i.cmp, ikey, k) >= 0 {
				index = h + 1 // preserves f(i-1) == false
			} else {
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	// Since keys are strictly increasing, if index > 0 then the restart point at
	// index-1 will be the largest whose key is <= the key sought.  If index ==
	// 0, then all keys in this block are larger than the key sought, and offset
	// remains at zero.
	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}
	i.readEntry()
	i.decodeInternalKey(i.key)

	// Iterate from that restart point to somewhere >= the key sought.
	for ; i.valid(); i.Next() {
		if base.InternalCompare(i.cmp, i.ikey, ikey) >= 0 {
			return &i.ikey, i.val
		}
	}

	return nil, nil
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package.
func (i *blockIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	// This should never be called as prefix iteration is handled by sstable.Iterator.
	panic("pebble: SeekPrefixGE unimplemented")
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *blockIter) SeekLT(key []byte, flags base.SeekLTFlags) (*InternalKey, []byte) {
	i.clearCache()

	ikey := base.MakeSearchKey(key)

	// Find the index of the smallest restart point whose key is >= the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	var index int32

	{
		// NB: manually inlined sort.Search is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			// For a restart point, there are 0 bytes shared with the previous key.
			// The varint encoding of 0 occupies 1 byte.
			ptr := unsafe.Pointer(uintptr(i.ptr) + uintptr(offset+1))

			// Decode the key at that restart point, and compare it to the key
			// sought. See the comment in readEntry for why we manually inline the
			// varint decoding.
			var v1 uint32
			if a := *((*uint8)(ptr)); a < 128 {
				v1 = uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
				v1 = uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
				v1 = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
				v1 = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
				v1 = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			if *((*uint8)(ptr)) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 1)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 2)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 3)
			} else if *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))) < 128 {
				ptr = unsafe.Pointer(uintptr(ptr) + 4)
			} else {
				ptr = unsafe.Pointer(uintptr(ptr) + 5)
			}

			// Manually inlining base.DecodeInternalKey provides a 5-10% speedup on
			// BlockIter benchmarks.
			s := getBytes(ptr, int(v1))
			var k InternalKey
			if n := len(s) - 8; n >= 0 {
				k.Trailer = binary.LittleEndian.Uint64(s[n:])
				k.UserKey = s[:n:n]
				// NB: We can't have duplicate keys if the globalSeqNum != 0, so we
				// leave the seqnum on this key as 0 as it won't affect our search
				// since ikey has the maximum seqnum.
			} else {
				k.Trailer = uint64(InternalKeyKindInvalid)
			}

			if base.InternalCompare(i.cmp, ikey, k) > 0 {
				index = h + 1 // preserves f(i-1) == false
			} else {
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	// Since keys are strictly increasing, if index > 0 then the restart point at
	// index-1 will be the largest whose key is < the key sought.
	targetOffset := i.restarts
	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
		if index < i.numRestarts {
			targetOffset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index):]))
		}
	} else if index == 0 {
		// If index == 0 then all keys in this block are larger than the key
		// sought.
		i.offset = -1
		i.nextOffset = 0
		return nil, nil
	}

	// Iterate from that restart point to somewhere >= the key sought, then back
	// up to the previous entry. The expectation is that we'll be performing
	// reverse iteration, so we cache the entries as we advance forward.
	i.nextOffset = i.offset

	for {
		i.offset = i.nextOffset
		i.readEntry()
		i.decodeInternalKey(i.key)

		if i.cmp(i.ikey.UserKey, ikey.UserKey) >= 0 {
			// The current key is greater than or equal to our search key. Back up to
			// the previous key which was less than our search key. Note that his for
			// loop will execute at least once with this if-block not being true, so
			// the key we are backing up to is the last one this loop cached.
			i.Prev()
			return &i.ikey, i.val
		}

		if i.nextOffset >= targetOffset {
			// We've reached the end of the current restart block. Return the current
			// key. When the restart interval is 1, the first iteration of the for
			// loop will bring us here. In that case ikey is backed by the block so
			// we get the desired key stability guarantee for the lifetime of the
			// blockIter.
			break
		}

		i.cacheEntry()
	}

	if !i.valid() {
		return nil, nil
	}
	return &i.ikey, i.val
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *blockIter) First() (*InternalKey, []byte) {
	i.offset = 0
	if !i.valid() {
		return nil, nil
	}
	i.clearCache()
	i.readEntry()
	i.decodeInternalKey(i.key)
	return &i.ikey, i.val
}

// Last implements internalIterator.Last, as documented in the pebble package.
func (i *blockIter) Last() (*InternalKey, []byte) {
	// Seek forward from the last restart point.
	i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(i.numRestarts-1):]))
	if !i.valid() {
		return nil, nil
	}

	i.readEntry()
	i.clearCache()

	for i.nextOffset < i.restarts {
		i.cacheEntry()
		i.offset = i.nextOffset
		i.readEntry()
	}

	i.decodeInternalKey(i.key)
	return &i.ikey, i.val
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
func (i *blockIter) Next() (*InternalKey, []byte) {
	if len(i.cachedBuf) > 0 {
		// We're switching from reverse iteration to forward iteration. We need to
		// populate i.fullKey with the current key we're positioned at so that
		// readEntry() can use i.fullKey for key prefix decompression. Note that we
		// don't know whether i.key is backed by i.cachedBuf or i.fullKey (if
		// SeekLT was the previous call, i.key may be backed by i.fullKey), but
		// copying into i.fullKey works for both cases.
		//
		// TODO(peter): Rather than clearing the cache, we could instead use the
		// cache until it is exhausted. This would likely be faster than falling
		// through to the normal forward iteration code below.
		i.fullKey = append(i.fullKey[:0], i.key...)
		i.clearCache()
	}

	i.offset = i.nextOffset
	if !i.valid() {
		return nil, nil
	}
	i.readEntry()
	// Manually inlined version of i.decodeInternalKey(i.key).
	if n := len(i.key) - 8; n >= 0 {
		i.ikey.Trailer = binary.LittleEndian.Uint64(i.key[n:])
		i.ikey.UserKey = i.key[:n:n]
		if i.globalSeqNum != 0 {
			i.ikey.SetSeqNum(i.globalSeqNum)
		}
	} else {
		i.ikey.Trailer = uint64(InternalKeyKindInvalid)
		i.ikey.UserKey = nil
	}
	return &i.ikey, i.val
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *blockIter) Prev() (*InternalKey, []byte) {
	if n := len(i.cached) - 1; n >= 0 {
		i.nextOffset = i.offset
		e := &i.cached[n]
		i.offset = e.offset
		i.val = getBytes(unsafe.Pointer(uintptr(i.ptr)+uintptr(e.valStart)), int(e.valSize))
		// Manually inlined version of i.decodeInternalKey(i.key).
		i.key = i.cachedBuf[e.keyStart:e.keyEnd]
		if n := len(i.key) - 8; n >= 0 {
			i.ikey.Trailer = binary.LittleEndian.Uint64(i.key[n:])
			i.ikey.UserKey = i.key[:n:n]
			if i.globalSeqNum != 0 {
				i.ikey.SetSeqNum(i.globalSeqNum)
			}
		} else {
			i.ikey.Trailer = uint64(InternalKeyKindInvalid)
			i.ikey.UserKey = nil
		}
		i.cached = i.cached[:n]
		return &i.ikey, i.val
	}

	i.clearCache()
	if i.offset <= 0 {
		i.offset = -1
		i.nextOffset = 0
		return nil, nil
	}

	targetOffset := i.offset
	var index int32

	{
		// NB: manually inlined sort.Sort is ~5% faster.
		//
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(index-1) == false, f(upper) == true.
		upper := i.numRestarts
		for index < upper {
			h := int32(uint(index+upper) >> 1) // avoid overflow when computing h
			// index ≤ h < upper
			offset := int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*h:]))
			if offset < targetOffset {
				index = h + 1 // preserves f(i-1) == false
			} else {
				upper = h // preserves f(j) == true
			}
		}
		// index == upper, f(index-1) == false, and f(upper) (= f(index)) == true
		// => answer is index.
	}

	i.offset = 0
	if index > 0 {
		i.offset = int32(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}

	i.readEntry()

	for i.nextOffset < targetOffset {
		i.cacheEntry()
		i.offset = i.nextOffset
		i.readEntry()
	}

	i.decodeInternalKey(i.key)
	return &i.ikey, i.val
}

// Key implements internalIterator.Key, as documented in the pebble package.
func (i *blockIter) Key() *InternalKey {
	return &i.ikey
}

// Value implements internalIterator.Value, as documented in the pebble
// package.
func (i *blockIter) Value() []byte {
	return i.val
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *blockIter) Error() error {
	return nil // infallible
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *blockIter) Close() error {
	i.cacheHandle.Release()
	i.cacheHandle = cache.Handle{}
	i.val = nil
	return nil
}

func (i *blockIter) SetBounds(lower, upper []byte) {
	// This should never be called as bounds are handled by sstable.Iterator.
	panic("pebble: SetBounds unimplemented")
}

func (i *blockIter) valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

// fragmentBlockIter wraps a blockIter, implementing the
// keyspan.FragmentIterator interface. It's used for reading range deletion and
// range key blocks.
//
// Range deletions and range keys are fragmented before they're persisted to the
// block. Overlapping fragments have identical bounds.  The fragmentBlockIter
// gathers all the fragments with identical bounds within a block and returns a
// single keyspan.Span describing all the keys defined over the span.
//
// Memory lifetime
//
// A Span returned by fragmentBlockIter is only guaranteed to be stable until
// the next fragmentBlockIter iteration positioning method. A Span's Keys slice
// may be reused, so the user must not assume it's stable.
//
// Blocks holding range deletions and range keys are configured to use a restart
// interval of 1. This provides key stability. The caller may treat the various
// byte slices (start, end, suffix, value) as stable for the lifetime of the
// iterator.
type fragmentBlockIter struct {
	blockIter blockIter
	keyBuf    [2]keyspan.Key
	span      keyspan.Span
	err       error
	dir       int8
	closeHook func(i keyspan.FragmentIterator) error
}

func (i *fragmentBlockIter) resetForReuse() fragmentBlockIter {
	return fragmentBlockIter{blockIter: i.blockIter.resetForReuse()}
}

func (i *fragmentBlockIter) decodeSpanKeys(k *InternalKey, internalValue []byte) {
	// TODO(jackson): The use of i.span.Keys to accumulate keys across multiple
	// calls to Decode is too confusing and subtle. Refactor to make it
	// explicit.

	// decode the contents of the fragment's value. This always includes at
	// least the end key: RANGEDELs store the end key directly as the value,
	// whereas the various range key kinds store are more complicated.  The
	// details of the range key internal value format are documented within the
	// internal/rangekey package.
	switch k.Kind() {
	case base.InternalKeyKindRangeDelete:
		i.span = rangedel.Decode(*k, internalValue, i.span.Keys)
		i.err = nil
	case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
		i.span, i.err = rangekey.Decode(*k, internalValue, i.span.Keys)
	default:
		i.span = keyspan.Span{}
		i.err = base.CorruptionErrorf("pebble: corrupt keyspan fragment of kind %d", k.Kind())
	}
}

// gatherForward gathers internal keys with identical bounds. Keys defined over
// spans of the keyspace are fragmented such that any overlapping key spans have
// identical bounds. When these spans are persisted to a range deletion or range
// key block, they may be persisted as multiple internal keys in order to encode
// multiple sequence numbers or key kinds.
//
// gatherForward iterates forward, re-combining the fragmented internal keys to
// reconstruct a keyspan.Span that holds all the keys defined over the span.
func (i *fragmentBlockIter) gatherForward(k *InternalKey, internalValue []byte) *keyspan.Span {
	i.span = keyspan.Span{}
	if k == nil || !i.blockIter.valid() {
		return nil
	}
	i.err = nil
	// Use the i.keyBuf array to back the Keys slice to prevent an allocation
	// when a span contains few keys.
	i.span.Keys = i.keyBuf[:0]

	// Decode the span's end key and individual keys from the value.
	i.decodeSpanKeys(k, internalValue)
	if i.err != nil {
		return nil
	}
	prevEnd := i.span.End

	// There might exist additional internal keys with identical bounds encoded
	// within the block. Iterate forward, accumulating all the keys with
	// identical bounds to s.
	k, internalValue = i.blockIter.Next()
	for k != nil && i.blockIter.cmp(k.UserKey, i.span.Start) == 0 {
		i.decodeSpanKeys(k, internalValue)
		if i.err != nil {
			return nil
		}

		// Since k indicates an equal start key, the encoded end key must
		// exactly equal the original end key from the first internal key.
		// Overlapping fragments are required to have exactly equal start and
		// end bounds.
		if i.blockIter.cmp(prevEnd, i.span.End) != 0 {
			i.err = base.CorruptionErrorf("pebble: corrupt keyspan fragmentation")
			i.span = keyspan.Span{}
			return nil
		}
		k, internalValue = i.blockIter.Next()
	}
	// i.blockIter is positioned over the first internal key for the next span.
	return &i.span
}

// gatherBackward gathers internal keys with identical bounds. Keys defined over
// spans of the keyspace are fragmented such that any overlapping key spans have
// identical bounds. When these spans are persisted to a range deletion or range
// key block, they may be persisted as multiple internal keys in order to encode
// multiple sequence numbers or key kinds.
//
// gatherBackward iterates backwards, re-combining the fragmented internal keys
// to reconstruct a keyspan.Span that holds all the keys defined over the span.
func (i *fragmentBlockIter) gatherBackward(k *InternalKey, internalValue []byte) *keyspan.Span {
	i.span = keyspan.Span{}
	if k == nil || !i.blockIter.valid() {
		return nil
	}
	i.err = nil
	// Use the i.keyBuf array to back the Keys slice to prevent an allocation
	// when a span contains few keys.
	i.span.Keys = i.keyBuf[:0]

	// Decode the span's end key and individual keys from the value.
	i.decodeSpanKeys(k, internalValue)
	if i.err != nil {
		return nil
	}
	prevEnd := i.span.End

	// There might exist additional internal keys with identical bounds encoded
	// within the block. Iterate backward, accumulating all the keys with
	// identical bounds to s.
	k, internalValue = i.blockIter.Prev()
	for k != nil && i.blockIter.cmp(k.UserKey, i.span.Start) == 0 {
		i.decodeSpanKeys(k, internalValue)
		if i.err != nil {
			return nil
		}

		// Since k indicates an equal start key, the encoded end key must
		// exactly equal the original end key from the first internal key.
		// Overlapping fragments are required to have exactly equal start and
		// end bounds.
		if i.blockIter.cmp(prevEnd, i.span.End) != 0 {
			i.err = base.CorruptionErrorf("pebble: corrupt keyspan fragmentation")
			i.span = keyspan.Span{}
			return nil
		}
		k, internalValue = i.blockIter.Prev()
	}
	// i.blockIter is positioned over the last internal key for the previous
	// span.

	// Backwards iteration encounters internal keys in the wrong order.
	keyspan.SortKeysByTrailer(&i.span.Keys)

	return &i.span
}

// Error implements (keyspan.FragmentIterator).Error.
func (i *fragmentBlockIter) Error() error {
	return i.err
}

// Close implements (keyspan.FragmentIterator).Close.
func (i *fragmentBlockIter) Close() error {
	var err error
	if i.closeHook != nil {
		err = i.closeHook(i)
	}
	err = firstError(err, i.blockIter.Close())
	return err
}

// First implements (keyspan.FragmentIterator).First
func (i *fragmentBlockIter) First() *keyspan.Span {
	i.dir = +1
	return i.gatherForward(i.blockIter.First())
}

// Last implements (keyspan.FragmentIterator).Last.
func (i *fragmentBlockIter) Last() *keyspan.Span {
	i.dir = -1
	return i.gatherBackward(i.blockIter.Last())
}

// Next implements (keyspan.FragmentIterator).Next.
func (i *fragmentBlockIter) Next() *keyspan.Span {
	switch {
	case i.dir == -1 && !i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is exhausted, before the first key. Move onto the first.
		i.blockIter.First()
		i.dir = +1
	case i.dir == -1 && i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is currently positioned over the last internal key for
		// the previous span. Next it once to move to the first internal key
		// that makes up the current span, and gatherForwaad to land on the
		// first internal key making up the next span.
		//
		// In the diagram below, if the last span returned to the user during
		// reverse iteration was [b,c), i.blockIter is currently positioned at
		// [a,b). The block iter must be positioned over [d,e) to gather the
		// next span's fragments.
		//
		//    ... [a,b) [b,c) [b,c) [b,c) [d,e) ...
		//          ^                       ^
		//     i.blockIter                 want
		if x := i.gatherForward(i.blockIter.Next()); invariants.Enabled && !x.Valid() {
			panic("pebble: invariant violation: next entry unexpectedly invalid")
		}
		i.dir = +1
	}
	return i.gatherForward(&i.blockIter.ikey, i.blockIter.val)
}

// Prev implements (keyspan.FragmentIterator).Prev.
func (i *fragmentBlockIter) Prev() *keyspan.Span {
	switch {
	case i.dir == +1 && !i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is exhausted, after the last key. Move onto the last.
		i.blockIter.Last()
		i.dir = -1
	case i.dir == +1 && i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is currently positioned over the first internal key for
		// the next span. Prev it once to move to the last internal key that
		// makes up the current span, and gatherBackward to land on the last
		// internal key making up the previous span.
		//
		// In the diagram below, if the last span returned to the user during
		// forward iteration was [b,c), i.blockIter is currently positioned at
		// [d,e). The block iter must be positioned over [a,b) to gather the
		// previous span's fragments.
		//
		//    ... [a,b) [b,c) [b,c) [b,c) [d,e) ...
		//          ^                       ^
		//        want                  i.blockIter
		if x := i.gatherBackward(i.blockIter.Prev()); invariants.Enabled && !x.Valid() {
			panic("pebble: invariant violation: previous entry unexpectedly invalid")
		}
		i.dir = -1
	}
	return i.gatherBackward(&i.blockIter.ikey, i.blockIter.val)
}

// SeekGE implements (keyspan.FragmentIterator).SeekGE.
func (i *fragmentBlockIter) SeekGE(k []byte) *keyspan.Span {
	i.dir = +1
	return i.gatherForward(i.blockIter.SeekGE(k, base.SeekGEFlags(0)))
}

// SeekLT implements (keyspan.FragmentIterator).SeekLT.
func (i *fragmentBlockIter) SeekLT(k []byte) *keyspan.Span {
	i.dir = -1
	return i.gatherBackward(i.blockIter.SeekLT(k, base.SeekLTFlagsNone))
}

// String implements fmt.Stringer.
func (i *fragmentBlockIter) String() string {
	return "fragment-block-iter"
}

// SetCloseHook implements sstable.FragmentIterator.
func (i *fragmentBlockIter) SetCloseHook(fn func(i keyspan.FragmentIterator) error) {
	i.closeHook = fn
}
