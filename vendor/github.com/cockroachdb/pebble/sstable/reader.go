// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/vfs"
)

var errCorruptIndexEntry = base.CorruptionErrorf("pebble/table: corrupt index entry")
var errReaderClosed = errors.New("pebble/table: reader is closed")

const (
	// Constants for dynamic readahead of data blocks. Note that the size values
	// make sense as some multiple of the default block size; and they should
	// both be larger than the default block size.
	minFileReadsForReadahead = 2
	// TODO(bilal): Have the initial size value be a factor of the block size,
	// as opposed to a hardcoded value.
	initialReadaheadSize = 64 << 10  /* 64KB */
	maxReadaheadSize     = 256 << 10 /* 256KB */
)

// decodeBlockHandle returns the block handle encoded at the start of src, as
// well as the number of bytes it occupies. It returns zero if given invalid
// input. A block handle for a data block or a first/lower level index block
// should not be decoded using decodeBlockHandle since the caller may validate
// that the number of bytes decoded is equal to the length of src, which will
// be false if the properties are not decoded. In those cases the caller
// should use decodeBlockHandleWithProperties.
func decodeBlockHandle(src []byte) (BlockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return BlockHandle{}, 0
	}
	return BlockHandle{offset, length}, n + m
}

// decodeBlockHandleWithProperties returns the block handle and properties
// encoded in src. src needs to be exactly the length that was encoded. This
// method must be used for data block and first/lower level index blocks. The
// properties in the block handle point to the bytes in src.
func decodeBlockHandleWithProperties(src []byte) (BlockHandleWithProperties, error) {
	bh, n := decodeBlockHandle(src)
	if n == 0 {
		return BlockHandleWithProperties{}, errors.Errorf("invalid BlockHandle")
	}
	return BlockHandleWithProperties{
		BlockHandle: bh,
		Props:       src[n:],
	}, nil
}

func encodeBlockHandle(dst []byte, b BlockHandle) int {
	n := binary.PutUvarint(dst, b.Offset)
	m := binary.PutUvarint(dst[n:], b.Length)
	return n + m
}

func encodeBlockHandleWithProperties(dst []byte, b BlockHandleWithProperties) []byte {
	n := encodeBlockHandle(dst, b.BlockHandle)
	dst = append(dst[:n], b.Props...)
	return dst
}

// block is a []byte that holds a sequence of key/value pairs plus an index
// over those pairs.
type block []byte

// Iterator iterates over an entire table of data.
type Iterator interface {
	base.InternalIterator

	// MaybeFilteredKeys may be called when an iterator is exhausted to indicate
	// whether or not the last positioning method may have skipped any keys due
	// to block-property filters. This is used by the Pebble levelIter to
	// control when an iterator steps to the next sstable.
	//
	// MaybeFilteredKeys may always return false positives, that is it may
	// return true when no keys were filtered. It should only be called when the
	// iterator is exhausted. It must never return false negatives when the
	// iterator is exhausted.
	MaybeFilteredKeys() bool

	SetCloseHook(fn func(i Iterator) error)
}

// singleLevelIterator iterates over an entire table of data. To seek for a given
// key, it first looks in the index for the block that contains that key, and then
// looks inside that block.
type singleLevelIterator struct {
	cmp Compare
	// Global lower/upper bound for the iterator.
	lower []byte
	upper []byte
	bpfs  *BlockPropertiesFilterer
	// Per-block lower/upper bound. Nil if the bound does not apply to the block
	// because we determined the block lies completely within the bound.
	blockLower []byte
	blockUpper []byte
	reader     *Reader
	index      blockIter
	data       blockIter
	dataRS     readaheadState
	// dataBH refers to the last data block that the iterator considered
	// loading. It may not actually have loaded the block, due to an error or
	// because it was considered irrelevant.
	dataBH    BlockHandle
	err       error
	closeHook func(i Iterator) error
	stats     *base.InternalIteratorStats

	// boundsCmp and positionedUsingLatestBounds are for optimizing iteration
	// that uses multiple adjacent bounds. The seek after setting a new bound
	// can use the fact that the iterator is either within the previous bounds
	// or exactly one key before or after the bounds. If the new bounds is
	// after/before the previous bounds, and we are already positioned at a
	// block that is relevant for the new bounds, we can try to first position
	// using Next/Prev (repeatedly) instead of doing a more expensive seek.
	//
	// When there are wide files at higher levels that match the bounds
	// but don't have any data for the bound, we will already be
	// positioned at the key beyond the bounds and won't need to do much
	// work -- given that most data is in L6, such files are likely to
	// dominate the performance of the mergingIter, and may be the main
	// benefit of this performance optimization (of course it also helps
	// when the file that has the data has successive seeks that stay in
	// the same block).
	//
	// Specifically, boundsCmp captures the relationship between the previous
	// and current bounds, if the iterator had been positioned after setting
	// the previous bounds. If it was not positioned, i.e., Seek/First/Last
	// were not called, we don't know where it is positioned and cannot
	// optimize.
	//
	// Example: Bounds moving forward, and iterator exhausted in forward direction.
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a  b  c  d  e  f  g  h  i  j  k ]
	//                                       ^
	//  new bounds = [j, k). Since positionedUsingLatestBounds=true, boundsCmp is
	//  set to +1. SeekGE(j) can use next (the optimization also requires that j
	//  is within the block, but that is not for correctness, but to limit the
	//  optimization to when it will actually be an optimization).
	//
	// Example: Bounds moving forward.
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a  b  c  d  e  f  g  h  i  j  k ]
	//                                 ^
	//  new bounds = [j, k). Since positionedUsingLatestBounds=true, boundsCmp is
	//  set to +1. SeekGE(j) can use next.
	//
	// Example: Bounds moving forward, but iterator not positioned using previous
	//  bounds.
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a  b  c  d  e  f  g  h  i  j  k ]
	//                                             ^
	//  new bounds = [i, j). Iterator is at j since it was never positioned using
	//  [f, h). So positionedUsingLatestBounds=false, and boundsCmp is set to 0.
	//  SeekGE(i) will not use next.
	//
	// Example: Bounds moving forward and sparse file
	//      bounds = [f, h), ^ shows block iterator position
	//  file contents [ a z ]
	//                    ^
	//  new bounds = [j, k). Since positionedUsingLatestBounds=true, boundsCmp is
	//  set to +1. SeekGE(j) notices that the iterator is already past j and does
	//  not need to do anything.
	//
	// Similar examples can be constructed for backward iteration.
	//
	// This notion of exactly one key before or after the bounds is not quite
	// true when block properties are used to ignore blocks. In that case we
	// can't stop precisely at the first block that is past the bounds since
	// we are using the index entries to enforce the bounds.
	//
	// e.g. 3 blocks with keys [b, c]  [f, g], [i, j, k] with index entries d,
	// h, l. And let the lower bound be k, and we are reverse iterating. If
	// the block [i, j, k] is ignored due to the block interval annotations we
	// do need to move the index to block [f, g] since the index entry for the
	// [i, j, k] block is l which is not less than the lower bound of k. So we
	// have passed the entries i, j.
	//
	// This behavior is harmless since the block property filters are fixed
	// for the lifetime of the iterator so i, j are irrelevant. In addition,
	// the current code will not load the [f, g] block, so the seek
	// optimization that attempts to use Next/Prev do not apply anyway.
	boundsCmp                   int
	positionedUsingLatestBounds bool

	// exhaustedBounds represents whether the iterator is exhausted for
	// iteration by reaching the upper or lower bound. +1 when exhausted
	// the upper bound, -1 when exhausted the lower bound, and 0 when
	// neither. It is used for invariant checking.
	exhaustedBounds int8

	// maybeFilteredKeysSingleLevel indicates whether the last iterator
	// positioning operation may have skipped any data blocks due to
	// block-property filters when positioning the index.
	maybeFilteredKeysSingleLevel bool

	// useFilter specifies whether the filter block in this sstable, if present,
	// should be used for prefix seeks or not. In some cases it is beneficial
	// to skip a filter block even if it exists (eg. if probability of a match
	// is high).
	useFilter              bool
	lastBloomFilterMatched bool
}

// singleLevelIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*singleLevelIterator)(nil)

var singleLevelIterPool = sync.Pool{
	New: func() interface{} {
		i := &singleLevelIterator{}
		// Note: this is a no-op if invariants are disabled or race is enabled.
		invariants.SetFinalizer(i, checkSingleLevelIterator)
		return i
	},
}

var twoLevelIterPool = sync.Pool{
	New: func() interface{} {
		i := &twoLevelIterator{}
		// Note: this is a no-op if invariants are disabled or race is enabled.
		invariants.SetFinalizer(i, checkTwoLevelIterator)
		return i
	},
}

// TODO(jackson): rangedel fragmentBlockIters can't be pooled because of some
// code paths that double Close the iters. Fix the double close and pool the
// *fragmentBlockIter type directly.

var rangeKeyFragmentBlockIterPool = sync.Pool{
	New: func() interface{} {
		i := &rangeKeyFragmentBlockIter{}
		// Note: this is a no-op if invariants are disabled or race is enabled.
		invariants.SetFinalizer(i, checkRangeKeyFragmentBlockIterator)
		return i
	},
}

func checkSingleLevelIterator(obj interface{}) {
	i := obj.(*singleLevelIterator)
	if p := i.data.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
	if p := i.index.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
}

func checkTwoLevelIterator(obj interface{}) {
	i := obj.(*twoLevelIterator)
	if p := i.data.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
	if p := i.index.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
}

func checkRangeKeyFragmentBlockIterator(obj interface{}) {
	i := obj.(*rangeKeyFragmentBlockIter)
	if p := i.blockIter.cacheHandle.Get(); p != nil {
		fmt.Fprintf(os.Stderr, "fragmentBlockIter.blockIter.cacheHandle is not nil: %p\n", p)
		os.Exit(1)
	}
}

// init initializes a singleLevelIterator for reading from the table. It is
// synonmous with Reader.NewIter, but allows for reusing of the iterator
// between different Readers.
func (i *singleLevelIterator) init(
	r *Reader,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	useFilter bool,
	stats *base.InternalIteratorStats,
) error {
	if r.err != nil {
		return r.err
	}
	indexH, err := r.readIndex()
	if err != nil {
		return err
	}

	i.lower = lower
	i.upper = upper
	i.bpfs = filterer
	i.useFilter = useFilter
	i.reader = r
	i.cmp = r.Compare
	i.stats = stats
	err = i.index.initHandle(i.cmp, indexH, r.Properties.GlobalSeqNum)
	if err != nil {
		// blockIter.Close releases indexH and always returns a nil error
		_ = i.index.Close()
		return err
	}
	i.dataRS.size = initialReadaheadSize
	return nil
}

// setupForCompaction sets up the singleLevelIterator for use with compactionIter.
// Currently, it skips readahead ramp-up. It should be called after init is called.
func (i *singleLevelIterator) setupForCompaction() {
	if i.reader.fs != nil {
		f, err := i.reader.fs.Open(i.reader.filename, vfs.SequentialReadsOption)
		if err == nil {
			// Given that this iterator is for a compaction, we can assume that it
			// will be read sequentially and we can skip the readahead ramp-up.
			i.dataRS.sequentialFile = f
		}
	}
}

func (i *singleLevelIterator) resetForReuse() singleLevelIterator {
	return singleLevelIterator{
		index: i.index.resetForReuse(),
		data:  i.data.resetForReuse(),
	}
}

func (i *singleLevelIterator) initBounds() {
	// Trim the iteration bounds for the current block. We don't have to check
	// the bounds on each iteration if the block is entirely contained within the
	// iteration bounds.
	i.blockLower = i.lower
	if i.blockLower != nil {
		key, _ := i.data.First()
		if key != nil && i.cmp(i.blockLower, key.UserKey) < 0 {
			// The lower-bound is less than the first key in the block. No need
			// to check the lower-bound again for this block.
			i.blockLower = nil
		}
	}
	i.blockUpper = i.upper
	if i.blockUpper != nil && i.cmp(i.blockUpper, i.index.Key().UserKey) > 0 {
		// The upper-bound is greater than the index key which itself is greater
		// than or equal to every key in the block. No need to check the
		// upper-bound again for this block.
		i.blockUpper = nil
	}
}

type loadBlockResult int8

const (
	loadBlockOK loadBlockResult = iota
	// Could be due to error or because no block left to load.
	loadBlockFailed
	loadBlockIrrelevant
)

// loadBlock loads the block at the current index position and leaves i.data
// unpositioned. If unsuccessful, it sets i.err to any error encountered, which
// may be nil if we have simply exhausted the entire table.
func (i *singleLevelIterator) loadBlock(dir int8) loadBlockResult {
	if !i.index.valid() {
		// Ensure the data block iterator is invalidated even if loading of the block
		// fails.
		i.data.invalidate()
		return loadBlockFailed
	}
	// Load the next block.
	v := i.index.Value()
	bhp, err := decodeBlockHandleWithProperties(v)
	if i.dataBH == bhp.BlockHandle && i.data.valid() {
		// We're already at the data block we want to load. Reset bounds in case
		// they changed since the last seek, but don't reload the block from cache
		// or disk.
		//
		// It's safe to leave i.data in its original state here, as all callers to
		// loadBlock make an absolute positioning call (i.e. a seek, first, or last)
		// to `i.data` right after loadBlock returns loadBlockOK.
		i.initBounds()
		return loadBlockOK
	}
	// Ensure the data block iterator is invalidated even if loading of the block
	// fails.
	i.data.invalidate()
	i.dataBH = bhp.BlockHandle
	if err != nil {
		i.err = errCorruptIndexEntry
		return loadBlockFailed
	}
	if i.bpfs != nil {
		intersects, err := i.bpfs.intersects(bhp.Props)
		if err != nil {
			i.err = errCorruptIndexEntry
			return loadBlockFailed
		}
		if intersects == blockMaybeExcluded {
			intersects = i.resolveMaybeExcluded(dir)
		}
		if intersects == blockExcluded {
			i.maybeFilteredKeysSingleLevel = true
			return loadBlockIrrelevant
		}
		// blockIntersects
	}
	block, err := i.readBlockWithStats(i.dataBH, &i.dataRS)
	if err != nil {
		i.err = err
		return loadBlockFailed
	}
	i.err = i.data.initHandle(i.cmp, block, i.reader.Properties.GlobalSeqNum)
	if i.err != nil {
		// The block is partially loaded, and we don't want it to appear valid.
		i.data.invalidate()
		return loadBlockFailed
	}
	i.initBounds()
	return loadBlockOK
}

// resolveMaybeExcluded is invoked when the block-property filterer has found
// that a block is excluded according to its properties but only if its bounds
// fall within the filter's current bounds.  This function consults the
// apprioriate bound, depending on the iteration direction, and returns either
// `blockIntersects` or `blockMaybeExcluded`.
func (i *singleLevelIterator) resolveMaybeExcluded(dir int8) intersectsResult {
	// TODO(jackson): We could first try comparing to top-level index block's
	// key, and if within bounds avoid per-data block key comparisons.

	// This iterator is configured with a bound-limited block property
	// filter. The bpf determined this block could be excluded from
	// iteration based on the property encoded in the block handle.
	// However, we still need to determine if the block is wholly
	// contained within the filter's key bounds.
	//
	// External guarantees ensure all the block's keys are ≥ the
	// filter's lower bound during forward iteration, and that all the
	// block's keys are < the filter's upper bound during backward
	// iteration. We only need to determine if the opposite bound is
	// also met.
	//
	// The index separator in index.Key() provides an inclusive
	// upper-bound for the data block's keys, guaranteeing that all its
	// keys are ≤ index.Key(). For forward iteration, this is all we
	// need.
	if dir > 0 {
		// Forward iteration.
		if i.bpfs.boundLimitedFilter.KeyIsWithinUpperBound(i.index.Key()) {
			return blockExcluded
		}
		return blockIntersects
	}

	// Reverse iteration.
	//
	// Because we're iterating in the reverse direction, we don't yet have
	// enough context available to determine if the block is wholly contained
	// within its bounds. This case arises only during backward iteration,
	// because of the way the index is structured.
	//
	// Consider a bound-limited bpf limited to the bounds [b,d), loading the
	// block with separator `c`. During reverse iteration, the guarantee that
	// all the block's keys are < `d` is externally provided, but no guarantee
	// is made on the bpf's lower bound. The separator `c` only provides an
	// inclusive upper bound on the block's keys, indicating that the
	// corresponding block handle points to a block containing only keys ≤ `c`.
	//
	// To establish a lower bound, we step the index backwards to read the
	// previous block's separator, which provides an inclusive lower bound on
	// the original block's keys. Afterwards, we step forward to restore our
	// index position.
	if peekKey, _ := i.index.Prev(); peekKey == nil {
		// The original block points to the first block of this index block. If
		// there's a two-level index, it could potentially provide a lower
		// bound, but the code refactoring necessary to read it doesn't seem
		// worth the payoff. We fall through to loading the block.
	} else if i.bpfs.boundLimitedFilter.KeyIsWithinLowerBound(peekKey) {
		// The lower-bound on the original block falls within the filter's
		// bounds, and we can skip the block (after restoring our current index
		// position).
		_, _ = i.index.Next()
		return blockExcluded
	}
	_, _ = i.index.Next()
	return blockIntersects
}

func (i *singleLevelIterator) readBlockWithStats(
	bh BlockHandle, raState *readaheadState,
) (cache.Handle, error) {
	block, cacheHit, err := i.reader.readBlock(bh, nil /* transform */, raState)
	if err == nil && i.stats != nil {
		n := bh.Length
		i.stats.BlockBytes += n
		if cacheHit {
			i.stats.BlockBytesInCache += n
		}
	}
	return block, err
}

func (i *singleLevelIterator) initBoundsForAlreadyLoadedBlock() {
	if i.data.firstKey.UserKey == nil {
		panic("initBoundsForAlreadyLoadedBlock must not be called on empty or corrupted block")
	}
	i.blockLower = i.lower
	if i.blockLower != nil {
		if i.data.firstKey.UserKey != nil && i.cmp(i.blockLower, i.data.firstKey.UserKey) < 0 {
			// The lower-bound is less than the first key in the block. No need
			// to check the lower-bound again for this block.
			i.blockLower = nil
		}
	}
	i.blockUpper = i.upper
	if i.blockUpper != nil && i.cmp(i.blockUpper, i.index.Key().UserKey) > 0 {
		// The upper-bound is greater than the index key which itself is greater
		// than or equal to every key in the block. No need to check the
		// upper-bound again for this block.
		i.blockUpper = nil
	}
}

// The number of times to call Next/Prev in a block before giving up and seeking.
// The value of 4 is arbitrary.
// TODO(sumeer): experiment with dynamic adjustment based on the history of
// seeks for a particular iterator.
const numStepsBeforeSeek = 4

func (i *singleLevelIterator) trySeekGEUsingNextWithinBlock(
	key []byte,
) (k *InternalKey, v []byte, done bool) {
	k, v = i.data.Key(), i.data.Value()
	for j := 0; j < numStepsBeforeSeek; j++ {
		curKeyCmp := i.cmp(k.UserKey, key)
		if curKeyCmp >= 0 {
			if i.blockUpper != nil && i.cmp(k.UserKey, i.blockUpper) >= 0 {
				i.exhaustedBounds = +1
				return nil, nil, true
			}
			return k, v, true
		}
		k, v = i.data.Next()
		if k == nil {
			break
		}
	}
	return k, v, false
}

func (i *singleLevelIterator) trySeekLTUsingPrevWithinBlock(
	key []byte,
) (k *InternalKey, v []byte, done bool) {
	k, v = i.data.Key(), i.data.Value()
	for j := 0; j < numStepsBeforeSeek; j++ {
		curKeyCmp := i.cmp(k.UserKey, key)
		if curKeyCmp < 0 {
			if i.blockLower != nil && i.cmp(k.UserKey, i.blockLower) < 0 {
				i.exhaustedBounds = -1
				return nil, nil, true
			}
			return k, v, true
		}
		k, v = i.data.Prev()
		if k == nil {
			break
		}
	}
	return k, v, false
}

func (i *singleLevelIterator) recordOffset() uint64 {
	offset := i.dataBH.Offset
	if i.data.valid() {
		// - i.dataBH.Length/len(i.data.data) is the compression ratio. If
		//   uncompressed, this is 1.
		// - i.data.nextOffset is the uncompressed position of the current record
		//   in the block.
		// - i.dataBH.Offset is the offset of the block in the sstable before
		//   decompression.
		offset += (uint64(i.data.nextOffset) * i.dataBH.Length) / uint64(len(i.data.data))
	} else {
		// Last entry in the block must increment bytes iterated by the size of the block trailer
		// and restart points.
		offset += i.dataBH.Length + blockTrailerLen
	}
	return offset
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator) SeekGE(key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	// The i.exhaustedBounds comparison indicates that the upper bound was
	// reached. The i.data.isDataInvalidated() indicates that the sstable was
	// exhausted.
	if flags.TrySeekUsingNext() && (i.exhaustedBounds == +1 || i.data.isDataInvalidated()) {
		// Already exhausted, so return nil.
		return nil, nil
	}

	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	boundsCmp := i.boundsCmp
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	i.positionedUsingLatestBounds = true
	return i.seekGEHelper(key, boundsCmp, flags)
}

// seekGEHelper contains the common functionality for SeekGE and SeekPrefixGE.
func (i *singleLevelIterator) seekGEHelper(
	key []byte, boundsCmp int, flags base.SeekGEFlags,
) (*InternalKey, []byte) {
	// Invariant: trySeekUsingNext => !i.data.isDataInvalidated() && i.exhaustedBounds != +1

	// SeekGE performs various step-instead-of-seeking optimizations: eg enabled
	// by trySeekUsingNext, or by monotonically increasing bounds (i.boundsCmp).
	// Care must be taken to ensure that when performing these optimizations and
	// the iterator becomes exhausted, i.maybeFilteredKeys is set appropriately.
	// Consider a previous SeekGE that filtered keys from k until the current
	// iterator position.
	//
	// If the previous SeekGE exhausted the iterator, it's possible keys greater
	// than or equal to the current search key were filtered. We must not reuse
	// the current iterator position without remembering the previous value of
	// maybeFilteredKeys.

	var dontSeekWithinBlock bool
	if !i.data.isDataInvalidated() && !i.index.isDataInvalidated() && i.data.valid() && i.index.valid() &&
		boundsCmp > 0 && i.cmp(key, i.index.Key().UserKey) <= 0 {
		// Fast-path: The bounds have moved forward and this SeekGE is
		// respecting the lower bound (guaranteed by Iterator). We know that
		// the iterator must already be positioned within or just outside the
		// previous bounds. Therefore it cannot be positioned at a block (or
		// the position within that block) that is ahead of the seek position.
		// However it can be positioned at an earlier block. This fast-path to
		// use Next() on the block is only applied when we are already at the
		// block that the slow-path (the else-clause) would load -- this is
		// the motivation for the i.cmp(key, i.index.Key().UserKey) <= 0
		// predicate.
		i.initBoundsForAlreadyLoadedBlock()
		ikey, val, done := i.trySeekGEUsingNextWithinBlock(key)
		if done {
			return ikey, val
		}
		if ikey == nil {
			// Done with this block.
			dontSeekWithinBlock = true
		}
	} else {
		// Cannot use bounds monotonicity. But may be able to optimize if
		// caller claimed externally known invariant represented by
		// flags.TrySeekUsingNext().
		if flags.TrySeekUsingNext() {
			// seekPrefixGE or SeekGE has already ensured
			// !i.data.isDataInvalidated() && i.exhaustedBounds != +1
			currKey := i.data.Key()
			value := i.data.Value()
			less := i.cmp(currKey.UserKey, key) < 0
			// We could be more sophisticated and confirm that the seek
			// position is within the current block before applying this
			// optimization. But there may be some benefit even if it is in
			// the next block, since we can avoid seeking i.index.
			for j := 0; less && j < numStepsBeforeSeek; j++ {
				currKey, value = i.Next()
				if currKey == nil {
					return nil, nil
				}
				less = i.cmp(currKey.UserKey, key) < 0
			}
			if !less {
				if i.blockUpper != nil && i.cmp(currKey.UserKey, i.blockUpper) >= 0 {
					i.exhaustedBounds = +1
					return nil, nil
				}
				return currKey, value
			}
		}

		// Slow-path.

		// Since we're re-seeking the iterator, the previous value of
		// maybeFilteredKeysSingleLevel is irrelevant. If we filter out blocks
		// during seeking, loadBlock will set it to true.
		i.maybeFilteredKeysSingleLevel = false

		var ikey *InternalKey
		if ikey, _ = i.index.SeekGE(key, flags.DisableTrySeekUsingNext()); ikey == nil {
			// The target key is greater than any key in the index block.
			// Invalidate the block iterator so that a subsequent call to Prev()
			// will return the last key in the table.
			i.data.invalidate()
			return nil, nil
		}
		result := i.loadBlock(+1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving
			// to the next block if upper bound is already exceeded. Note that
			// the next block starts with keys >= ikey.UserKey since even
			// though this is the block separator, the same user key can span
			// multiple blocks. Since upper is exclusive we use >= below.
			if i.upper != nil && i.cmp(ikey.UserKey, i.upper) >= 0 {
				i.exhaustedBounds = +1
				return nil, nil
			}
			// Want to skip to the next block.
			dontSeekWithinBlock = true
		}
	}
	if !dontSeekWithinBlock {
		if ikey, val := i.data.SeekGE(key, flags.DisableTrySeekUsingNext()); ikey != nil {
			if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
				i.exhaustedBounds = +1
				return nil, nil
			}
			return ikey, val
		}
	}
	return i.skipForward()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *singleLevelIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	k, v := i.seekPrefixGE(prefix, key, flags, i.useFilter)
	return k, v
}

func (i *singleLevelIterator) seekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags, checkFilter bool,
) (k *InternalKey, value []byte) {
	i.err = nil // clear cached iteration error
	if checkFilter && i.reader.tableFilter != nil {
		if !i.lastBloomFilterMatched {
			// Iterator is not positioned based on last seek.
			flags = flags.DisableTrySeekUsingNext()
		}
		i.lastBloomFilterMatched = false
		// Check prefix bloom filter.
		var dataH cache.Handle
		dataH, i.err = i.reader.readFilter()
		if i.err != nil {
			i.data.invalidate()
			return nil, nil
		}
		mayContain := i.reader.tableFilter.mayContain(dataH.Get(), prefix)
		dataH.Release()
		if !mayContain {
			// This invalidation may not be necessary for correctness, and may
			// be a place to optimize later by reusing the already loaded
			// block. It was necessary in earlier versions of the code since
			// the caller was allowed to call Next when SeekPrefixGE returned
			// nil. This is no longer allowed.
			i.data.invalidate()
			return nil, nil
		}
		i.lastBloomFilterMatched = true
	}
	// The i.exhaustedBounds comparison indicates that the upper bound was
	// reached. The i.data.isDataInvalidated() indicates that the sstable was
	// exhausted.
	if flags.TrySeekUsingNext() && (i.exhaustedBounds == +1 || i.data.isDataInvalidated()) {
		// Already exhausted, so return nil.
		return nil, nil
	}
	// Bloom filter matches, or skipped, so this method will position the
	// iterator.
	i.exhaustedBounds = 0
	boundsCmp := i.boundsCmp
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	i.positionedUsingLatestBounds = true
	k, value = i.seekGEHelper(key, boundsCmp, flags)
	return k, value
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *singleLevelIterator) SeekLT(key []byte, flags base.SeekLTFlags) (*InternalKey, []byte) {
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	boundsCmp := i.boundsCmp
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	// Seeking operations perform various step-instead-of-seeking optimizations:
	// eg by considering monotonically increasing bounds (i.boundsCmp). Care
	// must be taken to ensure that when performing these optimizations and the
	// iterator becomes exhausted i.maybeFilteredKeysSingleLevel is set
	// appropriately.  Consider a previous SeekLT that filtered keys from k
	// until the current iterator position.
	//
	// If the previous SeekLT did exhausted the iterator, it's possible keys
	// less than the current search key were filtered. We must not reuse the
	// current iterator position without remembering the previous value of
	// maybeFilteredKeysSingleLevel.

	i.positionedUsingLatestBounds = true

	var dontSeekWithinBlock bool
	if !i.data.isDataInvalidated() && !i.index.isDataInvalidated() && i.data.valid() && i.index.valid() &&
		boundsCmp < 0 && i.cmp(i.data.firstKey.UserKey, key) < 0 {
		// Fast-path: The bounds have moved backward, and this SeekLT is
		// respecting the upper bound (guaranteed by Iterator). We know that
		// the iterator must already be positioned within or just outside the
		// previous bounds. Therefore it cannot be positioned at a block (or
		// the position within that block) that is behind the seek position.
		// However it can be positioned at a later block. This fast-path to
		// use Prev() on the block is only applied when we are already at the
		// block that can satisfy this seek -- this is the motivation for the
		// the i.cmp(i.data.firstKey.UserKey, key) < 0 predicate.
		i.initBoundsForAlreadyLoadedBlock()
		ikey, val, done := i.trySeekLTUsingPrevWithinBlock(key)
		if done {
			return ikey, val
		}
		if ikey == nil {
			// Done with this block.
			dontSeekWithinBlock = true
		}
	} else {
		// Slow-path.
		i.maybeFilteredKeysSingleLevel = false
		var ikey *InternalKey

		// NB: If a bound-limited block property filter is configured, it's
		// externally ensured that the filter is disabled (through returning
		// Intersects=false irrespective of the block props provided) during
		// seeks.
		if ikey, _ = i.index.SeekGE(key, base.SeekGEFlagsNone); ikey == nil {
			ikey, _ = i.index.Last()
			if ikey == nil {
				return nil, nil
			}
		}
		// INVARIANT: ikey != nil.
		result := i.loadBlock(-1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the lower bound here since don't want to bother moving
			// to the previous block if lower bound is already exceeded. Note
			// that the previous block starts with keys <= ikey.UserKey since
			// even though this is the current block's separator, the same
			// user key can span multiple blocks.
			if i.lower != nil && i.cmp(ikey.UserKey, i.lower) < 0 {
				i.exhaustedBounds = -1
				return nil, nil
			}
			// Want to skip to the previous block.
			dontSeekWithinBlock = true
		}
	}
	if !dontSeekWithinBlock {
		if ikey, val := i.data.SeekLT(key, flags); ikey != nil {
			if i.blockLower != nil && i.cmp(ikey.UserKey, i.blockLower) < 0 {
				i.exhaustedBounds = -1
				return nil, nil
			}
			return ikey, val
		}
	}
	// The index contains separator keys which may lie between
	// user-keys. Consider the user-keys:
	//
	//   complete
	// ---- new block ---
	//   complexion
	//
	// If these two keys end one block and start the next, the index key may
	// be chosen as "compleu". The SeekGE in the index block will then point
	// us to the block containing "complexion". If this happens, we want the
	// last key from the previous data block.
	return i.skipBackward()
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *singleLevelIterator) First() (*InternalKey, []byte) {
	if i.lower != nil {
		panic("singleLevelIterator.First() used despite lower bound")
	}
	i.positionedUsingLatestBounds = true
	i.maybeFilteredKeysSingleLevel = false
	return i.firstInternal()
}

// firstInternal is a helper used for absolute positioning in a single-level
// index file, or for positioning in the second-level index in a two-level
// index file. For the latter, one cannot make any claims about absolute
// positioning.
func (i *singleLevelIterator) firstInternal() (*InternalKey, []byte) {
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var ikey *InternalKey
	if ikey, _ = i.index.First(); ikey == nil {
		i.data.invalidate()
		return nil, nil
	}
	result := i.loadBlock(+1)
	if result == loadBlockFailed {
		return nil, nil
	}
	if result == loadBlockOK {
		if ikey, val := i.data.First(); ikey != nil {
			if i.blockUpper != nil && i.cmp(ikey.UserKey, i.blockUpper) >= 0 {
				i.exhaustedBounds = +1
				return nil, nil
			}
			return ikey, val
		}
		// Else fall through to skipForward.
	} else {
		// result == loadBlockIrrelevant. Enforce the upper bound here since
		// don't want to bother moving to the next block if upper bound is
		// already exceeded. Note that the next block starts with keys >=
		// ikey.UserKey since even though this is the block separator, the
		// same user key can span multiple blocks. Since upper is exclusive we
		// use >= below.
		if i.upper != nil && i.cmp(ikey.UserKey, i.upper) >= 0 {
			i.exhaustedBounds = +1
			return nil, nil
		}
		// Else fall through to skipForward.
	}

	return i.skipForward()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *singleLevelIterator) Last() (*InternalKey, []byte) {
	if i.upper != nil {
		panic("singleLevelIterator.Last() used despite upper bound")
	}
	i.positionedUsingLatestBounds = true
	i.maybeFilteredKeysSingleLevel = false
	return i.lastInternal()
}

// lastInternal is a helper used for absolute positioning in a single-level
// index file, or for positioning in the second-level index in a two-level
// index file. For the latter, one cannot make any claims about absolute
// positioning.
func (i *singleLevelIterator) lastInternal() (*InternalKey, []byte) {
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var ikey *InternalKey
	if ikey, _ = i.index.Last(); ikey == nil {
		i.data.invalidate()
		return nil, nil
	}
	result := i.loadBlock(-1)
	if result == loadBlockFailed {
		return nil, nil
	}
	if result == loadBlockOK {
		if ikey, val := i.data.Last(); ikey != nil {
			if i.blockLower != nil && i.cmp(ikey.UserKey, i.blockLower) < 0 {
				i.exhaustedBounds = -1
				return nil, nil
			}
			return ikey, val
		}
		// Else fall through to skipBackward.
	} else {
		// result == loadBlockIrrelevant. Enforce the lower bound here since
		// don't want to bother moving to the previous block if lower bound is
		// already exceeded. Note that the previous block starts with keys <=
		// key.UserKey since even though this is the current block's
		// separator, the same user key can span multiple blocks.
		if i.lower != nil && i.cmp(ikey.UserKey, i.lower) < 0 {
			i.exhaustedBounds = -1
			return nil, nil
		}
	}

	return i.skipBackward()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
// Note: compactionIterator.Next mirrors the implementation of Iterator.Next
// due to performance. Keep the two in sync.
func (i *singleLevelIterator) Next() (*InternalKey, []byte) {
	if i.exhaustedBounds == +1 {
		panic("Next called even though exhausted upper bound")
	}
	i.exhaustedBounds = 0
	i.maybeFilteredKeysSingleLevel = false
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	if i.err != nil {
		return nil, nil
	}
	if key, val := i.data.Next(); key != nil {
		if i.blockUpper != nil && i.cmp(key.UserKey, i.blockUpper) >= 0 {
			i.exhaustedBounds = +1
			return nil, nil
		}
		return key, val
	}
	return i.skipForward()
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *singleLevelIterator) Prev() (*InternalKey, []byte) {
	if i.exhaustedBounds == -1 {
		panic("Prev called even though exhausted lower bound")
	}
	i.exhaustedBounds = 0
	i.maybeFilteredKeysSingleLevel = false
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	if i.err != nil {
		return nil, nil
	}
	if key, val := i.data.Prev(); key != nil {
		if i.blockLower != nil && i.cmp(key.UserKey, i.blockLower) < 0 {
			i.exhaustedBounds = -1
			return nil, nil
		}
		return key, val
	}
	return i.skipBackward()
}

func (i *singleLevelIterator) skipForward() (*InternalKey, []byte) {
	for {
		var key *InternalKey
		if key, _ = i.index.Next(); key == nil {
			i.data.invalidate()
			break
		}
		result := i.loadBlock(+1)
		if result != loadBlockOK {
			if i.err != nil {
				break
			}
			if result == loadBlockFailed {
				// We checked that i.index was at a valid entry, so
				// loadBlockFailed could not have happened due to to i.index
				// being exhausted, and must be due to an error.
				panic("loadBlock should not have failed with no error")
			}
			// result == loadBlockIrrelevant. Enforce the upper bound here
			// since don't want to bother moving to the next block if upper
			// bound is already exceeded. Note that the next block starts with
			// keys >= key.UserKey since even though this is the block
			// separator, the same user key can span multiple blocks. Since
			// upper is exclusive we use >= below.
			if i.upper != nil && i.cmp(key.UserKey, i.upper) >= 0 {
				i.exhaustedBounds = +1
				return nil, nil
			}
			continue
		}
		if key, val := i.data.First(); key != nil {
			if i.blockUpper != nil && i.cmp(key.UserKey, i.blockUpper) >= 0 {
				i.exhaustedBounds = +1
				return nil, nil
			}
			return key, val
		}
	}
	return nil, nil
}

func (i *singleLevelIterator) skipBackward() (*InternalKey, []byte) {
	for {
		var key *InternalKey
		if key, _ = i.index.Prev(); key == nil {
			i.data.invalidate()
			break
		}
		result := i.loadBlock(-1)
		if result != loadBlockOK {
			if i.err != nil {
				break
			}
			if result == loadBlockFailed {
				// We checked that i.index was at a valid entry, so
				// loadBlockFailed could not have happened due to to i.index
				// being exhausted, and must be due to an error.
				panic("loadBlock should not have failed with no error")
			}
			// result == loadBlockIrrelevant. Enforce the lower bound here
			// since don't want to bother moving to the previous block if lower
			// bound is already exceeded. Note that the previous block starts with
			// keys <= key.UserKey since even though this is the current block's
			// separator, the same user key can span multiple blocks.
			if i.lower != nil && i.cmp(key.UserKey, i.lower) < 0 {
				i.exhaustedBounds = -1
				return nil, nil
			}
			continue
		}
		key, val := i.data.Last()
		if key == nil {
			return nil, nil
		}
		if i.blockLower != nil && i.cmp(key.UserKey, i.blockLower) < 0 {
			i.exhaustedBounds = -1
			return nil, nil
		}
		return key, val
	}
	return nil, nil
}

// Error implements internalIterator.Error, as documented in the pebble
// package.
func (i *singleLevelIterator) Error() error {
	if err := i.data.Error(); err != nil {
		return err
	}
	return i.err
}

// MaybeFilteredKeys may be called when an iterator is exhausted to indicate
// whether or not the last positioning method may have skipped any keys due to
// block-property filters.
func (i *singleLevelIterator) MaybeFilteredKeys() bool {
	return i.maybeFilteredKeysSingleLevel
}

// SetCloseHook sets a function that will be called when the iterator is
// closed.
func (i *singleLevelIterator) SetCloseHook(fn func(i Iterator) error) {
	i.closeHook = fn
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *singleLevelIterator) Close() error {
	var err error
	if i.closeHook != nil {
		err = firstError(err, i.closeHook(i))
	}
	err = firstError(err, i.data.Close())
	err = firstError(err, i.index.Close())
	if i.dataRS.sequentialFile != nil {
		err = firstError(err, i.dataRS.sequentialFile.Close())
		i.dataRS.sequentialFile = nil
	}
	err = firstError(err, i.err)
	if i.bpfs != nil {
		releaseBlockPropertiesFilterer(i.bpfs)
	}
	*i = i.resetForReuse()
	singleLevelIterPool.Put(i)
	return err
}

func (i *singleLevelIterator) String() string {
	return i.reader.fileNum.String()
}

// Deterministic disabling of the bounds-based optimization that avoids seeking.
// Uses the iterator pointer, since we want diversity in iterator behavior for
// the same SetBounds call. Used for tests.
func disableBoundsOpt(bound []byte, ptr uintptr) bool {
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	simpleHash := (11400714819323198485 * uint64(ptr)) >> 63
	return bound[len(bound)-1]&byte(1) == 0 && simpleHash == 0
}

// SetBounds implements internalIterator.SetBounds, as documented in the pebble
// package.
func (i *singleLevelIterator) SetBounds(lower, upper []byte) {
	i.boundsCmp = 0
	if i.positionedUsingLatestBounds {
		if i.upper != nil && lower != nil && i.cmp(i.upper, lower) <= 0 {
			i.boundsCmp = +1
			if invariants.Enabled && disableBoundsOpt(lower, uintptr(unsafe.Pointer(i))) {
				i.boundsCmp = 0
			}
		} else if i.lower != nil && upper != nil && i.cmp(upper, i.lower) <= 0 {
			i.boundsCmp = -1
			if invariants.Enabled && disableBoundsOpt(upper, uintptr(unsafe.Pointer(i))) {
				i.boundsCmp = 0
			}
		}
		i.positionedUsingLatestBounds = false
	}
	i.lower = lower
	i.upper = upper
	i.blockLower = nil
	i.blockUpper = nil
}

var _ base.InternalIterator = &singleLevelIterator{}
var _ base.InternalIterator = &twoLevelIterator{}

// compactionIterator is similar to Iterator but it increments the number of
// bytes that have been iterated through.
type compactionIterator struct {
	*singleLevelIterator
	bytesIterated *uint64
	prevOffset    uint64
}

// compactionIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*compactionIterator)(nil)

func (i *compactionIterator) String() string {
	return i.reader.fileNum.String()
}

func (i *compactionIterator) SeekGE(key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	panic("pebble: SeekGE unimplemented")
}

func (i *compactionIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (i *compactionIterator) SeekLT(key []byte, flags base.SeekLTFlags) (*InternalKey, []byte) {
	panic("pebble: SeekLT unimplemented")
}

func (i *compactionIterator) First() (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error
	return i.skipForward(i.singleLevelIterator.First())
}

func (i *compactionIterator) Last() (*InternalKey, []byte) {
	panic("pebble: Last unimplemented")
}

// Note: compactionIterator.Next mirrors the implementation of Iterator.Next
// due to performance. Keep the two in sync.
func (i *compactionIterator) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	return i.skipForward(i.data.Next())
}

func (i *compactionIterator) Prev() (*InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

func (i *compactionIterator) skipForward(key *InternalKey, val []byte) (*InternalKey, []byte) {
	if key == nil {
		for {
			if key, _ := i.index.Next(); key == nil {
				break
			}
			result := i.loadBlock(+1)
			if result != loadBlockOK {
				if i.err != nil {
					break
				}
				switch result {
				case loadBlockFailed:
					// We checked that i.index was at a valid entry, so
					// loadBlockFailed could not have happened due to to i.index
					// being exhausted, and must be due to an error.
					panic("loadBlock should not have failed with no error")
				case loadBlockIrrelevant:
					panic("compactionIter should not be using block intervals for skipping")
				default:
					panic(fmt.Sprintf("unexpected case %d", result))
				}
			}
			// result == loadBlockOK
			if key, val = i.data.First(); key != nil {
				break
			}
		}
	}

	curOffset := i.recordOffset()
	*i.bytesIterated += uint64(curOffset - i.prevOffset)
	i.prevOffset = curOffset
	return key, val
}

type twoLevelIterator struct {
	singleLevelIterator
	// maybeFilteredKeysSingleLevel indicates whether the last iterator
	// positioning operation may have skipped any index blocks due to
	// block-property filters when positioning the top-level-index.
	maybeFilteredKeysTwoLevel bool
	topLevelIndex             blockIter
}

// twoLevelIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*twoLevelIterator)(nil)

// loadIndex loads the index block at the current top level index position and
// leaves i.index unpositioned. If unsuccessful, it gets i.err to any error
// encountered, which may be nil if we have simply exhausted the entire table.
// This is used for two level indexes.
func (i *twoLevelIterator) loadIndex(dir int8) loadBlockResult {
	// Ensure the data block iterator is invalidated even if loading of the
	// index fails.
	i.data.invalidate()
	if !i.topLevelIndex.valid() {
		i.index.offset = 0
		i.index.restarts = 0
		return loadBlockFailed
	}
	bhp, err := decodeBlockHandleWithProperties(i.topLevelIndex.Value())
	if err != nil {
		i.err = base.CorruptionErrorf("pebble/table: corrupt top level index entry")
		return loadBlockFailed
	}
	if i.bpfs != nil {
		intersects, err := i.bpfs.intersects(bhp.Props)
		if err != nil {
			i.err = errCorruptIndexEntry
			return loadBlockFailed
		}
		if intersects == blockMaybeExcluded {
			intersects = i.resolveMaybeExcluded(dir)
		}
		if intersects == blockExcluded {
			i.maybeFilteredKeysTwoLevel = true
			return loadBlockIrrelevant
		}
		// blockIntersects
	}
	indexBlock, err := i.readBlockWithStats(bhp.BlockHandle, nil /* readaheadState */)
	if err != nil {
		i.err = err
		return loadBlockFailed
	}
	if i.err = i.index.initHandle(
		i.cmp, indexBlock, i.reader.Properties.GlobalSeqNum); i.err == nil {
		return loadBlockOK
	}
	return loadBlockFailed
}

// resolveMaybeExcluded is invoked when the block-property filterer has found
// that an index block is excluded according to its properties but only if its
// bounds fall within the filter's current bounds. This function consults the
// apprioriate bound, depending on the iteration direction, and returns either
// `blockIntersects` or
// `blockMaybeExcluded`.
func (i *twoLevelIterator) resolveMaybeExcluded(dir int8) intersectsResult {
	// This iterator is configured with a bound-limited block property filter.
	// The bpf determined this entire index block could be excluded from
	// iteration based on the property encoded in the block handle. However, we
	// still need to determine if the index block is wholly contained within the
	// filter's key bounds.
	//
	// External guarantees ensure all its data blocks' keys are ≥ the filter's
	// lower bound during forward iteration, and that all its data blocks' keys
	// are < the filter's upper bound during backward iteration. We only need to
	// determine if the opposite bound is also met.
	//
	// The index separator in topLevelIndex.Key() provides an inclusive
	// upper-bound for the index block's keys, guaranteeing that all its keys
	// are ≤ topLevelIndex.Key(). For forward iteration, this is all we need.
	if dir > 0 {
		// Forward iteration.
		if i.bpfs.boundLimitedFilter.KeyIsWithinUpperBound(i.topLevelIndex.Key()) {
			return blockExcluded
		}
		return blockIntersects
	}

	// Reverse iteration.
	//
	// Because we're iterating in the reverse direction, we don't yet have
	// enough context available to determine if the block is wholly contained
	// within its bounds. This case arises only during backward iteration,
	// because of the way the index is structured.
	//
	// Consider a bound-limited bpf limited to the bounds [b,d), loading the
	// block with separator `c`. During reverse iteration, the guarantee that
	// all the block's keys are < `d` is externally provided, but no guarantee
	// is made on the bpf's lower bound. The separator `c` only provides an
	// inclusive upper bound on the block's keys, indicating that the
	// corresponding block handle points to a block containing only keys ≤ `c`.
	//
	// To establish a lower bound, we step the top-level index backwards to read
	// the previous block's separator, which provides an inclusive lower bound
	// on the original index block's keys. Afterwards, we step forward to
	// restore our top-level index position.
	if peekKey, _ := i.topLevelIndex.Prev(); peekKey == nil {
		// The original block points to the first index block of this table. If
		// we knew the lower bound for the entire table, it could provide a
		// lower bound, but the code refactoring necessary to read it doesn't
		// seem worth the payoff. We fall through to loading the block.
	} else if i.bpfs.boundLimitedFilter.KeyIsWithinLowerBound(peekKey) {
		// The lower-bound on the original index block falls within the filter's
		// bounds, and we can skip the block (after restoring our current
		// top-level index position).
		_, _ = i.topLevelIndex.Next()
		return blockExcluded
	}
	_, _ = i.topLevelIndex.Next()
	return blockIntersects
}

func (i *twoLevelIterator) init(
	r *Reader,
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	useFilter bool,
	stats *base.InternalIteratorStats,
) error {
	if r.err != nil {
		return r.err
	}
	topLevelIndexH, err := r.readIndex()
	if err != nil {
		return err
	}

	i.lower = lower
	i.upper = upper
	i.bpfs = filterer
	i.useFilter = useFilter
	i.reader = r
	i.cmp = r.Compare
	i.stats = stats
	err = i.topLevelIndex.initHandle(i.cmp, topLevelIndexH, r.Properties.GlobalSeqNum)
	if err != nil {
		// blockIter.Close releases topLevelIndexH and always returns a nil error
		_ = i.topLevelIndex.Close()
		return err
	}
	return nil
}

func (i *twoLevelIterator) String() string {
	return i.reader.fileNum.String()
}

// MaybeFilteredKeys may be called when an iterator is exhausted to indicate
// whether or not the last positioning method may have skipped any keys due to
// block-property filters.
func (i *twoLevelIterator) MaybeFilteredKeys() bool {
	// While reading sstables with two-level indexes, knowledge of whether we've
	// filtered keys is tracked separately for each index level. The
	// seek-using-next optimizations have different criteria. We can only reset
	// maybeFilteredKeys back to false during a seek when NOT using the
	// fast-path that uses the current iterator position.
	//
	// If either level might have filtered keys to arrive at the current
	// iterator position, return MaybeFilteredKeys=true.
	return i.maybeFilteredKeysTwoLevel || i.maybeFilteredKeysSingleLevel
}

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package. Note that SeekGE only checks the upper bound. It is up to the
// caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekGE(key []byte, flags base.SeekGEFlags) (*InternalKey, []byte) {
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error

	// SeekGE performs various step-instead-of-seeking optimizations: eg enabled
	// by trySeekUsingNext, or by monotonically increasing bounds (i.boundsCmp).
	// Care must be taken to ensure that when performing these optimizations and
	// the iterator becomes exhausted, i.maybeFilteredKeys is set appropriately.
	// Consider a previous SeekGE that filtered keys from k until the current
	// iterator position.
	//
	// If the previous SeekGE exhausted the iterator while seeking within the
	// two-level index, it's possible keys greater than or equal to the current
	// search key were filtered through skipped index blocks. We must not reuse
	// the position of the two-level index iterator without remembering the
	// previous value of maybeFilteredKeys.

	var dontSeekWithinSingleLevelIter bool
	if i.topLevelIndex.isDataInvalidated() || !i.topLevelIndex.valid() ||
		(i.boundsCmp <= 0 && !flags.TrySeekUsingNext()) || i.cmp(key, i.topLevelIndex.Key().UserKey) > 0 {
		// Slow-path: need to position the topLevelIndex.
		i.maybeFilteredKeysTwoLevel = false
		flags = flags.DisableTrySeekUsingNext()
		var ikey *InternalKey
		if ikey, _ = i.topLevelIndex.SeekGE(key, flags); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil, nil
		}

		result := i.loadIndex(+1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving
			// to the next entry in the top level index if upper bound is
			// already exceeded. Note that the next entry starts with keys >=
			// ikey.UserKey since even though this is the block separator, the
			// same user key can span multiple index blocks. Since upper is
			// exclusive we use >= below.
			if i.upper != nil && i.cmp(ikey.UserKey, i.upper) >= 0 {
				i.exhaustedBounds = +1
			}
			// Fall through to skipForward.
			dontSeekWithinSingleLevelIter = true
		}
	}
	// Else fast-path: There are two possible cases, from
	// (i.boundsCmp > 0 || flags.TrySeekUsingNext()):
	//
	// 1) The bounds have moved forward (i.boundsCmp > 0) and this SeekGE is
	// respecting the lower bound (guaranteed by Iterator). We know that
	// the iterator must already be positioned within or just outside the
	// previous bounds. Therefore the topLevelIndex iter cannot be
	// positioned at an entry ahead of the seek position (though it can be
	// positioned behind). The !i.cmp(key, i.topLevelIndex.Key().UserKey) > 0
	// confirms that it is not behind. Since it is not ahead and not behind
	// it must be at the right position.
	//
	// 2) This SeekGE will land on a key that is greater than the key we are
	// currently at (guaranteed by trySeekUsingNext), but since
	// i.cmp(key, i.topLevelIndex.Key().UserKey) <= 0, we are at the correct
	// lower level index block. No need to reset the state of singleLevelIterator.

	if !dontSeekWithinSingleLevelIter {
		// Note that while trySeekUsingNext could be false here, singleLevelIterator
		// could do its own boundsCmp-based optimization to seek using next.
		if ikey, val := i.singleLevelIterator.SeekGE(key, flags); ikey != nil {
			return ikey, val
		}
	}
	return i.skipForward()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package. Note that SeekPrefixGE only checks the upper bound. It is up
// to the caller to ensure that key is greater than or equal to the lower bound.
func (i *twoLevelIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	i.err = nil // clear cached iteration error

	// Check prefix bloom filter.
	if i.reader.tableFilter != nil && i.useFilter {
		if !i.lastBloomFilterMatched {
			// Iterator is not positioned based on last seek.
			flags = flags.DisableTrySeekUsingNext()
		}
		i.lastBloomFilterMatched = false
		var dataH cache.Handle
		dataH, i.err = i.reader.readFilter()
		if i.err != nil {
			i.data.invalidate()
			return nil, nil
		}
		mayContain := i.reader.tableFilter.mayContain(dataH.Get(), prefix)
		dataH.Release()
		if !mayContain {
			// This invalidation may not be necessary for correctness, and may
			// be a place to optimize later by reusing the already loaded
			// block. It was necessary in earlier versions of the code since
			// the caller was allowed to call Next when SeekPrefixGE returned
			// nil. This is no longer allowed.
			i.data.invalidate()
			return nil, nil
		}
		i.lastBloomFilterMatched = true
	}

	// Bloom filter matches.
	i.exhaustedBounds = 0

	// SeekPrefixGE performs various step-instead-of-seeking optimizations: eg
	// enabled by trySeekUsingNext, or by monotonically increasing bounds
	// (i.boundsCmp).  Care must be taken to ensure that when performing these
	// optimizations and the iterator becomes exhausted,
	// i.maybeFilteredKeysTwoLevel is set appropriately.  Consider a previous
	// SeekPrefixGE that filtered keys from k until the current iterator
	// position.
	//
	// If the previous SeekPrefixGE exhausted the iterator while seeking within
	// the two-level index, it's possible keys greater than or equal to the
	// current search key were filtered through skipped index blocks. We must
	// not reuse the position of the two-level index iterator without
	// remembering the previous value of maybeFilteredKeysTwoLevel.

	var dontSeekWithinSingleLevelIter bool
	if i.topLevelIndex.isDataInvalidated() || !i.topLevelIndex.valid() ||
		i.boundsCmp <= 0 || i.cmp(key, i.topLevelIndex.Key().UserKey) > 0 {
		// Slow-path: need to position the topLevelIndex.
		//
		// TODO(sumeer): improve this slow-path to be able to use Next, when
		// flags.TrySeekUsingNext() is true, since the fast path never applies
		// for practical uses of SeekPrefixGE in CockroachDB (they never set
		// monotonic bounds). To apply it here, we would need to confirm that
		// the topLevelIndex can continue using the same second level index
		// block, and in that case we don't need to invalidate and reload the
		// singleLevelIterator state.
		i.maybeFilteredKeysTwoLevel = false
		flags = flags.DisableTrySeekUsingNext()
		var ikey *InternalKey
		if ikey, _ = i.topLevelIndex.SeekGE(key, flags); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil, nil
		}

		result := i.loadIndex(+1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockIrrelevant {
			// Enforce the upper bound here since don't want to bother moving
			// to the next entry in the top level index if upper bound is
			// already exceeded. Note that the next entry starts with keys >=
			// ikey.UserKey since even though this is the block separator, the
			// same user key can span multiple index blocks. Since upper is
			// exclusive we use >= below.
			if i.upper != nil && i.cmp(ikey.UserKey, i.upper) >= 0 {
				i.exhaustedBounds = +1
			}
			// Fall through to skipForward.
			dontSeekWithinSingleLevelIter = true
		}
	}
	// Else fast-path: The bounds have moved forward and this SeekGE is
	// respecting the lower bound (guaranteed by Iterator). We know that
	// the iterator must already be positioned within or just outside the
	// previous bounds. Therefore the topLevelIndex iter cannot be
	// positioned at an entry ahead of the seek position (though it can be
	// positioned behind). The !i.cmp(key, i.topLevelIndex.Key().UserKey) > 0
	// confirms that it is not behind. Since it is not ahead and not behind
	// it must be at the right position.

	if !dontSeekWithinSingleLevelIter {
		if ikey, val := i.singleLevelIterator.seekPrefixGE(
			prefix, key, flags, false /* checkFilter */); ikey != nil {
			return ikey, val
		}
	}
	// NB: skipForward checks whether exhaustedBounds is already +1.
	return i.skipForward()
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package. Note that SeekLT only checks the lower bound. It is up to the
// caller to ensure that key is less than the upper bound.
func (i *twoLevelIterator) SeekLT(key []byte, flags base.SeekLTFlags) (*InternalKey, []byte) {
	i.exhaustedBounds = 0
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var result loadBlockResult
	var ikey *InternalKey
	// NB: Unlike SeekGE, we don't have a fast-path here since we don't know
	// whether the topLevelIndex is positioned after the position that would
	// be returned by doing i.topLevelIndex.SeekGE(). To know this we would
	// need to know the index key preceding the current one.
	// NB: If a bound-limited block property filter is configured, it's
	// externally ensured that the filter is disabled (through returning
	// Intersects=false irrespective of the block props provided) during seeks.
	i.maybeFilteredKeysTwoLevel = false
	if ikey, _ = i.topLevelIndex.SeekGE(key, base.SeekGEFlagsNone); ikey == nil {
		if ikey, _ = i.topLevelIndex.Last(); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil, nil
		}

		result = i.loadIndex(-1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockOK {
			if ikey, val := i.singleLevelIterator.lastInternal(); ikey != nil {
				return ikey, val
			}
			// Fall through to skipBackward since the singleLevelIterator did
			// not have any blocks that satisfy the block interval
			// constraints, or the lower bound was reached.
		}
		// Else loadBlockIrrelevant, so fall through.
	} else {
		result = i.loadIndex(-1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockOK {
			if ikey, val := i.singleLevelIterator.SeekLT(key, flags); ikey != nil {
				return ikey, val
			}
			// Fall through to skipBackward since the singleLevelIterator did
			// not have any blocks that satisfy the block interval
			// constraint, or the lower bound was reached.
		}
		// Else loadBlockIrrelevant, so fall through.
	}
	if result == loadBlockIrrelevant {
		// Enforce the lower bound here since don't want to bother moving to
		// the previous entry in the top level index if lower bound is already
		// exceeded. Note that the previous entry starts with keys <=
		// ikey.UserKey since even though this is the current block's
		// separator, the same user key can span multiple index blocks.
		if i.lower != nil && i.cmp(ikey.UserKey, i.lower) < 0 {
			i.exhaustedBounds = -1
		}
	}
	// NB: skipBackward checks whether exhaustedBounds is already -1.
	return i.skipBackward()
}

// First implements internalIterator.First, as documented in the pebble
// package. Note that First only checks the upper bound. It is up to the caller
// to ensure that key is greater than or equal to the lower bound (e.g. via a
// call to SeekGE(lower)).
func (i *twoLevelIterator) First() (*InternalKey, []byte) {
	if i.lower != nil {
		panic("twoLevelIterator.First() used despite lower bound")
	}
	i.exhaustedBounds = 0
	i.maybeFilteredKeysTwoLevel = false
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var ikey *InternalKey
	if ikey, _ = i.topLevelIndex.First(); ikey == nil {
		return nil, nil
	}

	result := i.loadIndex(+1)
	if result == loadBlockFailed {
		return nil, nil
	}
	if result == loadBlockOK {
		if ikey, val := i.singleLevelIterator.First(); ikey != nil {
			return ikey, val
		}
		// Else fall through to skipForward.
	} else {
		// result == loadBlockIrrelevant. Enforce the upper bound here since
		// don't want to bother moving to the next entry in the top level
		// index if upper bound is already exceeded. Note that the next entry
		// starts with keys >= ikey.UserKey since even though this is the
		// block separator, the same user key can span multiple index blocks.
		// Since upper is exclusive we use >= below.
		if i.upper != nil && i.cmp(ikey.UserKey, i.upper) >= 0 {
			i.exhaustedBounds = +1
		}
	}
	// NB: skipForward checks whether exhaustedBounds is already +1.
	return i.skipForward()
}

// Last implements internalIterator.Last, as documented in the pebble
// package. Note that Last only checks the lower bound. It is up to the caller
// to ensure that key is less than the upper bound (e.g. via a call to
// SeekLT(upper))
func (i *twoLevelIterator) Last() (*InternalKey, []byte) {
	if i.upper != nil {
		panic("twoLevelIterator.Last() used despite upper bound")
	}
	i.exhaustedBounds = 0
	i.maybeFilteredKeysTwoLevel = false
	i.err = nil // clear cached iteration error
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0

	var ikey *InternalKey
	if ikey, _ = i.topLevelIndex.Last(); ikey == nil {
		return nil, nil
	}

	result := i.loadIndex(-1)
	if result == loadBlockFailed {
		return nil, nil
	}
	if result == loadBlockOK {
		if ikey, val := i.singleLevelIterator.Last(); ikey != nil {
			return ikey, val
		}
		// Else fall through to skipBackward.
	} else {
		// result == loadBlockIrrelevant. Enforce the lower bound here
		// since don't want to bother moving to the previous entry in the
		// top level index if lower bound is already exceeded. Note that
		// the previous entry starts with keys <= ikey.UserKey since even
		// though this is the current block's separator, the same user key
		// can span multiple index blocks.
		if i.lower != nil && i.cmp(ikey.UserKey, i.lower) < 0 {
			i.exhaustedBounds = -1
		}
	}
	// NB: skipBackward checks whether exhaustedBounds is already -1.
	return i.skipBackward()
}

// Next implements internalIterator.Next, as documented in the pebble
// package.
// Note: twoLevelCompactionIterator.Next mirrors the implementation of
// twoLevelIterator.Next due to performance. Keep the two in sync.
func (i *twoLevelIterator) Next() (*InternalKey, []byte) {
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	i.maybeFilteredKeysTwoLevel = false
	if i.err != nil {
		return nil, nil
	}
	if key, val := i.singleLevelIterator.Next(); key != nil {
		return key, val
	}
	return i.skipForward()
}

// Prev implements internalIterator.Prev, as documented in the pebble
// package.
func (i *twoLevelIterator) Prev() (*InternalKey, []byte) {
	// Seek optimization only applies until iterator is first positioned after SetBounds.
	i.boundsCmp = 0
	i.maybeFilteredKeysTwoLevel = false
	if i.err != nil {
		return nil, nil
	}
	if key, val := i.singleLevelIterator.Prev(); key != nil {
		return key, val
	}
	return i.skipBackward()
}

func (i *twoLevelIterator) skipForward() (*InternalKey, []byte) {
	for {
		if i.err != nil || i.exhaustedBounds > 0 {
			return nil, nil
		}
		i.exhaustedBounds = 0
		var ikey *InternalKey
		if ikey, _ = i.topLevelIndex.Next(); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil, nil
		}
		result := i.loadIndex(+1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockOK {
			if ikey, val := i.singleLevelIterator.firstInternal(); ikey != nil {
				return ikey, val
			}
			// Next iteration will return if singleLevelIterator set
			// exhaustedBounds = +1.
		} else {
			// result == loadBlockIrrelevant. Enforce the upper bound here
			// since don't want to bother moving to the next entry in the top
			// level index if upper bound is already exceeded. Note that the
			// next entry starts with keys >= ikey.UserKey since even though
			// this is the block separator, the same user key can span
			// multiple index blocks. Since upper is exclusive we use >=
			// below.
			if i.upper != nil && i.cmp(ikey.UserKey, i.upper) >= 0 {
				i.exhaustedBounds = +1
				// Next iteration will return.
			}
		}
	}
}

func (i *twoLevelIterator) skipBackward() (*InternalKey, []byte) {
	for {
		if i.err != nil || i.exhaustedBounds < 0 {
			return nil, nil
		}
		i.exhaustedBounds = 0
		var ikey *InternalKey
		if ikey, _ = i.topLevelIndex.Prev(); ikey == nil {
			i.data.invalidate()
			i.index.invalidate()
			return nil, nil
		}
		result := i.loadIndex(-1)
		if result == loadBlockFailed {
			return nil, nil
		}
		if result == loadBlockOK {
			if ikey, val := i.singleLevelIterator.lastInternal(); ikey != nil {
				return ikey, val
			}
			// Next iteration will return if singleLevelIterator set
			// exhaustedBounds = -1.
		} else {
			// result == loadBlockIrrelevant. Enforce the lower bound here
			// since don't want to bother moving to the previous entry in the
			// top level index if lower bound is already exceeded. Note that
			// the previous entry starts with keys <= ikey.UserKey since even
			// though this is the current block's separator, the same user key
			// can span multiple index blocks.
			if i.lower != nil && i.cmp(ikey.UserKey, i.lower) < 0 {
				i.exhaustedBounds = -1
				// Next iteration will return.
			}
		}
	}
}

// Close implements internalIterator.Close, as documented in the pebble
// package.
func (i *twoLevelIterator) Close() error {
	var err error
	if i.closeHook != nil {
		err = firstError(err, i.closeHook(i))
	}
	err = firstError(err, i.data.Close())
	err = firstError(err, i.index.Close())
	err = firstError(err, i.topLevelIndex.Close())
	if i.dataRS.sequentialFile != nil {
		err = firstError(err, i.dataRS.sequentialFile.Close())
		i.dataRS.sequentialFile = nil
	}
	err = firstError(err, i.err)
	if i.bpfs != nil {
		releaseBlockPropertiesFilterer(i.bpfs)
	}
	*i = twoLevelIterator{
		singleLevelIterator: i.singleLevelIterator.resetForReuse(),
		topLevelIndex:       i.topLevelIndex.resetForReuse(),
	}
	twoLevelIterPool.Put(i)
	return err
}

// Note: twoLevelCompactionIterator and compactionIterator are very similar but
// were separated due to performance.
type twoLevelCompactionIterator struct {
	*twoLevelIterator
	bytesIterated *uint64
	prevOffset    uint64
}

// twoLevelCompactionIterator implements the base.InternalIterator interface.
var _ base.InternalIterator = (*twoLevelCompactionIterator)(nil)

func (i *twoLevelCompactionIterator) Close() error {
	return i.twoLevelIterator.Close()
}

func (i *twoLevelCompactionIterator) SeekGE(
	key []byte, flags base.SeekGEFlags,
) (*InternalKey, []byte) {
	panic("pebble: SeekGE unimplemented")
}

func (i *twoLevelCompactionIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, []byte) {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (i *twoLevelCompactionIterator) SeekLT(
	key []byte, flags base.SeekLTFlags,
) (*InternalKey, []byte) {
	panic("pebble: SeekLT unimplemented")
}

func (i *twoLevelCompactionIterator) First() (*InternalKey, []byte) {
	i.err = nil // clear cached iteration error
	return i.skipForward(i.twoLevelIterator.First())
}

func (i *twoLevelCompactionIterator) Last() (*InternalKey, []byte) {
	panic("pebble: Last unimplemented")
}

// Note: twoLevelCompactionIterator.Next mirrors the implementation of
// twoLevelIterator.Next due to performance. Keep the two in sync.
func (i *twoLevelCompactionIterator) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	return i.skipForward(i.singleLevelIterator.Next())
}

func (i *twoLevelCompactionIterator) Prev() (*InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

func (i *twoLevelCompactionIterator) String() string {
	return i.reader.fileNum.String()
}

func (i *twoLevelCompactionIterator) skipForward(
	key *InternalKey, val []byte,
) (*InternalKey, []byte) {
	if key == nil {
		for {
			if key, _ := i.topLevelIndex.Next(); key == nil {
				break
			}
			result := i.loadIndex(+1)
			if result != loadBlockOK {
				if i.err != nil {
					break
				}
				switch result {
				case loadBlockFailed:
					// We checked that i.index was at a valid entry, so
					// loadBlockFailed could not have happened due to to i.index
					// being exhausted, and must be due to an error.
					panic("loadBlock should not have failed with no error")
				case loadBlockIrrelevant:
					panic("compactionIter should not be using block intervals for skipping")
				default:
					panic(fmt.Sprintf("unexpected case %d", result))
				}
			}
			// result == loadBlockOK
			if key, val = i.singleLevelIterator.First(); key != nil {
				break
			}
		}
	}

	curOffset := i.recordOffset()
	*i.bytesIterated += uint64(curOffset - i.prevOffset)
	i.prevOffset = curOffset
	return key, val
}

type blockTransform func([]byte) ([]byte, error)

// readaheadState contains state variables related to readahead. Updated on
// file reads.
type readaheadState struct {
	// Number of sequential reads.
	numReads int64
	// Size issued to the next call to Prefetch. Starts at or above
	// initialReadaheadSize and grows exponentially until maxReadaheadSize.
	size int64
	// prevSize is the size used in the last Prefetch call.
	prevSize int64
	// The byte offset up to which the OS has been asked to read ahead / cached.
	// When reading ahead, reads up to this limit should not incur an IO
	// operation. Reads after this limit can benefit from a new call to
	// Prefetch.
	limit int64
	// sequentialFile holds a file descriptor to the same underlying File,
	// except with fadvise(FADV_SEQUENTIAL) called on it to take advantage of
	// OS-level readahead. Initialized when the iterator has been consistently
	// reading blocks in a sequential access pattern. Once this is non-nil,
	// the other variables in readaheadState don't matter much as we defer
	// to OS-level readahead.
	sequentialFile vfs.File
}

func (rs *readaheadState) recordCacheHit(offset, blockLength int64) {
	currentReadEnd := offset + blockLength
	if rs.sequentialFile != nil {
		// Using OS-level readahead instead, so do nothing.
		return
	}
	if rs.numReads >= minFileReadsForReadahead {
		if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
			// This is a read that would have resulted in a readahead, had it
			// not been a cache hit.
			rs.limit = currentReadEnd
			return
		}
		if currentReadEnd < rs.limit-rs.prevSize || offset > rs.limit+maxReadaheadSize {
			// We read too far away from rs.limit to benefit from readahead in
			// any scenario. Reset all variables.
			rs.numReads = 1
			rs.limit = currentReadEnd
			rs.size = initialReadaheadSize
			rs.prevSize = 0
			return
		}
		// Reads in the range [rs.limit - rs.prevSize, rs.limit] end up
		// here. This is a read that is potentially benefitting from a past
		// readahead.
		return
	}
	if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
		// Blocks are being read sequentially and would benefit from readahead
		// down the line.
		rs.numReads++
		return
	}
	// We read too far ahead of the last read, or before it. This indicates
	// a random read, where readahead is not desirable. Reset all variables.
	rs.numReads = 1
	rs.limit = currentReadEnd
	rs.size = initialReadaheadSize
	rs.prevSize = 0
}

// maybeReadahead updates state and determines whether to issue a readahead /
// prefetch call for a block read at offset for blockLength bytes.
// Returns a size value (greater than 0) that should be prefetched if readahead
// would be beneficial.
func (rs *readaheadState) maybeReadahead(offset, blockLength int64) int64 {
	currentReadEnd := offset + blockLength
	if rs.sequentialFile != nil {
		// Using OS-level readahead instead, so do nothing.
		return 0
	}
	if rs.numReads >= minFileReadsForReadahead {
		// The minimum threshold of sequential reads to justify reading ahead
		// has been reached.
		// There are two intervals: the interval being read:
		// [offset, currentReadEnd]
		// as well as the interval where a read would benefit from read ahead:
		// [rs.limit, rs.limit + rs.size]
		// We increase the latter interval to
		// [rs.limit, rs.limit + maxReadaheadSize] to account for cases where
		// readahead may not be beneficial with a small readahead size, but over
		// time the readahead size would increase exponentially to make it
		// beneficial.
		if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
			// We are doing a read in the interval ahead of
			// the last readahead range. In the diagrams below, ++++ is the last
			// readahead range, ==== is the range represented by
			// [rs.limit, rs.limit + maxReadaheadSize], and ---- is the range
			// being read.
			//
			//               rs.limit           rs.limit + maxReadaheadSize
			//         ++++++++++|===========================|
			//
			//              |-------------|
			//            offset       currentReadEnd
			//
			// This case is also possible, as are all cases with an overlap
			// between [rs.limit, rs.limit + maxReadaheadSize] and [offset,
			// currentReadEnd]:
			//
			//               rs.limit           rs.limit + maxReadaheadSize
			//         ++++++++++|===========================|
			//
			//                                            |-------------|
			//                                         offset       currentReadEnd
			//
			//
			rs.numReads++
			rs.limit = offset + rs.size
			rs.prevSize = rs.size
			// Increase rs.size for the next read.
			rs.size *= 2
			if rs.size > maxReadaheadSize {
				rs.size = maxReadaheadSize
			}
			return rs.prevSize
		}
		if currentReadEnd < rs.limit-rs.prevSize || offset > rs.limit+maxReadaheadSize {
			// The above conditional has rs.limit > rs.prevSize to confirm that
			// rs.limit - rs.prevSize would not underflow.
			// We read too far away from rs.limit to benefit from readahead in
			// any scenario. Reset all variables.
			// The case where we read too far ahead:
			//
			// (rs.limit - rs.prevSize)    (rs.limit)   (rs.limit + maxReadaheadSize)
			//                    |+++++++++++++|=============|
			//
			//                                                  |-------------|
			//                                             offset       currentReadEnd
			//
			// Or too far behind:
			//
			// (rs.limit - rs.prevSize)    (rs.limit)   (rs.limit + maxReadaheadSize)
			//                    |+++++++++++++|=============|
			//
			//    |-------------|
			// offset       currentReadEnd
			//
			rs.numReads = 1
			rs.limit = currentReadEnd
			rs.size = initialReadaheadSize
			rs.prevSize = 0
			return 0
		}
		// Reads in the range [rs.limit - rs.prevSize, rs.limit] end up
		// here. This is a read that is potentially benefitting from a past
		// readahead, but there's no reason to issue a readahead call at the
		// moment.
		//
		// (rs.limit - rs.prevSize)            (rs.limit + maxReadaheadSize)
		//                    |+++++++++++++|===============|
		//                             (rs.limit)
		//
		//                        |-------|
		//                     offset    currentReadEnd
		//
		rs.numReads++
		return 0
	}
	if currentReadEnd >= rs.limit && offset <= rs.limit+maxReadaheadSize {
		// Blocks are being read sequentially and would benefit from readahead
		// down the line.
		//
		//                       (rs.limit)   (rs.limit + maxReadaheadSize)
		//                         |=============|
		//
		//                    |-------|
		//                offset    currentReadEnd
		//
		rs.numReads++
		return 0
	}
	// We read too far ahead of the last read, or before it. This indicates
	// a random read, where readahead is not desirable. Reset all variables.
	//
	// (rs.limit - maxReadaheadSize)  (rs.limit)   (rs.limit + maxReadaheadSize)
	//                     |+++++++++++++|=============|
	//
	//                                                    |-------|
	//                                                offset    currentReadEnd
	//
	rs.numReads = 1
	rs.limit = currentReadEnd
	rs.size = initialReadaheadSize
	rs.prevSize = 0
	return 0
}

// ReaderOption provide an interface to do work on Reader while it is being
// opened.
type ReaderOption interface {
	// readerApply is called on the reader during opening in order to set internal
	// parameters.
	readerApply(*Reader)
}

// Comparers is a map from comparer name to comparer. It is used for debugging
// tools which may be used on multiple databases configured with different
// comparers. Comparers implements the OpenOption interface and can be passed
// as a parameter to NewReader.
type Comparers map[string]*Comparer

func (c Comparers) readerApply(r *Reader) {
	if r.Compare != nil || r.Properties.ComparerName == "" {
		return
	}
	if comparer, ok := c[r.Properties.ComparerName]; ok {
		r.Compare = comparer.Compare
		r.FormatKey = comparer.FormatKey
		r.Split = comparer.Split
	}
}

// Mergers is a map from merger name to merger. It is used for debugging tools
// which may be used on multiple databases configured with different
// mergers. Mergers implements the OpenOption interface and can be passed as
// a parameter to NewReader.
type Mergers map[string]*Merger

func (m Mergers) readerApply(r *Reader) {
	if r.mergerOK || r.Properties.MergerName == "" {
		return
	}
	_, r.mergerOK = m[r.Properties.MergerName]
}

// cacheOpts is a Reader open option for specifying the cache ID and sstable file
// number. If not specified, a unique cache ID will be used.
type cacheOpts struct {
	cacheID uint64
	fileNum base.FileNum
}

// Marker function to indicate the option should be applied before reading the
// sstable properties and, in the write path, before writing the default
// sstable properties.
func (c *cacheOpts) preApply() {}

func (c *cacheOpts) readerApply(r *Reader) {
	if r.cacheID == 0 {
		r.cacheID = c.cacheID
	}
	if r.fileNum == 0 {
		r.fileNum = c.fileNum
	}
}

func (c *cacheOpts) writerApply(w *Writer) {
	if w.cacheID == 0 {
		w.cacheID = c.cacheID
	}
	if w.fileNum == 0 {
		w.fileNum = c.fileNum
	}
}

// FileReopenOpt is specified if this reader is allowed to reopen additional
// file descriptors for this file. Used to take advantage of OS-level readahead.
type FileReopenOpt struct {
	FS       vfs.FS
	Filename string
}

func (f FileReopenOpt) readerApply(r *Reader) {
	if r.fs == nil {
		r.fs = f.FS
		r.filename = f.Filename
	}
}

// rawTombstonesOpt is a Reader open option for specifying that range
// tombstones returned by Reader.NewRangeDelIter() should not be
// fragmented. Used by debug tools to get a raw view of the tombstones
// contained in an sstable.
type rawTombstonesOpt struct{}

func (rawTombstonesOpt) preApply() {}

func (rawTombstonesOpt) readerApply(r *Reader) {
	r.rawTombstones = true
}

func init() {
	private.SSTableCacheOpts = func(cacheID uint64, fileNum base.FileNum) interface{} {
		return &cacheOpts{cacheID, fileNum}
	}
	private.SSTableRawTombstonesOpt = rawTombstonesOpt{}
}

// Reader is a table reader.
type Reader struct {
	file              ReadableFile
	fs                vfs.FS
	filename          string
	cacheID           uint64
	fileNum           base.FileNum
	rawTombstones     bool
	err               error
	indexBH           BlockHandle
	filterBH          BlockHandle
	rangeDelBH        BlockHandle
	rangeKeyBH        BlockHandle
	rangeDelTransform blockTransform
	propertiesBH      BlockHandle
	metaIndexBH       BlockHandle
	footerBH          BlockHandle
	opts              ReaderOptions
	Compare           Compare
	FormatKey         base.FormatKey
	Split             Split
	mergerOK          bool
	checksumType      ChecksumType
	tableFilter       *tableFilterReader
	tableFormat       TableFormat
	Properties        Properties
}

// Close implements DB.Close, as documented in the pebble package.
func (r *Reader) Close() error {
	r.opts.Cache.Unref()

	if r.err != nil {
		if r.file != nil {
			r.file.Close()
			r.file = nil
		}
		return r.err
	}
	if r.file != nil {
		r.err = r.file.Close()
		r.file = nil
		if r.err != nil {
			return r.err
		}
	}
	// Make any future calls to Get, NewIter or Close return an error.
	r.err = errReaderClosed
	return nil
}

// NewIterWithBlockPropertyFilters returns an iterator for the contents of the
// table. If an error occurs, NewIterWithBlockPropertyFilters cleans up after
// itself and returns a nil iterator.
func (r *Reader) NewIterWithBlockPropertyFilters(
	lower, upper []byte,
	filterer *BlockPropertiesFilterer,
	useFilterBlock bool,
	stats *base.InternalIteratorStats,
) (Iterator, error) {
	// NB: pebble.tableCache wraps the returned iterator with one which performs
	// reference counting on the Reader, preventing the Reader from being closed
	// until the final iterator closes.
	if r.Properties.IndexType == twoLevelIndex {
		i := twoLevelIterPool.Get().(*twoLevelIterator)
		err := i.init(r, lower, upper, filterer, useFilterBlock, stats)
		if err != nil {
			return nil, err
		}
		return i, nil
	}

	i := singleLevelIterPool.Get().(*singleLevelIterator)
	err := i.init(r, lower, upper, filterer, useFilterBlock, stats)
	if err != nil {
		return nil, err
	}
	return i, nil
}

// NewIter returns an iterator for the contents of the table. If an error
// occurs, NewIter cleans up after itself and returns a nil iterator.
func (r *Reader) NewIter(lower, upper []byte) (Iterator, error) {
	return r.NewIterWithBlockPropertyFilters(lower, upper, nil, true /* useFilterBlock */, nil /* stats */)
}

// NewCompactionIter returns an iterator similar to NewIter but it also increments
// the number of bytes iterated. If an error occurs, NewCompactionIter cleans up
// after itself and returns a nil iterator.
func (r *Reader) NewCompactionIter(bytesIterated *uint64) (Iterator, error) {
	if r.Properties.IndexType == twoLevelIndex {
		i := twoLevelIterPool.Get().(*twoLevelIterator)
		err := i.init(r, nil /* lower */, nil /* upper */, nil, false /* useFilter */, nil /* stats */)
		if err != nil {
			return nil, err
		}
		i.setupForCompaction()
		return &twoLevelCompactionIterator{
			twoLevelIterator: i,
			bytesIterated:    bytesIterated,
		}, nil
	}
	i := singleLevelIterPool.Get().(*singleLevelIterator)
	err := i.init(r, nil /* lower */, nil /* upper */, nil, false /* useFilter */, nil /* stats */)
	if err != nil {
		return nil, err
	}
	i.setupForCompaction()
	return &compactionIterator{
		singleLevelIterator: i,
		bytesIterated:       bytesIterated,
	}, nil
}

// NewRawRangeDelIter returns an internal iterator for the contents of the
// range-del block for the table. Returns nil if the table does not contain
// any range deletions.
func (r *Reader) NewRawRangeDelIter() (keyspan.FragmentIterator, error) {
	if r.rangeDelBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeDel()
	if err != nil {
		return nil, err
	}
	i := &fragmentBlockIter{}
	if err := i.blockIter.initHandle(r.Compare, h, r.Properties.GlobalSeqNum); err != nil {
		return nil, err
	}
	return i, nil
}

// NewRawRangeKeyIter returns an internal iterator for the contents of the
// range-key block for the table. Returns nil if the table does not contain any
// range keys.
func (r *Reader) NewRawRangeKeyIter() (keyspan.FragmentIterator, error) {
	if r.rangeKeyBH.Length == 0 {
		return nil, nil
	}
	h, err := r.readRangeKey()
	if err != nil {
		return nil, err
	}
	i := rangeKeyFragmentBlockIterPool.Get().(*rangeKeyFragmentBlockIter)
	if err := i.blockIter.initHandle(r.Compare, h, r.Properties.GlobalSeqNum); err != nil {
		return nil, err
	}
	return i, nil
}

type rangeKeyFragmentBlockIter struct {
	fragmentBlockIter
}

func (i *rangeKeyFragmentBlockIter) Close() error {
	err := i.fragmentBlockIter.Close()
	i.fragmentBlockIter = i.fragmentBlockIter.resetForReuse()
	rangeKeyFragmentBlockIterPool.Put(i)
	return err
}

func (r *Reader) readIndex() (cache.Handle, error) {
	h, _, err :=
		r.readBlock(r.indexBH, nil /* transform */, nil /* readaheadState */)
	return h, err
}

func (r *Reader) readFilter() (cache.Handle, error) {
	h, _, err :=
		r.readBlock(r.filterBH, nil /* transform */, nil /* readaheadState */)
	return h, err
}

func (r *Reader) readRangeDel() (cache.Handle, error) {
	h, _, err :=
		r.readBlock(r.rangeDelBH, r.rangeDelTransform, nil /* readaheadState */)
	return h, err
}

func (r *Reader) readRangeKey() (cache.Handle, error) {
	h, _, err :=
		r.readBlock(r.rangeKeyBH, nil /* transform */, nil /* readaheadState */)
	return h, err
}

func checkChecksum(
	checksumType ChecksumType, b []byte, bh BlockHandle, fileNum base.FileNum,
) error {
	expectedChecksum := binary.LittleEndian.Uint32(b[bh.Length+1:])
	var computedChecksum uint32
	switch checksumType {
	case ChecksumTypeCRC32c:
		computedChecksum = crc.New(b[:bh.Length+1]).Value()
	case ChecksumTypeXXHash64:
		computedChecksum = uint32(xxhash.Sum64(b[:bh.Length+1]))
	default:
		return errors.Errorf("unsupported checksum type: %d", checksumType)
	}

	if expectedChecksum != computedChecksum {
		return base.CorruptionErrorf(
			"pebble/table: invalid table %s (checksum mismatch at %d/%d)",
			errors.Safe(fileNum), errors.Safe(bh.Offset), errors.Safe(bh.Length))
	}
	return nil
}

// readBlock reads and decompresses a block from disk into memory.
func (r *Reader) readBlock(
	bh BlockHandle, transform blockTransform, raState *readaheadState,
) (_ cache.Handle, cacheHit bool, _ error) {
	if h := r.opts.Cache.Get(r.cacheID, r.fileNum, bh.Offset); h.Get() != nil {
		if raState != nil {
			raState.recordCacheHit(int64(bh.Offset), int64(bh.Length+blockTrailerLen))
		}
		return h, true, nil
	}
	file := r.file

	if raState != nil {
		if raState.sequentialFile != nil {
			file = raState.sequentialFile
		} else if readaheadSize := raState.maybeReadahead(int64(bh.Offset), int64(bh.Length+blockTrailerLen)); readaheadSize > 0 {
			if readaheadSize >= maxReadaheadSize {
				// We've reached the maximum readahead size. Beyond this
				// point, rely on OS-level readahead. Note that we can only
				// reopen a new file handle with this optimization if
				// r.fs != nil. This reader must have been created with the
				// FileReopenOpt for this field to be set.
				if r.fs != nil {
					f, err := r.fs.Open(r.filename, vfs.SequentialReadsOption)
					if err == nil {
						// Use this new file handle for all sequential reads by
						// this iterator going forward.
						raState.sequentialFile = f
						file = f
					}

					// If we tried to load a table that doesn't exist, panic
					// immediately.  Something is seriously wrong if a table
					// doesn't exist.
					// See cockroachdb/cockroach#56490.
					base.MustExist(r.fs, r.filename, panicFataler{}, err)
				}
			}
			if raState.sequentialFile == nil {
				type fd interface {
					Fd() uintptr
				}
				if f, ok := r.file.(fd); ok {
					_ = vfs.Prefetch(f.Fd(), bh.Offset, uint64(readaheadSize))
				}
			}
		}
	}

	v := r.opts.Cache.Alloc(int(bh.Length + blockTrailerLen))
	b := v.Buf()
	if _, err := file.ReadAt(b, int64(bh.Offset)); err != nil {
		r.opts.Cache.Free(v)
		return cache.Handle{}, false, err
	}

	if err := checkChecksum(r.checksumType, b, bh, r.fileNum); err != nil {
		r.opts.Cache.Free(v)
		return cache.Handle{}, false, err
	}

	typ := blockType(b[bh.Length])
	b = b[:bh.Length]
	v.Truncate(len(b))

	decoded, err := decompressBlock(r.opts.Cache, typ, b)
	if decoded != nil {
		r.opts.Cache.Free(v)
		v = decoded
		b = v.Buf()
	} else if err != nil {
		r.opts.Cache.Free(v)
		return cache.Handle{}, false, err
	}

	if transform != nil {
		// Transforming blocks is rare, so the extra copy of the transformed data
		// is not problematic.
		var err error
		b, err = transform(b)
		if err != nil {
			r.opts.Cache.Free(v)
			return cache.Handle{}, false, err
		}
		newV := r.opts.Cache.Alloc(len(b))
		copy(newV.Buf(), b)
		r.opts.Cache.Free(v)
		v = newV
	}

	h := r.opts.Cache.Set(r.cacheID, r.fileNum, bh.Offset, v)
	return h, false, nil
}

func (r *Reader) transformRangeDelV1(b []byte) ([]byte, error) {
	// Convert v1 (RocksDB format) range-del blocks to v2 blocks on the fly. The
	// v1 format range-del blocks have unfragmented and unsorted range
	// tombstones. We need properly fragmented and sorted range tombstones in
	// order to serve from them directly.
	iter := &blockIter{}
	if err := iter.init(r.Compare, b, r.Properties.GlobalSeqNum); err != nil {
		return nil, err
	}
	var tombstones []keyspan.Span
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		t := keyspan.Span{
			Start: key.UserKey,
			End:   value,
			Keys:  []keyspan.Key{{Trailer: key.Trailer}},
		}
		tombstones = append(tombstones, t)
	}
	keyspan.Sort(r.Compare, tombstones)

	// Fragment the tombstones, outputting them directly to a block writer.
	rangeDelBlock := blockWriter{
		restartInterval: 1,
	}
	frag := keyspan.Fragmenter{
		Cmp:    r.Compare,
		Format: r.FormatKey,
		Emit: func(s keyspan.Span) {
			for _, k := range s.Keys {
				startIK := InternalKey{UserKey: s.Start, Trailer: k.Trailer}
				rangeDelBlock.add(startIK, s.End)
			}
		},
	}
	for i := range tombstones {
		frag.Add(tombstones[i])
	}
	frag.Finish()

	// Return the contents of the constructed v2 format range-del block.
	return rangeDelBlock.finish(), nil
}

func (r *Reader) readMetaindex(metaindexBH BlockHandle) error {
	b, _, err := r.readBlock(metaindexBH, nil /* transform */, nil /* readaheadState */)
	if err != nil {
		return err
	}
	data := b.Get()
	defer b.Release()

	if uint64(len(data)) != metaindexBH.Length {
		return base.CorruptionErrorf("pebble/table: unexpected metaindex block size: %d vs %d",
			errors.Safe(len(data)), errors.Safe(metaindexBH.Length))
	}

	i, err := newRawBlockIter(bytes.Compare, data)
	if err != nil {
		return err
	}

	meta := map[string]BlockHandle{}
	for valid := i.First(); valid; valid = i.Next() {
		bh, n := decodeBlockHandle(i.Value())
		if n == 0 {
			return base.CorruptionErrorf("pebble/table: invalid table (bad filter block handle)")
		}
		meta[string(i.Key().UserKey)] = bh
	}
	if err := i.Close(); err != nil {
		return err
	}

	if bh, ok := meta[metaPropertiesName]; ok {
		b, _, err = r.readBlock(bh, nil /* transform */, nil /* readaheadState */)
		if err != nil {
			return err
		}
		r.propertiesBH = bh
		err := r.Properties.load(b.Get(), bh.Offset)
		b.Release()
		if err != nil {
			return err
		}
	}

	if bh, ok := meta[metaRangeDelV2Name]; ok {
		r.rangeDelBH = bh
	} else if bh, ok := meta[metaRangeDelName]; ok {
		r.rangeDelBH = bh
		if !r.rawTombstones {
			r.rangeDelTransform = r.transformRangeDelV1
		}
	}

	if bh, ok := meta[metaRangeKeyName]; ok {
		r.rangeKeyBH = bh
	}

	for name, fp := range r.opts.Filters {
		types := []struct {
			ftype  FilterType
			prefix string
		}{
			{TableFilter, "fullfilter."},
		}
		var done bool
		for _, t := range types {
			if bh, ok := meta[t.prefix+name]; ok {
				r.filterBH = bh

				switch t.ftype {
				case TableFilter:
					r.tableFilter = newTableFilterReader(fp)
				default:
					return base.CorruptionErrorf("unknown filter type: %v", errors.Safe(t.ftype))
				}

				done = true
				break
			}
		}
		if done {
			break
		}
	}
	return nil
}

// Layout returns the layout (block organization) for an sstable.
func (r *Reader) Layout() (*Layout, error) {
	if r.err != nil {
		return nil, r.err
	}

	l := &Layout{
		Data:       make([]BlockHandleWithProperties, 0, r.Properties.NumDataBlocks),
		Filter:     r.filterBH,
		RangeDel:   r.rangeDelBH,
		RangeKey:   r.rangeKeyBH,
		Properties: r.propertiesBH,
		MetaIndex:  r.metaIndexBH,
		Footer:     r.footerBH,
	}

	indexH, err := r.readIndex()
	if err != nil {
		return nil, err
	}
	defer indexH.Release()

	var alloc []byte

	if r.Properties.IndexPartitions == 0 {
		l.Index = append(l.Index, r.indexBH)
		iter, _ := newBlockIter(r.Compare, indexH.Get())
		for key, value := iter.First(); key != nil; key, value = iter.Next() {
			dataBH, err := decodeBlockHandleWithProperties(value)
			if err != nil {
				return nil, errCorruptIndexEntry
			}
			if len(dataBH.Props) > 0 {
				if len(alloc) < len(dataBH.Props) {
					alloc = make([]byte, 256<<10)
				}
				n := copy(alloc, dataBH.Props)
				dataBH.Props = alloc[:n:n]
				alloc = alloc[n:]
			}
			l.Data = append(l.Data, dataBH)
		}
	} else {
		l.TopIndex = r.indexBH
		topIter, _ := newBlockIter(r.Compare, indexH.Get())
		iter := &blockIter{}
		for key, value := topIter.First(); key != nil; key, value = topIter.Next() {
			indexBH, err := decodeBlockHandleWithProperties(value)
			if err != nil {
				return nil, errCorruptIndexEntry
			}
			l.Index = append(l.Index, indexBH.BlockHandle)

			subIndex, _, err := r.readBlock(
				indexBH.BlockHandle, nil /* transform */, nil /* readaheadState */)
			if err != nil {
				return nil, err
			}
			if err := iter.init(r.Compare, subIndex.Get(), 0 /* globalSeqNum */); err != nil {
				return nil, err
			}
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				dataBH, err := decodeBlockHandleWithProperties(value)
				if len(dataBH.Props) > 0 {
					if len(alloc) < len(dataBH.Props) {
						alloc = make([]byte, 256<<10)
					}
					n := copy(alloc, dataBH.Props)
					dataBH.Props = alloc[:n:n]
					alloc = alloc[n:]
				}
				if err != nil {
					return nil, errCorruptIndexEntry
				}
				l.Data = append(l.Data, dataBH)
			}
			subIndex.Release()
			*iter = iter.resetForReuse()
		}
	}

	return l, nil
}

// ValidateBlockChecksums validates the checksums for each block in the SSTable.
func (r *Reader) ValidateBlockChecksums() error {
	// Pre-compute the BlockHandles for the underlying file.
	l, err := r.Layout()
	if err != nil {
		return err
	}

	// Construct the set of blocks to check. Note that the footer is not checked
	// as it is not a block with a checksum.
	blocks := make([]BlockHandle, len(l.Data))
	for i := range l.Data {
		blocks[i] = l.Data[i].BlockHandle
	}
	blocks = append(blocks, l.Index...)
	blocks = append(blocks, l.TopIndex, l.Filter, l.RangeDel, l.RangeKey, l.Properties, l.MetaIndex)

	// Sorting by offset ensures we are performing a sequential scan of the
	// file.
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	})

	// Check all blocks sequentially. Make use of read-ahead, given we are
	// scanning the entire file from start to end.
	blockRS := &readaheadState{
		size: initialReadaheadSize,
	}
	for _, bh := range blocks {
		// Certain blocks may not be present, in which case we skip them.
		if bh.Length == 0 {
			continue
		}

		// Read the block, which validates the checksum.
		h, _, err := r.readBlock(bh, nil /* transform */, blockRS)
		if err != nil {
			return err
		}
		h.Release()
	}

	return nil
}

// EstimateDiskUsage returns the total size of data blocks overlapping the range
// `[start, end]`. Even if a data block partially overlaps, or we cannot
// determine overlap due to abbreviated index keys, the full data block size is
// included in the estimation.
//
// This function does not account for any metablock space usage. Assumes there
// is at least partial overlap, i.e., `[start, end]` falls neither completely
// before nor completely after the file's range.
//
// Only blocks containing point keys are considered. Range deletion and range
// key blocks are not considered.
//
// TODO(ajkr): account for metablock space usage. Perhaps look at the fraction of
// data blocks overlapped and add that same fraction of the metadata blocks to the
// estimate.
func (r *Reader) EstimateDiskUsage(start, end []byte) (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}

	indexH, err := r.readIndex()
	if err != nil {
		return 0, err
	}
	defer indexH.Release()

	// Iterators over the bottom-level index blocks containing start and end.
	// These may be different in case of partitioned index but will both point
	// to the same blockIter over the single index in the unpartitioned case.
	var startIdxIter, endIdxIter *blockIter
	if r.Properties.IndexPartitions == 0 {
		iter, err := newBlockIter(r.Compare, indexH.Get())
		if err != nil {
			return 0, err
		}
		startIdxIter = iter
		endIdxIter = iter
	} else {
		topIter, err := newBlockIter(r.Compare, indexH.Get())
		if err != nil {
			return 0, err
		}

		key, val := topIter.SeekGE(start, base.SeekGEFlagsNone)
		if key == nil {
			// The range falls completely after this file, or an error occurred.
			return 0, topIter.Error()
		}
		startIdxBH, err := decodeBlockHandleWithProperties(val)
		if err != nil {
			return 0, errCorruptIndexEntry
		}
		startIdxBlock, _, err := r.readBlock(
			startIdxBH.BlockHandle, nil /* transform */, nil /* readaheadState */)
		if err != nil {
			return 0, err
		}
		defer startIdxBlock.Release()
		startIdxIter, err = newBlockIter(r.Compare, startIdxBlock.Get())
		if err != nil {
			return 0, err
		}

		key, val = topIter.SeekGE(end, base.SeekGEFlagsNone)
		if key == nil {
			if err := topIter.Error(); err != nil {
				return 0, err
			}
		} else {
			endIdxBH, err := decodeBlockHandleWithProperties(val)
			if err != nil {
				return 0, errCorruptIndexEntry
			}
			endIdxBlock, _, err := r.readBlock(
				endIdxBH.BlockHandle, nil /* transform */, nil /* readaheadState */)
			if err != nil {
				return 0, err
			}
			defer endIdxBlock.Release()
			endIdxIter, err = newBlockIter(r.Compare, endIdxBlock.Get())
			if err != nil {
				return 0, err
			}
		}
	}
	// startIdxIter should not be nil at this point, while endIdxIter can be if the
	// range spans past the end of the file.

	key, val := startIdxIter.SeekGE(start, base.SeekGEFlagsNone)
	if key == nil {
		// The range falls completely after this file, or an error occurred.
		return 0, startIdxIter.Error()
	}
	startBH, err := decodeBlockHandleWithProperties(val)
	if err != nil {
		return 0, errCorruptIndexEntry
	}

	if endIdxIter == nil {
		// The range spans beyond this file. Include data blocks through the last.
		return r.Properties.DataSize - startBH.Offset, nil
	}
	key, val = endIdxIter.SeekGE(end, base.SeekGEFlagsNone)
	if key == nil {
		if err := endIdxIter.Error(); err != nil {
			return 0, err
		}
		// The range spans beyond this file. Include data blocks through the last.
		return r.Properties.DataSize - startBH.Offset, nil
	}
	endBH, err := decodeBlockHandleWithProperties(val)
	if err != nil {
		return 0, errCorruptIndexEntry
	}
	return endBH.Offset + endBH.Length + blockTrailerLen - startBH.Offset, nil
}

// TableFormat returns the format version for the table.
func (r *Reader) TableFormat() (TableFormat, error) {
	if r.err != nil {
		return TableFormatUnspecified, r.err
	}
	return r.tableFormat, nil
}

// ReadableFile describes subset of vfs.File required for reading SSTs.
type ReadableFile interface {
	io.ReaderAt
	io.Closer
	Stat() (os.FileInfo, error)
}

// NewReader returns a new table reader for the file. Closing the reader will
// close the file.
func NewReader(f ReadableFile, o ReaderOptions, extraOpts ...ReaderOption) (*Reader, error) {
	o = o.ensureDefaults()
	r := &Reader{
		file: f,
		opts: o,
	}
	if r.opts.Cache == nil {
		r.opts.Cache = cache.New(0)
	} else {
		r.opts.Cache.Ref()
	}

	if f == nil {
		r.err = errors.New("pebble/table: nil file")
		return nil, r.Close()
	}

	// Note that the extra options are applied twice. First here for pre-apply
	// options, and then below for post-apply options. Pre and post refer to
	// before and after reading the metaindex and properties.
	type preApply interface{ preApply() }
	for _, opt := range extraOpts {
		if _, ok := opt.(preApply); ok {
			opt.readerApply(r)
		}
	}
	if r.cacheID == 0 {
		r.cacheID = r.opts.Cache.NewID()
	}

	footer, err := readFooter(f)
	if err != nil {
		r.err = err
		return nil, r.Close()
	}
	r.checksumType = footer.checksum
	r.tableFormat = footer.format
	// Read the metaindex.
	if err := r.readMetaindex(footer.metaindexBH); err != nil {
		r.err = err
		return nil, r.Close()
	}
	r.indexBH = footer.indexBH
	r.metaIndexBH = footer.metaindexBH
	r.footerBH = footer.footerBH

	if r.Properties.ComparerName == "" || o.Comparer.Name == r.Properties.ComparerName {
		r.Compare = o.Comparer.Compare
		r.FormatKey = o.Comparer.FormatKey
		r.Split = o.Comparer.Split
	}

	if o.MergerName == r.Properties.MergerName {
		r.mergerOK = true
	}

	// Apply the extra options again now that the comparer and merger names are
	// known.
	for _, opt := range extraOpts {
		if _, ok := opt.(preApply); !ok {
			opt.readerApply(r)
		}
	}

	if r.Compare == nil {
		r.err = errors.Errorf("pebble/table: %d: unknown comparer %s",
			errors.Safe(r.fileNum), errors.Safe(r.Properties.ComparerName))
	}
	if !r.mergerOK {
		if name := r.Properties.MergerName; name != "" && name != "nullptr" {
			r.err = errors.Errorf("pebble/table: %d: unknown merger %s",
				errors.Safe(r.fileNum), errors.Safe(r.Properties.MergerName))
		}
	}
	if r.err != nil {
		return nil, r.Close()
	}
	return r, nil
}

// Layout describes the block organization of an sstable.
type Layout struct {
	// NOTE: changes to fields in this struct should also be reflected in
	// ValidateBlockChecksums, which validates a static list of BlockHandles
	// referenced in this struct.

	Data       []BlockHandleWithProperties
	Index      []BlockHandle
	TopIndex   BlockHandle
	Filter     BlockHandle
	RangeDel   BlockHandle
	RangeKey   BlockHandle
	Properties BlockHandle
	MetaIndex  BlockHandle
	Footer     BlockHandle
}

// Describe returns a description of the layout. If the verbose parameter is
// true, details of the structure of each block are returned as well.
func (l *Layout) Describe(
	w io.Writer, verbose bool, r *Reader, fmtRecord func(key *base.InternalKey, value []byte),
) {
	type block struct {
		BlockHandle
		name string
	}
	var blocks []block

	for i := range l.Data {
		blocks = append(blocks, block{l.Data[i].BlockHandle, "data"})
	}
	for i := range l.Index {
		blocks = append(blocks, block{l.Index[i], "index"})
	}
	if l.TopIndex.Length != 0 {
		blocks = append(blocks, block{l.TopIndex, "top-index"})
	}
	if l.Filter.Length != 0 {
		blocks = append(blocks, block{l.Filter, "filter"})
	}
	if l.RangeDel.Length != 0 {
		blocks = append(blocks, block{l.RangeDel, "range-del"})
	}
	if l.RangeKey.Length != 0 {
		blocks = append(blocks, block{l.RangeKey, "range-key"})
	}
	if l.Properties.Length != 0 {
		blocks = append(blocks, block{l.Properties, "properties"})
	}
	if l.MetaIndex.Length != 0 {
		blocks = append(blocks, block{l.MetaIndex, "meta-index"})
	}
	if l.Footer.Length != 0 {
		if l.Footer.Length == levelDBFooterLen {
			blocks = append(blocks, block{l.Footer, "leveldb-footer"})
		} else {
			blocks = append(blocks, block{l.Footer, "footer"})
		}
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	})

	for i := range blocks {
		b := &blocks[i]
		fmt.Fprintf(w, "%10d  %s (%d)\n", b.Offset, b.name, b.Length)

		if !verbose {
			continue
		}
		if b.name == "filter" {
			continue
		}

		if b.name == "footer" || b.name == "leveldb-footer" {
			trailer, offset := make([]byte, b.Length), b.Offset
			_, _ = r.file.ReadAt(trailer, int64(offset))

			if b.name == "footer" {
				checksumType := ChecksumType(trailer[0])
				fmt.Fprintf(w, "%10d    checksum type: %s\n", offset, checksumType)
				trailer, offset = trailer[1:], offset+1
			}

			metaHandle, n := binary.Uvarint(trailer)
			metaLen, m := binary.Uvarint(trailer[n:])
			fmt.Fprintf(w, "%10d    meta: offset=%d, length=%d\n", offset, metaHandle, metaLen)
			trailer, offset = trailer[n+m:], offset+uint64(n+m)

			indexHandle, n := binary.Uvarint(trailer)
			indexLen, m := binary.Uvarint(trailer[n:])
			fmt.Fprintf(w, "%10d    index: offset=%d, length=%d\n", offset, indexHandle, indexLen)
			trailer, offset = trailer[n+m:], offset+uint64(n+m)

			fmt.Fprintf(w, "%10d    [padding]\n", offset)

			trailing := 12
			if b.name == "leveldb-footer" {
				trailing = 8
			}

			offset += uint64(len(trailer) - trailing)
			trailer = trailer[len(trailer)-trailing:]

			if b.name == "footer" {
				version := trailer[:4]
				fmt.Fprintf(w, "%10d    version: %d\n", offset, binary.LittleEndian.Uint32(version))
				trailer, offset = trailer[4:], offset+4
			}

			magicNumber := trailer
			fmt.Fprintf(w, "%10d    magic number: 0x%x\n", offset, magicNumber)

			continue
		}

		h, _, err := r.readBlock(b.BlockHandle, nil /* transform */, nil /* readaheadState */)
		if err != nil {
			fmt.Fprintf(w, "  [err: %s]\n", err)
			continue
		}

		getRestart := func(data []byte, restarts, i int32) int32 {
			return int32(binary.LittleEndian.Uint32(data[restarts+4*i:]))
		}

		formatIsRestart := func(data []byte, restarts, numRestarts, offset int32) {
			i := sort.Search(int(numRestarts), func(i int) bool {
				return getRestart(data, restarts, int32(i)) >= offset
			})
			if i < int(numRestarts) && getRestart(data, restarts, int32(i)) == offset {
				fmt.Fprintf(w, " [restart]\n")
			} else {
				fmt.Fprintf(w, "\n")
			}
		}

		formatRestarts := func(data []byte, restarts, numRestarts int32) {
			for i := int32(0); i < numRestarts; i++ {
				offset := getRestart(data, restarts, i)
				fmt.Fprintf(w, "%10d    [restart %d]\n",
					b.Offset+uint64(restarts+4*i), b.Offset+uint64(offset))
			}
		}

		formatTrailer := func() {
			trailer := make([]byte, blockTrailerLen)
			offset := int64(b.Offset + b.Length)
			_, _ = r.file.ReadAt(trailer, offset)
			bt := blockType(trailer[0])
			checksum := binary.LittleEndian.Uint32(trailer[1:])
			fmt.Fprintf(w, "%10d    [trailer compression=%s checksum=0x%04x]\n", offset, bt, checksum)
		}

		var lastKey InternalKey
		switch b.name {
		case "data", "range-del", "range-key":
			iter, _ := newBlockIter(r.Compare, h.Get())
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				ptr := unsafe.Pointer(uintptr(iter.ptr) + uintptr(iter.offset))
				shared, ptr := decodeVarint(ptr)
				unshared, ptr := decodeVarint(ptr)
				value2, _ := decodeVarint(ptr)

				total := iter.nextOffset - iter.offset
				// The format of the numbers in the record line is:
				//
				//   (<total> = <length> [<shared>] + <unshared> + <value>)
				//
				// <total>    is the total number of bytes for the record.
				// <length>   is the size of the 3 varint encoded integers for <shared>,
				//            <unshared>, and <value>.
				// <shared>   is the number of key bytes shared with the previous key.
				// <unshared> is the number of unshared key bytes.
				// <value>    is the number of value bytes.
				fmt.Fprintf(w, "%10d    record (%d = %d [%d] + %d + %d)",
					b.Offset+uint64(iter.offset), total,
					total-int32(unshared+value2), shared, unshared, value2)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
				if fmtRecord != nil {
					fmt.Fprintf(w, "              ")
					fmtRecord(key, value)
				}

				if base.InternalCompare(r.Compare, lastKey, *key) >= 0 {
					fmt.Fprintf(w, "              WARNING: OUT OF ORDER KEYS!\n")
				}
				lastKey.Trailer = key.Trailer
				lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
			formatTrailer()
		case "index", "top-index":
			iter, _ := newBlockIter(r.Compare, h.Get())
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				bh, err := decodeBlockHandleWithProperties(value)
				if err != nil {
					fmt.Fprintf(w, "%10d    [err: %s]\n", b.Offset+uint64(iter.offset), err)
					continue
				}
				fmt.Fprintf(w, "%10d    block:%d/%d",
					b.Offset+uint64(iter.offset), bh.Offset, bh.Length)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
			formatTrailer()
		case "properties":
			iter, _ := newRawBlockIter(r.Compare, h.Get())
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(w, "%10d    %s (%d)",
					b.Offset+uint64(iter.offset), iter.Key().UserKey, iter.nextOffset-iter.offset)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
			formatTrailer()
		case "meta-index":
			iter, _ := newRawBlockIter(r.Compare, h.Get())
			for valid := iter.First(); valid; valid = iter.Next() {
				value := iter.Value()
				bh, n := decodeBlockHandle(value)
				if n == 0 || n != len(value) {
					fmt.Fprintf(w, "%10d    [err: %s]\n", b.Offset+uint64(iter.offset), err)
					continue
				}

				fmt.Fprintf(w, "%10d    %s block:%d/%d",
					b.Offset+uint64(iter.offset), iter.Key().UserKey,
					bh.Offset, bh.Length)
				formatIsRestart(iter.data, iter.restarts, iter.numRestarts, iter.offset)
			}
			formatRestarts(iter.data, iter.restarts, iter.numRestarts)
			formatTrailer()
		}

		h.Release()
	}

	last := blocks[len(blocks)-1]
	fmt.Fprintf(w, "%10d  EOF\n", last.Offset+last.Length)
}

type panicFataler struct{}

func (panicFataler) Fatalf(format string, args ...interface{}) {
	panic(errors.Errorf(format, args...))
}
