// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

// encodedBHPEstimatedSize estimates the size of the encoded BlockHandleWithProperties.
// It would also be nice to account for the length of the data block properties here,
// but isn't necessary since this is an estimate.
const encodedBHPEstimatedSize = binary.MaxVarintLen64 * 2

var errWriterClosed = errors.New("pebble: writer is closed")

// WriterMetadata holds info about a finished sstable.
type WriterMetadata struct {
	Size          uint64
	SmallestPoint InternalKey
	// LargestPoint, LargestRangeKey, LargestRangeDel should not be accessed
	// before Writer.Close is called, because they may only be set on
	// Writer.Close.
	LargestPoint     InternalKey
	SmallestRangeDel InternalKey
	LargestRangeDel  InternalKey
	SmallestRangeKey InternalKey
	LargestRangeKey  InternalKey
	HasPointKeys     bool
	HasRangeDelKeys  bool
	HasRangeKeys     bool
	SmallestSeqNum   uint64
	LargestSeqNum    uint64
	Properties       Properties
}

// SetSmallestPointKey sets the smallest point key to the given key.
// NB: this method set the "absolute" smallest point key. Any existing key is
// overridden.
func (m *WriterMetadata) SetSmallestPointKey(k InternalKey) {
	m.SmallestPoint = k
	m.HasPointKeys = true
}

// SetSmallestRangeDelKey sets the smallest rangedel key to the given key.
// NB: this method set the "absolute" smallest rangedel key. Any existing key is
// overridden.
func (m *WriterMetadata) SetSmallestRangeDelKey(k InternalKey) {
	m.SmallestRangeDel = k
	m.HasRangeDelKeys = true
}

// SetSmallestRangeKey sets the smallest range key to the given key.
// NB: this method set the "absolute" smallest range key. Any existing key is
// overridden.
func (m *WriterMetadata) SetSmallestRangeKey(k InternalKey) {
	m.SmallestRangeKey = k
	m.HasRangeKeys = true
}

// SetLargestPointKey sets the largest point key to the given key.
// NB: this method set the "absolute" largest point key. Any existing key is
// overridden.
func (m *WriterMetadata) SetLargestPointKey(k InternalKey) {
	m.LargestPoint = k
	m.HasPointKeys = true
}

// SetLargestRangeDelKey sets the largest rangedel key to the given key.
// NB: this method set the "absolute" largest rangedel key. Any existing key is
// overridden.
func (m *WriterMetadata) SetLargestRangeDelKey(k InternalKey) {
	m.LargestRangeDel = k
	m.HasRangeDelKeys = true
}

// SetLargestRangeKey sets the largest range key to the given key.
// NB: this method set the "absolute" largest range key. Any existing key is
// overridden.
func (m *WriterMetadata) SetLargestRangeKey(k InternalKey) {
	m.LargestRangeKey = k
	m.HasRangeKeys = true
}

func (m *WriterMetadata) updateSeqNum(seqNum uint64) {
	if m.SmallestSeqNum > seqNum {
		m.SmallestSeqNum = seqNum
	}
	if m.LargestSeqNum < seqNum {
		m.LargestSeqNum = seqNum
	}
}

type flusher interface {
	Flush() error
}

type writeCloseSyncer interface {
	io.WriteCloser
	Sync() error
}

// Writer is a table writer.
type Writer struct {
	writer    io.Writer
	bufWriter *bufio.Writer
	syncer    writeCloseSyncer
	meta      WriterMetadata
	err       error
	// cacheID and fileNum are used to remove blocks written to the sstable from
	// the cache, providing a defense in depth against bugs which cause cache
	// collisions.
	cacheID uint64
	fileNum base.FileNum
	// The following fields are copied from Options.
	blockSize               int
	blockSizeThreshold      int
	indexBlockSize          int
	indexBlockSizeThreshold int
	compare                 Compare
	split                   Split
	formatKey               base.FormatKey
	compression             Compression
	separator               Separator
	successor               Successor
	tableFormat             TableFormat
	cache                   *cache.Cache
	restartInterval         int
	checksumType            ChecksumType
	// disableKeyOrderChecks disables the checks that keys are added to an
	// sstable in order. It is intended for internal use only in the construction
	// of invalid sstables for testing. See tool/make_test_sstables.go.
	disableKeyOrderChecks bool
	// With two level indexes, the index/filter of a SST file is partitioned into
	// smaller blocks with an additional top-level index on them. When reading an
	// index/filter, only the top-level index is loaded into memory. The two level
	// index/filter then uses the top-level index to load on demand into the block
	// cache the partitions that are required to perform the index/filter query.
	//
	// Two level indexes are enabled automatically when there is more than one
	// index block.
	//
	// This is useful when there are very large index blocks, which generally occurs
	// with the usage of large keys. With large index blocks, the index blocks fight
	// the data blocks for block cache space and the index blocks are likely to be
	// re-read many times from the disk. The top level index, which has a much
	// smaller memory footprint, can be used to prevent the entire index block from
	// being loaded into the block cache.
	twoLevelIndex bool
	// Internal flag to allow creation of range-del-v1 format blocks. Only used
	// for testing. Note that v2 format blocks are backwards compatible with v1
	// format blocks.
	rangeDelV1Format    bool
	indexBlock          *indexBlockBuf
	rangeDelBlock       blockWriter
	rangeKeyBlock       blockWriter
	topLevelIndexBlock  blockWriter
	props               Properties
	propCollectors      []TablePropertyCollector
	blockPropCollectors []BlockPropertyCollector
	blockPropsEncoder   blockPropertiesEncoder
	// filter accumulates the filter block. If populated, the filter ingests
	// either the output of w.split (i.e. a prefix extractor) if w.split is not
	// nil, or the full keys otherwise.
	filter          filterWriter
	indexPartitions []indexBlockAndBlockProperties

	// indexBlockAlloc is used to bulk-allocate byte slices used to store index
	// blocks in indexPartitions. These live until the index finishes.
	indexBlockAlloc []byte
	// indexSepAlloc is used to bulk-allocate index block seperator slices stored
	// in indexPartitions. These live until the index finishes.
	indexSepAlloc []byte

	// To allow potentially overlapping (i.e. un-fragmented) range keys spans to
	// be added to the Writer, a keyspan.Fragmenter is used to retain the keys
	// and values, emitting fragmented, coalesced spans as appropriate. Range
	// keys must be added in order of their start user-key.
	fragmenter        keyspan.Fragmenter
	rangeKeyEncoder   rangekey.Encoder
	rangeKeyCoalesced keyspan.Span
	rkBuf             []byte
	// dataBlockBuf consists of the state which is currently owned by and used by
	// the Writer client goroutine. This state can be handed off to other goroutines.
	dataBlockBuf *dataBlockBuf
	// blockBuf consists of the state which is owned by and used by the Writer client
	// goroutine.
	blockBuf blockBuf

	coordination coordinationState
}

type coordinationState struct {
	parallelismEnabled bool

	// writeQueue is used to write data blocks to disk. The writeQueue is primarily
	// used to maintain the order in which data blocks must be written to disk. For
	// this reason, every single data block write must be done through the writeQueue.
	writeQueue *writeQueue

	sizeEstimate dataBlockEstimates
}

func (c *coordinationState) init(parallelismEnabled bool, writer *Writer) {
	c.parallelismEnabled = parallelismEnabled
	c.sizeEstimate.useMutex = parallelismEnabled

	// writeQueueSize determines the size of the write queue, or the number
	// of items which can be added to the queue without blocking. By default, we
	// use a writeQueue size of 0, since we won't be doing any block writes in
	// parallel.
	writeQueueSize := 0
	if parallelismEnabled {
		writeQueueSize = runtime.GOMAXPROCS(0)
	}
	c.writeQueue = newWriteQueue(writeQueueSize, writer)
}

type sizeEstimate struct {
	// emptySize is the size when there is no inflight data, and numEntries is 0.
	// emptySize is constant once set.
	emptySize uint64

	// inflightSize is the estimated size of some inflight data which hasn't
	// been written yet.
	inflightSize uint64

	// totalSize is the total size of the data which has already been written.
	totalSize uint64

	// numWrittenEntries is the total number of entries which have already been
	// written.
	numWrittenEntries uint64
	// numInflightEntries is the total number of entries which are inflight, and
	// haven't been written.
	numInflightEntries uint64

	// maxEstimatedSize stores the maximum result returned from sizeEstimate.size.
	// It ensures that values returned from subsequent calls to Writer.EstimatedSize
	// never decrease.
	maxEstimatedSize uint64

	// We assume that the entries added to the sizeEstimate can be compressed.
	// For this reason, we keep track of a compressedSize and an uncompressedSize
	// to compute a compression ratio for the inflight entries. If the entries
	// aren't being compressed, then compressedSize and uncompressedSize must be
	// equal.
	compressedSize   uint64
	uncompressedSize uint64
}

func (s *sizeEstimate) init(emptySize uint64) {
	s.emptySize = emptySize
}

func (s *sizeEstimate) size() uint64 {
	ratio := float64(1)
	if s.uncompressedSize > 0 {
		ratio = float64(s.compressedSize) / float64(s.uncompressedSize)
	}
	estimatedInflightSize := uint64(float64(s.inflightSize) * ratio)
	total := s.totalSize + estimatedInflightSize
	if total > s.maxEstimatedSize {
		s.maxEstimatedSize = total
	} else {
		total = s.maxEstimatedSize
	}

	if total == 0 {
		return s.emptySize
	}

	return total
}

func (s *sizeEstimate) numTotalEntries() uint64 {
	return s.numWrittenEntries + s.numInflightEntries
}

func (s *sizeEstimate) addInflight(size int) {
	s.numInflightEntries++
	s.inflightSize += uint64(size)
}

func (s *sizeEstimate) written(newTotalSize uint64, inflightSize int, finalEntrySize int) {
	s.inflightSize -= uint64(inflightSize)
	if inflightSize > 0 {
		// This entry was previously inflight, so we should decrement inflight
		// entries.
		s.numInflightEntries--
	}
	s.numWrittenEntries++
	s.totalSize = newTotalSize

	s.uncompressedSize += uint64(inflightSize)
	s.compressedSize += uint64(finalEntrySize)
}

func (s *sizeEstimate) clear() {
	*s = sizeEstimate{emptySize: s.emptySize}
}

type indexBlockBuf struct {
	// block will only be accessed from the writeQueue.
	block blockWriter

	size struct {
		useMutex bool
		mu       sync.Mutex
		estimate sizeEstimate
	}

	// restartInterval matches indexBlockBuf.block.restartInterval. We store it twice, because the `block`
	// must only be accessed from the writeQueue goroutine.
	restartInterval int
}

func (i *indexBlockBuf) clear() {
	i.block.clear()
	if i.size.useMutex {
		i.size.mu.Lock()
		defer i.size.mu.Unlock()
	}
	i.size.estimate.clear()
	i.restartInterval = 0
}

var indexBlockBufPool = sync.Pool{
	New: func() interface{} {
		return &indexBlockBuf{}
	},
}

const indexBlockRestartInterval = 1

func newIndexBlockBuf(useMutex bool) *indexBlockBuf {
	i := indexBlockBufPool.Get().(*indexBlockBuf)
	i.size.useMutex = useMutex
	i.restartInterval = indexBlockRestartInterval
	i.block.restartInterval = indexBlockRestartInterval
	i.size.estimate.init(emptyBlockSize)
	return i
}

func (i *indexBlockBuf) shouldFlush(
	sep InternalKey, valueLen, targetBlockSize, sizeThreshold int,
) bool {
	if i.size.useMutex {
		i.size.mu.Lock()
		defer i.size.mu.Unlock()
	}

	// nEntries := i.size.estimate.numWrittenEntries + i.size.estimate.numInflightEntries
	nEntries := i.size.estimate.numTotalEntries()
	return shouldFlush(
		sep, valueLen, i.restartInterval, int(i.size.estimate.size()),
		int(nEntries), targetBlockSize, sizeThreshold)
}

func (i *indexBlockBuf) add(key InternalKey, value []byte, inflightSize int) {
	i.block.add(key, value)
	size := i.block.estimatedSize()
	if i.size.useMutex {
		i.size.mu.Lock()
		defer i.size.mu.Unlock()
	}
	// Since, we're not compressing index entries when adding them to index blocks,
	// we assume that the size of entry written to the index block is equal to the
	// size of the inflight entry, giving us a compression ratio of 1.
	i.size.estimate.written(uint64(size), inflightSize, inflightSize)
}

func (i *indexBlockBuf) finish() []byte {
	b := i.block.finish()
	return b
}

func (i *indexBlockBuf) addInflight(inflightSize int) {
	if i.size.useMutex {
		i.size.mu.Lock()
		defer i.size.mu.Unlock()
	}
	i.size.estimate.addInflight(inflightSize)
}

func (i *indexBlockBuf) estimatedSize() uint64 {
	if i.size.useMutex {
		i.size.mu.Lock()
		defer i.size.mu.Unlock()
	}

	// Make sure that the size estimation works as expected when parallelism
	// is disabled.
	if invariants.Enabled && !i.size.useMutex {
		if i.size.estimate.inflightSize != 0 {
			panic("unexpected inflight entry in index block size estimation")
		}

		// NB: The i.block should only be accessed from the writeQueue goroutine,
		// when parallelism is enabled. We break that invariant here, but that's
		// okay since parallelism is disabled.
		if i.size.estimate.size() != uint64(i.block.estimatedSize()) {
			panic("index block size estimation sans parallelism is incorrect")
		}
	}
	return i.size.estimate.size()
}

// sizeEstimate is used for sstable size estimation. sizeEstimate can be accessed by
// the Writer client, writeQueue, compressionQueue goroutines. Fields should only be
// read/updated through the functions defined on the *sizeEstimate type.
type dataBlockEstimates struct {
	// If we don't do block compression, block writes in parallel, then we don't need to take
	// the performance hit of synchronizing using this mutex.
	useMutex bool
	mu       sync.Mutex

	estimate sizeEstimate
}

// newTotalSize is the new w.meta.Size. inflightSize is the uncompressed block size estimate which
// was previously added to sizeEstimate.inflightSize. writtenSize is the compressed size of the block
// which was written to disk.
func (d *dataBlockEstimates) dataBlockWritten(
	newTotalSize uint64, inflightSize int, writtenSize int,
) {
	if d.useMutex {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	d.estimate.written(newTotalSize, inflightSize, writtenSize)
}

// size is an estimated size of datablock data which has been written to disk.
func (d *dataBlockEstimates) size() uint64 {
	if d.useMutex {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	// Use invariants to make sure that the size estimation works as expected
	// when parallelism is disabled.
	if invariants.Enabled && !d.useMutex {
		if d.estimate.inflightSize != 0 {
			panic("unexpected inflight entry in data block size estimation")
		}
	}

	return d.estimate.size()
}

func (d *dataBlockEstimates) addInflightDataBlock(size int) {
	if d.useMutex {
		d.mu.Lock()
		defer d.mu.Unlock()
	}

	d.estimate.addInflight(size)
}

var writeTaskPool = sync.Pool{
	New: func() interface{} {
		t := &writeTask{}
		t.compressionDone = make(chan bool, 1)
		return t
	},
}

type checksummer struct {
	checksumType ChecksumType
	xxHasher     *xxhash.Digest
}

func (c *checksummer) checksum(block []byte, blockType []byte) (checksum uint32) {
	// Calculate the checksum.
	switch c.checksumType {
	case ChecksumTypeCRC32c:
		checksum = crc.New(block).Update(blockType).Value()
	case ChecksumTypeXXHash64:
		if c.xxHasher == nil {
			c.xxHasher = xxhash.New()
		} else {
			c.xxHasher.Reset()
		}
		c.xxHasher.Write(block)
		c.xxHasher.Write(blockType)
		checksum = uint32(c.xxHasher.Sum64())
	default:
		panic(errors.Newf("unsupported checksum type: %d", c.checksumType))
	}
	return checksum
}

type blockBuf struct {
	// tmp is a scratch buffer, large enough to hold either footerLen bytes,
	// blockTrailerLen bytes, (5 * binary.MaxVarintLen64) bytes, and most
	// likely large enough for a block handle with properties.
	tmp [blockHandleLikelyMaxLen]byte
	// compressedBuf is the destination buffer for compression. It is re-used over the
	// lifetime of the blockBuf, avoiding the allocation of a temporary buffer for each block.
	compressedBuf []byte
	checksummer   checksummer
}

func (b *blockBuf) clear() {
	// We can't assign b.compressedBuf[:0] to compressedBuf because snappy relies
	// on the length of the buffer, and not the capacity to determine if it needs
	// to make an allocation.
	*b = blockBuf{
		compressedBuf: b.compressedBuf, checksummer: b.checksummer,
	}
}

// A dataBlockBuf holds all the state required to compress and write a data block to disk.
// A dataBlockBuf begins its lifecycle owned by the Writer client goroutine. The Writer
// client goroutine adds keys to the sstable, writing directly into a dataBlockBuf's blockWriter
// until the block is full. Once a dataBlockBuf's block is full, the dataBlockBuf may be passed
// to other goroutines for compression and file I/O.
type dataBlockBuf struct {
	blockBuf
	dataBlock blockWriter

	// uncompressed is a reference to a byte slice which is owned by the dataBlockBuf. It is the
	// next byte slice to be compressed. The uncompressed byte slice will be backed by the
	// dataBlock.buf.
	uncompressed []byte
	// compressed is a reference to a byte slice which is owned by the dataBlockBuf. It is the
	// compressed byte slice which must be written to disk. The compressed byte slice may be
	// backed by the dataBlock.buf, or the dataBlockBuf.compressedBuf, depending on whether
	// we use the result of the compression.
	compressed []byte

	// We're making calls to BlockPropertyCollectors from the Writer client goroutine. We need to
	// pass the encoded block properties over to the write queue. To prevent copies, and allocations,
	// we give each dataBlockBuf, a blockPropertiesEncoder.
	blockPropsEncoder blockPropertiesEncoder
	// dataBlockProps is set when Writer.finishDataBlockProps is called. The dataBlockProps slice is
	// a shallow copy of the internal buffer of the dataBlockBuf.blockPropsEncoder.
	dataBlockProps []byte

	// sepScratch is reusable scratch space for computing separator keys.
	sepScratch []byte
}

func (d *dataBlockBuf) clear() {
	d.blockBuf.clear()
	d.dataBlock.clear()

	d.uncompressed = nil
	d.compressed = nil
	d.dataBlockProps = nil
	d.sepScratch = d.sepScratch[:0]
}

var dataBlockBufPool = sync.Pool{
	New: func() interface{} {
		return &dataBlockBuf{}
	},
}

func newDataBlockBuf(restartInterval int, checksumType ChecksumType) *dataBlockBuf {
	d := dataBlockBufPool.Get().(*dataBlockBuf)
	d.dataBlock.restartInterval = restartInterval
	d.checksummer.checksumType = checksumType
	return d
}

func (d *dataBlockBuf) finish() {
	d.uncompressed = d.dataBlock.finish()
}

func (d *dataBlockBuf) compressAndChecksum(c Compression) {
	d.compressed = compressAndChecksum(d.uncompressed, c, &d.blockBuf)
}

func (d *dataBlockBuf) shouldFlush(
	key InternalKey, valueLen, targetBlockSize, sizeThreshold int,
) bool {
	return shouldFlush(
		key, valueLen, d.dataBlock.restartInterval, d.dataBlock.estimatedSize(),
		d.dataBlock.nEntries, targetBlockSize, sizeThreshold)
}

type indexBlockAndBlockProperties struct {
	nEntries int
	// sep is the last key added to this block, for computing a separator later.
	sep        InternalKey
	properties []byte
	// block is the encoded block produced by blockWriter.finish.
	block []byte
}

// Set sets the value for the given key. The sequence number is set to 0.
// Intended for use to externally construct an sstable before ingestion into a
// DB. For a given Writer, the keys passed to Set must be in strictly increasing
// order.
//
// TODO(peter): untested
func (w *Writer) Set(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addPoint(base.MakeInternalKey(key, 0, InternalKeyKindSet), value)
}

// Delete deletes the value for the given key. The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// TODO(peter): untested
func (w *Writer) Delete(key []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addPoint(base.MakeInternalKey(key, 0, InternalKeyKindDelete), nil)
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end). The sequence number is set to
// 0. Intended for use to externally construct an sstable before ingestion into
// a DB.
//
// TODO(peter): untested
func (w *Writer) DeleteRange(start, end []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addTombstone(base.MakeInternalKey(start, 0, InternalKeyKindRangeDelete), end)
}

// Merge adds an action to the DB that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator. The sequence number is set to 0. Intended for use to externally
// construct an sstable before ingestion into a DB.
//
// TODO(peter): untested
func (w *Writer) Merge(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addPoint(base.MakeInternalKey(key, 0, InternalKeyKindMerge), value)
}

// Add adds a key/value pair to the table being written. For a given Writer,
// the keys passed to Add must be in increasing order. The exception to this
// rule is range deletion tombstones. Range deletion tombstones need to be
// added ordered by their start key, but they can be added out of order from
// point entries. Additionally, range deletion tombstones must be fragmented
// (i.e. by keyspan.Fragmenter).
func (w *Writer) Add(key InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}

	switch key.Kind() {
	case InternalKeyKindRangeDelete:
		return w.addTombstone(key, value)
	case base.InternalKeyKindRangeKeyDelete,
		base.InternalKeyKindRangeKeySet,
		base.InternalKeyKindRangeKeyUnset:
		w.err = errors.Errorf(
			"pebble: range keys must be added via one of the RangeKey* functions")
		return w.err
	}
	return w.addPoint(key, value)
}

func (w *Writer) addPoint(key InternalKey, value []byte) error {
	if !w.disableKeyOrderChecks && w.dataBlockBuf.dataBlock.nEntries >= 1 {
		// curKey is guaranteed to be the last point key which was added to the Writer.
		// Inlining base.DecodeInternalKey has a 2-3% improve in the BenchmarkWriter
		// benchmark.
		encodedKey := w.dataBlockBuf.dataBlock.curKey
		n := len(encodedKey) - base.InternalTrailerLen
		var trailer uint64
		if n >= 0 {
			trailer = binary.LittleEndian.Uint64(encodedKey[n:])
			encodedKey = encodedKey[:n:n]
		} else {
			trailer = uint64(InternalKeyKindInvalid)
			encodedKey = nil
		}
		largestPointKey := InternalKey{
			UserKey: encodedKey,
			Trailer: trailer,
		}

		if largestPointKey.UserKey != nil {
			// TODO(peter): Manually inlined version of base.InternalCompare(). This is
			// 3.5% faster on BenchmarkWriter on go1.13. Remove if go1.14 or future
			// versions show this to not be a performance win.
			x := w.compare(largestPointKey.UserKey, key.UserKey)
			if x > 0 || (x == 0 && largestPointKey.Trailer <= key.Trailer) {
				w.err = errors.Errorf("pebble: keys must be added in strictly increasing order: %s, %s",
					largestPointKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
				return w.err
			}
		}
	}

	if err := w.maybeFlush(key, value); err != nil {
		return err
	}

	for i := range w.propCollectors {
		if err := w.propCollectors[i].Add(key, value); err != nil {
			w.err = err
			return err
		}
	}
	for i := range w.blockPropCollectors {
		if err := w.blockPropCollectors[i].Add(key, value); err != nil {
			w.err = err
			return err
		}
	}

	w.maybeAddToFilter(key.UserKey)
	w.dataBlockBuf.dataBlock.add(key, value)

	w.meta.updateSeqNum(key.SeqNum())

	if !w.meta.HasPointKeys {
		k := base.DecodeInternalKey(w.dataBlockBuf.dataBlock.curKey)
		// NB: We need to ensure that SmallestPoint.UserKey is set, so we create
		// an InternalKey which is semantically identical to the key, but won't
		// have a nil UserKey. We do this, because key.UserKey could be nil, and
		// we don't want SmallestPoint.UserKey to be nil.
		//
		// todo(bananabrick): Determine if it's okay to have a nil SmallestPoint
		// .UserKey now that we don't rely on a nil UserKey to determine if the
		// key has been set or not.
		w.meta.SetSmallestPointKey(k.Clone())
	}

	w.props.NumEntries++
	switch key.Kind() {
	case InternalKeyKindDelete:
		w.props.NumDeletions++
	case InternalKeyKindMerge:
		w.props.NumMergeOperands++
	}
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	return nil
}

func (w *Writer) prettyTombstone(k InternalKey, value []byte) fmt.Formatter {
	return keyspan.Span{
		Start: k.UserKey,
		End:   value,
		Keys:  []keyspan.Key{{Trailer: k.Trailer}},
	}.Pretty(w.formatKey)
}

func (w *Writer) addTombstone(key InternalKey, value []byte) error {
	if !w.disableKeyOrderChecks && !w.rangeDelV1Format && w.rangeDelBlock.nEntries > 0 {
		// Check that tombstones are being added in fragmented order. If the two
		// tombstones overlap, their start and end keys must be identical.
		prevKey := base.DecodeInternalKey(w.rangeDelBlock.curKey)
		switch c := w.compare(prevKey.UserKey, key.UserKey); {
		case c > 0:
			w.err = errors.Errorf("pebble: keys must be added in order: %s, %s",
				prevKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
			return w.err
		case c == 0:
			prevValue := w.rangeDelBlock.curValue
			if w.compare(prevValue, value) != 0 {
				w.err = errors.Errorf("pebble: overlapping tombstones must be fragmented: %s vs %s",
					w.prettyTombstone(prevKey, prevValue),
					w.prettyTombstone(key, value))
				return w.err
			}
			if prevKey.SeqNum() <= key.SeqNum() {
				w.err = errors.Errorf("pebble: keys must be added in strictly increasing order: %s, %s",
					prevKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
				return w.err
			}
		default:
			prevValue := w.rangeDelBlock.curValue
			if w.compare(prevValue, key.UserKey) > 0 {
				w.err = errors.Errorf("pebble: overlapping tombstones must be fragmented: %s vs %s",
					w.prettyTombstone(prevKey, prevValue),
					w.prettyTombstone(key, value))
				return w.err
			}
		}
	}

	if key.Trailer == InternalKeyRangeDeleteSentinel {
		w.err = errors.Errorf("pebble: cannot add range delete sentinel: %s", key.Pretty(w.formatKey))
		return w.err
	}

	for i := range w.propCollectors {
		if err := w.propCollectors[i].Add(key, value); err != nil {
			w.err = err
			return err
		}
	}

	w.meta.updateSeqNum(key.SeqNum())

	switch {
	case w.rangeDelV1Format:
		// Range tombstones are not fragmented in the v1 (i.e. RocksDB) range
		// deletion block format, so we need to track the largest range tombstone
		// end key as every range tombstone is added.
		//
		// Note that writing the v1 format is only supported for tests.
		if w.props.NumRangeDeletions == 0 {
			w.meta.SetSmallestRangeDelKey(key.Clone())
			w.meta.SetLargestRangeDelKey(base.MakeRangeDeleteSentinelKey(value).Clone())
		} else {
			if base.InternalCompare(w.compare, w.meta.SmallestRangeDel, key) > 0 {
				w.meta.SetSmallestRangeDelKey(key.Clone())
			}
			end := base.MakeRangeDeleteSentinelKey(value)
			if base.InternalCompare(w.compare, w.meta.LargestRangeDel, end) < 0 {
				w.meta.SetLargestRangeDelKey(end.Clone())
			}
		}

	default:
		// Range tombstones are fragmented in the v2 range deletion block format,
		// so the start key of the first range tombstone added will be the smallest
		// range tombstone key. The largest range tombstone key will be determined
		// in Writer.Close() as the end key of the last range tombstone added.
		if w.props.NumRangeDeletions == 0 {
			w.meta.SetSmallestRangeDelKey(key.Clone())
		}
	}

	w.props.NumEntries++
	w.props.NumDeletions++
	w.props.NumRangeDeletions++
	w.props.RawKeySize += uint64(key.Size())
	w.props.RawValueSize += uint64(len(value))
	w.rangeDelBlock.add(key, value)
	return nil
}

// RangeKeySet sets a range between start (inclusive) and end (exclusive) with
// the given suffix to the given value.
//
// Keys must be added to the table in increasing order of start key. Spans are
// not required to be fragmented.
func (w *Writer) RangeKeySet(start, end, suffix, value []byte) error {
	return w.addRangeKeySpan(keyspan.Span{
		Start: w.tempRangeKeyCopy(start),
		End:   w.tempRangeKeyCopy(end),
		Keys: []keyspan.Key{
			{
				Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeySet),
				Suffix:  w.tempRangeKeyCopy(suffix),
				Value:   w.tempRangeKeyCopy(value),
			},
		},
	})
}

// RangeKeyUnset un-sets a range between start (inclusive) and end (exclusive)
// with the given suffix.
//
// Keys must be added to the table in increasing order of start key. Spans are
// not required to be fragmented.
func (w *Writer) RangeKeyUnset(start, end, suffix []byte) error {
	return w.addRangeKeySpan(keyspan.Span{
		Start: w.tempRangeKeyCopy(start),
		End:   w.tempRangeKeyCopy(end),
		Keys: []keyspan.Key{
			{
				Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeyUnset),
				Suffix:  w.tempRangeKeyCopy(suffix),
			},
		},
	})
}

// RangeKeyDelete deletes a range between start (inclusive) and end (exclusive).
//
// Keys must be added to the table in increasing order of start key. Spans are
// not required to be fragmented.
func (w *Writer) RangeKeyDelete(start, end []byte) error {
	return w.addRangeKeySpan(keyspan.Span{
		Start: w.tempRangeKeyCopy(start),
		End:   w.tempRangeKeyCopy(end),
		Keys: []keyspan.Key{
			{Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeyDelete)},
		},
	})
}

// AddRangeKey adds a range key set, unset, or delete key/value pair to the
// table being written.
//
// Range keys must be supplied in strictly ascending order of start key (i.e.
// user key ascending, sequence number descending, and key type descending).
// Ranges added must also be supplied in fragmented span order - i.e. other than
// spans that are perfectly aligned (same start and end keys), spans may not
// overlap. Range keys may be added out of order relative to point keys and
// range deletions.
func (w *Writer) AddRangeKey(key InternalKey, value []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.addRangeKey(key, value)
}

func (w *Writer) addRangeKeySpan(span keyspan.Span) error {
	if w.fragmenter.Start() != nil && w.compare(w.fragmenter.Start(), span.Start) > 0 {
		return errors.Errorf("pebble: spans must be added in order: %s > %s",
			w.formatKey(w.fragmenter.Start()), w.formatKey(span.Start))
	}
	// Add this span to the fragmenter.
	w.fragmenter.Add(span)
	return w.err
}

func (w *Writer) coalesceSpans(span keyspan.Span) {
	// This method is the emit function of the Fragmenter, so span.Keys is only
	// owned by this span and it's safe to mutate.
	w.rangeKeyCoalesced.Start = span.Start
	w.rangeKeyCoalesced.End = span.End
	err := rangekey.Coalesce(w.compare, span.Keys, &w.rangeKeyCoalesced.Keys)
	if err != nil {
		w.err = errors.Newf("sstable: could not coalesce span: %s", err)
		return
	}

	// NB: The span only contains range keys and is internally consistent (eg,
	// no duplicate suffixes, no additional keys after a RANGEKEYDEL).
	w.err = firstError(w.err, w.rangeKeyEncoder.Encode(&w.rangeKeyCoalesced))
}

func (w *Writer) addRangeKey(key InternalKey, value []byte) error {
	if !w.disableKeyOrderChecks && w.rangeKeyBlock.nEntries > 0 {
		prevStartKey := base.DecodeInternalKey(w.rangeKeyBlock.curKey)
		prevEndKey, _, ok := rangekey.DecodeEndKey(prevStartKey.Kind(), w.rangeKeyBlock.curValue)
		if !ok {
			// We panic here as we should have previously decoded and validated this
			// key and value when it was first added to the range key block.
			panic(errors.Errorf("pebble: invalid end key for span: %s",
				prevStartKey.Pretty(w.formatKey)))
		}

		curStartKey := key
		curEndKey, _, ok := rangekey.DecodeEndKey(curStartKey.Kind(), value)
		if !ok {
			w.err = errors.Errorf("pebble: invalid end key for span: %s",
				curStartKey.Pretty(w.formatKey))
			return w.err
		}

		// Start keys must be strictly increasing.
		if base.InternalCompare(w.compare, prevStartKey, curStartKey) >= 0 {
			w.err = errors.Errorf(
				"pebble: range keys starts must be added in increasing order: %s, %s",
				prevStartKey.Pretty(w.formatKey), key.Pretty(w.formatKey))
			return w.err
		}

		// Start keys are increasing. If the start user keys are equal, the
		// end keys must be equal (i.e. aligned spans).
		if w.compare(prevStartKey.UserKey, curStartKey.UserKey) == 0 {
			if w.compare(prevEndKey, curEndKey) != 0 {
				w.err = errors.Errorf("pebble: overlapping range keys must be fragmented: %s, %s",
					prevStartKey.Pretty(w.formatKey),
					curStartKey.Pretty(w.formatKey))
				return w.err
			}
		} else if w.compare(prevEndKey, curStartKey.UserKey) > 0 {
			// If the start user keys are NOT equal, the spans must be disjoint (i.e.
			// no overlap).
			// NOTE: the inequality excludes zero, as we allow the end key of the
			// lower span be the same as the start key of the upper span, because
			// the range end key is considered an exclusive bound.
			w.err = errors.Errorf("pebble: overlapping range keys must be fragmented: %s, %s",
				prevStartKey.Pretty(w.formatKey),
				curStartKey.Pretty(w.formatKey))
			return w.err
		}
	}

	// TODO(travers): Add an invariant-gated check to ensure that suffix-values
	// are sorted within coalesced spans.

	// Range-keys and point-keys are intended to live in "parallel" keyspaces.
	// However, we track a single seqnum in the table metadata that spans both of
	// these keyspaces.
	// TODO(travers): Consider tracking range key seqnums separately.
	w.meta.updateSeqNum(key.SeqNum())

	// Range tombstones are fragmented, so the start key of the first range key
	// added will be the smallest. The largest range key is determined in
	// Writer.Close() as the end key of the last range key added to the block.
	if w.props.NumRangeKeys() == 0 {
		w.meta.SetSmallestRangeKey(key.Clone())
	}

	// Update block properties.
	w.props.RawRangeKeyKeySize += uint64(key.Size())
	w.props.RawRangeKeyValueSize += uint64(len(value))
	switch key.Kind() {
	case base.InternalKeyKindRangeKeyDelete:
		w.props.NumRangeKeyDels++
	case base.InternalKeyKindRangeKeySet:
		w.props.NumRangeKeySets++
	case base.InternalKeyKindRangeKeyUnset:
		w.props.NumRangeKeyUnsets++
	default:
		panic(errors.Errorf("pebble: invalid range key type: %s", key.Kind()))
	}

	for i := range w.blockPropCollectors {
		if err := w.blockPropCollectors[i].Add(key, value); err != nil {
			return err
		}
	}

	// Add the key to the block.
	w.rangeKeyBlock.add(key, value)
	return nil
}

// tempRangeKeyBuf returns a slice of length n from the Writer's rkBuf byte
// slice. Any byte written to the returned slice is retained for the lifetime of
// the Writer.
func (w *Writer) tempRangeKeyBuf(n int) []byte {
	if cap(w.rkBuf)-len(w.rkBuf) < n {
		size := len(w.rkBuf) + 2*n
		if size < 2*cap(w.rkBuf) {
			size = 2 * cap(w.rkBuf)
		}
		buf := make([]byte, len(w.rkBuf), size)
		copy(buf, w.rkBuf)
		w.rkBuf = buf
	}
	b := w.rkBuf[len(w.rkBuf) : len(w.rkBuf)+n]
	w.rkBuf = w.rkBuf[:len(w.rkBuf)+n]
	return b
}

// tempRangeKeyCopy returns a copy of the provided slice, stored in the Writer's
// range key buffer.
func (w *Writer) tempRangeKeyCopy(k []byte) []byte {
	if len(k) == 0 {
		return nil
	}
	buf := w.tempRangeKeyBuf(len(k))
	copy(buf, k)
	return buf
}

func (w *Writer) maybeAddToFilter(key []byte) {
	if w.filter != nil {
		if w.split != nil {
			prefix := key[:w.split(key)]
			w.filter.addKey(prefix)
		} else {
			w.filter.addKey(key)
		}
	}
}

func (w *Writer) flush(key InternalKey) error {
	estimatedUncompressedSize := w.dataBlockBuf.dataBlock.estimatedSize()
	w.coordination.sizeEstimate.addInflightDataBlock(estimatedUncompressedSize)

	var err error

	// We're finishing a data block.
	err = w.finishDataBlockProps(w.dataBlockBuf)
	if err != nil {
		return err
	}

	w.dataBlockBuf.finish()
	w.dataBlockBuf.compressAndChecksum(w.compression)

	// Determine if the index block should be flushed. Since we're accessing the
	// dataBlockBuf.dataBlock.curKey here, we have to make sure that once we start
	// to pool the dataBlockBufs, the curKey isn't used by the Writer once the
	// dataBlockBuf is added back to a sync.Pool. In this particular case, the
	// byte slice which supports "sep" will eventually be copied when "sep" is
	// added to the index block.
	prevKey := base.DecodeInternalKey(w.dataBlockBuf.dataBlock.curKey)
	sep := w.indexEntrySep(prevKey, key, w.dataBlockBuf)
	// We determine that we should flush an index block from the Writer client
	// goroutine, but we actually finish the index block from the writeQueue.
	// When we determine that an index block should be flushed, we need to call
	// BlockPropertyCollector.FinishIndexBlock. But block property collector
	// calls must happen sequentially from the Writer client. Therefore, we need
	// to determine that we are going to flush the index block from the Writer
	// client.
	shouldFlushIndexBlock := supportsTwoLevelIndex(w.tableFormat) && w.indexBlock.shouldFlush(
		sep, encodedBHPEstimatedSize, w.indexBlockSize, w.indexBlockSizeThreshold,
	)

	var indexProps []byte
	var flushableIndexBlock *indexBlockBuf
	if shouldFlushIndexBlock {
		flushableIndexBlock = w.indexBlock
		w.indexBlock = newIndexBlockBuf(w.coordination.parallelismEnabled)
		// Call BlockPropertyCollector.FinishIndexBlock, since we've decided to
		// flush the index block.
		indexProps, err = w.finishIndexBlockProps()
		if err != nil {
			return err
		}
	}

	// We've called BlockPropertyCollector.FinishDataBlock, and, if necessary,
	// BlockPropertyCollector.FinishIndexBlock. Since we've decided to finish
	// the data block, we can call
	// BlockPropertyCollector.AddPrevDataBlockToIndexBlock.
	w.addPrevDataBlockToIndexBlockProps()

	// Schedule a write.
	writeTask := writeTaskPool.Get().(*writeTask)
	// We're setting compressionDone to indicate that compression of this block
	// has already been completed.
	writeTask.compressionDone <- true
	writeTask.buf = w.dataBlockBuf
	writeTask.indexEntrySep = sep
	writeTask.inflightSize = estimatedUncompressedSize
	writeTask.currIndexBlock = w.indexBlock
	writeTask.indexInflightSize = sep.Size() + encodedBHPEstimatedSize
	writeTask.finishedIndexProps = indexProps
	writeTask.flushableIndexBlock = flushableIndexBlock

	// The writeTask corresponds to an unwritten index entry.
	w.indexBlock.addInflight(writeTask.indexInflightSize)

	w.dataBlockBuf = nil
	if w.coordination.parallelismEnabled {
		w.coordination.writeQueue.add(writeTask)
	} else {
		err = w.coordination.writeQueue.addSync(writeTask)
	}
	w.dataBlockBuf = newDataBlockBuf(w.restartInterval, w.checksumType)

	return err
}

func (w *Writer) maybeFlush(key InternalKey, value []byte) error {
	if !w.dataBlockBuf.shouldFlush(key, len(value), w.blockSize, w.blockSizeThreshold) {
		return nil
	}

	err := w.flush(key)

	if err != nil {
		w.err = err
		return err
	}

	return nil
}

// dataBlockBuf.dataBlockProps set by this method must be encoded before any future use of the
// dataBlockBuf.blockPropsEncoder, since the properties slice will get reused by the
// blockPropsEncoder.
func (w *Writer) finishDataBlockProps(buf *dataBlockBuf) error {
	if len(w.blockPropCollectors) == 0 {
		return nil
	}
	var err error
	buf.blockPropsEncoder.resetProps()
	for i := range w.blockPropCollectors {
		scratch := buf.blockPropsEncoder.getScratchForProp()
		if scratch, err = w.blockPropCollectors[i].FinishDataBlock(scratch); err != nil {
			return err
		}
		if len(scratch) > 0 {
			buf.blockPropsEncoder.addProp(shortID(i), scratch)
		}
	}

	buf.dataBlockProps = buf.blockPropsEncoder.unsafeProps()
	return nil
}

// The BlockHandleWithProperties returned by this method must be encoded before any future use of
// the Writer.blockPropsEncoder, since the properties slice will get reused by the blockPropsEncoder.
// maybeAddBlockPropertiesToBlockHandle should only be called if block is being written synchronously
// with the Writer client.
func (w *Writer) maybeAddBlockPropertiesToBlockHandle(
	bh BlockHandle,
) (BlockHandleWithProperties, error) {
	err := w.finishDataBlockProps(w.dataBlockBuf)
	if err != nil {
		return BlockHandleWithProperties{}, err
	}
	return BlockHandleWithProperties{BlockHandle: bh, Props: w.dataBlockBuf.dataBlockProps}, nil
}

func (w *Writer) indexEntrySep(prevKey, key InternalKey, dataBlockBuf *dataBlockBuf) InternalKey {
	// Make a rough guess that we want key-sized scratch to compute the separator.
	if cap(dataBlockBuf.sepScratch) < key.Size() {
		dataBlockBuf.sepScratch = make([]byte, 0, key.Size()*2)
	}

	var sep InternalKey
	if key.UserKey == nil && key.Trailer == 0 {
		sep = prevKey.Successor(w.compare, w.successor, dataBlockBuf.sepScratch[:0])
	} else {
		sep = prevKey.Separator(w.compare, w.separator, dataBlockBuf.sepScratch[:0], key)
	}
	return sep
}

// addIndexEntry adds an index entry for the specified key and block handle.
// addIndexEntry can be called from both the Writer client goroutine, and the
// writeQueue goroutine. If the flushIndexBuf != nil, then the indexProps, as
// they're used when the index block is finished.
//
// Invariant:
// 1. addIndexEntry must not store references to the sep InternalKey, the tmp
//    byte slice, bhp.Props. That is, these must be either deep copied or
//    encoded.
// 2. addIndexEntry must not hold references to the flushIndexBuf, and the writeTo
//    indexBlockBufs.
func (w *Writer) addIndexEntry(
	sep InternalKey,
	bhp BlockHandleWithProperties,
	tmp []byte,
	flushIndexBuf *indexBlockBuf,
	writeTo *indexBlockBuf,
	inflightSize int,
	indexProps []byte,
) error {
	if bhp.Length == 0 {
		// A valid blockHandle must be non-zero.
		// In particular, it must have a non-zero length.
		return nil
	}

	encoded := encodeBlockHandleWithProperties(tmp, bhp)

	if flushIndexBuf != nil {
		if cap(w.indexPartitions) == 0 {
			w.indexPartitions = make([]indexBlockAndBlockProperties, 0, 32)
		}
		// Enable two level indexes if there is more than one index block.
		w.twoLevelIndex = true
		if err := w.finishIndexBlock(flushIndexBuf, indexProps); err != nil {
			return err
		}
	}

	writeTo.add(sep, encoded, inflightSize)
	return nil
}

func (w *Writer) addPrevDataBlockToIndexBlockProps() {
	for i := range w.blockPropCollectors {
		w.blockPropCollectors[i].AddPrevDataBlockToIndexBlock()
	}
}

// addIndexEntrySync adds an index entry for the specified key and block handle.
// Writer.addIndexEntry is only called synchronously once Writer.Close is called.
// addIndexEntrySync should only be called if we're sure that index entries
// aren't being written asynchronously.
//
// Invariant:
// 1. addIndexEntrySync must not store references to the prevKey, key InternalKey's,
//    the tmp byte slice. That is, these must be either deep copied or encoded.
func (w *Writer) addIndexEntrySync(
	prevKey, key InternalKey, bhp BlockHandleWithProperties, tmp []byte,
) error {
	sep := w.indexEntrySep(prevKey, key, w.dataBlockBuf)
	shouldFlush := supportsTwoLevelIndex(
		w.tableFormat) && w.indexBlock.shouldFlush(
		sep, encodedBHPEstimatedSize, w.indexBlockSize, w.indexBlockSizeThreshold,
	)
	var flushableIndexBlock *indexBlockBuf
	var props []byte
	var err error
	if shouldFlush {
		flushableIndexBlock = w.indexBlock
		w.indexBlock = newIndexBlockBuf(w.coordination.parallelismEnabled)

		// Call BlockPropertyCollector.FinishIndexBlock, since we've decided to
		// flush the index block.
		props, err = w.finishIndexBlockProps()
		if err != nil {
			return err
		}
	}

	err = w.addIndexEntry(sep, bhp, tmp, flushableIndexBlock, w.indexBlock, 0, props)
	if flushableIndexBlock != nil {
		flushableIndexBlock.clear()
		indexBlockBufPool.Put(flushableIndexBlock)
	}
	w.addPrevDataBlockToIndexBlockProps()
	return err
}

func shouldFlush(
	key InternalKey,
	valueLen int,
	restartInterval, estimatedBlockSize, numEntries, targetBlockSize, sizeThreshold int,
) bool {
	if numEntries == 0 {
		return false
	}

	if estimatedBlockSize >= targetBlockSize {
		return true
	}

	// The block is currently smaller than the target size.
	if estimatedBlockSize <= sizeThreshold {
		// The block is smaller than the threshold size at which we'll consider
		// flushing it.
		return false
	}

	newSize := estimatedBlockSize + key.Size() + valueLen
	if numEntries%restartInterval == 0 {
		newSize += 4
	}
	newSize += 4                              // varint for shared prefix length
	newSize += uvarintLen(uint32(key.Size())) // varint for unshared key bytes
	newSize += uvarintLen(uint32(valueLen))   // varint for value size
	// Flush if the block plus the new entry is larger than the target size.
	return newSize > targetBlockSize
}

const keyAllocSize = 256 << 10

func cloneKeyWithBuf(k InternalKey, buf []byte) ([]byte, InternalKey) {
	if len(k.UserKey) == 0 {
		return buf, k
	}
	if len(buf) < len(k.UserKey) {
		buf = make([]byte, len(k.UserKey)+keyAllocSize)
	}
	n := copy(buf, k.UserKey)
	return buf[n:], InternalKey{UserKey: buf[:n:n], Trailer: k.Trailer}
}

// Invariants: The byte slice returned by finishIndexBlockProps is heap-allocated
//  and has its own lifetime, independent of the Writer and the blockPropsEncoder,
// and it is safe to:
// 1. Reuse w.blockPropsEncoder without first encoding the byte slice returned.
// 2. Store the byte slice in the Writer since it is a copy and not supported by
//    an underlying buffer.
func (w *Writer) finishIndexBlockProps() ([]byte, error) {
	w.blockPropsEncoder.resetProps()
	for i := range w.blockPropCollectors {
		scratch := w.blockPropsEncoder.getScratchForProp()
		var err error
		if scratch, err = w.blockPropCollectors[i].FinishIndexBlock(scratch); err != nil {
			return nil, err
		}
		if len(scratch) > 0 {
			w.blockPropsEncoder.addProp(shortID(i), scratch)
		}
	}
	return w.blockPropsEncoder.props(), nil
}

// finishIndexBlock finishes the current index block and adds it to the top
// level index block. This is only used when two level indexes are enabled.
//
// Invariants:
// 1. The props slice passed into finishedIndexBlock must not be a
//    owned by any other struct, since it will be stored in the Writer.indexPartitions
//    slice.
// 2. None of the buffers owned by indexBuf will be shallow copied and stored elsewhere.
//    That is, it must be safe to reuse indexBuf after finishIndexBlock has been called.
func (w *Writer) finishIndexBlock(indexBuf *indexBlockBuf, props []byte) error {
	part := indexBlockAndBlockProperties{
		nEntries: indexBuf.block.nEntries, properties: props,
	}
	w.indexSepAlloc, part.sep = cloneKeyWithBuf(
		base.DecodeInternalKey(indexBuf.block.curKey), w.indexSepAlloc,
	)
	bk := indexBuf.finish()
	if len(w.indexBlockAlloc) < len(bk) {
		// Allocate enough bytes for approximately 16 index blocks.
		w.indexBlockAlloc = make([]byte, len(bk)*16)
	}
	n := copy(w.indexBlockAlloc, bk)
	part.block = w.indexBlockAlloc[:n:n]
	w.indexBlockAlloc = w.indexBlockAlloc[n:]
	w.indexPartitions = append(w.indexPartitions, part)
	return nil
}

func (w *Writer) writeTwoLevelIndex() (BlockHandle, error) {
	props, err := w.finishIndexBlockProps()
	if err != nil {
		return BlockHandle{}, err
	}
	// Add the final unfinished index.
	if err = w.finishIndexBlock(w.indexBlock, props); err != nil {
		return BlockHandle{}, err
	}

	for i := range w.indexPartitions {
		b := &w.indexPartitions[i]
		w.props.NumDataBlocks += uint64(b.nEntries)

		data := b.block
		w.props.IndexSize += uint64(len(data))
		bh, err := w.writeBlock(data, w.compression, &w.blockBuf)
		if err != nil {
			return BlockHandle{}, err
		}
		bhp := BlockHandleWithProperties{
			BlockHandle: bh,
			Props:       b.properties,
		}
		encoded := encodeBlockHandleWithProperties(w.blockBuf.tmp[:], bhp)
		w.topLevelIndexBlock.add(b.sep, encoded)
	}

	// NB: RocksDB includes the block trailer length in the index size
	// property, though it doesn't include the trailer in the top level
	// index size property.
	w.props.IndexPartitions = uint64(len(w.indexPartitions))
	w.props.TopLevelIndexSize = uint64(w.topLevelIndexBlock.estimatedSize())
	w.props.IndexSize += w.props.TopLevelIndexSize + blockTrailerLen

	return w.writeBlock(w.topLevelIndexBlock.finish(), w.compression, &w.blockBuf)
}

func compressAndChecksum(b []byte, compression Compression, blockBuf *blockBuf) []byte {
	// Compress the buffer, discarding the result if the improvement isn't at
	// least 12.5%.
	blockType, compressed := compressBlock(compression, b, blockBuf.compressedBuf)
	if blockType != noCompressionBlockType && cap(compressed) > cap(blockBuf.compressedBuf) {
		blockBuf.compressedBuf = compressed[:cap(compressed)]
	}
	if len(compressed) < len(b)-len(b)/8 {
		b = compressed
	} else {
		blockType = noCompressionBlockType
	}

	blockBuf.tmp[0] = byte(blockType)

	// Calculate the checksum.
	checksum := blockBuf.checksummer.checksum(b, blockBuf.tmp[:1])
	binary.LittleEndian.PutUint32(blockBuf.tmp[1:5], checksum)
	return b
}

func (w *Writer) writeCompressedBlock(block []byte, blockTrailerBuf []byte) (BlockHandle, error) {
	bh := BlockHandle{Offset: w.meta.Size, Length: uint64(len(block))}

	if w.cacheID != 0 && w.fileNum != 0 {
		// Remove the block being written from the cache. This provides defense in
		// depth against bugs which cause cache collisions.
		//
		// TODO(peter): Alternatively, we could add the uncompressed value to the
		// cache.
		w.cache.Delete(w.cacheID, w.fileNum, bh.Offset)
	}

	// Write the bytes to the file.
	n, err := w.writer.Write(block)
	if err != nil {
		return BlockHandle{}, err
	}
	w.meta.Size += uint64(n)
	n, err = w.writer.Write(blockTrailerBuf[:blockTrailerLen])
	if err != nil {
		return BlockHandle{}, err
	}
	w.meta.Size += uint64(n)

	return bh, nil
}

func (w *Writer) writeBlock(
	b []byte, compression Compression, blockBuf *blockBuf,
) (BlockHandle, error) {
	b = compressAndChecksum(b, compression, blockBuf)
	return w.writeCompressedBlock(b, blockBuf.tmp[:])
}

// assertFormatCompatibility ensures that the features present on the table are
// compatible with the table format version.
func (w *Writer) assertFormatCompatibility() error {
	// PebbleDBv1: block properties.
	if len(w.blockPropCollectors) > 0 && w.tableFormat < TableFormatPebblev1 {
		return errors.Newf(
			"table format version %s is less than the minimum required version %s for block properties",
			w.tableFormat, TableFormatPebblev1,
		)
	}

	// PebbleDBv2: range keys.
	if w.props.NumRangeKeys() > 0 && w.tableFormat < TableFormatPebblev2 {
		return errors.Newf(
			"table format version %s is less than the minimum required version %s for range keys",
			w.tableFormat, TableFormatPebblev2,
		)
	}

	return nil
}

// Close finishes writing the table and closes the underlying file that the
// table was written to.
func (w *Writer) Close() (err error) {
	defer func() {
		if w.syncer == nil {
			return
		}
		err1 := w.syncer.Close()
		if err == nil {
			err = err1
		}
		w.syncer = nil
	}()

	// finish must be called before we check for an error, because finish will
	// block until every single task added to the writeQueue has been processed,
	// and an error could be encountered while any of those tasks are processed.
	if err = w.coordination.writeQueue.finish(); err != nil {
		w.err = err
	}

	if w.err != nil {
		return w.err
	}

	// The w.meta.LargestPointKey is only used once the Writer is closed, so it is safe to set it
	// when the Writer is closed.
	//
	// The following invariants ensure that setting the largest key at this point of a Writer close
	// is correct:
	// 1. Keys must only be added to the Writer in an increasing order.
	// 2. The current w.dataBlockBuf is guaranteed to have the latest key added to the Writer. This
	//    must be true, because a w.dataBlockBuf is only switched out when a dataBlock is flushed,
	//    however, if a dataBlock is flushed, then we add a key to the new w.dataBlockBuf in the
	//    addPoint function after the flush occurs.
	if w.dataBlockBuf.dataBlock.nEntries >= 1 {
		w.meta.SetLargestPointKey(base.DecodeInternalKey(w.dataBlockBuf.dataBlock.curKey).Clone())
	}

	// Finish the last data block, or force an empty data block if there
	// aren't any data blocks at all.
	if w.dataBlockBuf.dataBlock.nEntries > 0 || w.indexBlock.block.nEntries == 0 {
		bh, err := w.writeBlock(w.dataBlockBuf.dataBlock.finish(), w.compression, &w.dataBlockBuf.blockBuf)
		if err != nil {
			w.err = err
			return w.err
		}
		var bhp BlockHandleWithProperties
		if bhp, err = w.maybeAddBlockPropertiesToBlockHandle(bh); err != nil {
			w.err = err
			return err
		}
		prevKey := base.DecodeInternalKey(w.dataBlockBuf.dataBlock.curKey)
		if err = w.addIndexEntrySync(prevKey, InternalKey{}, bhp, w.dataBlockBuf.tmp[:]); err != nil {
			w.err = err
			return err
		}
	}
	w.props.DataSize = w.meta.Size

	// Write the filter block.
	var metaindex rawBlockWriter
	metaindex.restartInterval = 1
	if w.filter != nil {
		b, err := w.filter.finish()
		if err != nil {
			w.err = err
			return w.err
		}
		bh, err := w.writeBlock(b, NoCompression, &w.blockBuf)
		if err != nil {
			w.err = err
			return w.err
		}
		n := encodeBlockHandle(w.blockBuf.tmp[:], bh)
		metaindex.add(InternalKey{UserKey: []byte(w.filter.metaName())}, w.blockBuf.tmp[:n])
		w.props.FilterPolicyName = w.filter.policyName()
		w.props.FilterSize = bh.Length
	}

	var indexBH BlockHandle
	if w.twoLevelIndex {
		w.props.IndexType = twoLevelIndex
		// Write the two level index block.
		indexBH, err = w.writeTwoLevelIndex()
		if err != nil {
			w.err = err
			return w.err
		}
	} else {
		w.props.IndexType = binarySearchIndex
		// NB: RocksDB includes the block trailer length in the index size
		// property, though it doesn't include the trailer in the filter size
		// property.
		w.props.IndexSize = uint64(w.indexBlock.estimatedSize()) + blockTrailerLen
		w.props.NumDataBlocks = uint64(w.indexBlock.block.nEntries)

		// Write the single level index block.
		indexBH, err = w.writeBlock(w.indexBlock.finish(), w.compression, &w.blockBuf)
		if err != nil {
			w.err = err
			return w.err
		}
	}

	// Write the range-del block. The block handle must added to the meta index block
	// after the properties block has been written. This is because the entries in the
	// metaindex block must be sorted by key.
	var rangeDelBH BlockHandle
	if w.props.NumRangeDeletions > 0 {
		if !w.rangeDelV1Format {
			// Because the range tombstones are fragmented in the v2 format, the end
			// key of the last added range tombstone will be the largest range
			// tombstone key. Note that we need to make this into a range deletion
			// sentinel because sstable boundaries are inclusive while the end key of
			// a range deletion tombstone is exclusive. A Clone() is necessary as
			// rangeDelBlock.curValue is the same slice that will get passed
			// into w.writer, and some implementations of vfs.File mutate the
			// slice passed into Write(). Also, w.meta will often outlive the
			// blockWriter, and so cloning curValue allows the rangeDelBlock's
			// internal buffer to get gc'd.
			k := base.MakeRangeDeleteSentinelKey(w.rangeDelBlock.curValue).Clone()
			w.meta.SetLargestRangeDelKey(k)
		}
		rangeDelBH, err = w.writeBlock(w.rangeDelBlock.finish(), NoCompression, &w.blockBuf)
		if err != nil {
			w.err = err
			return w.err
		}
	}

	// Write the range-key block, flushing any remaining spans from the
	// fragmenter first.
	w.fragmenter.Finish()

	var rangeKeyBH BlockHandle
	if w.props.NumRangeKeys() > 0 {
		key := base.DecodeInternalKey(w.rangeKeyBlock.curKey)
		kind := key.Kind()
		endKey, _, ok := rangekey.DecodeEndKey(kind, w.rangeKeyBlock.curValue)
		if !ok {
			w.err = errors.Newf("invalid end key: %s", w.rangeKeyBlock.curValue)
			return w.err
		}
		k := base.MakeExclusiveSentinelKey(kind, endKey).Clone()
		w.meta.SetLargestRangeKey(k)
		// TODO(travers): The lack of compression on the range key block matches the
		// lack of compression on the range-del block. Revisit whether we want to
		// enable compression on this block.
		rangeKeyBH, err = w.writeBlock(w.rangeKeyBlock.finish(), NoCompression, &w.blockBuf)
		if err != nil {
			w.err = err
			return w.err
		}
	}

	// Add the range key block handle to the metaindex block. Note that we add the
	// block handle to the metaindex block before the other meta blocks as the
	// metaindex block entries must be sorted, and the range key block name sorts
	// before the other block names.
	if w.props.NumRangeKeys() > 0 {
		n := encodeBlockHandle(w.blockBuf.tmp[:], rangeKeyBH)
		metaindex.add(InternalKey{UserKey: []byte(metaRangeKeyName)}, w.blockBuf.tmp[:n])
	}

	{
		userProps := make(map[string]string)
		for i := range w.propCollectors {
			if err := w.propCollectors[i].Finish(userProps); err != nil {
				w.err = err
				return err
			}
		}
		for i := range w.blockPropCollectors {
			scratch := w.blockPropsEncoder.getScratchForProp()
			// Place the shortID in the first byte.
			scratch = append(scratch, byte(i))
			buf, err :=
				w.blockPropCollectors[i].FinishTable(scratch)
			if err != nil {
				w.err = err
				return err
			}
			var prop string
			if len(buf) > 0 {
				prop = string(buf)
			}
			// NB: The property is populated in the map even if it is the
			// empty string, since the presence in the map is what indicates
			// that the block property collector was used when writing.
			userProps[w.blockPropCollectors[i].Name()] = prop
		}
		if len(userProps) > 0 {
			w.props.UserProperties = userProps
		}

		// Write the properties block.
		var raw rawBlockWriter
		// The restart interval is set to infinity because the properties block
		// is always read sequentially and cached in a heap located object. This
		// reduces table size without a significant impact on performance.
		raw.restartInterval = propertiesBlockRestartInterval
		w.props.CompressionOptions = rocksDBCompressionOptions
		w.props.save(&raw)
		bh, err := w.writeBlock(raw.finish(), NoCompression, &w.blockBuf)
		if err != nil {
			w.err = err
			return w.err
		}
		n := encodeBlockHandle(w.blockBuf.tmp[:], bh)
		metaindex.add(InternalKey{UserKey: []byte(metaPropertiesName)}, w.blockBuf.tmp[:n])
	}

	// Add the range deletion block handle to the metaindex block.
	if w.props.NumRangeDeletions > 0 {
		n := encodeBlockHandle(w.blockBuf.tmp[:], rangeDelBH)
		// The v2 range-del block encoding is backwards compatible with the v1
		// encoding. We add meta-index entries for both the old name and the new
		// name so that old code can continue to find the range-del block and new
		// code knows that the range tombstones in the block are fragmented and
		// sorted.
		metaindex.add(InternalKey{UserKey: []byte(metaRangeDelName)}, w.blockBuf.tmp[:n])
		if !w.rangeDelV1Format {
			metaindex.add(InternalKey{UserKey: []byte(metaRangeDelV2Name)}, w.blockBuf.tmp[:n])
		}
	}

	// Write the metaindex block. It might be an empty block, if the filter
	// policy is nil. NoCompression is specified because a) RocksDB never
	// compresses the meta-index block and b) RocksDB has some code paths which
	// expect the meta-index block to not be compressed.
	metaindexBH, err := w.writeBlock(metaindex.blockWriter.finish(), NoCompression, &w.blockBuf)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the table footer.
	footer := footer{
		format:      w.tableFormat,
		checksum:    w.blockBuf.checksummer.checksumType,
		metaindexBH: metaindexBH,
		indexBH:     indexBH,
	}
	var n int
	if n, err = w.writer.Write(footer.encode(w.blockBuf.tmp[:])); err != nil {
		w.err = err
		return w.err
	}
	w.meta.Size += uint64(n)
	w.meta.Properties = w.props

	// Flush the buffer.
	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			w.err = err
			return err
		}
	}

	// Check that the features present in the table are compatible with the format
	// configured for the table.
	if err = w.assertFormatCompatibility(); err != nil {
		w.err = err
		return w.err
	}

	if err := w.syncer.Sync(); err != nil {
		w.err = err
		return err
	}

	w.dataBlockBuf.clear()
	dataBlockBufPool.Put(w.dataBlockBuf)
	w.dataBlockBuf = nil
	w.indexBlock.clear()
	indexBlockBufPool.Put(w.indexBlock)
	w.indexBlock = nil

	// Make any future calls to Set or Close return an error.
	if w.err != nil {
		return w.err
	}
	w.err = errWriterClosed
	return nil
}

// EstimatedSize returns the estimated size of the sstable being written if a
// call to Finish() was made without adding additional keys.
func (w *Writer) EstimatedSize() uint64 {
	if invariants.Enabled && !w.coordination.parallelismEnabled {
		// The w.meta.Size should only be accessed from the writeQueue goroutine
		// if parallelism is enabled, but since it isn't we break that invariant
		// here.
		if w.coordination.sizeEstimate.size() != w.meta.Size {
			panic("sstable size estimation sans parallelism is incorrect")
		}
	}
	return w.coordination.sizeEstimate.size() +
		uint64(w.dataBlockBuf.dataBlock.estimatedSize()) +
		w.indexBlock.estimatedSize()
}

// Metadata returns the metadata for the finished sstable. Only valid to call
// after the sstable has been finished.
func (w *Writer) Metadata() (*WriterMetadata, error) {
	if w.syncer != nil {
		return nil, errors.New("pebble: writer is not closed")
	}
	return &w.meta, nil
}

// WriterOption provide an interface to do work on Writer while it is being
// opened.
type WriterOption interface {
	// writerApply is called on the writer during opening in order to set
	// internal parameters.
	writerApply(*Writer)
}

// PreviousPointKeyOpt is a WriterOption that provides access to the last
// point key written to the writer while building a sstable.
type PreviousPointKeyOpt struct {
	w *Writer
}

// UnsafeKey returns the last point key written to the writer to which this
// option was passed during creation. The returned key points directly into
// a buffer belonging to the Writer. The value's lifetime ends the next time a
// point key is added to the Writer.
// Invariant: UnsafeKey isn't and shouldn't be called after the Writer is closed.
func (o PreviousPointKeyOpt) UnsafeKey() base.InternalKey {
	if o.w == nil {
		return base.InvalidInternalKey
	}

	if o.w.dataBlockBuf.dataBlock.nEntries >= 1 {
		// o.w.dataBlockBuf.dataBlock.curKey is guaranteed to point to the last point key
		// which was added to the Writer.
		return base.DecodeInternalKey(o.w.dataBlockBuf.dataBlock.curKey)
	}
	return base.InternalKey{}
}

func (o *PreviousPointKeyOpt) writerApply(w *Writer) {
	o.w = w
}

// internalTableOpt is a WriterOption that sets properties for sstables being
// created by the db itself (i.e. through flushes and compactions), as opposed
// to those meant for ingestion.
type internalTableOpt struct{}

func (i internalTableOpt) writerApply(w *Writer) {
	// Set the external sst version to 0. This is what RocksDB expects for
	// db-internal sstables; otherwise, it could apply a global sequence number.
	w.props.ExternalFormatVersion = 0
}

// NewWriter returns a new table writer for the file. Closing the writer will
// close the file.
func NewWriter(f writeCloseSyncer, o WriterOptions, extraOpts ...WriterOption) *Writer {
	o = o.ensureDefaults()
	w := &Writer{
		syncer: f,
		meta: WriterMetadata{
			SmallestSeqNum: math.MaxUint64,
		},
		blockSize:               o.BlockSize,
		blockSizeThreshold:      (o.BlockSize*o.BlockSizeThreshold + 99) / 100,
		indexBlockSize:          o.IndexBlockSize,
		indexBlockSizeThreshold: (o.IndexBlockSize*o.BlockSizeThreshold + 99) / 100,
		compare:                 o.Comparer.Compare,
		split:                   o.Comparer.Split,
		formatKey:               o.Comparer.FormatKey,
		compression:             o.Compression,
		separator:               o.Comparer.Separator,
		successor:               o.Comparer.Successor,
		tableFormat:             o.TableFormat,
		cache:                   o.Cache,
		restartInterval:         o.BlockRestartInterval,
		checksumType:            o.Checksum,
		indexBlock:              newIndexBlockBuf(o.Parallelism),
		rangeDelBlock: blockWriter{
			restartInterval: 1,
		},
		rangeKeyBlock: blockWriter{
			restartInterval: 1,
		},
		topLevelIndexBlock: blockWriter{
			restartInterval: 1,
		},
		fragmenter: keyspan.Fragmenter{
			Cmp:    o.Comparer.Compare,
			Format: o.Comparer.FormatKey,
		},
	}

	w.dataBlockBuf = newDataBlockBuf(w.restartInterval, w.checksumType)

	w.blockBuf = blockBuf{
		checksummer: checksummer{checksumType: o.Checksum},
	}

	w.coordination.init(o.Parallelism, w)

	if f == nil {
		w.err = errors.New("pebble: nil file")
		return w
	}

	// Note that WriterOptions are applied in two places; the ones with a
	// preApply() method are applied here, and the rest are applied after
	// default properties are set.
	type preApply interface{ preApply() }
	for _, opt := range extraOpts {
		if _, ok := opt.(preApply); ok {
			opt.writerApply(w)
		}
	}

	w.props.PrefixExtractorName = "nullptr"
	if o.FilterPolicy != nil {
		switch o.FilterType {
		case TableFilter:
			w.filter = newTableFilterWriter(o.FilterPolicy)
			if w.split != nil {
				w.props.PrefixExtractorName = o.Comparer.Name
				w.props.PrefixFiltering = true
			} else {
				w.props.WholeKeyFiltering = true
			}
		default:
			panic(fmt.Sprintf("unknown filter type: %v", o.FilterType))
		}
	}

	w.props.ColumnFamilyID = math.MaxInt32
	w.props.ComparerName = o.Comparer.Name
	w.props.CompressionName = o.Compression.String()
	w.props.MergerName = o.MergerName
	w.props.PropertyCollectorNames = "[]"
	w.props.ExternalFormatVersion = rocksDBExternalFormatVersion

	if len(o.TablePropertyCollectors) > 0 || len(o.BlockPropertyCollectors) > 0 {
		var buf bytes.Buffer
		buf.WriteString("[")
		if len(o.TablePropertyCollectors) > 0 {
			w.propCollectors = make([]TablePropertyCollector, len(o.TablePropertyCollectors))
			for i := range o.TablePropertyCollectors {
				w.propCollectors[i] = o.TablePropertyCollectors[i]()
				if i > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(w.propCollectors[i].Name())
			}
		}
		if len(o.BlockPropertyCollectors) > 0 {
			// shortID is a uint8, so we cannot exceed that number of block
			// property collectors.
			if len(o.BlockPropertyCollectors) > math.MaxUint8 {
				w.err = errors.New("pebble: too many block property collectors")
				return w
			}
			// The shortID assigned to a collector is the same as its index in
			// this slice.
			w.blockPropCollectors = make([]BlockPropertyCollector, len(o.BlockPropertyCollectors))
			for i := range o.BlockPropertyCollectors {
				w.blockPropCollectors[i] = o.BlockPropertyCollectors[i]()
				if i > 0 || len(o.TablePropertyCollectors) > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(w.blockPropCollectors[i].Name())
			}
		}
		buf.WriteString("]")
		w.props.PropertyCollectorNames = buf.String()
	}

	// Apply the remaining WriterOptions that do not have a preApply() method.
	for _, opt := range extraOpts {
		if _, ok := opt.(preApply); !ok {
			opt.writerApply(w)
		}
	}

	// Initialize the range key fragmenter and encoder.
	w.fragmenter.Emit = w.coalesceSpans
	w.rangeKeyEncoder.Emit = w.addRangeKey

	// If f does not have a Flush method, do our own buffering.
	if _, ok := f.(flusher); ok {
		w.writer = f
	} else {
		w.bufWriter = bufio.NewWriter(f)
		w.writer = w.bufWriter
	}
	return w
}

func init() {
	private.SSTableWriterDisableKeyOrderChecks = func(i interface{}) {
		w := i.(*Writer)
		w.disableKeyOrderChecks = true
	}
	private.SSTableInternalTableOpt = internalTableOpt{}
}
