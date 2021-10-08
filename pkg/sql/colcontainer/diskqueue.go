// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colcontainer

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
)

const (
	// compressionSizeReductionThreshold is the factor used to determine whether
	// to write compressed bytes or not. If the compressed bytes are larger than
	// 1-1/compressionSizeReductionThreshold of the original size, compression is
	// not used. This is to avoid paying the cost of decompression if the space
	// savings are not sufficient.
	compressionSizeReductionThreshold = 8
	// bytesPerSync is the amount of bytes written to a file before Sync is
	// called (implemented by using a vfs.SyncingFile).
	bytesPerSync = 512 << 10 /* 512 KiB */
)

// file represents in-memory state used by a diskQueue to keep track of the
// state of a file.
type file struct {
	name string
	// offsets represent the start and ends of logical regions of a file to be
	// read at once. This allows a region of coldata.Batches to be deserialized
	// without reading a whole file into memory.
	offsets []int
	// curOffsetIdx is an index into offsets.
	curOffsetIdx int
	totalSize    int
	// finishedWriting specifies whether this file will be written to in the
	// future or not. If finishedWriting is true and the reader reaches the end
	// of the file, the file represented by this struct should be closed and
	// (if the disk queue is not rewindable) removed.
	finishedWriting bool
}

// diskQueueWriter is an object that encapsulates the writing logic of a
// diskQueue. As bytes are written to it, they are buffered until
// compressAndFlush is called, which compresses all bytes and writes them to the
// wrapped io.Writer.
type diskQueueWriter struct {
	// testingKnobAlwaysCompress specifies whether the writer should always
	// compress writes (i.e. don't bother measuring whether compression passes
	// a certain threshold of size improvement before writing compressed bytes).
	testingKnobAlwaysCompress bool
	buffer                    bytes.Buffer
	wrapped                   io.Writer
	scratch                   struct {
		// blockType is a single byte that specifies whether the following block on
		// disk (i.e. compressedBuf in memory) is compressed or not. It is an array
		// due to having to pass this byte in as a slice to Write.
		blockType     [1]byte
		compressedBuf []byte
	}
}

const (
	snappyUncompressedBlock byte = 0
	snappyCompressedBlock   byte = 1
)

func (w *diskQueueWriter) Write(p []byte) (int, error) {
	return w.buffer.Write(p)
}

// reset resets the diskQueueWriter's wrapped writer and discards any buffered
// bytes.
func (w *diskQueueWriter) reset(wrapped io.Writer) {
	w.wrapped = wrapped
	w.buffer.Reset()
}

// compressAndFlush compresses all buffered bytes and writes them to the wrapped
// io.Writer. The number of total bytes written to the wrapped writer is
// returned if no error occurred, otherwise 0, err is returned.
func (w *diskQueueWriter) compressAndFlush() (int, error) {
	b := w.buffer.Bytes()
	compressed := snappy.Encode(w.scratch.compressedBuf, b)
	w.scratch.compressedBuf = compressed[:cap(compressed)]

	blockType := snappyUncompressedBlock
	// Discard result if < 12.5% size reduction. All code that uses snappy
	// compression (including pebble and the higher-level snappy implementation)
	// has this threshold in place.
	if w.testingKnobAlwaysCompress || len(compressed) < len(b)-len(b)/compressionSizeReductionThreshold {
		blockType = snappyCompressedBlock
		b = compressed
	}

	// Write whether this data is compressed or not.
	w.scratch.blockType[0] = blockType
	nType, err := w.wrapped.Write(w.scratch.blockType[:])
	if err != nil {
		return 0, err
	}

	nBody, err := w.wrapped.Write(b)
	if err != nil {
		return 0, err
	}
	w.buffer.Reset()
	return nType + nBody, err
}

func (w *diskQueueWriter) numBytesBuffered() int {
	return w.buffer.Len()
}

// diskQueueState describes the current state of the disk queue. Used to assert
// that an invalid state transition doesn't happen when a DiskQueue is in
// DiskQueueCacheMode{ClearAnd}ReuseCache.
type diskQueueState int

const (
	diskQueueStateEnqueueing diskQueueState = iota
	diskQueueStateDequeueing
)

// diskQueue is an on-disk queue of coldata.Batches that implements the Queue
// interface. coldata.Batches are serialized and buffered up, after which they
// are compressed and flushed to a file. A directory with a random UUID name
// will be created in cfg.Path, and files will be created in that directory
// using sequence numbers.
// When a file reaches DiskQueueCfg.MaxFileSizeBytes, a new file is created with
// the next sequential file number to store the next batches in the queue.
// Note that, if diskQueue is not rewindable, files will be cleaned up as
// coldata.Batches are dequeued from the diskQueue. DiskQueueCfg.Dir will also
// be removed on Close, deleting all files.
// A diskQueue will never use more memory than cfg.BufferSizeBytes, but not all
// the available memory will be used to buffer only writes. Refer to the
// DiskQueueCacheMode comment as to how cfg.BufferSizeBytes is divided in each
// mode.
type diskQueue struct {
	// dirName is the directory in cfg.Path that holds this queue's files.
	dirName string

	typs  []*types.T
	cfg   DiskQueueCfg
	files []file
	seqNo int

	state      diskQueueState
	rewindable bool

	// done is set when a coldata.ZeroBatch has been Enqueued.
	done bool

	serializer *colserde.FileSerializer
	// numBufferedBatches is the number of batches buffered that haven't been
	// flushed to disk. This is useful for a reader to determine whether to flush
	// or not, since the number of buffered bytes will always be > 0 even though
	// no batches have been enqueued (due to metadata).
	numBufferedBatches int
	writer             *diskQueueWriter
	// writeBufferLimit is the limit on the number of uncompressed write bytes
	// written before a compress and flush.
	writeBufferLimit  int
	writeFileIdx      int
	writeFile         fs.File
	deserializerState struct {
		*colserde.FileDeserializer
		curBatch int
	}
	// readFileIdx is an index into the current file in files the deserializer is
	// reading from.
	readFileIdx                  int
	readFile                     fs.File
	scratchDecompressedReadBytes []byte

	diskAcc *mon.BoundAccount

	// diskSpillingMetricsHelper keeps track of various disk spilling metrics.
	diskSpillingMetricsHelper struct {
		mu struct {
			syncutil.Mutex
			querySpilled bool
		}
	}
}

var _ RewindableQueue = &diskQueue{}

// Queue describes a simple queue interface to which coldata.Batches can be
// Enqueued and Dequeued.
type Queue interface {
	// Enqueue enqueues a coldata.Batch to this queue. A zero-length batch should
	// be enqueued when no more elements will be enqueued.
	// WARNING: Selection vectors are ignored.
	Enqueue(context.Context, coldata.Batch) error
	// Dequeue dequeues a coldata.Batch from the queue into the batch that is
	// passed in. The boolean returned specifies whether the queue was not empty
	// (i.e. whether there was a batch to Dequeue). If true is returned and the
	// batch has a length of zero, the Queue is finished and will not be Enqueued
	// to. If an error is returned, the batch and boolean returned are
	// meaningless.
	Dequeue(context.Context, coldata.Batch) (bool, error)
	// CloseRead closes the read file descriptor. If Dequeued, the file may be
	// reopened.
	CloseRead() error
	// Close closes any resources associated with the Queue.
	Close(context.Context) error
}

// RewindableQueue is a Queue that can be read from multiple times. Note that
// in order for this Queue to return the same data after rewinding, all
// Enqueueing *must* occur before any Dequeueing.
type RewindableQueue interface {
	Queue
	// Rewind resets the Queue so that it Dequeues all Enqueued batches from the
	// start.
	Rewind() error
}

const (
	// defaultBufferSizeBytesDefaultCacheMode is the default buffer size used when
	// the DiskQueue is in DiskQueueCacheModeDefault.
	// This value was chosen by running BenchmarkQueue.
	defaultBufferSizeBytesDefaultCacheMode = 128 << 10 /* 128 KiB */
	// defaultBufferSizeBytesReuseCacheMode is the default buffer size used when
	// the DiskQueue is in DiskQueueCacheMode{ClearAnd}ReuseCache.
	defaultBufferSizeBytesReuseCacheMode = 64 << 10 /* 64 KiB */
	// defaultMaxFileSizeBytes is the default maximum file size after which the
	// DiskQueue rolls over to a new file. This value was chosen by running
	// BenchmarkQueue.
	defaultMaxFileSizeBytes = 32 << 20 /* 32 MiB */
)

// DiskQueueCacheMode specifies a pattern that a DiskQueue should use regarding
// its cache.
type DiskQueueCacheMode int

const (
	// DiskQueueCacheModeDefault is the default mode for DiskQueue cache behavior.
	// The cache (DiskQueueCfg.BufferSizeBytes) will be divided as follows:
	// - 1/3 for buffered writes (before compression)
	// - 1/3 for compressed writes, this is distinct from the previous 1/3 because
	//   it is a requirement of the snappy library that the compressed memory may
	//   not overlap with the uncompressed memory. This memory is reused to read
	//   compressed bytes from disk.
	// - 1/3 for buffered reads after decompression. Kept separate from the write
	//   memory to allow for Enqueues to come in while unread batches are held in
	//   memory.
	// In this mode, Enqueues and Dequeues may happen in any order.
	DiskQueueCacheModeDefault DiskQueueCacheMode = iota
	// DiskQueueCacheModeReuseCache imposes a limitation that all Enqueues happen
	// before all Dequeues to be able to reuse more memory. In this mode the cache
	// will be divided as follows:
	// - 1/2 for buffered writes and buffered reads.
	// - 1/2 for compressed write and reads (given the limitation that this memory
	//   has to be non-overlapping.
	DiskQueueCacheModeReuseCache
	// DiskQueueCacheModeClearAndReuseCache is the same as
	// DiskQueueCacheModeReuseCache with the additional behavior that when a
	// coldata.ZeroBatch is Enqueued, the cache will be released to the GC.
	DiskQueueCacheModeClearAndReuseCache
)

// GetPather is an object that has a temporary directory.
type GetPather interface {
	// GetPath returns where this object's temporary directory is.
	// Note that the directory is created lazily on the first call to GetPath.
	GetPath(context.Context) string
}

type getPatherFunc struct {
	f func(ctx context.Context) string
}

func (f getPatherFunc) GetPath(ctx context.Context) string {
	return f.f(ctx)
}

// GetPatherFunc returns a GetPather initialized from a closure.
func GetPatherFunc(f func(ctx context.Context) string) GetPather {
	return getPatherFunc{
		f: f,
	}
}

// DiskQueueCfg is a struct holding the configuration options for a DiskQueue.
type DiskQueueCfg struct {
	// FS is the filesystem interface to use.
	FS fs.FS
	// DistSQLMetrics contains metrics for monitoring DistSQL processing. This
	// can be nil if these metrics are not needed.
	DistSQLMetrics *execinfra.DistSQLMetrics
	// GetPather returns where the temporary directory that will contain this
	// DiskQueue's files has been created. The directory name will be a UUID.
	// Note that the directory is created lazily on the first call to GetPath.
	GetPather GetPather
	// CacheMode defines the way a DiskQueue should use its cache. Refer to the
	// comment of DiskQueueCacheModes for more information.
	CacheMode DiskQueueCacheMode
	// BufferSizeBytes is the number of bytes to buffer before compressing and
	// writing to disk.
	BufferSizeBytes int
	// MaxFileSizeBytes is the maximum size an on-disk file should reach before
	// rolling over to a new one.
	MaxFileSizeBytes int

	// TestingKnobs are used to test the queue implementation.
	TestingKnobs struct {
		// AlwaysCompress, if true, will skip a check that determines whether
		// compression is used for a given write or not given the percentage size
		// improvement. This allows us to test compression.
		AlwaysCompress bool
	}
}

// EnsureDefaults ensures that optional fields are set to reasonable defaults.
// If any necessary options have been elided, an error is returned.
func (cfg *DiskQueueCfg) EnsureDefaults() error {
	if cfg.FS == nil {
		return errors.New("FS unset on DiskQueueCfg")
	}
	if cfg.BufferSizeBytes == 0 {
		cfg.SetDefaultBufferSizeBytesForCacheMode()
	}
	if cfg.MaxFileSizeBytes == 0 {
		cfg.MaxFileSizeBytes = defaultMaxFileSizeBytes
	}
	return nil
}

// SetDefaultBufferSizeBytesForCacheMode sets the default BufferSizeBytes
// according to the set CacheMode.
func (cfg *DiskQueueCfg) SetDefaultBufferSizeBytesForCacheMode() {
	if cfg.CacheMode == DiskQueueCacheModeDefault {
		cfg.BufferSizeBytes = defaultBufferSizeBytesDefaultCacheMode
	} else {
		cfg.BufferSizeBytes = defaultBufferSizeBytesReuseCacheMode
	}
}

// NewDiskQueue creates a Queue that spills to disk.
func NewDiskQueue(
	ctx context.Context, typs []*types.T, cfg DiskQueueCfg, diskAcc *mon.BoundAccount,
) (Queue, error) {
	return newDiskQueue(ctx, typs, cfg, diskAcc)
}

// NewRewindableDiskQueue creates a RewindableQueue that spills to disk.
func NewRewindableDiskQueue(
	ctx context.Context, typs []*types.T, cfg DiskQueueCfg, diskAcc *mon.BoundAccount,
) (RewindableQueue, error) {
	d, err := newDiskQueue(ctx, typs, cfg, diskAcc)
	if err != nil {
		return nil, err
	}
	d.rewindable = true
	return d, nil
}

func (d *diskQueue) querySpilled() {
	d.diskSpillingMetricsHelper.mu.Lock()
	defer d.diskSpillingMetricsHelper.mu.Unlock()
	if d.cfg.DistSQLMetrics != nil && !d.diskSpillingMetricsHelper.mu.querySpilled {
		d.diskSpillingMetricsHelper.mu.querySpilled = true
		d.cfg.DistSQLMetrics.QueriesSpilled.Inc(1)
	}
}

func newDiskQueue(
	ctx context.Context, typs []*types.T, cfg DiskQueueCfg, diskAcc *mon.BoundAccount,
) (*diskQueue, error) {
	if err := cfg.EnsureDefaults(); err != nil {
		return nil, err
	}
	d := &diskQueue{
		dirName:          uuid.FastMakeV4().String(),
		typs:             typs,
		cfg:              cfg,
		files:            make([]file, 0, 4),
		writeBufferLimit: cfg.BufferSizeBytes / 3,
		diskAcc:          diskAcc,
	}
	// Refer to the DiskQueueCacheMode comment for why this division of
	// BufferSizeBytes.
	if d.cfg.CacheMode != DiskQueueCacheModeDefault {
		d.writeBufferLimit = d.cfg.BufferSizeBytes / 2
	}
	if err := cfg.FS.MkdirAll(filepath.Join(cfg.GetPather.GetPath(ctx), d.dirName)); err != nil {
		return nil, err
	}
	d.querySpilled()
	// rotateFile will create a new file to write to.
	return d, d.rotateFile(ctx)
}

func (d *diskQueue) CloseRead() error {
	if d.readFile != nil {
		if err := d.readFile.Close(); err != nil {
			return err
		}
		d.readFile = nil
	}
	return nil
}

func (d *diskQueue) closeFileDeserializer() error {
	if d.deserializerState.FileDeserializer != nil {
		if err := d.deserializerState.Close(); err != nil {
			return err
		}
	}
	d.deserializerState.FileDeserializer = nil
	return nil
}

func (d *diskQueue) Close(ctx context.Context) error {
	defer func() {
		// Zero out the structure completely upon return. If users of this diskQueue
		// retain a pointer to it, and we don't remove all references to large
		// backing slices (various scratch spaces in this struct and children),
		// we'll be "leaking memory" until users remove their references.
		*d = diskQueue{}
	}()
	if d.serializer != nil {
		if err := d.writeFooterAndFlush(ctx); err != nil {
			return err
		}
		d.serializer = nil
	}
	if err := d.closeFileDeserializer(); err != nil {
		return err
	}
	if d.writeFile != nil {
		if err := d.writeFile.Close(); err != nil {
			return err
		}
		d.writeFile = nil
	}
	// The readFile will be removed below in DeleteDirAndFiles.
	if err := d.CloseRead(); err != nil {
		return err
	}
	if err := d.cfg.FS.RemoveAll(filepath.Join(d.cfg.GetPather.GetPath(ctx), d.dirName)); err != nil {
		return err
	}
	totalSize := int64(0)
	leftOverFileIdx := 0
	if !d.rewindable {
		leftOverFileIdx = d.readFileIdx
	}
	for _, file := range d.files[leftOverFileIdx : d.writeFileIdx+1] {
		totalSize += int64(file.totalSize)
	}
	if totalSize > d.diskAcc.Used() {
		totalSize = d.diskAcc.Used()
	}
	d.diskAcc.Shrink(ctx, totalSize)
	return nil
}

// rotateFile performs file rotation for the diskQueue. i.e. it creates a new
// file to write to and sets the diskQueue state up to write to that file when
// Enqueue is called.
// It is valid to call rotateFile when the diskQueue is not currently writing to
// any file (i.e. during initialization). This will simply create the first file
// to write to.
func (d *diskQueue) rotateFile(ctx context.Context) error {
	fName := filepath.Join(d.cfg.GetPather.GetPath(ctx), d.dirName, strconv.Itoa(d.seqNo))
	f, err := d.cfg.FS.CreateWithSync(fName, bytesPerSync)
	if err != nil {
		return err
	}
	d.seqNo++

	if d.serializer == nil {
		writer := &diskQueueWriter{testingKnobAlwaysCompress: d.cfg.TestingKnobs.AlwaysCompress, wrapped: f}
		d.serializer, err = colserde.NewFileSerializer(writer, d.typs)
		if err != nil {
			return err
		}
		d.writer = writer
	} else {
		if err := d.writeFooterAndFlush(ctx); err != nil {
			return err
		}
		if err := d.resetWriters(f); err != nil {
			return err
		}
	}

	if d.writeFile != nil {
		d.files[d.writeFileIdx].finishedWriting = true
		if err := d.writeFile.Close(); err != nil {
			return err
		}
	}

	d.writeFileIdx = len(d.files)
	d.files = append(d.files, file{name: fName, offsets: make([]int, 1, 16)})
	d.writeFile = f
	return nil
}

func (d *diskQueue) resetWriters(f fs.File) error {
	d.writer.reset(f)
	return d.serializer.Reset(d.writer)
}

func (d *diskQueue) writeFooterAndFlush(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			// If an error occurs, set the serializer to nil to avoid any future
			// attempts to call writeFooterAndFlush during valid operation (e.g.
			// calling Close after an error).
			d.serializer = nil
		}
	}()
	if err := d.serializer.Finish(); err != nil {
		return err
	}
	written, err := d.writer.compressAndFlush()
	if err != nil {
		return err
	}
	d.numBufferedBatches = 0
	d.files[d.writeFileIdx].totalSize += written
	if d.cfg.DistSQLMetrics != nil {
		d.cfg.DistSQLMetrics.SpilledBytesWritten.Inc(int64(written))
	}
	if err := d.diskAcc.Grow(ctx, int64(written)); err != nil {
		return err
	}
	// Append offset for the readers.
	d.files[d.writeFileIdx].offsets = append(d.files[d.writeFileIdx].offsets, d.files[d.writeFileIdx].totalSize)
	return nil
}

func (d *diskQueue) Enqueue(ctx context.Context, b coldata.Batch) error {
	if d.state == diskQueueStateDequeueing {
		if d.cfg.CacheMode != DiskQueueCacheModeDefault {
			return errors.Errorf("attempted to Enqueue to DiskQueue in mode that disallows it: %d", d.cfg.CacheMode)
		}
		if d.rewindable {
			return errors.Errorf("attempted to Enqueue to RewindableDiskQueue after Dequeue has been called")
		}
	}
	d.state = diskQueueStateEnqueueing
	if b.Length() == 0 {
		if d.done {
			// Already done.
			return nil
		}
		if err := d.writeFooterAndFlush(ctx); err != nil {
			return err
		}
		if err := d.writeFile.Close(); err != nil {
			return err
		}
		d.files[d.writeFileIdx].finishedWriting = true
		d.writeFile = nil
		// Done with the serializer. Not setting this will cause us to attempt to
		// flush the serializer on Close.
		d.serializer = nil
		// The write file will be closed in Close.
		d.done = true
		if d.cfg.CacheMode == DiskQueueCacheModeClearAndReuseCache {
			// Clear the cache. d.scratchDecompressedReadBytes should already be nil
			// since we don't allow writes once reads happen in this mode.
			d.scratchDecompressedReadBytes = nil
			// Clear the write side of the cache.
			d.writer.buffer = bytes.Buffer{}
			d.writer.scratch.compressedBuf = nil
		}
		return nil
	}
	if err := d.serializer.AppendBatch(b); err != nil {
		return err
	}
	d.numBufferedBatches++

	bufferSizeLimitReached := d.writer.numBytesBuffered() > d.writeBufferLimit
	fileSizeLimitReached := d.files[d.writeFileIdx].totalSize+d.writer.numBytesBuffered() > d.cfg.MaxFileSizeBytes
	if bufferSizeLimitReached || fileSizeLimitReached {
		if fileSizeLimitReached {
			// rotateFile will flush and reset writers.
			return d.rotateFile(ctx)
		}
		if err := d.writeFooterAndFlush(ctx); err != nil {
			return err
		}
		return d.resetWriters(d.writeFile)
	}
	return nil
}

func (d *diskQueue) maybeInitDeserializer(ctx context.Context) (bool, error) {
	if d.deserializerState.FileDeserializer != nil {
		return true, nil
	}
	if d.readFileIdx >= len(d.files) {
		// There is no valid file to read from. Either more data will be enqueued or
		// not, but the behavior there depends on the caller.
		return false, nil
	}
	fileToRead := d.files[d.readFileIdx]
	if fileToRead.curOffsetIdx == len(fileToRead.offsets)-1 {
		// The current offset index is the last element in offsets. This means that
		// either the region to read from next is currently being written to or the
		// writer has rotated to a new file.
		if fileToRead.finishedWriting {
			// Close current file.
			if err := d.CloseRead(); err != nil {
				return false, err
			}
			if !d.rewindable {
				// Remove current file.
				if err := d.cfg.FS.Remove(d.files[d.readFileIdx].name); err != nil {
					return false, err
				}
				fileSize := int64(d.files[d.readFileIdx].totalSize)
				if fileSize > d.diskAcc.Used() {
					fileSize = d.diskAcc.Used()
				}
				d.diskAcc.Shrink(ctx, fileSize)
			}
			d.readFile = nil
			// Read next file.
			d.readFileIdx++
			return d.maybeInitDeserializer(ctx)
		}
		// Not finished writing. there is currently no data to read.
		return false, nil
	}
	if d.readFile == nil {
		// File is not open.
		f, err := d.cfg.FS.Open(fileToRead.name)
		if err != nil {
			return false, err
		}
		d.readFile = f
	}
	readRegionStart := fileToRead.offsets[fileToRead.curOffsetIdx]
	readRegionLength := fileToRead.offsets[fileToRead.curOffsetIdx+1] - readRegionStart
	if cap(d.writer.scratch.compressedBuf) < readRegionLength {
		// Not enough capacity, we have to allocate a new compressedBuf.
		d.writer.scratch.compressedBuf = make([]byte, readRegionLength)
	}
	// Slice the compressedBuf to be of the desired length, encoded in
	// readRegionLength.
	d.writer.scratch.compressedBuf = d.writer.scratch.compressedBuf[0:readRegionLength]
	// Read the desired length starting at readRegionStart.
	n, err := d.readFile.ReadAt(d.writer.scratch.compressedBuf, int64(readRegionStart))
	if err != nil && err != io.EOF {
		return false, err
	}
	if d.cfg.DistSQLMetrics != nil {
		d.cfg.DistSQLMetrics.SpilledBytesRead.Inc(int64(n))
	}
	if n != len(d.writer.scratch.compressedBuf) {
		return false, errors.Errorf("expected to read %d bytes but read %d", len(d.writer.scratch.compressedBuf), n)
	}

	blockType := d.writer.scratch.compressedBuf[0]
	compressedBytes := d.writer.scratch.compressedBuf[1:]
	var decompressedBytes []byte
	if blockType == snappyCompressedBlock {
		decompressedBytes, err = snappy.Decode(d.scratchDecompressedReadBytes, compressedBytes)
		if err != nil {
			return false, err
		}
		d.scratchDecompressedReadBytes = decompressedBytes[:cap(decompressedBytes)]
	} else {
		// Copy the result for safety since we're reusing the diskQueueWriter's
		// compressed write buffer. If an Enqueue were to arrive between Dequeue
		// calls of the same buffered coldata.Batches to return, the memory would
		// be corrupted. The following code ensures that
		// scratchDecompressedReadBytes is of the required capacity.
		if cap(d.scratchDecompressedReadBytes) < len(compressedBytes) {
			d.scratchDecompressedReadBytes = make([]byte, len(compressedBytes))
		}
		// Slice up to the length of compressedBytes so that the copy below will
		// copy all desired bytes.
		d.scratchDecompressedReadBytes = d.scratchDecompressedReadBytes[:len(compressedBytes)]
		copy(d.scratchDecompressedReadBytes, compressedBytes)
		decompressedBytes = d.scratchDecompressedReadBytes
	}

	deserializer, err := colserde.NewFileDeserializerFromBytes(d.typs, decompressedBytes)
	if err != nil {
		return false, err
	}
	d.deserializerState.FileDeserializer = deserializer
	d.deserializerState.curBatch = 0
	if d.deserializerState.NumBatches() == 0 {
		// Zero batches to deserialize in this region. This shouldn't happen but we
		// might as well handle it.
		if err := d.closeFileDeserializer(); err != nil {
			return false, err
		}
		d.files[d.readFileIdx].curOffsetIdx++
		return d.maybeInitDeserializer(ctx)
	}
	return true, nil
}

// Dequeue dequeues a batch from disk and deserializes it into b. Note that the
// deserialized batch is only valid until the next call to Dequeue.
func (d *diskQueue) Dequeue(ctx context.Context, b coldata.Batch) (bool, error) {
	if d.serializer != nil && d.numBufferedBatches > 0 {
		if err := d.writeFooterAndFlush(ctx); err != nil {
			return false, err
		}
		if err := d.resetWriters(d.writeFile); err != nil {
			return false, err
		}
	}
	if d.state == diskQueueStateEnqueueing && d.cfg.CacheMode != DiskQueueCacheModeDefault {
		// This is the first Dequeue after Enqueues, so reuse the write cache for
		// reads. Note that the buffer for compressed reads is reused in
		// maybeInitDeserializer in either case, so there is nothing to do here for
		// that.
		d.writer.buffer.Reset()
		d.scratchDecompressedReadBytes = d.writer.buffer.Bytes()
	}
	d.state = diskQueueStateDequeueing

	if d.deserializerState.FileDeserializer != nil && d.deserializerState.curBatch >= d.deserializerState.NumBatches() {
		// Finished all the batches, set the deserializer to nil to initialize a new
		// one to read the next region.
		if err := d.closeFileDeserializer(); err != nil {
			return false, err
		}
		d.files[d.readFileIdx].curOffsetIdx++
	}

	if dataToRead, err := d.maybeInitDeserializer(ctx); err != nil {
		return false, err
	} else if !dataToRead {
		// No data to read.
		if !d.done {
			// Data might still be added.
			return false, nil
		}
		// No data will be added.
		b.SetLength(0)
	} else {
		if err := d.deserializerState.GetBatch(d.deserializerState.curBatch, b); err != nil {
			return false, err
		}
		d.deserializerState.curBatch++
	}

	return true, nil
}

// Rewind is part of the RewindableQueue interface.
func (d *diskQueue) Rewind() error {
	if err := d.closeFileDeserializer(); err != nil {
		return err
	}
	if err := d.CloseRead(); err != nil {
		return err
	}
	d.deserializerState.curBatch = 0
	d.readFile = nil
	d.readFileIdx = 0
	for i := range d.files {
		d.files[i].curOffsetIdx = 0
	}
	return nil
}
