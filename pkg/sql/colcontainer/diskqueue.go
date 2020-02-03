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
	"io"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
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
	// future or not. If finishedWriting is true and the reader reaches the end of
	// the file, the file represented by this struct should be closed and removed.
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

// diskQueue is an on-disk queue of coldata.Batches that implements the Queue
// interface. coldata.Batches are serialized and buffered up until
// DiskQueueCfg.BufferSizeBytes are reached, after which they are compressed and
// flushed to a file. A directory with a random UUID name will be created in
// cfg.Path, and files will be created in that directory using sequence numbers.
// When a file reaches DiskQueueCfg.MaxFileSizeBytes, a new file is created with
// the next sequential file number to store the next batches in the queue.
// Note that files will be cleaned up as coldata.Batches are dequeued from the
// diskQueue. DiskQueueCfg.Dir will also be removed on Close, deleting all files.
// A diskQueue will never use more memory than cfg.BufferSizeBytes, but not all
// the available memory will be used to buffer only writes. A third will be used
// to buffer uncompressed writes, a third for the compressed writes and
// compressed reads, and a final third for the decompressed writes. The division
// of space is done in this particular way simply in order to give reads and
// writes the same amount of buffer space.
// NOTE: We could reuse the memory used to buffer uncompressed writes to buffer
// uncompressed reads, but this would only work with the limitation that all
// writes happen before all reads.
// TODO(asubiotto): The improvement mentioned above might be worth it once we
//  ensure that we only use DiskQueues for the write-everything, read-everything
//  pattern.
type diskQueue struct {
	// dirName is the directory in cfg.Path that holds this queue's files.
	dirName string

	typs  []coltypes.T
	cfg   DiskQueueCfg
	files []file
	seqNo int

	done bool

	serializer *colserde.FileSerializer
	// numBufferedBatches is the number of batches buffered that haven't been
	// flushed to disk. This is useful for a reader to determine whether to flush
	// or not, since the number of buffered bytes will always be > 0 even though
	// no batches have been enqueued (due to metadata).
	numBufferedBatches int
	writer             *diskQueueWriter
	writeFileIdx       int
	writeFile          engine.File
	deserializerState  struct {
		*colserde.FileDeserializer
		curBatch int
	}
	// readFileIdx is an index into the current file in files the deserializer is
	// reading from.
	readFileIdx                  int
	readFile                     engine.File
	scratchDecompressedReadBytes []byte
}

var _ Queue = &diskQueue{}

// Queue describes a simple queue interface to which coldata.Batches can be
// Enqueued and Dequeued.
type Queue interface {
	// Enqueue enqueues a coldata.Batch to this queue. A zero-length batch should
	// be enqueued when no more elements will be enqueued.
	// WARNING: Selection vectors are ignored.
	Enqueue(coldata.Batch) error
	// Dequeue dequeues a coldata.Batch from the queue into the batch that is
	// passed in. The boolean returned specifies whether the queue was not empty
	// (i.e. whether there was a batch to Dequeue). If true is returned and the
	// batch has a length of zero, the Queue is finished and will not be Enqueued
	// to. If an error is returned, the batch and boolean returned are
	// meaningless.
	Dequeue(coldata.Batch) (bool, error)
	// Close closes any resources associated with the Queue.
	Close() error
}

const (
	// These values were chosen by running BenchmarkQueue.
	defaultBufferSizeBytes  = 128 << 10 /* 128 KiB */
	defaultMaxFileSizeBytes = 32 << 20  /* 32 MiB */
)

// DiskQueueCfg is a struct holding the configuration options for a DiskQueue.
type DiskQueueCfg struct {
	// FS is the filesystem interface to use.
	FS engine.FS
	// Path is where the temporary directory that will contain this DiskQueue's
	// files should be created. The directory name will be a UUID.
	Path string
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
		cfg.BufferSizeBytes = defaultBufferSizeBytes
	}
	if cfg.MaxFileSizeBytes == 0 {
		cfg.MaxFileSizeBytes = defaultMaxFileSizeBytes
	}
	return nil
}

// NewDiskQueue creates a Queue that spills to disk.
func NewDiskQueue(typs []coltypes.T, cfg DiskQueueCfg) (Queue, error) {
	if err := cfg.EnsureDefaults(); err != nil {
		return nil, err
	}
	d := &diskQueue{
		dirName: uuid.FastMakeV4().String(),
		typs:    typs,
		cfg:     cfg,
		files:   make([]file, 0, 4),
	}
	if err := cfg.FS.CreateDir(filepath.Join(cfg.Path, d.dirName)); err != nil {
		return nil, err
	}
	// rotateFile will create a new file to write to.
	return d, d.rotateFile()
}

func (d *diskQueue) Close() error {
	if d.serializer != nil {
		if err := d.writeFooterAndFlush(); err != nil {
			return err
		}
		d.serializer = nil
	}
	if d.deserializerState.FileDeserializer != nil {
		if err := d.deserializerState.Close(); err != nil {
			return err
		}
		d.deserializerState.FileDeserializer = nil
	}
	if d.writeFile != nil {
		if err := d.writeFile.Close(); err != nil {
			return err
		}
		d.writeFile = nil
	}
	if d.readFile != nil {
		if err := d.readFile.Close(); err != nil {
			return err
		}
		d.readFile = nil
		// The readFile will be removed below in RemoveAll.
	}
	if err := d.cfg.FS.DeleteDir(filepath.Join(d.cfg.Path, d.dirName)); err != nil {
		return err
	}
	return nil
}

// rotateFile performs file rotation for the diskQueue. i.e. it creates a new
// file to write to and sets the diskQueue state up to write to that file when
// Enqueue is called.
// It is valid to call rotateFile when the diskQueue is not currently writing to
// any file (i.e. during initialization). This will simply create the first file
// to write to.
func (d *diskQueue) rotateFile() error {
	fName := filepath.Join(d.cfg.Path, d.dirName, strconv.Itoa(d.seqNo))
	f, err := d.cfg.FS.CreateFileWithSync(fName, bytesPerSync)
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
		if err := d.writeFooterAndFlush(); err != nil {
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

func (d *diskQueue) resetWriters(f engine.File) error {
	d.writer.reset(f)
	return d.serializer.Reset(d.writer)
}

func (d *diskQueue) writeFooterAndFlush() error {
	err := d.serializer.Finish()
	if err != nil {
		return err
	}
	written, err := d.writer.compressAndFlush()
	if err != nil {
		return err
	}
	d.numBufferedBatches = 0
	// Append offset for the readers.
	d.files[d.writeFileIdx].totalSize += written
	d.files[d.writeFileIdx].offsets = append(d.files[d.writeFileIdx].offsets, d.files[d.writeFileIdx].totalSize)
	return nil
}

func (d *diskQueue) Enqueue(b coldata.Batch) error {
	if b.Length() == 0 {
		if err := d.writeFooterAndFlush(); err != nil {
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
		return nil
	}
	if err := d.serializer.AppendBatch(b); err != nil {
		return err
	}
	d.numBufferedBatches++

	// Only buffer up to a third of the available buffer space, we need another
	// third to compress these reads and buffer compressed writes, as well as a
	// final third to buffer decompressed reads. Refer to the diskQueue struct
	// comment for a more thorough explanation of this space division.
	bufferSizeLimitReached := d.writer.numBytesBuffered() > d.cfg.BufferSizeBytes/3
	fileSizeLimitReached := d.files[d.writeFileIdx].totalSize+d.writer.numBytesBuffered() > d.cfg.MaxFileSizeBytes
	if bufferSizeLimitReached || fileSizeLimitReached {
		if fileSizeLimitReached {
			// rotateFile will flush and reset writers.
			return d.rotateFile()
		}
		if err := d.writeFooterAndFlush(); err != nil {
			return err
		}
		return d.resetWriters(d.writeFile)
	}
	return nil
}

func (d *diskQueue) maybeInitDeserializer() (bool, error) {
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
			// Close and remove current file.
			if err := d.readFile.Close(); err != nil {
				return false, err
			}
			if err := d.cfg.FS.DeleteFile(d.files[d.readFileIdx].name); err != nil {
				return false, err
			}
			d.readFile = nil
			// Read next file.
			d.readFileIdx++
			return d.maybeInitDeserializer()
		}
		// Not finished writing. there is currently no data to read.
		return false, nil
	}
	if d.readFile == nil {
		// File is not open.
		f, err := d.cfg.FS.OpenFile(fileToRead.name)
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

	deserializer, err := colserde.NewFileDeserializerFromBytes(decompressedBytes)
	if err != nil {
		return false, err
	}
	d.deserializerState.FileDeserializer = deserializer
	d.deserializerState.curBatch = 0
	if d.deserializerState.NumBatches() == 0 {
		// Zero batches to deserialize in this region. This shouldn't happen but we
		// might as well handle it.
		if err := d.deserializerState.FileDeserializer.Close(); err != nil {
			return false, err
		}
		d.deserializerState.FileDeserializer = nil
		d.files[d.readFileIdx].curOffsetIdx++
		return d.maybeInitDeserializer()
	}
	return true, nil
}

func (d *diskQueue) Dequeue(b coldata.Batch) (bool, error) {
	if d.serializer != nil && d.numBufferedBatches > 0 {
		if err := d.writeFooterAndFlush(); err != nil {
			return false, err
		}
		if err := d.resetWriters(d.writeFile); err != nil {
			return false, err
		}
	}

	if d.deserializerState.FileDeserializer != nil && d.deserializerState.curBatch >= d.deserializerState.NumBatches() {
		// Finished all the batches, set the deserializer to nil to initialize a new
		// one to read the next region.
		if err := d.deserializerState.FileDeserializer.Close(); err != nil {
			return false, err
		}
		d.deserializerState.FileDeserializer = nil
		d.files[d.readFileIdx].curOffsetIdx++
	}

	if dataToRead, err := d.maybeInitDeserializer(); err != nil {
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
		if d.deserializerState.curBatch == 0 {
			vecs := b.ColVecs()
			for i := range vecs {
				// When we deserialize a new memory region, we create new memory that
				// the batch to deserialize into will point to. This is due to
				// https://github.com/cockroachdb/cockroach/issues/43964, which could
				// result in corrupting memory if we naively allow the arrow batch
				// converter to call Reset() on a batch that points to memory that has
				// still not been read. Doing this avoids reallocating a new
				// scratchDecompressedReadBytes every time we perform a read from the
				// file and constrains the downside to allocating a new batch every
				// couple of batches.
				// TODO(asubiotto): This is a stop-gap solution. The issue is that
				//  ownership semantics are a bit murky. Can we do better? Refer to the
				//  issue.
				vecs[i] = coldata.NewMemColumn(d.typs[i], int(coldata.BatchSize()))
			}
		}
		if err := d.deserializerState.GetBatch(d.deserializerState.curBatch, b); err != nil {
			return false, err
		}
		d.deserializerState.curBatch++
	}

	return true, nil
}
