// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.

package colserde

import (
	"bytes"
	"io"
	"path"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/golang/snappy"
)

type PebbleQueueCfg struct {
	BaseQueueCfg
	// SkipRecreateIter skips recreating an iterator every time a dequeue is performed. This should be set to true if
	// all writes will happen before all reads. If not, a read could miss a write? Is this true?
	// TODO(asubiotto): Confirm.
	SkipRecreateIter  bool
	MaxValueSizeBytes int
}

type file struct {
	name            string
	offsets         []int
	curOffsetIdx    int
	totalSize       int
	finishedWriting bool
}

// diskQueueWriter is an object that encapsulates the writing logic of a
// DiskQueue. As bytes are written to it, they are buffered until
// compressAndFlush is called, which compresses all bytes and writes them to the
// wrapped io.Writer.
type diskQueueWriter struct {
	buffer  bytes.Buffer
	wrapped io.Writer
	scratch struct {
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

/*
TODO still:
remove file when done reading.
implement file naming scheme?
*/

// compressAndFlush compresses all buffered bytes and writes them to the wrapped
// io.Writer. The number of total bytes written to the wrapped writer is
// returned if no error occurred, otherwise 0, err is returned.
func (w *diskQueueWriter) compressAndFlush() (int, error) {
	b := w.buffer.Bytes()
	compressed := snappy.Encode(w.scratch.compressedBuf, b)
	w.scratch.compressedBuf = compressed[:cap(compressed)]

	blockType := snappyUncompressedBlock
	// Discard result if < 12.5% size reduction.
	// TODO(asubiotto): Add testing knob.
	if len(compressed) < len(b)-len(b)/8 {
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

// DiskQueue is not currently safe for concurrent use.
type DiskQueue struct {
	cfg   FlatQueueCfg
	fs    vfs.FS
	files []file
	seqNo int

	done bool

	serializer        *FileSerializer
	writer            *diskQueueWriter
	writeFileIdx      int
	writeFile         vfs.File
	deserializerState struct {
		*FileDeserializer
		curBatch      int
		batchToReturn coldata.Batch
	}
	// readFileIdx is an index into the current file in files the deserializer is
	// reading from.
	readFileIdx           int
	readFile              vfs.File
	decompressedReadBytes []byte
}

type Queue interface {
	// Enqueue enqueues a coldata.Batch to this queue. A zero-length batch should be enqueued when no
	// more elements will be enqueued.
	Enqueue(coldata.Batch) error
	// Dequeue dequeues a coldata.Batch from the queue. A nil Batch means the queue is currently empty, while a zero-length
	// batch means no more elements will be enqueued. The dequeued batch should only be considered valid until the next
	// call to Dequeue.
	Dequeue() (coldata.Batch, error)
	Close() error
}

type FlatQueueCfg struct {
	BaseQueueCfg
	MaxFileSizeBytes int
}

type BaseQueueCfg struct {
	// Path is where the temporary files should be created.
	Path string
	// FilenamePrefix is the prefix of file names
	FilenamePrefix string
	// In production we would also have a cacheSize for a circular buffer that would hold the first elements in the queue,
	// but since we are just benchmarking disk accesses, we elide that.
	BufferSizeBytes int
}

var queueTyps = []coltypes.T{coltypes.Int64}

func NewDiskQueue(cfg FlatQueueCfg, fs vfs.FS) *DiskQueue {
	d := &DiskQueue{
		cfg:   cfg,
		fs:    fs,
		files: make([]file, 0, 4),
	}
	d.deserializerState.batchToReturn = coldata.NewMemBatch(queueTyps)
	return d
}

func (d *DiskQueue) Init() error {
	return d.rotateFile()
}

func (d *DiskQueue) Close() error {
	if d.done {
		return nil
	}
	if d.serializer != nil {
		if err := d.writeFooterAndFlush(); err != nil {
			return err
		}
	}
	if d.deserializerState.FileDeserializer != nil {
		if err := d.deserializerState.Close(); err != nil {
			return err
		}
	}
	if d.writeFile != nil {
		if err := d.writeFile.Close(); err != nil {
			return err
		}
	}
	if d.readFile != nil {
		if err := d.readFile.Close(); err != nil {
			return err
		}
		if err := d.fs.Remove(d.files[d.readFileIdx].name); err != nil {
			return err
		}
	}
	d.done = true
	return nil
}

func (d *DiskQueue) rotateFile() error {
	fName := path.Join(d.cfg.Path, "queuefile"+strconv.Itoa(d.seqNo))
	f, err := d.fs.Create(fName)
	if err != nil {
		return err
	}
	d.seqNo++
	f = vfs.NewSyncingFile(f, vfs.SyncingFileOptions{
		BytesPerSync: 512 << 10, /* 512 KiB */
	})

	if d.serializer == nil {
		// All writes are buffered using a generic buffered writer that flushes to a buffered snappy writer, which in turn
		// flushes to the file.
		writer := &diskQueueWriter{wrapped: f}
		d.serializer, err = NewFileSerializer(writer, queueTyps)
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

func (d *DiskQueue) resetWriters(f vfs.File) error {
	d.writer.reset(f)
	return d.serializer.Reset(d.writer)
}

func (d *DiskQueue) writeFooterAndFlush() error {
	err := d.serializer.Finish()
	if err != nil {
		return err
	}
	written, err := d.writer.compressAndFlush()
	if err != nil {
		return err
	}
	// Append offset for the readers.
	d.files[d.writeFileIdx].totalSize += written
	d.files[d.writeFileIdx].offsets = append(d.files[d.writeFileIdx].offsets, d.files[d.writeFileIdx].totalSize)
	return nil
}

func (d *DiskQueue) Enqueue(b coldata.Batch) error {
	if b.Length() == 0 {
		if err := d.writeFooterAndFlush(); err != nil {
			return err
		}
		// Done with the serializer. Not setting this will cause us to attempt to flush the serializer on Close.
		d.serializer = nil
		// The write file will be closed in Close.
		// TODO(asubiotto): Take another look at d.done
		d.done = true
		return nil
	}
	if err := d.serializer.AppendBatch(b); err != nil {
		return err
	}
	bufferSizeLimitReached := d.writer.numBytesBuffered() > d.cfg.BufferSizeBytes
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

func (d *DiskQueue) maybeInitDeserializer() (bool, error) {
	if d.deserializerState.FileDeserializer != nil {
		return true, nil
	}
	if d.readFileIdx >= len(d.files) {
		// There is no valid file to read from. Either more data will be enqueued or not, but the
		// behavior there depends on the caller.
		return false, nil
	}
	fileToRead := d.files[d.readFileIdx]
	if fileToRead.curOffsetIdx == len(fileToRead.offsets)-1 {
		// The current offset index is the last element in offsets. This means that either the region
		// to read from next is currently being written to or the writer has rotated to a new file.
		if fileToRead.finishedWriting {
			// Close current file.
			if err := d.readFile.Close(); err != nil {
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
		f, err := d.fs.Open(fileToRead.name)
		if err != nil {
			return false, err
		}
		d.readFile = f
	}
	readRegionStart := fileToRead.offsets[fileToRead.curOffsetIdx]
	readRegionLength := fileToRead.offsets[fileToRead.curOffsetIdx+1] - readRegionStart
	if cap(d.writer.scratch.compressedBuf) < readRegionLength {
		// Not enough capacity, we have to allocate a new compressedReadBytes.
		d.writer.scratch.compressedBuf = make([]byte, readRegionLength)
	}
	d.writer.scratch.compressedBuf = d.writer.scratch.compressedBuf[0:readRegionLength]
	n, err := d.readFile.ReadAt(d.writer.scratch.compressedBuf, int64(readRegionStart))
	if err != nil {
		return false, err
	}
	if n != len(d.writer.scratch.compressedBuf) {
		return false, errors.Errorf("expected to read %d bytes but read %d", len(d.writer.scratch.compressedBuf), n)
	}

	blockType := d.writer.scratch.compressedBuf[0]
	decompressed := d.writer.scratch.compressedBuf[1:]
	if blockType == snappyCompressedBlock {
		// TODO(asubiotto): think about the memory story with decompressed bytes
		decompressed, err = snappy.Decode(d.decompressedReadBytes, d.writer.scratch.compressedBuf[1:])
		if err != nil {
			return false, err
		}
		d.decompressedReadBytes = decompressed[:cap(decompressed)]
	}

	deserializer, err := NewFileDeserializerFromBytes(decompressed)
	if err != nil {
		return false, err
	}
	d.deserializerState.FileDeserializer = deserializer
	d.deserializerState.curBatch = 0
	if d.deserializerState.NumBatches() == 0 {
		// This would be weird, but let's handle it.
		if err := d.deserializerState.FileDeserializer.Close(); err != nil {
			return false, err
		}
		d.deserializerState.FileDeserializer = nil
		return false, nil
	}
	return true, nil
}

func (d *DiskQueue) Dequeue() (coldata.Batch, error) {
	if d.serializer != nil {
		if err := d.writeFooterAndFlush(); err != nil {
			return nil, err
		}
		if err := d.resetWriters(d.writeFile); err != nil {
			return nil, err
		}
	}

	if d.deserializerState.FileDeserializer != nil && d.deserializerState.curBatch >= d.deserializerState.NumBatches() {
		// Finished all the batches, set the deserializer to nil to initialize a new
		// one to read the next region.
		if err := d.deserializerState.FileDeserializer.Close(); err != nil {
			return nil, err
		}
		d.deserializerState.FileDeserializer = nil
		d.files[d.readFileIdx].curOffsetIdx++
	}

	if dataToRead, err := d.maybeInitDeserializer(); err != nil {
		return nil, err
	} else if !dataToRead {
		// No data to read.
		if !d.done {
			// Data might still be added.
			return nil, nil
		}
		// No data will be added.
		d.deserializerState.batchToReturn.SetLength(0)
	} else {
		if err := d.deserializerState.GetBatch(d.deserializerState.curBatch, d.deserializerState.batchToReturn); err != nil {
			return nil, err
		}
		d.deserializerState.curBatch++
	}

	// In the real implementation we will deserialize as much as we need to place
	// all but one batch (the batch to return) in the circular buffer.
	// TODO(asubiotto): Maybe add a TODO or something?
	return d.deserializerState.batchToReturn, nil
}
