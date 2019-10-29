// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colserde

import (
	"bufio"
	"path"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

type file struct {
	name            string
	offsets         []int
	curOffsetIdx    int
	totalSize       int
	finishedWriting bool
}

// DiskQueue is not currently safe for concurrent use.
type DiskQueue struct {
	cfg   flatQueueCfg
	fs    vfs.FS
	files []file
	seqNo int

	done bool

	serializer        *FileSerializer
	bufferedWriter    *bufio.Writer
	writeFileIdx      int
	deserializerState struct {
		*FileDeserializer
		curBatch      int
		batchToReturn coldata.Batch
	}
	// readFileIdx is an index into the current file in files the deserializer is reading from.
	readFileIdx int
	// fileDescriptorsMu manages the current file descriptors to read and write from. Note that only updating these file
	// handles requires a mutex, the actual reading and writing is handled by the OS.
	write     vfs.File
	read      vfs.File
	readBytes []byte
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

type flatQueueCfg struct {
	baseQueueCfg
	maxFileSizeBytes int
}

type pebbleQueueCfg struct {
	baseQueueCfg
	// skipRecreateIter skips recreating an iterator every time a dequeue is performed. This should be set to true if
	// all writes will happen before all reads. If not, a read could miss a write? Is this true?
	// TODO(asubiotto): Confirm.
	skipRecreateIter  bool
	maxValueSizeBytes int
}

type baseQueueCfg struct {
	// path is where the temporary files should be created.
	path string
	// In production we would also have a cacheSize for a circular buffer that would hold the first elements in the queue,
	// but since we are just benchmarking disk accesses, we elide that.
	bufferSizeBytes int
}

var queueTyps = []coltypes.T{coltypes.Int64}

func NewDiskQueue(cfg flatQueueCfg, fs vfs.FS) *DiskQueue {
	d := &DiskQueue{
		cfg:   cfg,
		fs:    fs,
		files: make([]file, 0, 4),
		// We can probably pre-allocate a large enough slice for this in the future (worst case is that every single read
		// keeps on growing, so we can't reuse the previous slice and will allocate each time we read a block of batches).
		readBytes: make([]byte, 0),
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
		if _, err := d.serializer.Finish(); err != nil {
			return err
		}
	}
	if d.deserializerState.FileDeserializer != nil {
		if err := d.deserializerState.Close(); err != nil {
			return err
		}
	}
	if d.write != nil {
		if err := d.write.Close(); err != nil {
			return err
		}
	}
	if d.read != nil {
		if err := d.read.Close(); err != nil {
			return err
		}
	}
	d.done = true
	return nil
}

func (d *DiskQueue) rotateFile() error {
	fName := path.Join(d.cfg.path, "queuefile"+strconv.Itoa(d.seqNo))
	f, err := d.fs.Create(fName)
	if err != nil {
		return err
	}
	d.seqNo++
	f = vfs.NewSyncingFile(f, vfs.SyncingFileOptions{
		BytesPerSync: 512 << 10, /* 512 KiB */
	})

	if d.serializer == nil {
		b := bufio.NewWriterSize(f, d.cfg.bufferSizeBytes)
		d.serializer, err = NewFileSerializer(b, queueTyps)
		if err != nil {
			return err
		}
		d.bufferedWriter = b
	} else {
		if err := d.bufferedWriter.Flush(); err != nil {
			return err
		}
		d.bufferedWriter.Reset(f)
		if err := d.serializer.Reset(d.bufferedWriter); err != nil {
			return err
		}
	}

	if d.write != nil {
		d.files[d.writeFileIdx].finishedWriting = true
		// Close the file if we're not currently reading from it. The read
		if err := d.write.Close(); err != nil {
			return err
		}
	}

	d.writeFileIdx = len(d.files)
	d.files = append(d.files, file{name: fName, offsets: make([]int, 1, 16)})
	d.write = f
	return nil
}

func (d *DiskQueue) writeFooterAndFlush() error {
	written, err := d.serializer.Finish()
	if err != nil {
		return err
	}
	// Append offset for the readers.
	d.files[d.writeFileIdx].totalSize += written
	d.files[d.writeFileIdx].offsets = append(d.files[d.writeFileIdx].offsets, d.files[d.writeFileIdx].totalSize)
	return d.bufferedWriter.Flush()
}

func (d *DiskQueue) Enqueue(b coldata.Batch) error {
	if b.Length() == 0 {
		if err := d.writeFooterAndFlush(); err != nil {
			return err
		}
		// Done with the serializer. Not setting this will cause us to attempt to flush the serializer on Close.
		d.serializer = nil
		// The write file will be closed in Close.
		d.done = true
		return nil
	}
	if err := d.serializer.AppendBatch(b); err != nil {
		return err
	}
	bufferSizeLimitReached := d.serializer.Written() > d.cfg.bufferSizeBytes
	fileSizeLimitReached := d.files[d.writeFileIdx].totalSize+d.serializer.Written() > d.cfg.maxFileSizeBytes
	if bufferSizeLimitReached || fileSizeLimitReached {
		if err := d.writeFooterAndFlush(); err != nil {
			return err
		}
		if fileSizeLimitReached {
			return d.rotateFile()
		}
		d.bufferedWriter.Reset(d.write)
		if err := d.serializer.Reset(d.bufferedWriter); err != nil {
			return err
		}
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
			d.readFileIdx++
			// Close current file.
			if err := d.read.Close(); err != nil {
				return false, err
			}
			d.read = nil
			return d.maybeInitDeserializer()
		}
		// Not finished writing. there is currently no data to read.
		return false, nil
	}
	if d.read == nil {
		// File is not open.
		f, err := d.fs.Open(fileToRead.name)
		if err != nil {
			return false, err
		}
		d.read = f
	}
	readRegionStart := fileToRead.offsets[fileToRead.curOffsetIdx]
	readRegionLength := fileToRead.offsets[fileToRead.curOffsetIdx+1] - readRegionStart
	if cap(d.readBytes) < readRegionLength {
		// Not enough capacity, we have to allocate a new readBytes.
		d.readBytes = make([]byte, readRegionLength)
	}
	d.readBytes = d.readBytes[0:readRegionLength]
	n, err := d.read.ReadAt(d.readBytes, int64(readRegionStart))
	if err != nil {
		return false, err
	}
	if n != len(d.readBytes) {
		return false, errors.Errorf("expected to read %d bytes but read %d", len(d.readBytes), n)
	}
	deserializer, err := NewFileDeserializerFromBytes(d.readBytes)
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
	// Flush the pending writes. In a real use, we would first fetch from the in-memory circular buffer before resorting
	// to reading from disk/flushing.
	if err := d.bufferedWriter.Flush(); err != nil {
		return nil, err
	}

	if d.deserializerState.FileDeserializer != nil && d.deserializerState.curBatch >= d.deserializerState.NumBatches() {
		// Finished all the batches, set the deserializer to nil to initialize a new one to read the next region.
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

	// In the real implementation we will deserialize as much as we need to place all but one batch
	// (the batch to return) in the circular buffer.
	return d.deserializerState.batchToReturn, nil
}
