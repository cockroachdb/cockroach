// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/pkg/errors"
)

// SSTWriter writes SSTables.
type SSTWriter struct {
	fw *sstable.Writer
	f  *memFile
	// DataSize tracks the total key and value bytes added so far.
	DataSize int64
	scratch  []byte
	// lastTruncatedAt tracks the last byte of memFile at which Truncate() was
	// called. Only return bytes in Finish after this point.
	lastTruncatedAt uint64
}

// MakeSSTWriter creates a new SSTWriter.
func MakeSSTWriter() SSTWriter {
	opts := DefaultPebbleOptions().MakeWriterOptions(0)
	opts.TableFormat = sstable.TableFormatLevelDB
	opts.MergerName = "nullptr"
	f := &memFile{}
	sst := sstable.NewWriter(f, opts)
	return SSTWriter{fw: sst, f: f}
}

// Finish finalizes the writer and returns the constructed file's contents,
// since the last call to Truncate (if any). At least one kv entry must have been added.
func (fw *SSTWriter) Finish() ([]byte, error) {
	if fw.fw == nil {
		return nil, errors.New("cannot call Finish on a closed writer")
	}
	if err := fw.fw.Close(); err != nil {
		return nil, err
	}
	fw.fw = nil
	return fw.f.data[fw.lastTruncatedAt:], nil
}

// ClearRange implements the Writer interface.
func (fw *SSTWriter) ClearRange(start, end MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call ClearRange on a closed writer")
	}
	fw.DataSize += int64(len(start.Key)) + int64(len(end.Key))
	fw.scratch = EncodeKeyToBuf(fw.scratch[:0], start)
	return fw.fw.DeleteRange(fw.scratch, EncodeKey(end))
}

// Put puts a kv entry into the sstable being built. An error is returned if it
// is not greater than any previously added entry (according to the comparator
// configured during writer creation). `Close` cannot have been called.
func (fw *SSTWriter) Put(key MVCCKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Put on a closed writer")
	}
	fw.DataSize += int64(len(key.Key)) + int64(len(value))
	fw.scratch = EncodeKeyToBuf(fw.scratch[:0], key)
	return fw.fw.Set(fw.scratch, value)
}

// Truncate returns the in-memory buffer from the previous call to Truncate
// (or the start), until now.
func (fw *SSTWriter) Truncate() ([]byte, error) {
	oldData := fw.f.data[fw.lastTruncatedAt:]
	fw.lastTruncatedAt = uint64(len(fw.f.data))
	return oldData, nil
}

// ApplyBatchRepr implements the Writer interface.
func (fw *SSTWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("unimplemented")
}

// Clear implements the Writer interface.
func (fw *SSTWriter) Clear(key MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call Clear on a closed writer")
	}
	fw.scratch = EncodeKeyToBuf(fw.scratch[:0], key)
	fw.DataSize += int64(len(key.Key))
	return fw.fw.Delete(fw.scratch)
}

// SingleClear implements the Writer interface.
func (fw *SSTWriter) SingleClear(key MVCCKey) error {
	panic("unimplemented")
}

// ClearIterRange implements the Writer interface.
func (fw *SSTWriter) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	if fw.fw == nil {
		return errors.New("cannot call ClearIterRange on a closed writer")
	}

	// Set an upper bound on the iterator. This is okay because all calls to
	// ClearIterRange are with throwaway iterators, so there should be no new
	// side effects.
	iter.SetUpperBound(end)
	iter.Seek(MakeMVCCMetadataKey(start))

	valid, err := iter.Valid()
	for valid && err == nil {
		key := iter.UnsafeKey()
		fw.scratch = EncodeKeyToBuf(fw.scratch[:0], key)
		fw.DataSize += int64(len(key.Key))
		if err := fw.fw.Delete(fw.scratch); err != nil {
			return err
		}

		iter.Next()
		valid, err = iter.Valid()
	}
	return err
}

// Merge implements the Writer interface.
func (fw *SSTWriter) Merge(key MVCCKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Merge on a closed writer")
	}
	fw.DataSize += int64(len(key.Key)) + int64(len(value))
	fw.scratch = EncodeKeyToBuf(fw.scratch[:0], key)
	return fw.fw.Merge(fw.scratch, value)
}

// LogData implements the Writer interface.
func (fw *SSTWriter) LogData(data []byte) error {
	// No-op.
	return nil
}

// LogLogicalOp implements the Writer interface.
func (fw *SSTWriter) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op.
}

// Close finishes and frees memory and other resources. Close is idempotent.
func (fw *SSTWriter) Close() {
	if fw.fw == nil {
		return
	}
	// pebble.Writer *does* return interesting errors from Close... but normally
	// we already called its Close() in Finish() and we no-op here. Thus the only
	// time we expect to be here is in a deferred Close(), in which case the caller
	// probably is already returning some other error, so returning one from this
	// method just makes for messy defers.
	_ = fw.fw.Close()
	fw.fw = nil
}

type memFile struct {
	data []byte
	pos  int
}

func (*memFile) Close() error {
	return nil
}

func (*memFile) Sync() error {
	return nil
}

func (f *memFile) Read(p []byte) (int, error) {
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	return copy(p, f.data[off:]), nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.data = append(f.data, p...)
	return len(p), nil
}
