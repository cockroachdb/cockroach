// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

// SSTWriter writes SSTables.
type SSTWriter struct {
	fw *sstable.Writer
	f  writeCloseSyncer
	// DataSize tracks the total key and value bytes added so far.
	DataSize int64
	scratch  []byte
}

// writeCloseSyncer interface copied from pebble.sstable.
type writeCloseSyncer interface {
	io.WriteCloser
	Sync() error
}

// MakeBackupSSTWriter creates a new SSTWriter tailored for backup SSTs. These
// SSTs have bloom filters disabled and format set to LevelDB.
func MakeBackupSSTWriter(f writeCloseSyncer) SSTWriter {
	opts := DefaultPebbleOptions().MakeWriterOptions(0)
	opts.TableFormat = sstable.TableFormatLevelDB
	// Disable bloom filters to produce SSTs matching those from
	// RocksDBSstFileWriter.
	opts.FilterPolicy = nil
	opts.MergerName = "nullptr"
	sst := sstable.NewWriter(f, opts)
	return SSTWriter{fw: sst, f: f}
}

// MakeIngestionSSTWriter creates a new SSTWriter tailored for ingestion SSTs.
// These SSTs have bloom filters enabled (as set in DefaultPebbleOptions) and
// format set to RocksDBv2.
func MakeIngestionSSTWriter(f writeCloseSyncer) SSTWriter {
	opts := DefaultPebbleOptions().MakeWriterOptions(0)
	opts.TableFormat = sstable.TableFormatRocksDBv2
	opts.MergerName = "nullptr"
	sst := sstable.NewWriter(f, opts)
	return SSTWriter{fw: sst, f: f}
}

// Finish finalizes the writer and returns the constructed file's contents,
// since the last call to Truncate (if any). At least one kv entry must have been added.
func (fw *SSTWriter) Finish() error {
	if fw.fw == nil {
		return errors.New("cannot call Finish on a closed writer")
	}
	if err := fw.fw.Close(); err != nil {
		return err
	}
	fw.fw = nil
	return nil
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
	iter.SeekGE(MakeMVCCMetadataKey(start))

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

// MemFile is a file-like struct that buffers all data written to it in memory.
// Implements the writeCloseSyncer interface and is intended for use with
// SSTWriter.
type MemFile struct {
	bytes.Buffer
}

// Close implements the writeCloseSyncer interface.
func (*MemFile) Close() error {
	return nil
}

// Sync implements the writeCloseSyncer interface.
func (*MemFile) Sync() error {
	return nil
}

// Data returns the in-memory buffer behind this MemFile.
func (f *MemFile) Data() []byte {
	return f.Bytes()
}
