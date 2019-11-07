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

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/pkg/errors"
)

// SSTWriter writes SSTables.
type SSTWriter struct {
	fw *sstable.Writer
	f  *memFile
	// DataSize tracks the total key and value bytes added so far.
	DataSize uint64
	scratch  []byte
}

// MakeSSTWriter creates a new SSTWriter.
func MakeSSTWriter() SSTWriter {
	opts := sstable.WriterOptions{
		BlockSize:               32 * 1024,
		TableFormat:             pebble.TableFormatLevelDB,
		Comparer:                MVCCComparer,
		MergerName:              "nullptr",
		TablePropertyCollectors: PebbleTablePropertyCollectors,
	}
	f := &memFile{}
	sst := sstable.NewWriter(f, opts)
	return SSTWriter{fw: sst, f: f}
}

// Add puts a kv entry into the sstable being built. An error is returned if it
// is not greater than any previously added entry (according to the comparator
// configured during writer creation). `Close` cannot have been called.
func (fw *SSTWriter) Add(kv MVCCKeyValue) error {
	if fw.fw == nil {
		return errors.New("cannot call Open on a closed writer")
	}
	fw.DataSize += uint64(len(kv.Key.Key)) + uint64(len(kv.Value))
	fw.scratch = EncodeKeyToBuf(fw.scratch[:0], kv.Key)
	return fw.fw.Set(fw.scratch, kv.Value)
}

// Finish finalizes the writer and returns the constructed file's contents. At
// least one kv entry must have been added.
func (fw *SSTWriter) Finish() ([]byte, error) {
	if fw.fw == nil {
		return nil, errors.New("cannot call Finish on a closed writer")
	}
	if err := fw.fw.Close(); err != nil {
		return nil, err
	}
	fw.fw = nil
	return fw.f.data, nil
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
