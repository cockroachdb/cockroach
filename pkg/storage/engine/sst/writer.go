// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sst

import (
	"bytes"
	"io"
	"math"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/pkg/errors"
)

type writeCloseSyncer interface {
	io.WriteCloser
	Sync() error
}

// Writer writes SSTables.
type Writer struct {
	fw *sstable.Writer
	f  writeCloseSyncer
	// DataSize tracks the total key and value bytes added so far.
	DataSize uint64
	scratch  []byte
}

var _ engine.Writer = &Writer{}

var mvccComparer = &pebble.Comparer{
	Compare: engine.MVCCKeyCompare,
	AbbreviatedKey: func(k []byte) uint64 {
		key, _, ok := enginepb.SplitMVCCKey(k)
		if !ok {
			return 0
		}
		return pebble.DefaultComparer.AbbreviatedKey(key)
	},

	Separator: func(dst, a, b []byte) []byte {
		return append(dst, a...)
	},

	Successor: func(dst, a []byte) []byte {
		return append(dst, a...)
	},
	Split: func(k []byte) int {
		if len(k) == 0 {
			return len(k)
		}
		// This is similar to what enginepb.SplitMVCCKey does.
		tsLen := int(k[len(k)-1])
		keyPartEnd := len(k) - 1 - tsLen
		if keyPartEnd < 0 {
			return len(k)
		}
		return keyPartEnd
	},

	Name: "cockroach_comparator",
}

// timeboundPropCollector implements a property collector for MVCC Timestamps.
// Its behavior matches TimeBoundTblPropCollector in table_props.cc.
type timeboundPropCollector struct {
	min, max []byte
}

var _ pebble.TablePropertyCollector = &timeboundPropCollector{}

func (t *timeboundPropCollector) Add(key pebble.InternalKey, value []byte) error {
	_, ts, ok := enginepb.SplitMVCCKey(key.UserKey)
	if !ok {
		return errors.Errorf("failed to split MVCC key")
	}
	if len(ts) > 0 {
		if len(t.min) == 0 || bytes.Compare(ts, t.min) < 0 {
			t.min = append(t.min[:0], ts...)
		}
		if len(t.max) == 0 || bytes.Compare(ts, t.max) > 0 {
			t.max = append(t.max[:0], ts...)
		}
	}
	return nil
}

func (t *timeboundPropCollector) Finish(userProps map[string]string) error {
	userProps["crdb.ts.min"] = string(t.min)
	userProps["crdb.ts.max"] = string(t.max)
	return nil
}

func (t *timeboundPropCollector) Name() string {
	return "TimeBoundTblPropCollectorFactory"
}

// dummyDeleteRangeCollector is a stub collector that just identifies itself.
// This stub can be installed so that SSTs claim to have the same props as those
// written by the Rocks writer, using the collector in table_props.cc.
//
// TODO(jeffreyxiao): The implementation of this collector is different from
// the one in table_props.cc because Pebble does not expose a NeedCompact
// function. The actual behavior should not differ from the RocksDB
// implementation because although NeedsCompact() is true and the
// marked_for_compaction tag is set for the RocksDB implementation, the tag is
// never checked in IngestExternalFiles.
type dummyDeleteRangeCollector struct{}

var _ pebble.TablePropertyCollector = &dummyDeleteRangeCollector{}

func (dummyDeleteRangeCollector) Add(key pebble.InternalKey, value []byte) error {
	return nil
}

func (dummyDeleteRangeCollector) Finish(userProps map[string]string) error {
	return nil
}

func (dummyDeleteRangeCollector) Name() string {
	return "DeleteRangeTblPropCollectorFactory"
}

var pebbleOpts = func() *pebble.Options {
	merger := *pebble.DefaultMerger
	merger.Name = "nullptr"
	opts := &pebble.Options{
		TableFormat: pebble.TableFormatLevelDB,
		Comparer:    engine.MVCCComparer,
		Merger:      &merger,
	}
	opts.EnsureDefaults()
	opts.TablePropertyCollectors = append(
		opts.TablePropertyCollectors,
		func() pebble.TablePropertyCollector { return &timeboundPropCollector{} },
		func() pebble.TablePropertyCollector { return &dummyDeleteRangeCollector{} },
	)
	return opts
}()

// MakeWriter creates a new Writer.
func MakeWriter(f writeCloseSyncer) Writer {
	// Setting the IndexBlockSize to MaxInt disables twoLevelIndexes in Pebble.
	// TODO(pbardea): Remove the IndexBlockSize option when https://github.com/cockroachdb/pebble/issues/285 is resolved.
	sst := sstable.NewWriter(f, pebbleOpts, pebble.LevelOptions{BlockSize: 64 * 1024, IndexBlockSize: math.MaxInt32})
	return Writer{fw: sst, f: f}
}

// ApplyBatchRepr implements the Writer interface.
func (fw *Writer) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("unimplemented")
}

// Clear implements the Writer interface. Note that it inserts a tombstone
// rather than actually remove the entry from the storage engine. An error is
// returned if it is not greater than any previous key used in Put or Clear
// (according to the comparator configured during writer creation). Close
// cannot have been called.
func (fw *Writer) Clear(key engine.MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call Clear on a closed writer")
	}
	fw.DataSize += uint64(len(key.Key))
	fw.scratch = engine.EncodeKeyToBuf(fw.scratch[:0], key)
	return fw.fw.Delete(fw.scratch)
}

// SingleClear implements the Writer interface.
func (fw *Writer) SingleClear(key engine.MVCCKey) error {
	panic("unimplemented")
}

// ClearRange implements the Writer interface. Note that it inserts a range deletion
// tombstone rather than actually remove the entries from the storage engine.
// It can be called at any time with respect to Put and Clear.
func (fw *Writer) ClearRange(start, end engine.MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call ClearRange on a closed writer")
	}
	fw.DataSize += uint64(len(start.Key)) + uint64(len(end.Key))
	fw.scratch = engine.EncodeKeyToBuf(fw.scratch[:0], start)
	startScratchLen := len(fw.scratch)
	fw.scratch = engine.EncodeKeyToBuf(fw.scratch, end)
	return fw.fw.DeleteRange(fw.scratch[:startScratchLen], fw.scratch[startScratchLen:])
}

// ClearIterRange implements the Writer interface. It inserts range deletion
// tombstones for all keys from start (inclusive) to end (exclusive) in
// the provided iterator.
func (fw *Writer) ClearIterRange(iter engine.Iterator, start, end engine.MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call ClearIterRange on a closed writer")
	}
	iter.Seek(start)
	for {
		valid, err := iter.Valid()
		if err != nil {
			return err
		}
		if !valid || !iter.Key().Less(end) {
			break
		}
		if err := fw.Clear(iter.Key()); err != nil {
			return err
		}
		iter.Next()
	}
	return nil
}

// Merge implements the Writer interface.
func (fw *Writer) Merge(key engine.MVCCKey, value []byte) error {
	panic("unimplemented")
}

// Put implements the Writer interface. It puts a kv entry into the sstable
// being built. An error is returned if it is not greater than any previous key
// used in Put or Clear (according to the comparator configured during writer
// creation). Close cannot have been called.
func (fw *Writer) Put(key engine.MVCCKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Open on a closed writer")
	}
	fw.DataSize += uint64(len(key.Key)) + uint64(len(value))
	fw.scratch = engine.EncodeKeyToBuf(fw.scratch[:0], key)
	return fw.fw.Set(fw.scratch, value)
}

// LogData implements the Writer interface.
func (fw *Writer) LogData(data []byte) error {
	panic("unimplemented")
}

// LogLogicalOp implements the Writer interface.
func (fw *Writer) LogLogicalOp(op engine.MVCCLogicalOpType, details engine.MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// Finish finalizes the writer. At least one kv entry must have been added.
func (fw *Writer) Finish() error {
	if fw.fw == nil {
		return errors.New("cannot call Finish on a closed writer")
	}
	// The Pebble SST Writer calls Sync on the backing file when closing.
	if err := fw.fw.Close(); err != nil {
		return err
	}
	fw.fw = nil
	return nil
}

// Close finishes and frees memory and other resources. Close is idempotent.
func (fw *Writer) Close() {
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

// MemFile is an in-memory SST file.
type MemFile struct {
	data []byte
	pos  int
}

var _ writeCloseSyncer = &MemFile{}

// Close closes the MemFile. This is a no-op.
func (*MemFile) Close() error {
	return nil
}

// Sync syncs the MemFile. This is a no-op.
func (*MemFile) Sync() error {
	return nil
}

// Read copies the data from the current position in the MemFile to the
// provided buffer.
func (f *MemFile) Read(p []byte) (int, error) {
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

// ReadAt copies the data from the specified offset in the MemFile to the
// provided buffer.
func (f *MemFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	return copy(p, f.data[off:]), nil
}

// Data returns the underlying buffer that backs the MemFile.
func (f *MemFile) Data() []byte {
	return f.data
}

// Write writes data to the MemFile.
func (f *MemFile) Write(p []byte) (int, error) {
	f.data = append(f.data, p...)
	return len(p), nil
}
