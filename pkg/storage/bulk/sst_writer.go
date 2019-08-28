// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"
	"io"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/sstable"
	"github.com/pkg/errors"
)

// SSTWriter writes SSTables.
type SSTWriter struct {
	fw *sstable.Writer
	f  *memFile
	// DataSize tracks the total key and value bytes added so far.
	DataSize int64
	scratch  []byte
}

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
// written by the Rocks writer, using the collector in table_props.cc. However
// since bulk-ingestion SSTs never contain deletions (range or otherwise), there
// is no actual implementation needed here.
type dummyDeleteRangeCollector struct{}

var _ pebble.TablePropertyCollector = &dummyDeleteRangeCollector{}

func (dummyDeleteRangeCollector) Add(key pebble.InternalKey, value []byte) error {
	if key.Kind() != pebble.InternalKeyKindSet {
		return errors.Errorf("unsupported key kind %v", key.Kind())
	}
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
		Comparer:    mvccComparer,
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

// MakeSSTWriter creates a new SSTWriter.
func MakeSSTWriter() SSTWriter {
	f := &memFile{}
	sst := sstable.NewWriter(f, pebbleOpts, pebble.LevelOptions{BlockSize: 64 * 1024})
	return SSTWriter{fw: sst, f: f}
}

// Add puts a kv entry into the sstable being built. An error is returned if it
// is not greater than any previously added entry (according to the comparator
// configured during writer creation). `Close` cannot have been called.
func (fw *SSTWriter) Add(kv engine.MVCCKeyValue) error {
	if fw.fw == nil {
		return errors.New("cannot call Open on a closed writer")
	}
	fw.DataSize += int64(len(kv.Key.Key)) + int64(len(kv.Value))
	fw.scratch = engine.EncodeKeyToBuf(fw.scratch[:0], kv.Key)
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
