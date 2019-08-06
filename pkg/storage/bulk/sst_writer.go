// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package bulk

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	min, max hlc.Timestamp
}

func (t *timeboundPropCollector) Add(key pebble.InternalKey, value []byte) error {
	_, ts, err := enginepb.DecodeKey(key.UserKey)
	if err != nil {
		return err
	}

	if !ts.IsEmpty() {
		if ts.Less(t.min) || t.min.IsEmpty() {
			t.min = ts
		}
		if t.max.Less(ts) {
			t.max = ts
		}
	}
	return nil
}

func (t *timeboundPropCollector) Finish(userProps map[string]string) error {
	var minBuf, maxBuf []byte
	if t.min.WallTime > 0 {
		minBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(minBuf, uint64(t.min.WallTime))
	}
	if t.min.Logical > 0 {
		minBuf = append(minBuf, []byte{0, 0, 0, 0}...)
		binary.BigEndian.PutUint32(minBuf, uint32(t.min.Logical))
	}
	userProps["crdb.ts.min"] = string(minBuf)
	if t.max.WallTime > 0 {
		maxBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(maxBuf, uint64(t.max.WallTime))
	}
	if t.max.Logical > 0 {
		maxBuf = append(maxBuf, []byte{0, 0, 0, 0}...)
		binary.BigEndian.PutUint32(maxBuf, uint32(t.max.Logical))
	}
	userProps["crdb.ts.max"] = string(maxBuf)
	return nil
}

func (t *timeboundPropCollector) Name() string {
	return "TimeBoundTblPropCollectorFactory"
}

// dummyDeleteRangeCollector is a stub collector that just identifies itself.
// This stub can be installed so that SSTs claim to have the same props as those
// written by the Rocks writer using the collector in table_props.cc, however
// since bulk-ingestion SSTs never contain deletions (range or otherwise), there
// is no actual implementation needed here.
type dummyDeleteRangeCollector struct{}

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
	f := newMemFile(nil)
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
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return fw.f.data, nil
}

// Close finishes and frees memory and other resources. Close is idempotent.
func (fw *SSTWriter) Close() error {
	if fw.fw == nil {
		return nil
	}
	if err := fw.fw.Close(); err != nil {
		return err
	}
	fw.fw = nil
	return nil
}

type memFile struct {
	data []byte
	pos  int
}

func newMemFile(content []byte) *memFile {
	return &memFile{data: content}
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
