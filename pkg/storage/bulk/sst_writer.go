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
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/golang/leveldb/db"
	pebbledb "github.com/petermattis/pebble/db"
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

var mvccComparer = &pebbledb.Comparer{
	Compare: engine.MVCCKeyCompare,
	AbbreviatedKey: func(k []byte) uint64 {
		key, _, ok := enginepb.SplitMVCCKey(k)
		if !ok {
			return 0
		}
		return pebbledb.DefaultComparer.AbbreviatedKey(key)
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
		tsLen := int(k[len(k)-1])
		keyPartEnd := len(k) - 1 - tsLen
		if keyPartEnd < 0 {
			return len(k)
		}
		return keyPartEnd
	},

	Name: "cockroach_comparator",
}

var pebbleOpts = func() *pebbledb.Options {
	opts := &pebbledb.Options{
		TableFormat: pebbledb.TableFormatLevelDB,
		Comparer:    mvccComparer,
	}
	opts.EnsureDefaults()
	return opts
}()

// MakeSSTWriter creates a new SSTWriter.
func MakeSSTWriter() SSTWriter {
	f := newMemFile(nil)
	sst := sstable.NewWriter(f, pebbleOpts, pebbledb.LevelOptions{})
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

type memFileInfo int64

var _ os.FileInfo = memFileInfo(0)

func (i memFileInfo) Size() int64 {
	return int64(i)
}

func (memFileInfo) IsDir() bool {
	return false
}

func (memFileInfo) Name() string {
	panic("Name unsupported")
}

func (memFileInfo) Mode() os.FileMode {
	panic("Mode unsupported")
}

func (memFileInfo) ModTime() time.Time {
	panic("ModTime unsupported")
}

func (memFileInfo) Sys() interface{} {
	panic("Sys unsupported")
}

type memFile struct {
	data []byte
	pos  int
}

var _ db.File = &memFile{}

func newMemFile(content []byte) *memFile {
	return &memFile{data: content}
}

func (*memFile) Close() error {
	return nil
}

func (f *memFile) Stat() (os.FileInfo, error) {
	return memFileInfo(len(f.data)), nil
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
