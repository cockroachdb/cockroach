// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/golang/leveldb/db"
	"github.com/golang/leveldb/table"
	"github.com/pkg/errors"
)

var readerOpts = &db.Options{
	Comparer: cockroachComparer{},
}

type sstIterator struct {
	sst  *table.Reader
	iter db.Iterator

	valid   bool
	err     error
	mvccKey MVCCKey

	// For allocation avoidance in NextKey.
	nextKeyStart []byte

	// roachpb.Verify k/v pairs on each call to Next()
	verify bool
}

var _ SimpleIterator = &sstIterator{}

// NewSSTIterator returns a SimpleIterator for a leveldb formatted sstable on
// disk. It's compatible with sstables output by RocksDBSstFileWriter,
// which means the keys are CockroachDB mvcc keys and they each have the RocksDB
// trailer (of seqno & value type).
func NewSSTIterator(path string) (SimpleIterator, error) {
	file, err := db.DefaultFileSystem.Open(path)
	if err != nil {
		return nil, err
	}
	return &sstIterator{sst: table.NewReader(file, readerOpts)}, nil
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
	*bytes.Reader
	size memFileInfo
}

var _ db.File = &memFile{}

func newMemFile(content []byte) *memFile {
	return &memFile{Reader: bytes.NewReader(content), size: memFileInfo(len(content))}
}

func (*memFile) Close() error {
	return nil
}

func (*memFile) Write(_ []byte) (int, error) {
	panic("write unsupported")
}

func (f *memFile) Stat() (os.FileInfo, error) {
	return f.size, nil
}

func (*memFile) Sync() error {
	return nil
}

// NewMemSSTIterator returns a SimpleIterator for a leveldb format sstable in
// memory. It's compatible with sstables output by RocksDBSstFileWriter,
// which means the keys are CockroachDB mvcc keys and they each have the RocksDB
// trailer (of seqno & value type).
func NewMemSSTIterator(data []byte, verify bool) (SimpleIterator, error) {
	return &sstIterator{sst: table.NewReader(newMemFile(data), readerOpts), verify: verify}, nil
}

// Close implements the SimpleIterator interface.
func (r *sstIterator) Close() {
	if r.iter != nil {
		r.err = errors.Wrap(r.iter.Close(), "closing sstable iterator")
	}
	if err := r.sst.Close(); err != nil && r.err == nil {
		r.err = errors.Wrap(err, "closing sstable")
	}
}

// encodeInternalSeekKey encodes an engine.MVCCKey into the RocksDB
// representation and adds padding to the end such that it compares correctly
// with rocksdb "internal" keys which have an 8b suffix, which appear in SSTs
// created by rocks when read directly with a reader like LevelDB's Reader.
func encodeInternalSeekKey(key MVCCKey) []byte {
	return append(EncodeKey(key), []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
}

// Seek implements the SimpleIterator interface.
func (r *sstIterator) Seek(key MVCCKey) {
	if r.iter != nil {
		if r.err = errors.Wrap(r.iter.Close(), "resetting sstable iterator"); r.err != nil {
			return
		}
	}
	r.iter = r.sst.Find(encodeInternalSeekKey(key), nil)
	r.Next()
}

// Valid implements the SimpleIterator interface.
func (r *sstIterator) Valid() (bool, error) {
	return r.valid && r.err == nil, r.err
}

// Next implements the SimpleIterator interface.
func (r *sstIterator) Next() {
	if r.valid = r.iter.Next(); !r.valid {
		r.err = errors.Wrap(r.iter.Close(), "closing sstable iterator")
		r.iter = nil
		return
	}

	// RocksDB uses the last 8 bytes to pack the sequence number and value type
	// into a little-endian encoded uint64. The value type is stored in the
	// low byte and the sequence number is in the high 7 bytes. See dbformat.h.
	rocksdbInternalKey := r.iter.Key()
	if len(rocksdbInternalKey) < 8 {
		r.err = errors.Errorf("invalid rocksdb InternalKey: %x", rocksdbInternalKey)
		return
	}
	seqAndValueType := binary.LittleEndian.Uint64(rocksdbInternalKey[len(rocksdbInternalKey)-8:])
	if valueType := BatchType(seqAndValueType & 0xff); valueType != BatchTypeValue {
		r.err = errors.Errorf("value type not supported: %d", valueType)
		return
	}

	key := rocksdbInternalKey[:len(rocksdbInternalKey)-8]

	if k, ts, err := enginepb.DecodeKey(key); err == nil {
		r.mvccKey.Key = k
		r.mvccKey.Timestamp = ts
		r.err = nil
	} else {
		r.err = errors.Wrapf(err, "decoding key: %s", key)
		return
	}

	if r.verify {
		r.err = roachpb.Value{RawBytes: r.iter.Value()}.Verify(r.mvccKey.Key)
	}
}

// NextKey implements the SimpleIterator interface.
func (r *sstIterator) NextKey() {
	if !r.valid {
		return
	}
	r.nextKeyStart = append(r.nextKeyStart[:0], r.mvccKey.Key...)
	for r.Next(); r.valid && r.err == nil && bytes.Equal(r.nextKeyStart, r.mvccKey.Key); r.Next() {
	}
}

// UnsafeKey implements the SimpleIterator interface.
func (r *sstIterator) UnsafeKey() MVCCKey {
	return r.mvccKey
}

// UnsafeValue implements the SimpleIterator interface.
func (r *sstIterator) UnsafeValue() []byte {
	return r.iter.Value()
}

type cockroachComparer struct{}

var _ db.Comparer = cockroachComparer{}

// Compare implements the db.Comparer interface. This is used to compare raw SST
// keys in the sstIterator, and assumes that all keys compared are MVCC encoded
// and then wrapped by rocksdb into Internal keys.
func (cockroachComparer) Compare(a, b []byte) int {
	// We assume every key in these SSTs is a rocksdb "internal" key with an 8b
	// suffix and need to remove those to compare user keys below.
	if len(a) < 8 || len(b) < 8 {
		// Special case: either key is empty, so bytes.Compare should work.
		if len(a) == 0 || len(b) == 0 {
			return bytes.Compare(a, b)
		}
		panic(fmt.Sprintf("invalid keys: compare expects internal keys with 8b suffix: a: %v b: %v", a, b))
	}

	keyA, tsA, okA := enginepb.SplitMVCCKey(a[:len(a)-8])
	keyB, tsB, okB := enginepb.SplitMVCCKey(b[:len(b)-8])
	if !okA || !okB {
		// This should never happen unless there is some sort of corruption of
		// the keys. This is a little bizarre, but the behavior exactly matches
		// engine/db.cc:DBComparator.
		return bytes.Compare(a, b)
	}

	if c := bytes.Compare(keyA, keyB); c != 0 {
		return c
	}
	if len(tsA) == 0 {
		if len(tsB) == 0 {
			return 0
		}
		return -1
	} else if len(tsB) == 0 {
		return 1
	}
	if tsCmp := bytes.Compare(tsB, tsA); tsCmp != 0 {
		return tsCmp
	}
	// If decoded MVCC keys are the same, fallback to comparing raw internal keys
	// in case the internal suffix differentiates them.
	return bytes.Compare(a, b)
}

func (cockroachComparer) Name() string {
	// This must match the name in engine/db.cc:DBComparator::Name.
	return "cockroach_comparator"
}

func (cockroachComparer) AppendSeparator(dst, a, b []byte) []byte {
	panic("unimplemented")
}
