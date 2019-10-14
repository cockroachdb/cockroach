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
	"context"
	"fmt"
	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
)

// MVCCComparer is a pebble.Comparer object that implements MVCC-specific
// comparator settings for use with Pebble.
var MVCCComparer = &pebble.Comparer{
	Compare: MVCCKeyCompare,

	AbbreviatedKey: func(k []byte) uint64 {
		key, _, ok := enginepb.SplitMVCCKey(k)
		if !ok {
			return 0
		}
		return pebble.DefaultComparer.AbbreviatedKey(key)
	},

	Format: func(k []byte) fmt.Formatter {
		decoded, err := DecodeMVCCKey(k)
		if err != nil {
			return mvccKeyFormatter{err: err}
		}
		return mvccKeyFormatter{key: decoded}
	},

	// TODO(itsbilal): Improve separator to shorten index blocks in SSTables.
	// Current implementation mimics what we use with RocksDB.
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

// MVCCMerger is a pebble.Merger object that implements the merge operator used
// by Cockroach.
var MVCCMerger = &pebble.Merger{
	Name: "cockroach_merge_operator",

	Merge: func(key, oldValue, newValue, buf []byte) []byte {
		// TODO(itsbilal): Port the merge operator from C++ to Go.
		// Until then, call the C++ merge operator directly.
		ret, err := goMerge(oldValue, newValue)
		if err != nil {
			return nil
		}
		return ret
	},
}

// Pebble is a wrapper around a Pebble database instance.
type Pebble struct {
	db *pebble.DB

	closed bool
	path   string

	// Relevant options copied over from pebble.Options.
	fs       vfs.FS
	readOnly bool
}

var _ WithSSTables = &Pebble{}

// NewPebble creates a new Pebble instance, at the specified path.
func NewPebble(path string, cfg *pebble.Options) (*Pebble, error) {
	cfg.Comparer = MVCCComparer
	cfg.Merger = MVCCMerger

	// pebble.Open also calls EnsureDefaults, but only after doing a clone. Call
	// EnsureDefaults beforehand so we have a matching cfg here for when we save
	// cfg.FS and cfg.ReadOnly later on.
	cfg.EnsureDefaults()

	db, err := pebble.Open(path, cfg)
	if err != nil {
		return nil, err
	}

	return &Pebble{
		db:       db,
		path:     path,
		fs:       cfg.FS,
		readOnly: cfg.ReadOnly,
	}, nil
}

// Close implements the Engine interface.
func (p *Pebble) Close() {
	p.closed = true

	if p.readOnly {
		// Don't close the underlying handle; the non-ReadOnly instance will handle
		// that.
		return
	}
	_ = p.db.Close()
}

// Closed implements the Engine interface.
func (p *Pebble) Closed() bool {
	return p.closed
}

// Get implements the Engine interface.
func (p *Pebble) Get(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	ret, err := p.db.Get(EncodeKey(key))
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// GetProto implements the Engine interface.
func (p *Pebble) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		return false, 0, 0, emptyKeyError()
	}
	val, err := p.Get(key)
	if err != nil || val == nil {
		return false, 0, 0, err
	}

	err = protoutil.Unmarshal(val, msg)
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Helper function to implement Iterate() on Pebble, pebbleSnapshot,
// and pebbleBatch.
func iterateOnReader(
	reader Reader, start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error),
) error {
	if reader.Closed() {
		return errors.New("cannot call Iterate on a closed batch")
	}
	if !start.Less(end) {
		return nil
	}

	it := reader.NewIterator(IterOptions{UpperBound: end.Key})
	defer it.Close()

	it.Seek(start)
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}

		if done, err := f(MVCCKeyValue{Key: it.Key(), Value: it.Value()}); done || err != nil {
			return err
		}
	}
	return nil
}

// Iterate implements the Engine interface.
func (p *Pebble) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error)) error {
	return iterateOnReader(p, start, end, f)
}

// NewIterator implements the Engine interface.
func (p *Pebble) NewIterator(opts IterOptions) Iterator {
	iter := newPebbleIterator(p.db, opts)
	if iter == nil {
		panic("couldn't create a new iterator")
	}
	return iter
}

// ApplyBatchRepr implements the Engine interface.
func (p *Pebble) ApplyBatchRepr(repr []byte, sync bool) error {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}
	// batch.SetRepr takes ownership of the underlying slice, so make a copy.
	reprCopy := make([]byte, len(repr))
	copy(reprCopy, repr)

	batch := p.db.NewBatch()
	if err := batch.SetRepr(reprCopy); err != nil {
		return err
	}

	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return batch.Commit(opts)
}

// Clear implements the Engine interface.
func (p *Pebble) Clear(key MVCCKey) error {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	return p.db.Delete(EncodeKey(key), pebble.Sync)
}

// SingleClear implements the Engine interface.
func (p *Pebble) SingleClear(key MVCCKey) error {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	return p.db.SingleDelete(EncodeKey(key), pebble.Sync)
}

// ClearRange implements the Engine interface.
func (p *Pebble) ClearRange(start, end MVCCKey) error {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}

	bufStart := EncodeKey(start)
	bufEnd := EncodeKey(end)
	return p.db.DeleteRange(bufStart, bufEnd, pebble.Sync)
}

// ClearIterRange implements the Engine interface.
func (p *Pebble) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}

	pebbleIter, ok := iter.(*pebbleIterator)
	if !ok {
		return errors.Errorf("%T is not a pebble iterator", iter)
	}
	// Note that this method has the side effect of modifying iter's bounds.
	// Since all calls to `ClearIterRange` are on new throwaway iterators, this
	// should be fine.
	pebbleIter.lowerBoundBuf = EncodeKeyToBuf(pebbleIter.lowerBoundBuf[:0], start)
	pebbleIter.options.LowerBound = pebbleIter.lowerBoundBuf
	pebbleIter.upperBoundBuf = EncodeKeyToBuf(pebbleIter.upperBoundBuf[:0], end)
	pebbleIter.options.UpperBound = pebbleIter.upperBoundBuf
	pebbleIter.iter.SetBounds(pebbleIter.lowerBoundBuf, pebbleIter.upperBoundBuf)

	pebbleIter.Seek(start)
	for ; ; pebbleIter.Next() {
		ok, err := pebbleIter.Valid()
		if err != nil {
			return err
		} else if !ok || !pebbleIter.UnsafeKey().Less(end) {
			break
		}

		err = p.db.Delete(pebbleIter.iter.Key(), pebble.Sync)
		if err != nil {
			return err
		}
	}

	return nil
}

// Merge implements the Engine interface.
func (p *Pebble) Merge(key MVCCKey, value []byte) error {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	return p.db.Merge(EncodeKey(key), value, pebble.Sync)
}

// Put implements the Engine interface.
func (p *Pebble) Put(key MVCCKey, value []byte) error {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	return p.db.Set(EncodeKey(key), value, pebble.Sync)
}

// LogData implements the Engine interface.
func (p *Pebble) LogData(data []byte) error {
	return p.db.LogData(data, pebble.Sync)
}

// LogLogicalOp implements the Engine interface.
func (p *Pebble) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// Attrs implements the Engine interface.
func (p *Pebble) Attrs() roachpb.Attributes {
	// TODO(itsbilal): Implement this.
	return roachpb.Attributes{}
}

// Capacity implements the Engine interface.
func (p *Pebble) Capacity() (roachpb.StoreCapacity, error) {
	// Pebble doesn't have a capacity limiting parameter, so pass 0 for
	// maxSizeBytes to denote no limit.
	return computeCapacity(p.path, 0)
}

// Flush implements the Engine interface.
func (p *Pebble) Flush() error {
	return p.db.Flush()
}

// GetStats implements the Engine interface.
func (p *Pebble) GetStats() (*Stats, error) {
	// TODO(itsbilal): Implement this.
	return &Stats{}, nil
}

// GetEnvStats implements the Engine interface.
func (p *Pebble) GetEnvStats() (*EnvStats, error) {
	// TODO(itsbilal): Implement this.
	return &EnvStats{}, nil
}

// GetAuxiliaryDir implements the Engine interface.
func (p *Pebble) GetAuxiliaryDir() string {
	// Suggest an auxiliary subdirectory within the pebble data path.
	return p.fs.PathJoin(p.path, "auxiliary")
}

// NewBatch implements the Engine interface.
func (p *Pebble) NewBatch() Batch {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}
	return newPebbleBatch(p.db.NewIndexedBatch())
}

// NewReadOnly implements the Engine interface.
func (p *Pebble) NewReadOnly() ReadWriter {
	peb := &Pebble{
		db:       p.db,
		path:     p.path,
		readOnly: true,
		fs:       p.fs,
	}
	return peb
}

// NewWriteOnlyBatch implements the Engine interface.
func (p *Pebble) NewWriteOnlyBatch() Batch {
	if p.readOnly {
		panic("write operation called on read-only pebble instance")
	}
	return newPebbleBatch(p.db.NewBatch())
}

// NewSnapshot implements the Engine interface.
func (p *Pebble) NewSnapshot() Reader {
	return &pebbleSnapshot{
		snapshot: p.db.NewSnapshot(),
	}
}

// IngestExternalFiles implements the Engine interface.
func (p *Pebble) IngestExternalFiles(
	ctx context.Context, paths []string, skipWritingSeqNo, allowFileModifications bool,
) error {
	return p.db.Ingest(paths)
}

// PreIngestDelay implements the Engine interface.
func (p *Pebble) PreIngestDelay(_ context.Context) {
	// TODO(itsbilal): See if we need to add pre-ingestion delays, similar to
	// how we do with rocksdb.
}

// ApproximateDiskBytes implements the Engine interface.
func (p *Pebble) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	// TODO(itsbilal): Add functionality in Pebble to do this count internally,
	// instead of iterating over the range.
	count := uint64(0)
	_ = p.Iterate(MVCCKey{from, hlc.Timestamp{}}, MVCCKey{to, hlc.Timestamp{}}, func(kv MVCCKeyValue) (bool, error) {
		count += uint64(kv.Key.Len() + len(kv.Value))
		return false, nil
	})
	return count, nil
}

// CompactRange implements the Engine interface.
func (p *Pebble) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	bufStart := EncodeKey(MVCCKey{start, hlc.Timestamp{}})
	bufEnd := EncodeKey(MVCCKey{end, hlc.Timestamp{}})
	return p.db.Compact(bufStart, bufEnd)
}

// OpenFile implements the Engine interface.
func (p *Pebble) OpenFile(filename string) (DBFile, error) {
	file, err := p.fs.Open(p.fs.PathJoin(p.path, filename))
	if err != nil {
		return nil, err
	}

	pebbleFile := &pebbleFile{
		file: file,
	}
	return pebbleFile, nil
}

// ReadFile implements the Engine interface.
func (p *Pebble) ReadFile(filename string) ([]byte, error) {
	file, err := p.fs.Open(p.fs.PathJoin(p.path, filename))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return ioutil.ReadAll(file)
}

// DeleteFile implements the Engine interface.
func (p *Pebble) DeleteFile(filename string) error {
	return p.fs.Remove(filename)
}

// DeleteDirAndFiles implements the Engine interface.
func (p *Pebble) DeleteDirAndFiles(dir string) error {
	// TODO(itsbilal): Implement FS.RemoveAll then call that here instead.
	files, err := p.fs.List(dir)
	if err != nil {
		return err
	}

	// Recurse through all files, calling DeleteFile or DeleteDirAndFiles as
	// appropriate.
	for _, filename := range files {
		path := p.fs.PathJoin(dir, filename)
		stat, err := p.fs.Stat(path)
		if err != nil {
			return err
		}

		if stat.IsDir() {
			err = p.DeleteDirAndFiles(path)
		} else {
			err = p.DeleteFile(path)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// LinkFile implements the Engine interface.
func (p *Pebble) LinkFile(oldname, newname string) error {
	oldPath := p.fs.PathJoin(p.path, oldname)
	newPath := p.fs.PathJoin(p.path, newname)
	return p.fs.Link(oldPath, newPath)
}

// CreateCheckpoint implements the Engine interface.
func (p *Pebble) CreateCheckpoint(dir string) error {
	return p.db.Checkpoint(dir)
}

// GetSSTables implements the WithSSTables interface.
func (p *Pebble) GetSSTables() (sstables SSTableInfos) {
	for level, tables := range p.db.SSTables() {
		for _, table := range tables {
			startKey, _ := DecodeMVCCKey(table.Smallest.UserKey)
			endKey, _ := DecodeMVCCKey(table.Largest.UserKey)
			info := SSTableInfo{
				Level: level,
				Size:  int64(table.Size),
				Start: startKey,
				End:   endKey,
			}
			sstables = append(sstables, info)
		}
	}
	return sstables
}

// pebbleFile wraps a pebble File and implements the DBFile interface.
type pebbleFile struct {
	file vfs.File
}

var _ DBFile = &pebbleFile{}

// Append implements the DBFile interface.
func (p *pebbleFile) Append(data []byte) error {
	_, err := p.file.Write(data)
	return err
}

// Close implements the DBFile interface.
func (p *pebbleFile) Close() error {
	return p.file.Close()
}

// Close implements the DBFile interface.
func (p *pebbleFile) Sync() error {
	return p.file.Sync()
}

// pebbleSnapshot represents a snapshot created using Pebble.NewSnapshot().
type pebbleSnapshot struct {
	snapshot *pebble.Snapshot
	closed   bool
}

var _ Reader = &pebbleSnapshot{}

// Close implements the Reader interface.
func (p *pebbleSnapshot) Close() {
	_ = p.snapshot.Close()
	p.closed = true
}

// Closed implements the Reader interface.
func (p *pebbleSnapshot) Closed() bool {
	return p.closed
}

// Get implements the Reader interface.
func (p *pebbleSnapshot) Get(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}

	ret, err := p.snapshot.Get(EncodeKey(key))
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// GetProto implements the Reader interface.
func (p *pebbleSnapshot) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		return false, 0, 0, emptyKeyError()
	}

	val, err := p.snapshot.Get(EncodeKey(key))
	if err != nil || val == nil {
		return false, 0, 0, err
	}

	err = protoutil.Unmarshal(val, msg)
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Iterate implements the Reader interface.
func (p *pebbleSnapshot) Iterate(
	start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(p, start, end, f)
}

// NewIterator implements the Reader interface.
func (p pebbleSnapshot) NewIterator(opts IterOptions) Iterator {
	return newPebbleIterator(p.snapshot, opts)
}
