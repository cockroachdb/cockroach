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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		res, err := merge(key, oldValue, newValue, buf)
		if err != nil {
			log.Fatalf(context.Background(), "merge: %v", err)
		}
		return res
	},
}

// pebbleTimeBoundPropCollector implements a property collector for MVCC
// Timestamps.  Its behavior matches TimeBoundTblPropCollector in
// table_props.cc.
type pebbleTimeBoundPropCollector struct {
	min, max []byte
}

func (t *pebbleTimeBoundPropCollector) Add(key pebble.InternalKey, value []byte) error {
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

func (t *pebbleTimeBoundPropCollector) Finish(userProps map[string]string) error {
	userProps["crdb.ts.min"] = string(t.min)
	userProps["crdb.ts.max"] = string(t.max)
	return nil
}

func (t *pebbleTimeBoundPropCollector) Name() string {
	// This constant needs to match the one used by the RocksDB version of this
	// table property collector. DO NOT CHANGE.
	return "TimeBoundTblPropCollectorFactory"
}

// pebbleDeleteRangeCollector marks an sstable for compaction that contains a
// range tombstone.
type pebbleDeleteRangeCollector struct{}

func (pebbleDeleteRangeCollector) Add(key pebble.InternalKey, value []byte) error {
	// TODO(peter): track whether a range tombstone is present. Need to extend
	// the TablePropertyCollector interface.
	return nil
}

func (pebbleDeleteRangeCollector) Finish(userProps map[string]string) error {
	return nil
}

func (pebbleDeleteRangeCollector) Name() string {
	// This constant needs to match the one used by the RocksDB version of this
	// table property collector. DO NOT CHANGE.
	return "DeleteRangeTblPropCollectorFactory"
}

// PebbleTablePropertyCollectors is the list of Pebble TablePropertyCollectors.
var PebbleTablePropertyCollectors = []func() pebble.TablePropertyCollector{
	func() pebble.TablePropertyCollector { return &pebbleTimeBoundPropCollector{} },
	func() pebble.TablePropertyCollector { return &pebbleDeleteRangeCollector{} },
}

// Pebble is a wrapper around a Pebble database instance.
type Pebble struct {
	db *pebble.DB

	closed bool
	path   string
	attrs  roachpb.Attributes

	// Relevant options copied over from pebble.Options.
	fs vfs.FS
}

var _ WithSSTables = &Pebble{}

// NewPebble creates a new Pebble instance, at the specified path.
func NewPebble(path string, cfg *pebble.Options) (*Pebble, error) {
	cfg.Comparer = MVCCComparer
	cfg.Merger = MVCCMerger
	cfg.TablePropertyCollectors = PebbleTablePropertyCollectors

	// pebble.Open also calls EnsureDefaults, but only after doing a clone. Call
	// EnsureDefaults beforehand so we have a matching cfg here for when we save
	// cfg.FS and cfg.ReadOnly later on.
	cfg.EnsureDefaults()

	db, err := pebble.Open(path, cfg)
	if err != nil {
		return nil, err
	}

	return &Pebble{
		db:   db,
		path: path,
		fs:   cfg.FS,
	}, nil
}

// Close implements the Engine interface.
func (p *Pebble) Close() {
	p.closed = true
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
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Delete(EncodeKey(key), pebble.Sync)
}

// SingleClear implements the Engine interface.
func (p *Pebble) SingleClear(key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.SingleDelete(EncodeKey(key), pebble.Sync)
}

// ClearRange implements the Engine interface.
func (p *Pebble) ClearRange(start, end MVCCKey) error {
	bufStart := EncodeKey(start)
	bufEnd := EncodeKey(end)
	return p.db.DeleteRange(bufStart, bufEnd, pebble.Sync)
}

// ClearIterRange implements the Engine interface.
func (p *Pebble) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	// Write all the tombstones in one batch.
	batch := p.NewWriteOnlyBatch()
	defer batch.Close()

	if err := batch.ClearIterRange(iter, start, end); err != nil {
		return err
	}
	return batch.Commit(true)
}

// Merge implements the Engine interface.
func (p *Pebble) Merge(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Merge(EncodeKey(key), value, pebble.Sync)
}

// Put implements the Engine interface.
func (p *Pebble) Put(key MVCCKey, value []byte) error {
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

// SetAttrs sets the attributes returned by Atts(). This method is not safe for
// concurrent use.
func (p *Pebble) SetAttrs(attrs roachpb.Attributes) {
	p.attrs = attrs
}

// Attrs implements the Engine interface.
func (p *Pebble) Attrs() roachpb.Attributes {
	return p.attrs
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
	return newPebbleBatch(p.db, p.db.NewIndexedBatch())
}

// NewReadOnly implements the Engine interface.
func (p *Pebble) NewReadOnly() ReadWriter {
	return &pebbleReadOnly{
		parent: p,
	}
}

// NewWriteOnlyBatch implements the Engine interface.
func (p *Pebble) NewWriteOnlyBatch() Batch {
	return newPebbleBatch(p.db, p.db.NewBatch())
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
	// TODO(itsbilal): Create does not currently truncate the file if it already
	// exists. OpenFile in RocksDB truncates a file if it already exists. Unify
	// behavior by adding a CreateWithTruncate functionality to pebble's vfs.FS.
	return p.fs.Create(filename)
}

// ReadFile implements the Engine interface.
func (p *Pebble) ReadFile(filename string) ([]byte, error) {
	file, err := p.fs.Open(filename)
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
	return p.fs.Link(oldname, newname)
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

type pebbleReadOnly struct {
	parent *Pebble
	iter   pebbleIterator
	closed bool
}

var _ ReadWriter = &pebbleReadOnly{}

func (p *pebbleReadOnly) Close() {
	if p.closed {
		panic("closing an already-closed pebbleReadOnly")
	}
	p.closed = true
	p.iter.destroy()
}

func (p *pebbleReadOnly) Closed() bool {
	return p.closed
}

func (p *pebbleReadOnly) Get(key MVCCKey) ([]byte, error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	return p.parent.Get(key)
}

func (p *pebbleReadOnly) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	return p.parent.GetProto(key, msg)
}

func (p *pebbleReadOnly) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	return p.parent.Iterate(start, end, f)
}

func (p *pebbleReadOnly) NewIterator(opts IterOptions) Iterator {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		return newPebbleIterator(p.parent.db, opts)
	}

	if p.iter.inuse {
		panic("iterator already in use")
	}
	p.iter.inuse = true
	p.iter.reusable = true

	if p.iter.iter != nil {
		p.iter.setOptions(opts)
	} else {
		p.iter.init(p.parent.db, opts)
	}
	return &p.iter
}

// Writer methods are not implemented for pebbleReadOnly. Ideally, the code
// could be refactored so that a Reader could be supplied to evaluateBatch

// Writer is the write interface to an engine's data.
func (p *pebbleReadOnly) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) Clear(key MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) SingleClear(key MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearRange(start, end MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) Merge(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) Put(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) LogData(data []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	panic("not implemented")
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
