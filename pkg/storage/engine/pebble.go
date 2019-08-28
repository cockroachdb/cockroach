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

import "C"

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
)

const (
	MaxItersBeforeSeek = 10
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

// Pebble is a wrapper around a Pebble database instance.
type Pebble struct {
	db *pebble.DB

	// buffer for MVCC key encodings/decodings.
	buf []byte

	closed bool
	path string
	opts *pebble.Options
}

var _ Engine = &Pebble{}

// NewPebble creates a new Pebble instance, at the specified path.
func NewPebble(path string, cfg *pebble.Options) (*Pebble, error) {
	cfg.Comparer = MVCCComparer
	cfg.Merger = &pebble.Merger{
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

	db, err := pebble.Open(path, cfg)

	if err != nil {
		return nil, err
	}

	return &Pebble{
		db: db,
		closed: false,
		path: path,
		opts: cfg,
	}, nil
}

// Close implements the Engine interface.
func (p *Pebble) Close() {
	if p.opts.ReadOnly {
		// No op
		return
	}
	_ = p.db.Close()
	p.closed = true
}

// Closed implements the Engine interface.
func (p *Pebble) Closed() bool {
	return p.closed
}

// Get implements the Engine interface.
func (p *Pebble) Get(key MVCCKey) ([]byte, error) {
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	ret, err := p.db.Get(p.buf)
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// GetProto implements the Engine interface.
func (p *Pebble) GetProto(key MVCCKey, msg protoutil.Message) (ok bool, keyBytes, valBytes int64, err error) {
	val, err := p.Get(key)
	if err != nil || val == nil {
		return
	}

	ok = true
	if msg != nil {
		err = protoutil.Unmarshal(val, msg)
	}
	keyBytes = int64(len(p.buf))
	valBytes = int64(len(val))
	return
}

// Iterate implements the Engine interface.
func (p *Pebble) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error)) error {
	if p.closed {
		return errors.New("cannot call Iterate on a closed db")
	}
	if !start.Less(end) {
		return nil
	}

	it := newPebbleIterator(p, p.db, IterOptions{UpperBound: end.Key})
	defer it.Close()

	it.Seek(start)
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}

		k := it.Key()
		if !k.Less(end) {
			break
		}
		if done, err := f(MVCCKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	return nil
}

// NewIterator implements the Engine interface.
func (p *Pebble) NewIterator(opts IterOptions) Iterator {
	if p.closed {
		panic("tried to iterate on closed handle")
	}

	iter := newPebbleIterator(p, p.db, opts)
	if iter == nil {
		panic("couldn't create a new iterator")
	}
	return iter
}

// ApplyBatchRepr implements the Engine interface.
func (p *Pebble) ApplyBatchRepr(repr []byte, sync bool) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	batch := p.db.NewBatch()
	if err := batch.SetRepr(repr); err != nil {
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
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.db.Delete(p.buf, pebble.Sync)
}

// SingleClear implements the Engine interface.
func (p *Pebble) SingleClear(key MVCCKey) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.db.SingleDelete(p.buf, pebble.Sync)
}

// ClearRange implements the Engine interface.
func (p *Pebble) ClearRange(start, end MVCCKey) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], start)
	bufEnd := EncodeKey(end)
	return p.db.DeleteRange(p.buf, bufEnd, pebble.Sync)
}

// ClearIterRange implements the Engine interface.
func (p *Pebble) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	pebbleIter, ok := iter.(*pebbleIterator)
	if !ok {
		return errors.Errorf("%T is not a Pebble iterator", iter)
	}
	pebbleIter.Seek(start)
	for ; ; pebbleIter.Next() {
		ok, err := pebbleIter.Valid()
		if err != nil {
			return err
		} else if !ok {
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
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.db.Merge(p.buf, value, pebble.Sync)
}

// Put implements the Engine interface.
func (p *Pebble) Put(key MVCCKey, value []byte) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.db.Set(p.buf, value, pebble.Sync)
}

// LogData implements the Engine interface.
func (p *Pebble) LogData(data []byte) error {
	return p.db.LogData(p.buf, pebble.Sync)
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
	fileSystemUsage := gosigar.FileSystemUsage{}
	dir := p.path
	memTableSize := int64(p.opts.MemTableSize)
	if dir == "" {
		// This is an in-memory instance. Pretend we're empty since we
		// don't know better and only use this for testing. Using any
		// part of the actual file system here can throw off allocator
		// rebalancing in a hard-to-trace manner. See #7050.
		return roachpb.StoreCapacity{
			Capacity:  memTableSize,
			Available: memTableSize,
		}, nil
	}
	if err := fileSystemUsage.Get(dir); err != nil {
		return roachpb.StoreCapacity{}, err
	}

	if fileSystemUsage.Total > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Total), humanizeutil.IBytes(math.MaxInt64))
	}
	if fileSystemUsage.Avail > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Avail), humanizeutil.IBytes(math.MaxInt64))
	}
	fsuTotal := int64(fileSystemUsage.Total)
	fsuAvail := int64(fileSystemUsage.Avail)

	// Find the total size of all the files in the r.dir and all its
	// subdirectories.
	var totalUsedBytes int64
	if errOuter := filepath.Walk(p.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// This can happen if rocksdb removes files out from under us - just keep
			// going to get the best estimate we can.
			if os.IsNotExist(err) {
				return nil
			}
			// Special-case: if the store-dir is configured using the root of some fs,
			// e.g. "/mnt/db", we might have special fs-created files like lost+found
			// that we can't read, so just ignore them rather than crashing.
			if os.IsPermission(err) && filepath.Base(path) == "lost+found" {
				return nil
			}
			return err
		}
		if info.Mode().IsRegular() {
			totalUsedBytes += info.Size()
		}
		return nil
	}); errOuter != nil {
		return roachpb.StoreCapacity{}, errOuter
	}

	// If no size limitation have been placed on the store size or if the
	// limitation is greater than what's available, just return the actual
	// totals.
	if memTableSize == 0 || memTableSize >= fsuTotal || p.path == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
			Used:      totalUsedBytes,
		}, nil
	}

	available := memTableSize - totalUsedBytes
	if available > fsuAvail {
		available = fsuAvail
	}
	if available < 0 {
		available = 0
	}

	return roachpb.StoreCapacity{
		Capacity:  memTableSize,
		Available: available,
		Used:      totalUsedBytes,
	}, nil
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
	// Assuming the main Pebble directory works for ingesting files.
	return p.path
}

// NewBatch implements the Engine interface.
func (p *Pebble) NewBatch() Batch {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}
	return newPebbleBatch(p, false)
}

// NewReadOnly implements the Engine interface.
func (p *Pebble) NewReadOnly() ReadWriter {
	pebbleOpts := *p.opts
	pebbleOpts.ReadOnly = true
	peb := &Pebble{
		db:     p.db,
		closed: false,
		path:   p.path,
		opts:   &pebbleOpts,
	}
	return peb
}

// NewWriteOnlyBatch implements the Engine interface.
func (p *Pebble) NewWriteOnlyBatch() Batch {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}
	return newPebbleBatch(p, true)
}

// NewSnapshot implements the Engine interface.
func (p *Pebble) NewSnapshot() Reader {
	return &pebbleSnapshot{
		parent:   p,
		snapshot: p.db.NewSnapshot(),
	}
}

// IngestExternalFiles implements the Engine interface.
func (p *Pebble) IngestExternalFiles(ctx context.Context, paths []string, skipWritingSeqNo, allowFileModifications bool) error {
	return p.db.Ingest(paths)
}

// PreIngestDelay implements the Engine interface.
func (p *Pebble) PreIngestDelay(_ context.Context) {
	// This is a RocksDB-ism. Pebble takes care of any ingestion-induced waits.
	return
}

// ApproximateDiskBytes implements the Engine interface.
func (p *Pebble) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	count := uint64(0)
	_ = p.Iterate(MVCCKey{from, hlc.Timestamp{}}, MVCCKey{to, hlc.Timestamp{}}, func (kv MVCCKeyValue) (bool, error) {
		count += uint64(kv.Key.Len() + len(kv.Value))
		return false, nil
	})
	return count, nil
}

// CompactRange implements the Engine interface.
func (p *Pebble) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	p.buf = EncodeKeyToBuf(p.buf[:0], MVCCKey{start, hlc.Timestamp{}})
	buf2 := EncodeKey(MVCCKey{end, hlc.Timestamp{}})
	return p.db.Compact(p.buf, buf2)
}

// OpenFile implements the Engine interface.
func (p *Pebble) OpenFile(filename string) (DBFile, error) {
	fs := p.opts.FS
	file, err := fs.Open(path.Join(p.path, filename))
	if err != nil {
		return nil, err
	}

	pebbleFile := &pebbleFile{
		parent: p,
		file:   file,
	}
	return pebbleFile, nil
}

// ReadFile implements the Engine interface.
func (p *Pebble) ReadFile(filename string) ([]byte, error) {
	fs := p.opts.FS
	file, err := fs.Open(path.Join(p.path, filename))
	if err != nil {
		return nil, err
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	buf := make([]byte, fileInfo.Size())
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[:n]

	return buf, nil
}

// DeleteFile implements the Engine interface.
func (p *Pebble) DeleteFile(filename string) error {
	fs := p.opts.FS
	filePath := path.Join(p.path, filename)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return os.ErrNotExist
	}
	return fs.Remove(filePath)
}

// DeleteDirAndFiles implements the Engine interface.
func (p *Pebble) DeleteDirAndFiles(dir string) error {
	// Same behaviour as DeleteFile, except with directory specified.
	return p.DeleteFile(dir)
}

// LinkFile implements the Engine interface.
func (p *Pebble) LinkFile(oldname, newname string) error {
	fs := p.opts.FS
	oldPath := path.Join(p.path, oldname)
	newPath := path.Join(p.path, newname)
	return fs.Link(oldPath, newPath)
}

// CreateCheckpoint implements the Engine interface.
func (p *Pebble) CreateCheckpoint(_ string) error {
	// No-op for now: Pebble does not implement checkpoints.
	//
	// TODO(itsbilal): Update this when Pebble implements checkpoints:
	// https://github.com/cockroachdb/pebble/issues/304
	return nil
}

// pebbleFile wraps a pebble File and implements the DBFile interface.
type pebbleFile struct {
	parent   *Pebble
	file     vfs.File
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
	parent   *Pebble
	snapshot *pebble.Snapshot
	buf      []byte
	closed   bool
}

var _ Reader = &pebbleSnapshot{}

// Close implements the Reader interface.
func (p *pebbleSnapshot) Close() {
	p.snapshot.Close()
	p.closed = true
}

// Closed implements the Reader interface.
func (p *pebbleSnapshot) Closed() bool {
	return p.closed
}

// Get implements the Reader interface.
func (p *pebbleSnapshot) Get(key MVCCKey) ([]byte, error) {
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	ret, err := p.snapshot.Get(p.buf)
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// GetProto implements the Reader interface.
func (p *pebbleSnapshot) GetProto(key MVCCKey, msg protoutil.Message) (ok bool, keyBytes, valBytes int64, err error) {
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	val, err := p.snapshot.Get(p.buf)
	if err != nil || val == nil {
		return
	}

	ok = true
	if msg != nil {
		err = protoutil.Unmarshal(val, msg)
	}
	keyBytes = int64(len(p.buf))
	valBytes = int64(len(val))
	return
}

// Iterate implements the Reader interface.
func (p *pebbleSnapshot) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error)) error {
	if p.closed {
		return errors.New("cannot call Iterate on a closed batch")
	}
	if !start.Less(end) {
		return nil
	}

	it := p.NewIterator(IterOptions{UpperBound: end.Key})
	defer it.Close()

	it.Seek(start)
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}

		k := it.Key()
		if !k.Less(end) {
			break
		}
		if done, err := f(MVCCKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	return nil
}

// NewIterator implements the Reader interface.
func (p pebbleSnapshot) NewIterator(opts IterOptions) Iterator {
	return newPebbleIterator(p.parent, p.snapshot, opts)
}
