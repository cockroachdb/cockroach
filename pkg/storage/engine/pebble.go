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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	"golang.org/x/tools/container/intsets"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
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

func (p *Pebble) Close() {
	if p.opts.ReadOnly {
		// No op
		return
	}
	_ = p.db.Close()
	p.closed = true
}

func (p *Pebble) Closed() bool {
	return p.closed
}

func (p *Pebble) Get(key MVCCKey) ([]byte, error) {
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	ret, err := p.db.Get(p.buf)
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

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

func (p *Pebble) Clear(key MVCCKey) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	err := p.db.Delete(p.buf, pebble.Sync)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func (p *Pebble) SingleClear(key MVCCKey) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	err := p.db.SingleDelete(p.buf, pebble.Sync)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func (p *Pebble) ClearRange(start, end MVCCKey) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], start)
	bufEnd := EncodeKey(end)
	return p.db.DeleteRange(p.buf, bufEnd, pebble.Sync)
}

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

func (p *Pebble) Merge(key MVCCKey, value []byte) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.db.Merge(p.buf, value, pebble.Sync)
}

func (p *Pebble) Put(key MVCCKey, value []byte) error {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.db.Set(p.buf, value, pebble.Sync)
}

func (p *Pebble) LogData(data []byte) error {
	return p.db.LogData(p.buf, pebble.Sync)
}

func (p *Pebble) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

func (p *Pebble) Attrs() roachpb.Attributes {
	// TODO(itsbilal): Implement this.
	return roachpb.Attributes{}
}

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

func (p *Pebble) Flush() error {
	return p.db.Flush()
}

func (p *Pebble) GetStats() (*Stats, error) {
	// TODO(itsbilal): Implement this.
	return &Stats{}, nil
}

func (p *Pebble) GetEnvStats() (*EnvStats, error) {
	// TODO(itsbilal): Implement this.
	return &EnvStats{}, nil
}

func (p *Pebble) GetAuxiliaryDir() string {
	return p.path
}

func (p *Pebble) NewBatch() Batch {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}
	return newPebbleBatch(p, false)
}

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

func (p *Pebble) NewWriteOnlyBatch() Batch {
	if p.opts.ReadOnly {
		panic("write operation called on read-only pebble instance")
	}
	return newPebbleBatch(p, true)
}

func (p *Pebble) NewSnapshot() Reader {
	return &pebbleSnapshot{
		parent:   p,
		snapshot: p.db.NewSnapshot(),
	}
}

func (p *Pebble) IngestExternalFiles(ctx context.Context, paths []string, skipWritingSeqNo, allowFileModifications bool) error {
	return p.db.Ingest(paths)
}

func (p *Pebble) PreIngestDelay(_ context.Context) {
	// This is a RocksDB-ism. Pebble takes care of any ingestion-induced waits.
	return
}

func (p *Pebble) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	count := uint64(0)
	_ = p.Iterate(MVCCKey{from, hlc.Timestamp{}}, MVCCKey{to, hlc.Timestamp{}}, func (kv MVCCKeyValue) (bool, error) {
		count += uint64(kv.Key.Len() + len(kv.Value))
		return false, nil
	})
	return count, nil
}

func (p *Pebble) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	p.buf = EncodeKeyToBuf(p.buf[:0], MVCCKey{start, hlc.Timestamp{}})
	buf2 := EncodeKey(MVCCKey{end, hlc.Timestamp{}})
	return p.db.Compact(p.buf, buf2)
}

func (p *Pebble) OpenFile(filename string) (DBFile, error) {
	fs := vfs.Default
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

func (p *Pebble) ReadFile(filename string) ([]byte, error) {
	fs := vfs.Default
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

func (p *Pebble) DeleteFile(filename string) error {
	fs := vfs.Default
	filePath := path.Join(p.path, filename)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return os.ErrNotExist
	}
	return fs.Remove(filePath)
}

func (p *Pebble) DeleteDirAndFiles(dir string) error {
	// Same behaviour as DeleteFile, except with directory specified.
	return p.DeleteFile(dir)
}

func (p *Pebble) LinkFile(oldname, newname string) error {
	fs := vfs.Default
	oldPath := path.Join(p.path, oldname)
	newPath := path.Join(p.path, newname)
	return fs.Link(oldPath, newPath)
}

func (p *Pebble) CreateCheckpoint(_ string) error {
	// No-op: Pebble does not implement checkpoints.
	return nil
}

type pebbleFile struct {
	parent   *Pebble
	file     vfs.File
}

var _ DBFile = &pebbleFile{}

func (p *pebbleFile) Append(data []byte) error {
	_, err := p.file.Write(data)
	return err
}

func (p *pebbleFile) Close() error {
	return p.file.Close()
}

func (p *pebbleFile) Sync() error {
	return p.file.Sync()
}

type pebbleSnapshot struct {
	parent   *Pebble
	snapshot *pebble.Snapshot
	buf      []byte
	closed   bool
}

var _ Reader = &pebbleSnapshot{}

func (p *pebbleSnapshot) Close() {
	p.snapshot.Close()
	p.closed = true
}

func (p *pebbleSnapshot) Closed() bool {
	return p.closed
}

func (p *pebbleSnapshot) Get(key MVCCKey) ([]byte, error) {
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	ret, err := p.snapshot.Get(p.buf)
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

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

func (p pebbleSnapshot) NewIterator(opts IterOptions) Iterator {
	return newPebbleIterator(p.parent, p.snapshot, opts)
}

type pebbleIterator struct {
	parent        *Pebble
	iter          *pebble.Iterator
	options       pebble.IterOptions
	err           error
	keyBuf        []byte
	lowerBoundBuf []byte
	upperBoundBuf []byte
	key           MVCCKey
	value         []byte
	prefix        bool
	mvccScanner   pebbleMvccScanner
}

var _ Iterator = &pebbleIterator{}

var pebbleIterPool = sync.Pool{
	New: func() interface{} {
		return &pebbleIterator{}
	},
}

func newPebbleIterator(parent *Pebble, handle pebble.Reader, opts IterOptions) Iterator {
	iter := pebbleIterPool.Get().(*pebbleIterator)
	iter.init(parent, handle, opts)
	return iter
}

func (p *pebbleIterator) init(parent *Pebble, handle pebble.Reader, opts IterOptions) {
	*p = pebbleIterator{
		parent: parent,
		options: pebble.IterOptions{},
		lowerBoundBuf: p.lowerBoundBuf,
		upperBoundBuf: p.upperBoundBuf,
		prefix: opts.Prefix,
		mvccScanner:   pebbleMvccScanner{},
	}

	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	if opts.LowerBound != nil {
		// This is the same as
		// p.options.LowerBound = EncodeKeyToBuf(p.lowerBoundBuf[:0], MVCCKey{Key: opts.LowerBound}) .
		// Since we are encoding zero-timestamp MVCC Keys anyway, we can just append
		// the null byte instead of calling EncodeKey which will do the same thing.
		p.lowerBoundBuf = append(p.lowerBoundBuf[:0], opts.LowerBound...)
		p.options.LowerBound = append(p.lowerBoundBuf, 0x00)
	}
	if opts.UpperBound != nil {
		// Same as above.
		p.upperBoundBuf = append(p.upperBoundBuf[:0], opts.UpperBound...)
		p.options.UpperBound = append(p.upperBoundBuf, 0x00)
	}

	p.iter = handle.NewIter(&p.options)
	if p.iter == nil {
		panic("unable to create iterator")
	}
}

func (p *pebbleIterator) Close() {
	if p.iter != nil {
		err := p.iter.Close()
		if err != nil {
			panic(err)
		}
		p.iter = nil
	}
	*p = pebbleIterator{
		lowerBoundBuf: p.lowerBoundBuf,
		upperBoundBuf: p.upperBoundBuf,
	}

	pebbleIterPool.Put(p)
}

func (p *pebbleIterator) Seek(key MVCCKey) {
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)
	if (p.prefix) {
		p.iter.SeekPrefixGE(p.keyBuf)
	} else {
		p.iter.SeekGE(p.keyBuf)
	}
}

func (p *pebbleIterator) Valid() (bool, error) {
	return p.iter.Valid(), nil
}

func (p *pebbleIterator) Next() {
	p.iter.Next()
}

func (p *pebbleIterator) NextKey() {
	if valid, err := p.Valid(); err != nil || !valid {
		return
	}
	curKey := p.Key()

	for p.iter.Next() {
		if nextKey := p.UnsafeKey(); !bytes.Equal(curKey.Key, nextKey.Key) {
			break
		}
	}
}

func (p *pebbleIterator) UnsafeKey() MVCCKey {
	if valid, err := p.Valid(); err != nil || !valid {
		return MVCCKey{}
	}

	mvccKey, err := DecodeMVCCKey(p.iter.Key())
	if err != nil {
		return MVCCKey{}
	}

	return mvccKey
}

func (p *pebbleIterator) UnsafeValue() []byte {
	if valid, err := p.Valid(); err != nil || !valid {
		return nil
	}
	return p.iter.Value()
}

func (p *pebbleIterator) SeekReverse(key MVCCKey) {
	p.Seek(key)
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)

	// The new key could either be greater or equal to the supplied key.
	// Backtrack one step if it is greater.
	comp := MVCCKeyCompare(p.keyBuf, p.iter.Key())
	if comp < 0 && p.iter.Valid() {
		p.Prev()
	}
}

func (p *pebbleIterator) Prev() {
	p.iter.Prev()
}

func (p *pebbleIterator) PrevKey() {
	if valid, err := p.Valid(); err != nil || !valid {
		return
	}
	curKey := p.Key()
	for p.iter.Prev() {
		if nextKey := p.UnsafeKey(); !bytes.Equal(curKey.Key, nextKey.Key) {
			break
		}
	}
}

func (p *pebbleIterator) Key() MVCCKey {
	key := p.UnsafeKey()
	keyCopy := make([]byte, len(key.Key))
	copy(keyCopy, key.Key)
	key.Key = keyCopy
	return key
}

func (p *pebbleIterator) Value() []byte {
	value := p.UnsafeValue()
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy
}

func (p *pebbleIterator) ValueProto(msg protoutil.Message) error {
	value := p.Value()

	return msg.Unmarshal(value)
}

func (p *pebbleIterator) ComputeStats(start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error) {
	return ComputeStatsGo(p, start, end, nowNanos)
}

// Go-only version of IsValidSplitKey. Checks if the specified key is in
// NoSplitSpans.
func isValidSplitKey(key roachpb.Key) bool {
	for _, noSplitSpan := range keys.NoSplitSpans {
		if noSplitSpan.ContainsKey(key) {
			return false
		}
	}
	return true
}

func (p *pebbleIterator) FindSplitKey(start, end, minSplitKey MVCCKey, targetSize int64) (MVCCKey, error) {
	const (
		timestampLen = int64(12)
	)
	p.Seek(start)
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], end)
	minSplitKeyBuf := EncodeKey(minSplitKey)
	prevKey := make([]byte, 0)

	sizeSoFar := int64(0)
	bestDiff := int64(intsets.MaxInt)
	bestSplitKey := MVCCKey{}

	for ;p.iter.Valid() && MVCCKeyCompare(p.iter.Key(), p.keyBuf) < 0; p.iter.Next() {
		mvccKey, err := DecodeMVCCKey(p.iter.Key())
		if err != nil {
			return MVCCKey{}, err
		}

		diff := targetSize - sizeSoFar
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff && MVCCKeyCompare(p.iter.Key(), minSplitKeyBuf) >= 0 && isValidSplitKey(mvccKey.Key) {
			// We are going to have to copy bestSplitKey, since by the time we find
			// out it's the actual best split key, the underlying slice would have
			// changed (due to the iter.Next() call).
			bestDiff = diff
			bestSplitKey.Key = append(bestSplitKey.Key[:0], mvccKey.Key...)
			bestSplitKey.Timestamp = mvccKey.Timestamp
		}
		if diff > bestDiff && bestSplitKey.Key != nil {
			break
		}

		sizeSoFar += int64(len(p.iter.Value()))
		if mvccKey.IsValue() && bytes.Equal(prevKey, mvccKey.Key) {
			// We only advanced timestamps, but not new mvcc keys.
			sizeSoFar += timestampLen
		} else {
			sizeSoFar += int64(len(mvccKey.Key) + 1)
			if mvccKey.IsValue() {
				sizeSoFar += timestampLen
			}
		}

		prevKey = append(prevKey[:0], mvccKey.Key...)
	}

	return bestSplitKey, nil
}

type pebbleResults struct {
	count int64
	repr  []byte
}

// The repr that MVCCScan / MVCCGet expects to provide as output goes:
// <valueLen:Uint32><keyLen:Uint32><Key><Value>
// This function adds to repr in that format.
func (p *pebbleResults) put(key []byte, value []byte) {
	// Key value lengths take up 8 bytes (2 x Uint32).
	const kvLenSize = 8

	startIdx := len(p.repr)
	lenToAdd := kvLenSize + len(key) + len(value)

	if len(p.repr) + lenToAdd <= cap(p.repr) {
		p.repr = p.repr[:len(p.repr)+lenToAdd]
	} else {
		oldRepr := p.repr
		p.repr = make([]byte, len(oldRepr)+lenToAdd)
		copy(p.repr, oldRepr)
	}

	binary.LittleEndian.PutUint32(p.repr[startIdx:], uint32(len(value)))
	binary.LittleEndian.PutUint32(p.repr[startIdx+4:], uint32(len(key)))
	copy(p.repr[startIdx+kvLenSize:], key)
	copy(p.repr[startIdx+kvLenSize+len(key):], value)
	p.count++
}

// Go port of mvccScanner in libroach/mvcc.h .
type pebbleMvccScanner struct {
	parent                      *pebble.Iterator
	reverse                     bool
	start, end                  roachpb.Key
	startBuf, endBuf            []byte
	ts                          hlc.Timestamp
	maxKeys                     int64
	keyCount                    int64
	txn                         *roachpb.Transaction
	meta                        enginepb.MVCCMetadata
	inconsistent, tombstones    bool
	ignoreSeq, checkUncertainty bool
	keyBuf, savedBuf            []byte
	curRawKey, curKey, curValue []byte
	curTS                       hlc.Timestamp
	results                     pebbleResults
	intents                     pebble.Batch
	uncertaintyTS               hlc.Timestamp
	err                         error
	itersBeforeSeek             int
}

func (p *pebbleMvccScanner) init() {
	p.itersBeforeSeek = MaxItersBeforeSeek / 2

	mvccStartKey := MVCCKey{p.start, hlc.Timestamp{}}
	mvccEndKey := MVCCKey{p.end, hlc.Timestamp{}}
	p.startBuf = EncodeKeyToBuf(p.startBuf[:0], mvccStartKey)
	p.endBuf = EncodeKeyToBuf(p.endBuf[:0], mvccEndKey)
	p.parent.SetBounds(p.startBuf, p.endBuf)
}

// seekReverse seeks to the latest revision of the key before the specified key.
func (p *pebbleMvccScanner) seekReverse(key roachpb.Key) {
	mvccKey := MVCCKey{key, hlc.Timestamp{}}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], mvccKey)
	p.parent.SeekGE(p.keyBuf)

	if !p.parent.Valid() {
		// We might be past the end. Seek to the end key.
		p.parent.SeekLT(p.endBuf)
	}

	if p.parent.Valid() && MVCCKeyCompare(p.parent.Key(), p.keyBuf) >= 0 {
		p.parent.Prev()
	}

	if !p.parent.Valid() {
		return
	}

	mvccKey, err := DecodeMVCCKey(p.parent.Key())
	if err != nil {
		p.err = nil
		return
	}
	mvccKey.Timestamp = hlc.Timestamp{}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], mvccKey)
	p.parent.SeekGE(p.keyBuf)
	p.updateCurrent()
}

func (p *pebbleMvccScanner) seek(key roachpb.Key) {
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], MVCCKey{key, hlc.Timestamp{}})
	p.parent.SeekGE(p.keyBuf)
	p.updateCurrent()
}

func (p *pebbleMvccScanner) scan() {
	if p.reverse {
		p.seekReverse(p.end)
	} else {
		p.seek(p.start)
	}

	for p.results.count < p.maxKeys && p.getAndAdvance() {
	}

	if p.results.count < p.maxKeys || !p.parent.Valid() {
		p.curRawKey = nil
		p.curKey = nil
		p.curTS = hlc.Timestamp{}
	}
}

func (p *pebbleMvccScanner) get() {
	p.seek(p.start)
	p.getAndAdvance()
}

func (p *pebbleMvccScanner) incrementItersBeforeSeek() {
	p.itersBeforeSeek++
	if p.itersBeforeSeek > MaxItersBeforeSeek {
		p.itersBeforeSeek = MaxItersBeforeSeek
	}
}

func (p *pebbleMvccScanner) decrementItersBeforeSeek() {
	p.itersBeforeSeek--
	if p.itersBeforeSeek < 1 {
		p.itersBeforeSeek = 1
	}
}

func (p *pebbleMvccScanner) updateCurrent() bool {
	if !p.parent.Valid() {
		return false
	}

	p.curRawKey = append(p.curRawKey[:0], p.parent.Key()...)
	p.curValue = append(p.curValue[:0], p.parent.Value()...)

	mvccKey, err := DecodeMVCCKey(p.curRawKey)
	if err != nil {
		p.err = err
		return false
	}

	p.curKey = mvccKey.Key
	p.curTS = mvccKey.Timestamp

	return true
}

func (p *pebbleMvccScanner) advanceKey() {
	if p.reverse {
		p.prevKey()
	} else {
		p.nextKey()
	}
}

func (p *pebbleMvccScanner) prevKey() {
	iterCount := p.itersBeforeSeek
	mvccKey, err := DecodeMVCCKey(p.parent.Key())
	gotToPrevious := false

	for iterCount >= 0 && p.parent.Valid() && err == nil && bytes.Compare(mvccKey.Key , p.curKey) == 0 {
		p.parent.Prev()
		mvccKey, err = DecodeMVCCKey(p.parent.Key())
		iterCount--

		if err == nil && bytes.Compare(mvccKey.Key, p.curKey) != 0 && !gotToPrevious {
			// We've backed up to the previous key, but not the latest revision.
			// Update current then keep going until we get to the latest version of
			// that key.
			gotToPrevious = true
			p.updateCurrent()
		}
	}

	if err != nil {
		p.err = err
		return
	}

	if bytes.Compare(mvccKey.Key, p.curKey) == 0 {
		// We have to seek.
		if !gotToPrevious {
			// Seek to the latest revision of the key before p.curKey.
			p.seekReverse(p.curKey)
		} else {
			// p.curKey is already one key before where it was at the start of this
			// function. Just seek to the latest revision of it.
			p.seek(p.curKey)
		}

		p.decrementItersBeforeSeek()
		return
	}

	p.incrementItersBeforeSeek()
	p.updateCurrent()
}

func (p *pebbleMvccScanner) nextKey() {
	if !p.parent.Valid() {
		return
	}

	iterCount := p.itersBeforeSeek
	mvccKey, err := DecodeMVCCKey(p.parent.Key())

	for iterCount >= 0 && p.parent.Valid() && err == nil && bytes.Compare(mvccKey.Key , p.curKey) == 0 {
		p.parent.Next()
		mvccKey, err = DecodeMVCCKey(p.parent.Key())
	}

	if err != nil {
		p.err = err
		return
	}

	if bytes.Compare(mvccKey.Key, p.curKey) == 0 {
		// We have to seek. Append a null byte to the current key. Note that
		// appending to p.curKey could invalidate p.curRawKey, since p.curKey is
		// usually a sub-slice of the latter. But if we're just seeking to the next
		// key right afterward (seek calls updateCurrent), this is not an issue.
		p.curKey = append(p.curKey, 0x00)
		p.seek(p.curKey)

		p.decrementItersBeforeSeek()
		return
	}

	p.incrementItersBeforeSeek()
	p.updateCurrent()
}

func (p *pebbleMvccScanner) seekVersion(ts hlc.Timestamp, uncertaintyCheck bool) {
	mvccKey := MVCCKey{Key: p.curKey, Timestamp: ts}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], mvccKey)
	origKey := p.keyBuf[:len(p.curKey)]

	iterCount := p.itersBeforeSeek
	for iterCount >= 0 && MVCCKeyCompare(p.curRawKey, p.keyBuf) < 0 {
		if !p.parent.Valid() {
			// For reverse iterations, go back a key.
			if p.reverse {
				p.prevKey()
			}
			return
		}
		p.parent.Next()
		p.updateCurrent()
		iterCount--
	}

	if iterCount < 0 {
		p.parent.SeekGE(p.keyBuf)
		p.updateCurrent()

		p.decrementItersBeforeSeek()
		return
	} else {
		p.incrementItersBeforeSeek()
	}

	if !p.parent.Valid() {
		// For reverse iterations, go back a key.
		if p.reverse {
			p.prevKey()
		}
		return
	}
	if bytes.Compare(p.curKey, origKey) != 0 {
		// Could not find a value - we moved past to the next key.
		if p.reverse {
			p.prevKey()
		}
		return
	}

	if !(p.curTS.Less(ts) || p.curTS.Equal(ts)) {
		if ts == (hlc.Timestamp{}) {
			// Zero timestamps come at the start. This case means there's
			// no value at the zero timestamp, and we're sitting at a nonzero
			// timestamp for the same key. Skip to the next key.
			p.advanceKey()
			return
		}
		// Potential ordering issue - this should never happen.
		panic("timestamps encountered out of order")
	}

	if uncertaintyCheck && p.ts.Less(p.curTS) {
		p.err = p.uncertaintyError(p.curTS)
		return
	}

	// Check to ensure we don't unintentionally add an intent to results.
	if p.curTS != (hlc.Timestamp{}) {
		p.addKV(p.curRawKey, p.curValue)
	}
	p.advanceKey()
}

func (p *pebbleMvccScanner) addKV(key []byte, val []byte) {
	if len(val) > 0 || p.tombstones {
		p.results.put(key, val)
	}
}

func (p *pebbleMvccScanner) uncertaintyError(ts hlc.Timestamp) error {
	if ts.WallTime == 0 && ts.Logical == 0 {
		return nil
	}

	return roachpb.NewReadWithinUncertaintyIntervalError(
		p.ts, ts, p.txn)
}

func (p *pebbleMvccScanner) getFromIntentHistory() bool {
	intentHistory := p.meta.IntentHistory
	// upIdx is the index of the first intent in intentHistory with a sequence
	// number greater than our transaction's sequence number. Subtract 1 from it
	// to get the index of the intent with the highest sequence number that is
	// still less than or equal to p.txnSeq.
	upIdx := sort.Search(len(intentHistory), func(i int) bool {
		return intentHistory[i].Sequence > p.txn.Sequence
	})
	if upIdx == 0 {
		// It is possible that no intent exists such that the sequence is less
		// than the read sequence. In this case, we cannot read a value from the
		// intent history.
		return false
	}
	intent := p.meta.IntentHistory[upIdx - 1]
	p.addKV(p.curRawKey, intent.Value)
	return true
}

// Emit a tuple (using p.addKV) and return true if we have reason to believe
// iteration can continue.
func (p *pebbleMvccScanner) getAndAdvance() bool {
	if !p.parent.Valid() {
		return false
	}
	p.err = nil

	mvccKey := MVCCKey{p.curKey, p.curTS}
	if mvccKey.IsValue() {
		if !p.ts.Less(p.curTS) {
			// 1. Fast path: there is no intent and our read timestamp is newer than
			// the most recent version's timestamp.
			p.addKV(p.curRawKey, p.curValue)
			p.advanceKey()
			return true
		}

		if p.checkUncertainty {
			// 2. Our txn's read timestamp is less than the max timestamp
			// seen by the txn. We need to check for clock uncertainty
			// errors.
			if !p.txn.MaxTimestamp.Less(p.curTS) {
				p.err = p.uncertaintyError(p.curTS)
				return false
			}

			p.seekVersion(p.txn.MaxTimestamp, true)
			if p.err != nil {
				return false
			}
			return true
		}

		// 3. Our txn's read timestamp is greater than or equal to the
		// max timestamp seen by the txn so clock uncertainty checks are
		// unnecessary. We need to seek to the desired version of the
		// value (i.e. one with a timestamp earlier than our read
		// timestamp).
		p.seekVersion(p.ts, false)
		if p.err != nil {
			return false
		}
		return true
	}

	if len(p.curValue) == 0 {
		p.err = errors.Errorf("zero-length mvcc metadata")
		return false
	}
	p.meta.Reset()
	err := p.meta.Unmarshal(p.curValue)
	if err != nil {
		p.err = errors.Errorf("unable to decode MVCCMetadata: %s", err)
		return false
	}
	if len(p.meta.RawBytes) != 0 {
		// 4. Emit immediately if the value is inline.
		p.addKV(p.curRawKey, p.meta.RawBytes)
		p.advanceKey()
		return true
	}

	if p.meta.Txn == nil {
		p.err = errors.Errorf("intent without transaction")
		return false
	}
	metaTS := hlc.Timestamp(p.meta.Timestamp)

	// metaTS is the timestamp of an intent value, which we may or may
	// not end up ignoring, depending on factors codified below. If we do ignore
	// the intent then we want to read at a lower timestamp that's strictly
	// below the intent timestamp (to skip the intent), but also does not exceed
	// our read timestamp (to avoid erroneously picking up future committed
	// values); this timestamp is prevTS.
	prevTS := p.ts
	if !p.ts.Less(metaTS) {
		prevTS = metaTS.Prev()
	}

	ownIntent := p.txn != nil && p.meta.Txn.ID.Equal(p.txn.ID)
	maxVisibleTS := p.ts
	if p.checkUncertainty {
		maxVisibleTS = p.txn.MaxTimestamp
	}

	if maxVisibleTS.Less(metaTS) && !ownIntent {
		// 5. The key contains an intent, but we're reading before the
		// intent. Seek to the desired version. Note that if we own the
		// intent (i.e. we're reading transactionally) we want to read
		// the intent regardless of our read timestamp and fall into
		// case 8 below.
		p.seekVersion(p.ts, false)
		if p.err != nil {
			return false
		}
		return true
	}

	if p.inconsistent {
		// 6. The key contains an intent and we're doing an inconsistent
		// read at a timestamp newer than the intent. We ignore the
		// intent by insisting that the timestamp we're reading at is a
		// historical timestamp < the intent timestamp. However, we
		// return the intent separately; the caller may want to resolve
		// it.
		if p.results.count == p.maxKeys {
			// We've already retrieved the desired number of keys and now
			// we're adding the resume key. We don't want to add the
			// intent here as the intents should only correspond to KVs
			// that lie before the resume key.
			return false
		}
		p.err = p.intents.Set(p.curRawKey, p.curValue, nil)
		if p.err != nil {
			return false
		}

		p.seekVersion(prevTS, false)
		if p.err != nil {
			return false
		}
		return true
	}

	if !ownIntent {
		// 7. The key contains an intent which was not written by our
		// transaction and our read timestamp is newer than that of the
		// intent. Note that this will trigger an error on the Go
		// side. We continue scanning so that we can return all of the
		// intents in the scan range.
		p.err = p.intents.Set(p.curRawKey, p.curValue, nil)
		if p.err != nil {
			return false
		}
		p.advanceKey()
		if p.err != nil {
			return false
		}
		return true
	}

	if p.txn != nil && p.txn.Epoch == p.meta.Txn.Epoch {
		if p.ignoreSeq || (p.txn.Sequence >= p.meta.Txn.Sequence) {
			// 8. We're reading our own txn's intent at an equal or higher sequence.
			// Note that we read at the intent timestamp, not at our read timestamp
			// as the intent timestamp may have been pushed forward by another
			// transaction. Txn's always need to read their own writes.
			p.seekVersion(metaTS, false)
		} else {
			// 9. We're reading our own txn's intent at a lower sequence than is
			// currently present in the intent. This means the intent we're seeing
			// was written at a higher sequence than the read and that there may or
			// may not be earlier versions of the intent (with lower sequence
			// numbers) that we should read. If there exists a value in the intent
			// history that has a sequence number equal to or less than the read
			// sequence, read that value.
			found := p.getFromIntentHistory();
			if found {
				p.advanceKey()
				return true
			}
			// 10. If no value in the intent history has a sequence number equal to
			// or less than the read, we must ignore the intents laid down by the
			// transaction all together. We ignore the intent by insisting that the
			// timestamp we're reading at is a historical timestamp < the intent
			// timestamp.
			p.seekVersion(prevTS, false)
		}
		if p.err != nil {
			return false
		}
		return true
	}

	if p.txn != nil && (p.txn.Epoch < p.meta.Txn.Epoch) {
		// 11. We're reading our own txn's intent but the current txn has
		// an earlier epoch than the intent. Return an error so that the
		// earlier incarnation of our transaction aborts (presumably
		// this is some operation that was retried).
		p.err = errors.Errorf("failed to read with epoch %d due to a write intent with epoch %d",
			p.txn.Epoch, p.meta.Txn.Epoch)
	}

	// 12. We're reading our own txn's intent but the current txn has a
	// later epoch than the intent. This can happen if the txn was
	// restarted and an earlier iteration wrote the value we're now
	// reading. In this case, we ignore the intent and read the
	// previous value as if the transaction were starting fresh.
	p.seekVersion(prevTS, false)
	if p.err != nil {
		return false
	}
	return true

}

func (p *pebbleIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (value *roachpb.Value, intent *roachpb.Intent, err error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}
	if p.iter == nil {
		panic("uninitialized iterator")
	}

	// MVCCGet is implemented as an MVCCScan with an end key that sorts after the
	// start key.
	keyEnd := make([]byte, 0, len(key) + 1)
	keyEnd = append(keyEnd, key...)
	keyEnd = append(keyEnd, 0x00)

	p.mvccScanner = pebbleMvccScanner{
		parent:           p.iter,
		start:            key,
		end:              keyEnd,
		ts:               timestamp,
		maxKeys:          1,
		inconsistent:     opts.Inconsistent,
		tombstones:       opts.Tombstones,
		ignoreSeq:        opts.IgnoreSequence,
	}

	if opts.Txn != nil {
		p.mvccScanner.txn = opts.Txn
		p.mvccScanner.checkUncertainty = timestamp.Less(opts.Txn.MaxTimestamp)
	}

	p.mvccScanner.init()
	p.mvccScanner.get()

	// Init calls SetBounds. Reset it to what this iterator had at the start.
	defer func() {
		if p.iter != nil {
			p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
		}
	}()

	if p.mvccScanner.err != nil {
		return nil, nil, p.mvccScanner.err
	}
	intents, err := buildScanIntents(p.mvccScanner.intents.Repr())
	if err != nil {
		return nil, nil, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		return nil, nil, &roachpb.WriteIntentError{Intents: intents}
	}

	if len(intents) > 1 {
		return nil, nil, errors.Errorf("expected 0 or 1 intents, got %d", len(intents))
	} else if len(intents) == 1 {
		intent = &intents[0]
	}

	if len(p.mvccScanner.results.repr) == 0 {
		return nil, intent, nil
	}

	mvccKey, rawValue, _, err := MVCCScanDecodeKeyValue(p.mvccScanner.results.repr)
	if err != nil {
		return nil, nil, err
	}

	value = &roachpb.Value{
		RawBytes:  rawValue,
		Timestamp: mvccKey.Timestamp,
	}
	return
}

func (p *pebbleIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData []byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, 0, nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(end) == 0 {
		return nil, 0, nil, nil, emptyKeyError()
	}
	if max == 0 {
		resumeSpan = &roachpb.Span{Key: start, EndKey: end}
		return nil, 0, resumeSpan, nil, nil
	}
	if p.iter == nil {
		panic("uninitialized iterator")
	}

	p.mvccScanner = pebbleMvccScanner{
		parent:           p.iter,
		reverse:          opts.Reverse,
		start:            start,
		end:              end,
		ts:               timestamp,
		maxKeys:          max,
		inconsistent:     opts.Inconsistent,
		tombstones:       opts.Tombstones,
		ignoreSeq:        opts.IgnoreSequence,
	}

	if opts.Txn != nil {
		p.mvccScanner.txn = opts.Txn
		p.mvccScanner.checkUncertainty = timestamp.Less(opts.Txn.MaxTimestamp)
	}

	p.mvccScanner.init()
	p.mvccScanner.scan()

	// Init calls SetBounds. Reset it to what this iterator had at the start.
	defer func() {
		if p.iter != nil {
			p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
		}
	}()

	if p.mvccScanner.err != nil {
		return nil, 0, nil, nil, p.mvccScanner.err
	}

	kvData = p.mvccScanner.results.repr
	numKVs = p.mvccScanner.results.count

	if p.mvccScanner.curKey != nil {
		if opts.Reverse {
			resumeSpan = &roachpb.Span{
				Key:    p.mvccScanner.start,
				EndKey: p.mvccScanner.curKey,
			}
			resumeSpan.EndKey = resumeSpan.EndKey.Next()
		} else {
			resumeSpan = &roachpb.Span{
				Key:    p.mvccScanner.curKey,
				EndKey: p.mvccScanner.end,
			}
		}
	}
	intents, err = buildScanIntents(p.mvccScanner.intents.Repr())
	if err != nil {
		return nil, 0, nil, nil, err
	}

	if !opts.Inconsistent && len(intents) > 0 {
		return nil, 0, resumeSpan, nil, &roachpb.WriteIntentError{Intents: intents}
	}
	return
}

func (p *pebbleIterator) SetUpperBound(upperBound roachpb.Key) {
	p.upperBoundBuf = append(p.upperBoundBuf[:0], upperBound...)
	p.options.UpperBound = append(p.upperBoundBuf, 0x00)
	p.iter.SetBounds(p.options.LowerBound, p.options.UpperBound)
}

func (p *pebbleIterator) Stats() IteratorStats {
	// TODO(itsbilal): Implement this.
	panic("implement me")
}

type pebbleBatch struct {
	parent       *Pebble
	batch        *pebble.Batch
	buf          []byte
	iter         reusablePebbleBatchIterator
	closed       bool
	isDistinct   bool
	writeOnly    bool
	distinctOpen bool
	parentBatch  *pebbleBatch
}

var _ Batch = &pebbleBatch{}

var pebbleBatchPool = sync.Pool{
	New: func() interface{} {
		return &pebbleBatch{}
	},
}

func newPebbleBatch(p *Pebble, writeOnly bool) *pebbleBatch {
	var batch *pebble.Batch
	if writeOnly {
		batch = p.db.NewBatch()
	} else {
		batch = p.db.NewIndexedBatch()
	}
	pb := pebbleBatchPool.Get().(*pebbleBatch)
	*pb = pebbleBatch{
		parent:    p,
		batch:     batch,
		closed:    false,
		writeOnly: writeOnly,
	}

	return pb
}

func (p *pebbleBatch) Close() {
	if p.iter.iter.iter != nil {
		// Call pebble.Iterator.Close, not pebbleIterator.Close. This is because
		// the latter will put the pebbleIterator in its sync.Pool, which is not
		// what we want. The pebbleBatch owns the iterator and its memory.
		p.iter.iter.iter.Close()
		p.iter.batch = nil
		p.iter.iter = pebbleIterator{}
	}
	if !p.isDistinct {
		p.batch.Close()
		p.batch = nil
	} else {
		p.parentBatch.distinctOpen = false
		p.isDistinct = false
	}
	p.closed = true
	p.parent = nil
	pebbleBatchPool.Put(p)
}

func (p *pebbleBatch) Closed() bool {
	return p.closed
}

func (p *pebbleBatch) Get(key MVCCKey) ([]byte, error) {
	if p.writeOnly {
		panic("read operation called on write-only batch")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	ret, err := p.batch.Get(p.buf)
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

func (p *pebbleBatch) GetProto(key MVCCKey, msg protoutil.Message) (ok bool, keyBytes, valBytes int64, err error) {
	if p.writeOnly {
		panic("read operation called on write-only batch")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	val, err := p.batch.Get(p.buf)
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

func (p *pebbleBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error)) error {
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

func (p *pebbleBatch) ensureBatch() {
	if p.batch == nil {
		if p.parent == nil {
			panic("ensureBatch called with no pebble instance")
		}
		p.batch = p.parent.db.NewIndexedBatch()
	}
}

func (p *pebbleBatch) NewIterator(opts IterOptions) Iterator {
	if p.writeOnly {
		panic("read operation called on write-only batch")
	}
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	// Use the cached iterator.
	if p.iter.batch != nil {
		panic("iterator already in use")
	} else if p.iter.iter.iter != nil {
		p.iter.iter.iter.Close()
		p.iter.iter.iter = nil
	}

	p.ensureBatch()
	p.iter.iter.init(p.parent, p.batch, opts)

	p.iter.batch = p
	return &p.iter
}

func (p *pebbleBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	var batch pebble.Batch
	if err := batch.SetRepr(repr); err != nil {
		return err
	}

	return p.batch.Apply(&batch, nil)
}

func (p *pebbleBatch) Clear(key MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	err := p.batch.Delete(p.buf, nil)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func (p *pebbleBatch) SingleClear(key MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	err := p.batch.SingleDelete(p.buf, nil)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func (p *pebbleBatch) ClearRange(start, end MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], start)
	buf2 := EncodeKey(end)
	return p.batch.DeleteRange(p.buf, buf2, nil)
}

func (p *pebbleBatch) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	iter.SetUpperBound(end.Key)
	iter.Seek(start)

	for ; ; iter.Next() {
		valid, err := iter.Valid()
		if err != nil {
			return err
		} else if !valid {
			break
		}

		p.buf = EncodeKeyToBuf(p.buf[:0], iter.Key())
		err = p.batch.Delete(p.buf, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *pebbleBatch) Merge(key MVCCKey, value []byte) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.Merge(p.buf, value, nil)
}

func (p *pebbleBatch) Put(key MVCCKey, value []byte) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.Set(p.buf, value, nil)
}

func (p *pebbleBatch) LogData(data []byte) error {
	return p.batch.LogData(data, nil)
}

func (p *pebbleBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op.
}

func (p *pebbleBatch) Commit(sync bool) error {
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	if (p.batch == nil) {
		panic("called with nil batch")
	}
	err := p.batch.Commit(opts)
	if err != nil {
		panic(err)
	}
	return err
}

func (p *pebbleBatch) Distinct() ReadWriter {
	// Distinct batches are regular batches with isDistinct set to true.
	// The parent batch is stored in parentBatch, and all writes on it are
	// disallowed while the distinct batch is open.
	batch := &pebbleBatch{}
	batch.parent = p.parent
	batch.batch = p.batch
	batch.isDistinct = true
	p.distinctOpen = true
	batch.parentBatch = p

	return batch
}

func (p *pebbleBatch) Empty() bool {
	return p.batch.Count() == 0
}

func (p *pebbleBatch) Len() int {
	return len(p.batch.Repr())
}

func (p *pebbleBatch) Repr() []byte {
	return p.batch.Repr()
}

// pebbleBatchIterator wraps pebbleIterator and ensures that the buffered mutations
// in a batch are flushed before performing read operations.
type pebbleBatchIterator struct {
	iter  pebbleIterator
	batch *pebbleBatch
}

func (r *pebbleBatchIterator) Stats() IteratorStats {
	return r.iter.Stats()
}

func (r *pebbleBatchIterator) Close() {
	if r.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
	r.iter.iter.Close()
}

func (r *pebbleBatchIterator) Seek(key MVCCKey) {
	r.iter.Seek(key)
}

func (r *pebbleBatchIterator) SeekReverse(key MVCCKey) {
	r.iter.SeekReverse(key)
}

func (r *pebbleBatchIterator) Valid() (bool, error) {
	return r.iter.Valid()
}

func (r *pebbleBatchIterator) Next() {
	r.iter.Next()
}

func (r *pebbleBatchIterator) Prev() {
	r.iter.Prev()
}

func (r *pebbleBatchIterator) NextKey() {
	r.iter.NextKey()
}

func (r *pebbleBatchIterator) PrevKey() {
	r.iter.PrevKey()
}

func (r *pebbleBatchIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return r.iter.ComputeStats(start, end, nowNanos)
}

func (r *pebbleBatchIterator) FindSplitKey(
	start, end, minSplitKey MVCCKey, targetSize int64,
) (MVCCKey, error) {
	return r.iter.FindSplitKey(start, end, minSplitKey, targetSize)
}

func (r *pebbleBatchIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	return r.iter.MVCCGet(key, timestamp, opts)
}

func (r *pebbleBatchIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData []byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	return r.iter.MVCCScan(start, end, max, timestamp, opts)
}

func (r *pebbleBatchIterator) SetUpperBound(key roachpb.Key) {
	r.iter.SetUpperBound(key)
}

func (r *pebbleBatchIterator) Key() MVCCKey {
	return r.iter.Key()
}

func (r *pebbleBatchIterator) Value() []byte {
	return r.iter.Value()
}

func (r *pebbleBatchIterator) ValueProto(msg protoutil.Message) error {
	return r.iter.ValueProto(msg)
}

func (r *pebbleBatchIterator) UnsafeKey() MVCCKey {
	return r.iter.UnsafeKey()
}

func (r *pebbleBatchIterator) UnsafeValue() []byte {
	return r.iter.UnsafeValue()
}

// reusablePebbleBatchIterator wraps pebbleBatchIterator and makes the Close method a no-op
// to allow reuse of the iterator for the lifetime of the batch. The batch must
// call iter.destroy() when it closes itself.
type reusablePebbleBatchIterator struct {
	pebbleBatchIterator
}

func (r *reusablePebbleBatchIterator) Close() {
	// reusablepebbleBatchIterator.Close() leaves the underlying pebble iterator open
	// until the associated batch is closed.
	if r.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
}
