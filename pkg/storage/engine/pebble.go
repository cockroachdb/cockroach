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
	"io"
	"io/ioutil"
	"os"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
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
	Merge: func(_, value []byte) (pebble.ValueMerger, error) {
		res := &MVCCValueMerger{}
		err := res.MergeNewer(value)
		if err != nil {
			return nil, err
		}
		return res, nil
	},
}

// pebbleTimeBoundPropCollector implements a property collector for MVCC
// Timestamps. Its behavior matches TimeBoundTblPropCollector in
// table_props.cc.
//
// The handling of timestamps in intents is mildly complicated. Consider:
//
//   a@<meta>   -> <MVCCMetadata: Timestamp=t2>
//   a@t2       -> <value>
//   a@t1       -> <value>
//
// The metadata record (a.k.a. the intent) for a key always sorts first. The
// timestamp field always points to the next record. In this case, the meta
// record contains t2 and the next record is t2. Because of this duplication of
// the timestamp both in the intent and in the timestamped record that
// immediately follows it, we only need to unmarshal the MVCCMetadata if it is
// the last key in the sstable.
type pebbleTimeBoundPropCollector struct {
	min, max  []byte
	lastValue []byte
}

func (t *pebbleTimeBoundPropCollector) Add(key pebble.InternalKey, value []byte) error {
	_, ts, ok := enginepb.SplitMVCCKey(key.UserKey)
	if !ok {
		return errors.Errorf("failed to split MVCC key")
	}
	if len(ts) > 0 {
		t.lastValue = t.lastValue[:0]
		t.updateBounds(ts)
	} else {
		t.lastValue = append(t.lastValue[:0], value...)
	}
	return nil
}

func (t *pebbleTimeBoundPropCollector) Finish(userProps map[string]string) error {
	if len(t.lastValue) > 0 {
		// The last record in the sstable was an intent. Unmarshal the metadata and
		// update the bounds with the timestamp it contains.
		meta := &enginepb.MVCCMetadata{}
		if err := protoutil.Unmarshal(t.lastValue, meta); err != nil {
			// We're unable to parse the MVCCMetadata. Fail open by not setting the
			// min/max timestamp properties. THis mimics the behavior of
			// TimeBoundTblPropCollector.
			return nil
		}
		if meta.Txn != nil {
			ts := encodeTimestamp(hlc.Timestamp(meta.Timestamp))
			t.updateBounds(ts)
		}
	}

	userProps["crdb.ts.min"] = string(t.min)
	userProps["crdb.ts.max"] = string(t.max)
	return nil
}

func (t *pebbleTimeBoundPropCollector) updateBounds(ts []byte) {
	if len(t.min) == 0 || bytes.Compare(ts, t.min) < 0 {
		t.min = append(t.min[:0], ts...)
	}
	if len(t.max) == 0 || bytes.Compare(ts, t.max) > 0 {
		t.max = append(t.max[:0], ts...)
	}
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

// DefaultPebbleOptions returns the default pebble options.
func DefaultPebbleOptions() *pebble.Options {
	return &pebble.Options{
		Cleaner:               pebble.ArchiveCleaner{},
		Comparer:              MVCCComparer,
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 1000,
		LBaseMaxBytes:         64 << 20, // 64 MB
		Levels: []pebble.LevelOptions{{
			BlockSize:    32 << 10, // 32 KB
			FilterPolicy: bloom.FilterPolicy(10),
			FilterType:   pebble.TableFilter,
		}},
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
		Merger:                      MVCCMerger,
		MinFlushRate:                4 << 20, // 4 MB/sec
		TablePropertyCollectors:     PebbleTablePropertyCollectors,
	}
}

type pebbleLogger struct {
	ctx context.Context
}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	log.InfofDepth(l.ctx, 2, format, args...)
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.FatalfDepth(l.ctx, 2, format, args...)
}

// PebbleConfig holds all configuration parameters and knobs used in setting up
// a new Pebble instance.
type PebbleConfig struct {
	// StorageConfig contains storage configs for all storage engines.
	base.StorageConfig
	// Pebble specific options.
	Opts *pebble.Options
}

// Pebble is a wrapper around a Pebble database instance.
type Pebble struct {
	db *pebble.DB

	closed   bool
	path     string
	auxDir   string
	maxSize  int64
	attrs    roachpb.Attributes
	settings *cluster.Settings

	// Relevant options copied over from pebble.Options.
	fs     vfs.FS
	logger pebble.Logger
}

var _ Engine = &Pebble{}

// NewEncryptedEnvFunc creates an encrypted environment and returns the vfs.FS to use for reading
// and writing data. This should be initialized by calling engineccl.Init() before calling
// NewPebble(). The optionBytes is a binary serialized baseccl.EncryptionOptions, so that non-CCL
// code does not depend on CCL code.
var NewEncryptedEnvFunc func(fs vfs.FS, fr *PebbleFileRegistry, dbDir string, readOnly bool, optionBytes []byte) (vfs.FS, error)

// NewPebble creates a new Pebble instance, at the specified path.
func NewPebble(ctx context.Context, cfg PebbleConfig) (*Pebble, error) {
	// pebble.Open also calls EnsureDefaults, but only after doing a clone. Call
	// EnsureDefaults beforehand so we have a matching cfg here for when we save
	// cfg.FS and cfg.ReadOnly later on.
	cfg.Opts.EnsureDefaults()

	var auxDir string
	if cfg.Dir == "" {
		// TODO(peter): This is horribly hacky but matches what RocksDB does. For
		// in-memory instances, we create an on-disk auxiliary directory. This is
		// necessary because various tests expect the auxiliary directory to
		// actually exist on disk even though they don't actually write files to
		// the directory. See SSTSnapshotStorage for one example of this bad
		// behavior.
		var err error
		auxDir, err = ioutil.TempDir(os.TempDir(), "cockroach-auxiliary")
		if err != nil {
			return nil, err
		}
	} else {
		auxDir = cfg.Opts.FS.PathJoin(cfg.Dir, "auxiliary")
		if err := cfg.Opts.FS.MkdirAll(auxDir, 0755); err != nil {
			return nil, err
		}
	}

	fileRegistry := &PebbleFileRegistry{FS: cfg.Opts.FS, DBDir: cfg.Dir, ReadOnly: cfg.Opts.ReadOnly}
	if cfg.UseFileRegistry {
		if err := fileRegistry.Load(); err != nil {
			return nil, err
		}
	} else {
		if err := fileRegistry.checkNoRegistryFile(); err != nil {
			return nil, fmt.Errorf("encryption was used on this store before, but no encryption flags " +
				"specified. You need a CCL build and must fully specify the --enterprise-encryption flag")
		}
	}

	if len(cfg.ExtraOptions) > 0 {
		// Encryption is enabled.
		if !cfg.UseFileRegistry {
			return nil, fmt.Errorf("file registry is needed to support encryption")
		}
		if NewEncryptedEnvFunc == nil {
			return nil, fmt.Errorf("encryption is enabled but no function to create the encrypted env")
		}
		fs, err := NewEncryptedEnvFunc(cfg.Opts.FS, fileRegistry, cfg.Dir, cfg.Opts.ReadOnly, cfg.ExtraOptions)
		if err != nil {
			return nil, err
		}
		cfg.Opts.FS = fs
	}

	// The context dance here is done so that we have a clean context without
	// timeouts that has a copy of the log tags.
	cfg.Opts.Logger = pebbleLogger{
		ctx: logtags.WithTags(context.Background(), logtags.FromContext(ctx)),
	}
	cfg.Opts.EventListener = pebble.MakeLoggingEventListener(cfg.Opts.Logger)

	db, err := pebble.Open(cfg.StorageConfig.Dir, cfg.Opts)
	if err != nil {
		return nil, err
	}

	return &Pebble{
		db:       db,
		path:     cfg.Dir,
		auxDir:   auxDir,
		maxSize:  cfg.MaxSize,
		attrs:    cfg.Attrs,
		settings: cfg.Settings,
		fs:       cfg.Opts.FS,
		logger:   cfg.Opts.Logger,
	}, nil
}

func newPebbleInMem(ctx context.Context, attrs roachpb.Attributes, cacheSize int64) *Pebble {
	opts := DefaultPebbleOptions()
	opts.Cache = pebble.NewCache(cacheSize)
	opts.FS = vfs.NewMem()
	db, err := NewPebble(
		ctx,
		PebbleConfig{
			StorageConfig: base.StorageConfig{
				Attrs: attrs,
				// TODO(bdarnell): The hard-coded 512 MiB is wrong; see
				// https://github.com/cockroachdb/cockroach/issues/16750
				MaxSize: 512 << 20, /* 512 MiB */
			},
			Opts: opts,
		})
	if err != nil {
		panic(err)
	}
	return db
}

func (p *Pebble) String() string {
	dir := p.path
	if dir == "" {
		dir = "<in-mem>"
	}
	attrs := p.attrs.String()
	if attrs == "" {
		attrs = "<no-attributes>"
	}
	return fmt.Sprintf("%s=%s", attrs, dir)
}

// Close implements the Engine interface.
func (p *Pebble) Close() {
	if p.closed {
		p.logger.Infof("closing unopened pebble instance")
		return
	}
	p.closed = true

	if p.path == "" {
		// Remove the temporary directory when the engine is in-memory. This
		// matches the RocksDB behavior.
		//
		// TODO(peter): The aux-dir shouldn't be on-disk for in-memory
		// engines. This is just a wart that needs to be removed.
		if err := os.RemoveAll(p.auxDir); err != nil {
			p.logger.Infof("%v", err)
		}
	}

	_ = p.db.Close()
}

// Closed implements the Engine interface.
func (p *Pebble) Closed() bool {
	return p.closed
}

// ExportToSst is part of the engine.Reader interface.
func (p *Pebble) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	return pebbleExportToSst(p, startKey, endKey, startTS, endTS, exportAllRevisions, io)
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

// GetCompactionStats implements the Engine interface.
func (p *Pebble) GetCompactionStats() string {
	return p.db.Metrics().String()
}

// GetTickersAndHistograms implements the Engine interface.
func (p *Pebble) GetTickersAndHistograms() (*enginepb.TickersAndHistograms, error) {
	// TODO(hueypark): Implement this.
	return nil, nil
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

// Iterate implements the Engine interface.
func (p *Pebble) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
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
func (p *Pebble) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
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

// Attrs implements the Engine interface.
func (p *Pebble) Attrs() roachpb.Attributes {
	return p.attrs
}

// Capacity implements the Engine interface.
func (p *Pebble) Capacity() (roachpb.StoreCapacity, error) {
	return computeCapacity(p.path, p.maxSize)
}

// Flush implements the Engine interface.
func (p *Pebble) Flush() error {
	return p.db.Flush()
}

// GetStats implements the Engine interface.
func (p *Pebble) GetStats() (*Stats, error) {
	m := p.db.Metrics()
	return &Stats{
		BlockCacheHits:                 m.BlockCache.Hits,
		BlockCacheMisses:               m.BlockCache.Misses,
		BlockCacheUsage:                m.BlockCache.Size,
		BlockCachePinnedUsage:          0,
		BloomFilterPrefixChecked:       m.Filter.Hits + m.Filter.Misses,
		BloomFilterPrefixUseful:        m.Filter.Hits,
		MemtableTotalSize:              int64(m.MemTable.Size),
		Flushes:                        m.Flush.Count,
		Compactions:                    m.Compact.Count,
		TableReadersMemEstimate:        m.TableCache.Size,
		PendingCompactionBytesEstimate: int64(m.Compact.EstimatedDebt),
		L0FileCount:                    m.Levels[0].NumFiles,
	}, nil
}

// GetEncryptionRegistries implements the Engine interface.
func (p *Pebble) GetEncryptionRegistries() (*EncryptionRegistries, error) {
	// TODO(sumeer): Implement this. These are encryption-at-rest specific stats.
	return &EncryptionRegistries{}, nil
}

// GetEnvStats implements the Engine interface.
func (p *Pebble) GetEnvStats() (*EnvStats, error) {
	// TODO(sumeer): Implement this. These are encryption-at-rest specific stats.
	return &EnvStats{}, nil
}

// GetAuxiliaryDir implements the Engine interface.
func (p *Pebble) GetAuxiliaryDir() string {
	return p.auxDir
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

// Type implements the Engine interface.
func (p *Pebble) Type() enginepb.EngineType {
	return enginepb.EngineTypePebble
}

// IngestExternalFiles implements the Engine interface.
func (p *Pebble) IngestExternalFiles(ctx context.Context, paths []string) error {
	return p.db.Ingest(paths)
}

// PreIngestDelay implements the Engine interface.
func (p *Pebble) PreIngestDelay(ctx context.Context) {
	preIngestDelay(ctx, p, p.settings)
}

// ApproximateDiskBytes implements the Engine interface.
func (p *Pebble) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	// TODO(itsbilal): Add functionality in Pebble to do this count internally,
	// instead of iterating over the range.
	count := uint64(0)
	_ = p.Iterate(from, to, func(kv MVCCKeyValue) (bool, error) {
		count += uint64(kv.Key.Len() + len(kv.Value))
		return false, nil
	})
	return count, nil
}

// Compact implements the Engine interface.
func (p *Pebble) Compact() error {
	// TODO(hueypark): Implement this.
	return nil
}

// CompactRange implements the Engine interface.
func (p *Pebble) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	bufStart := EncodeKey(MVCCKey{start, hlc.Timestamp{}})
	bufEnd := EncodeKey(MVCCKey{end, hlc.Timestamp{}})
	return p.db.Compact(bufStart, bufEnd)
}

// InMem returns true if the receiver is an in-memory engine and false
// otherwise.
func (p *Pebble) InMem() bool {
	return p.path == ""
}

// OpenFile implements the Engine interface.
func (p *Pebble) OpenFile(filename string) (DBFile, error) {
	// TODO(peter): On RocksDB, the MemEnv allows creating a file when the parent
	// directory does not exist. Various tests in the storage package depend on
	// this because they are accidentally creating the required directory on the
	// actual filesystem instead of in the memory filesystem. See
	// diskSideloadedStorage and SSTSnapshotStrategy.
	if p.InMem() {
		_ = p.fs.MkdirAll(p.fs.PathDir(filename), 0755)
	}
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

// WriteFile writes data to a file in this RocksDB's env.
func (p *Pebble) WriteFile(filename string, data []byte) error {
	file, err := p.fs.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, bytes.NewReader(data))
	return err
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
	parent     *Pebble
	prefixIter pebbleIterator
	normalIter pebbleIterator
	closed     bool
}

var _ ReadWriter = &pebbleReadOnly{}

func (p *pebbleReadOnly) Close() {
	if p.closed {
		panic("closing an already-closed pebbleReadOnly")
	}
	p.closed = true
	p.prefixIter.destroy()
	p.normalIter.destroy()
}

func (p *pebbleReadOnly) Closed() bool {
	return p.closed
}

// ExportToSst is part of the engine.Reader interface.
func (p *pebbleReadOnly) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	return pebbleExportToSst(p, startKey, endKey, startTS, endTS, exportAllRevisions, io)
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

func (p *pebbleReadOnly) Iterate(start, end roachpb.Key, f func(MVCCKeyValue) (bool, error)) error {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	return iterateOnReader(p, start, end, f)
}

func (p *pebbleReadOnly) NewIterator(opts IterOptions) Iterator {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		return newPebbleIterator(p.parent.db, opts)
	}

	iter := &p.normalIter
	if opts.Prefix {
		iter = &p.prefixIter
	}
	if iter.inuse {
		panic("iterator already in use")
	}

	if iter.iter != nil {
		iter.setOptions(opts)
	} else {
		iter.init(p.parent.db, opts)
		iter.reusable = true
	}

	iter.inuse = true
	return iter
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

func (p *pebbleReadOnly) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
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

// ExportToSst is part of the engine.Reader interface.
func (p *pebbleSnapshot) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	return pebbleExportToSst(p, startKey, endKey, startTS, endTS, exportAllRevisions, io)
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
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(p, start, end, f)
}

// NewIterator implements the Reader interface.
func (p pebbleSnapshot) NewIterator(opts IterOptions) Iterator {
	return newPebbleIterator(p.snapshot, opts)
}

func pebbleExportToSst(
	e Reader,
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	sstFile := &MemFile{}
	sstWriter := MakeSSTWriter(sstFile)
	defer sstWriter.Close()

	var rows RowCounter
	iter := NewMVCCIncrementalIterator(
		e,
		MVCCIncrementalIterOptions{
			IterOptions: io,
			StartTime:   startTS,
			EndTime:     endTS,
		})
	defer iter.Close()
	for iter.Seek(MakeMVCCMetadataKey(startKey)); ; {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return nil, roachpb.BulkOpSummary{}, err
		}
		if !ok {
			break
		}
		unsafeKey := iter.UnsafeKey()
		if unsafeKey.Key.Compare(endKey) >= 0 {
			break
		}
		unsafeValue := iter.UnsafeValue()

		// Skip tombstone (len=0) records when start time is zero (non-incremental)
		// and we are not exporting all versions.
		skipTombstones := !exportAllRevisions && startTS.IsEmpty()
		if len(unsafeValue) > 0 || !skipTombstones {
			if err := rows.Count(unsafeKey.Key); err != nil {
				return nil, roachpb.BulkOpSummary{}, errors.Wrapf(err, "decoding %s", unsafeKey)
			}
			rows.BulkOpSummary.DataSize += int64(len(unsafeKey.Key) + len(unsafeValue))
			if err := sstWriter.Put(unsafeKey, unsafeValue); err != nil {
				return nil, roachpb.BulkOpSummary{}, errors.Wrapf(err, "adding key %s", unsafeKey)
			}
		}

		if exportAllRevisions {
			iter.Next()
		} else {
			iter.NextKey()
		}
	}

	if err := sstWriter.Finish(); err != nil {
		return nil, roachpb.BulkOpSummary{}, err
	}

	return sstFile.Data(), rows.BulkOpSummary, nil
}
