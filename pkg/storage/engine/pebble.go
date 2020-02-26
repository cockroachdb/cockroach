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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/fs"
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

	Separator: func(dst, a, b []byte) []byte {
		aKey, _, ok := enginepb.SplitMVCCKey(a)
		if !ok {
			return append(dst, a...)
		}
		bKey, _, ok := enginepb.SplitMVCCKey(b)
		if !ok {
			return append(dst, a...)
		}
		// If the keys are the same just return a.
		if bytes.Equal(aKey, bKey) {
			return append(dst, a...)
		}
		n := len(dst)
		// MVCC key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Separator implementation.
		dst = pebble.DefaultComparer.Separator(dst, aKey, bKey)
		// Did it pick a separator different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The separator is > aKey, so we only need to add the timestamp sentinel.
		return append(dst, 0)
	},

	Successor: func(dst, a []byte) []byte {
		aKey, _, ok := enginepb.SplitMVCCKey(a)
		if !ok {
			return append(dst, a...)
		}
		n := len(dst)
		// MVCC key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Successor implementation.
		dst = pebble.DefaultComparer.Successor(dst, aKey)
		// Did it pick a successor different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The successor is > aKey, so we only need to add the timestamp sentinel.
		return append(dst, 0)
	},

	Split: func(k []byte) int {
		key, _, ok := enginepb.SplitMVCCKey(k)
		if !ok {
			return len(k)
		}
		// This matches the behavior of libroach/KeyPrefix. RocksDB requires that
		// keys generated via a SliceTransform be comparable with normal encoded
		// MVCC keys. Encoded MVCC keys have a suffix indicating the number of
		// bytes of timestamp data. MVCC keys without a timestamp have a suffix of
		// 0. We're careful in EncodeKey to make sure that the user-key always has
		// a trailing 0. If there is no timestamp this falls out naturally. If
		// there is a timestamp we prepend a 0 to the encoded timestamp data.
		return len(key) + 1
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
			// min/max timestamp properties. This mimics the behavior of
			// TimeBoundTblPropCollector.
			// TODO(petermattis): Return the error here and in C++, see #43422.
			return nil //nolint:returnerrcheck
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
	// In RocksDB, the concurrency setting corresponds to both flushes and
	// compactions. In Pebble, there is always a slot for a flush, and
	// compactions are counted separately.
	maxConcurrentCompactions := rocksdbConcurrency - 1
	if maxConcurrentCompactions < 1 {
		maxConcurrentCompactions = 1
	}

	opts := &pebble.Options{
		Comparer:                    MVCCComparer,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    maxConcurrentCompactions,
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
		Merger:                      MVCCMerger,
		MinFlushRate:                4 << 20, // 4 MB/sec
		TablePropertyCollectors:     PebbleTablePropertyCollectors,
	}

	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}

	// Do not create bloom filters for the last level (i.e. the largest level
	// which contains data in the LSM store). This configuration reduces the size
	// of the bloom filters by 10x. This is significant given that bloom filters
	// require 1.25 bytes (10 bits) per key which can translate into gigabytes of
	// memory given typical key and value sizes. The downside is that bloom
	// filters will only be usable on the higher levels, but that seems
	// acceptable. We typically see read amplification of 5-6x on clusters
	// (i.e. there are 5-6 levels of sstables) which means we'll achieve 80-90%
	// of the benefit of having bloom filters on every level for only 10% of the
	// memory cost.
	opts.Levels[6].FilterPolicy = nil
	return opts
}

var pebbleLog *log.SecondaryLogger

// InitPebbleLogger initializes the logger to use for Pebble log messages. If
// not called, WARNING, ERROR, and FATAL logs will be output to the normal
// CockroachDB log.
func InitPebbleLogger(ctx context.Context) {
	pebbleLog = log.NewSecondaryLogger(ctx, nil, "pebble",
		true /* enableGC */, false /* forceSyncWrites */, false /* enableMsgCount */)
}

type pebbleLogger struct {
	ctx   context.Context
	depth int
}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	if pebbleLog != nil {
		pebbleLog.LogfDepth(l.ctx, l.depth, format, args...)
		// Only log INFO logs to the normal CockroachDB log at --v=3 and above.
		if !log.V(3) {
			return
		}
	}
	log.InfofDepth(l.ctx, l.depth, format, args...)
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.FatalfDepth(l.ctx, l.depth, format, args...)
}

// PebbleConfig holds all configuration parameters and knobs used in setting up
// a new Pebble instance.
type PebbleConfig struct {
	// StorageConfig contains storage configs for all storage engines.
	base.StorageConfig
	// Pebble specific options.
	Opts *pebble.Options
}

// EncryptionStatsHandler provides encryption related stats.
type EncryptionStatsHandler interface {
	// Returns a serialized enginepbccl.EncryptionStatus.
	GetEncryptionStatus() ([]byte, error)
	// Returns a serialized enginepbccl.DataKeysRegistry, scrubbed of key contents.
	GetDataKeysRegistry() ([]byte, error)
	// Returns the ID of the active data key, or "plain" if none.
	GetActiveDataKeyID() (string, error)
	// Returns the enum value of the encryption type.
	GetActiveStoreKeyType() int32
	// Returns the KeyID embedded in the serialized EncryptionSettings.
	GetKeyIDFromSettings(settings []byte) (string, error)
}

// Pebble is a wrapper around a Pebble database instance.
type Pebble struct {
	db *pebble.DB

	closed       bool
	path         string
	auxDir       string
	maxSize      int64
	attrs        roachpb.Attributes
	settings     *cluster.Settings
	statsHandler EncryptionStatsHandler
	fileRegistry *PebbleFileRegistry

	// Relevant options copied over from pebble.Options.
	fs     vfs.FS
	logger pebble.Logger
}

var _ Engine = &Pebble{}

// NewEncryptedEnvFunc creates an encrypted environment and returns the vfs.FS to use for reading
// and writing data. This should be initialized by calling engineccl.Init() before calling
// NewPebble(). The optionBytes is a binary serialized baseccl.EncryptionOptions, so that non-CCL
// code does not depend on CCL code.
var NewEncryptedEnvFunc func(fs vfs.FS, fr *PebbleFileRegistry, dbDir string, readOnly bool, optionBytes []byte) (vfs.FS, EncryptionStatsHandler, error)

// NewPebble creates a new Pebble instance, at the specified path.
func NewPebble(ctx context.Context, cfg PebbleConfig) (*Pebble, error) {
	// pebble.Open also calls EnsureDefaults, but only after doing a clone. Call
	// EnsureDefaults beforehand so we have a matching cfg here for when we save
	// cfg.FS and cfg.ReadOnly later on.
	cfg.Opts.EnsureDefaults()
	cfg.Opts.ErrorIfNotExists = cfg.MustExist
	if settings := cfg.Settings; settings != nil {
		cfg.Opts.WALMinSyncInterval = func() time.Duration {
			return minWALSyncInterval.Get(&settings.SV)
		}
	}

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
		auxDir = cfg.Opts.FS.PathJoin(cfg.Dir, base.AuxiliaryDir)
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
		fileRegistry = nil
	}

	var statsHandler EncryptionStatsHandler
	if len(cfg.ExtraOptions) > 0 {
		// Encryption is enabled.
		if !cfg.UseFileRegistry {
			return nil, fmt.Errorf("file registry is needed to support encryption")
		}
		if NewEncryptedEnvFunc == nil {
			return nil, fmt.Errorf("encryption is enabled but no function to create the encrypted env")
		}
		var err error
		cfg.Opts.FS, statsHandler, err =
			NewEncryptedEnvFunc(cfg.Opts.FS, fileRegistry, cfg.Dir, cfg.Opts.ReadOnly, cfg.ExtraOptions)
		if err != nil {
			return nil, err
		}
	}

	// The context dance here is done so that we have a clean context without
	// timeouts that has a copy of the log tags.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	cfg.Opts.Logger = pebbleLogger{
		ctx:   logCtx,
		depth: 1,
	}
	cfg.Opts.EventListener = pebble.MakeLoggingEventListener(pebbleLogger{
		ctx:   logCtx,
		depth: 2, // skip over the EventListener stack frame
	})

	db, err := pebble.Open(cfg.StorageConfig.Dir, cfg.Opts)
	if err != nil {
		return nil, err
	}

	return &Pebble{
		db:           db,
		path:         cfg.Dir,
		auxDir:       auxDir,
		maxSize:      cfg.MaxSize,
		attrs:        cfg.Attrs,
		settings:     cfg.Settings,
		statsHandler: statsHandler,
		fileRegistry: fileRegistry,
		fs:           cfg.Opts.FS,
		logger:       cfg.Opts.Logger,
	}, nil
}

func newTeeInMem(ctx context.Context, attrs roachpb.Attributes, cacheSize int64) *TeeEngine {
	// Note that we use the same unmodified directories for both pebble and
	// rocksdb. This is to make sure the file paths match up, and that we're
	// able to write to both and ingest from both memory filesystems.
	pebbleInMem := newPebbleInMem(ctx, attrs, cacheSize)
	rocksDBInMem := newRocksDBInMem(attrs, cacheSize)
	tee := NewTee(ctx, rocksDBInMem, pebbleInMem)
	tee.inMem = true
	return tee
}

func newPebbleInMem(ctx context.Context, attrs roachpb.Attributes, cacheSize int64) *Pebble {
	opts := DefaultPebbleOptions()
	opts.Cache = pebble.NewCache(cacheSize)
	defer opts.Cache.Unref()

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
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return pebbleExportToSst(p, startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

// Get implements the Engine interface.
func (p *Pebble) Get(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	ret, closer, err := p.db.Get(EncodeKey(key))
	if closer != nil {
		retCopy := make([]byte, len(ret))
		copy(retCopy, ret)
		ret = retCopy
		closer.Close()
	}
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// GetCompactionStats implements the Engine interface.
func (p *Pebble) GetCompactionStats() string {
	// NB: The initial blank line matches the formatting used by RocksDB and
	// ensures that compaction stats display will not contain the log prefix
	// (this method is only used for logging purposes).
	return "\n" + p.db.Metrics().String()
}

// GetProto implements the Engine interface.
func (p *Pebble) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		return false, 0, 0, emptyKeyError()
	}
	encodedKey := EncodeKey(key)
	val, closer, err := p.db.Get(encodedKey)
	if closer != nil {
		if msg != nil {
			err = protoutil.Unmarshal(val, msg)
		}
		keyBytes = int64(len(encodedKey))
		valBytes = int64(len(val))
		closer.Close()
		return true, keyBytes, valBytes, err
	}
	if err == pebble.ErrNotFound {
		return false, 0, 0, nil
	}
	return false, 0, 0, err
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
	rv := &EncryptionRegistries{}
	var err error
	if p.statsHandler != nil {
		rv.KeyRegistry, err = p.statsHandler.GetDataKeysRegistry()
		if err != nil {
			return nil, err
		}
	}
	if p.fileRegistry != nil {
		rv.FileRegistry = []byte(p.fileRegistry.getRegistryCopy().String())
	}
	return rv, nil
}

// GetEnvStats implements the Engine interface.
func (p *Pebble) GetEnvStats() (*EnvStats, error) {
	// TODO(sumeer): make the stats complete. There are no bytes stats. The TotalFiles is missing
	// files that are not in the registry (from before encryption was enabled).
	stats := &EnvStats{}
	if p.statsHandler == nil {
		return stats, nil
	}
	stats.EncryptionType = p.statsHandler.GetActiveStoreKeyType()
	var err error
	stats.EncryptionStatus, err = p.statsHandler.GetEncryptionStatus()
	if err != nil {
		return nil, err
	}
	fr := p.fileRegistry.getRegistryCopy()
	if fr != nil {
		stats.TotalFiles = uint64(len(fr.Files))
	}
	activeKeyID, err := p.statsHandler.GetActiveDataKeyID()
	if err != nil {
		return nil, err
	}
	for _, entry := range fr.Files {
		keyID, err := p.statsHandler.GetKeyIDFromSettings(entry.EncryptionSettings)
		if err != nil {
			return nil, err
		}
		if len(keyID) == 0 {
			keyID = "plain"
		}
		if keyID == activeKeyID {
			stats.ActiveKeyFiles++
		}
	}
	return stats, nil
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
	count, err := p.db.EstimateDiskUsage(from, to)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Compact implements the Engine interface.
func (p *Pebble) Compact() error {
	return p.db.Compact(nil, EncodeKey(MVCCKeyMax))
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

// DeleteFile implements the FS interface.
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

// LinkFile implements the FS interface.
func (p *Pebble) LinkFile(oldname, newname string) error {
	return p.fs.Link(oldname, newname)
}

var _ fs.FS = &Pebble{}

// CreateFile implements the FS interface.
func (p *Pebble) CreateFile(name string) (fs.File, error) {
	// TODO(peter): On RocksDB, the MemEnv allows creating a file when the parent
	// directory does not exist. Various tests in the storage package depend on
	// this because they are accidentally creating the required directory on the
	// actual filesystem instead of in the memory filesystem. See
	// diskSideloadedStorage and SSTSnapshotStrategy.
	if p.InMem() {
		_ = p.fs.MkdirAll(p.fs.PathDir(name), 0755)
	}
	return p.fs.Create(name)
}

// CreateFileWithSync implements the FS interface.
func (p *Pebble) CreateFileWithSync(name string, bytesPerSync int) (fs.File, error) {
	// TODO(peter): On RocksDB, the MemEnv allows creating a file when the parent
	// directory does not exist. Various tests in the storage package depend on
	// this because they are accidentally creating the required directory on the
	// actual filesystem instead of in the memory filesystem. See
	// diskSideloadedStorage and SSTSnapshotStrategy.
	if p.InMem() {
		_ = p.fs.MkdirAll(p.fs.PathDir(name), 0755)
	}
	f, err := p.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: bytesPerSync}), nil
}

// OpenFile implements the FS interface.
func (p *Pebble) OpenFile(name string) (fs.File, error) {
	return p.fs.Open(name)
}

// OpenDir implements the FS interface.
func (p *Pebble) OpenDir(name string) (fs.File, error) {
	return p.fs.OpenDir(name)
}

// RenameFile implements the FS interface.
func (p *Pebble) RenameFile(oldname, newname string) error {
	return p.fs.Rename(oldname, newname)
}

// CreateDir implements the FS interface.
func (p *Pebble) CreateDir(name string) error {
	return p.fs.MkdirAll(name, 0755)
}

// DeleteDir implements the FS interface.
func (p *Pebble) DeleteDir(name string) error {
	return p.fs.Remove(name)
}

// ListDir implements the FS interface.
func (p *Pebble) ListDir(name string) ([]string, error) {
	return p.fs.List(name)
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

	sort.Sort(sstables)
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
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return pebbleExportToSst(p, startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
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
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return pebbleExportToSst(p, startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

// Get implements the Reader interface.
func (p *pebbleSnapshot) Get(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}

	ret, closer, err := p.snapshot.Get(EncodeKey(key))
	if closer != nil {
		retCopy := make([]byte, len(ret))
		copy(retCopy, ret)
		ret = retCopy
		closer.Close()
	}
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
	encodedKey := EncodeKey(key)
	val, closer, err := p.snapshot.Get(encodedKey)
	if closer != nil {
		if msg != nil {
			err = protoutil.Unmarshal(val, msg)
		}
		keyBytes = int64(len(encodedKey))
		valBytes = int64(len(val))
		closer.Close()
		return true, keyBytes, valBytes, err
	}
	if err == pebble.ErrNotFound {
		return false, 0, 0, nil
	}
	return false, 0, 0, err
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
	reader Reader,
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	sstFile := &MemFile{}
	sstWriter := MakeBackupSSTWriter(sstFile)
	defer sstWriter.Close()

	var rows RowCounter
	iter := NewMVCCIncrementalIterator(
		reader,
		MVCCIncrementalIterOptions{
			IterOptions: io,
			StartTime:   startTS,
			EndTime:     endTS,
		})
	defer iter.Close()
	var curKey roachpb.Key // only used if exportAllRevisions
	var resumeKey roachpb.Key
	paginated := targetSize > 0
	for iter.SeekGE(MakeMVCCMetadataKey(startKey)); ; {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return nil, roachpb.BulkOpSummary{}, nil, err
		}
		if !ok {
			break
		}
		unsafeKey := iter.UnsafeKey()
		if unsafeKey.Key.Compare(endKey) >= 0 {
			break
		}
		unsafeValue := iter.UnsafeValue()
		isNewKey := !exportAllRevisions || !unsafeKey.Key.Equal(curKey)
		if paginated && exportAllRevisions && isNewKey {
			curKey = append(curKey[:0], unsafeKey.Key...)
		}

		// Skip tombstone (len=0) records when start time is zero (non-incremental)
		// and we are not exporting all versions.
		skipTombstones := !exportAllRevisions && startTS.IsEmpty()
		if len(unsafeValue) > 0 || !skipTombstones {
			if err := rows.Count(unsafeKey.Key); err != nil {
				return nil, roachpb.BulkOpSummary{}, nil, errors.Wrapf(err, "decoding %s", unsafeKey)
			}
			curSize := rows.BulkOpSummary.DataSize
			reachedTargetSize := curSize > 0 && uint64(curSize) >= targetSize
			if paginated && isNewKey && reachedTargetSize {
				// Allocate the right size for resumeKey rather than using curKey.
				resumeKey = append(make(roachpb.Key, 0, len(unsafeKey.Key)), unsafeKey.Key...)
				break
			}
			if err := sstWriter.Put(unsafeKey, unsafeValue); err != nil {
				return nil, roachpb.BulkOpSummary{}, nil, errors.Wrapf(err, "adding key %s", unsafeKey)
			}
			newSize := curSize + int64(len(unsafeKey.Key)+len(unsafeValue))
			if maxSize > 0 && newSize > int64(maxSize) {
				return nil, roachpb.BulkOpSummary{}, nil,
					errors.Errorf("export size (%d bytes) exceeds max size (%d bytes)", newSize, maxSize)
			}
			rows.BulkOpSummary.DataSize = newSize
		}

		if exportAllRevisions {
			iter.Next()
		} else {
			iter.NextKey()
		}
	}

	if rows.BulkOpSummary.DataSize == 0 {
		// If no records were added to the sstable, skip completing it and return a
		// nil slice – the export code will discard it anyway (based on 0 DataSize).
		return nil, roachpb.BulkOpSummary{}, nil, nil
	}

	if err := sstWriter.Finish(); err != nil {
		return nil, roachpb.BulkOpSummary{}, nil, err
	}

	return sstFile.Data(), rows.BulkOpSummary, resumeKey, nil
}
