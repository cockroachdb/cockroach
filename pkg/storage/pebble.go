// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
)

const (
	maxSyncDurationFatalOnExceededDefault = true
)

// Default for MaxSyncDuration below.
var maxSyncDurationDefault = envutil.EnvOrDefaultDuration("COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT", 60*time.Second)

// MaxSyncDuration is the threshold above which an observed engine sync duration
// triggers either a warning or a fatal error.
var MaxSyncDuration = settings.RegisterDurationSetting(
	"storage.max_sync_duration",
	"maximum duration for disk operations; any operations that take longer"+
		" than this setting trigger a warning log entry or process crash",
	maxSyncDurationDefault,
)

// MaxSyncDurationFatalOnExceeded governs whether disk stalls longer than
// MaxSyncDuration fatal the Cockroach process. Defaults to true.
var MaxSyncDurationFatalOnExceeded = settings.RegisterBoolSetting(
	"storage.max_sync_duration.fatal.enabled",
	"if true, fatal the process when a disk operation exceeds storage.max_sync_duration",
	maxSyncDurationFatalOnExceededDefault,
)

// EngineKeyCompare compares cockroach keys, including the version (which
// could be MVCC timestamps).
func EngineKeyCompare(a, b []byte) int {
	// NB: For performance, this routine manually splits the key into the
	// user-key and version components rather than using DecodeEngineKey. In
	// most situations, use DecodeEngineKey or GetKeyPartFromEngineKey or
	// SplitMVCCKey instead of doing this.
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}

	// Compute the index of the separator between the key and the version.
	aSep := aEnd - int(a[aEnd])
	bSep := bEnd - int(b[bEnd])
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}

	// Compare the "user key" part of the key.
	if c := bytes.Compare(a[:aSep], b[:bSep]); c != 0 {
		return c
	}

	// Compare the version part of the key. Note that when the version is a
	// timestamp, the timestamp encoding causes byte comparison to be equivalent
	// to timestamp comparison.
	aTS := a[aSep:aEnd]
	bTS := b[bSep:bEnd]
	if len(aTS) == 0 {
		if len(bTS) == 0 {
			return 0
		}
		return -1
	} else if len(bTS) == 0 {
		return 1
	}
	return bytes.Compare(bTS, aTS)
}

// EngineComparer is a pebble.Comparer object that implements MVCC-specific
// comparator settings for use with Pebble.
var EngineComparer = &pebble.Comparer{
	Compare: EngineKeyCompare,

	AbbreviatedKey: func(k []byte) uint64 {
		key, ok := GetKeyPartFromEngineKey(k)
		if !ok {
			return 0
		}
		return pebble.DefaultComparer.AbbreviatedKey(key)
	},

	FormatKey: func(k []byte) fmt.Formatter {
		decoded, ok := DecodeEngineKey(k)
		if !ok {
			return mvccKeyFormatter{err: errors.Errorf("invalid encoded engine key: %x", k)}
		}
		if decoded.IsMVCCKey() {
			mvccKey, err := decoded.ToMVCCKey()
			if err != nil {
				return mvccKeyFormatter{err: err}
			}
			return mvccKeyFormatter{key: mvccKey}
		}
		return EngineKeyFormatter{key: decoded}
	},

	Separator: func(dst, a, b []byte) []byte {
		aKey, ok := GetKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		bKey, ok := GetKeyPartFromEngineKey(b)
		if !ok {
			return append(dst, a...)
		}
		// If the keys are the same just return a.
		if bytes.Equal(aKey, bKey) {
			return append(dst, a...)
		}
		n := len(dst)
		// Engine key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Separator implementation.
		dst = pebble.DefaultComparer.Separator(dst, aKey, bKey)
		// Did it pick a separator different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The separator is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},

	Successor: func(dst, a []byte) []byte {
		aKey, ok := GetKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		n := len(dst)
		// Engine key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Successor implementation.
		dst = pebble.DefaultComparer.Successor(dst, aKey)
		// Did it pick a successor different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The successor is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},

	Split: func(k []byte) int {
		key, ok := GetKeyPartFromEngineKey(k)
		if !ok {
			return len(k)
		}
		// Pebble requires that keys generated via a split be comparable with
		// normal encoded engine keys. Encoded engine keys have a suffix
		// indicating the number of bytes of version data. Engine keys without a
		// version have a suffix of 0. We're careful in EncodeKey to make sure
		// that the user-key always has a trailing 0. If there is no version this
		// falls out naturally. If there is a version we prepend a 0 to the
		// encoded version data.
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
	engineKey, ok := DecodeEngineKey(key.UserKey)
	if !ok {
		return errors.Errorf("failed to split engine key")
	}
	if engineKey.IsMVCCKey() && len(engineKey.Version) > 0 {
		t.lastValue = t.lastValue[:0]
		t.updateBounds(engineKey.Version)
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
			ts := encodeTimestamp(meta.Timestamp.ToTimestamp())
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

// pebbleDeleteRangeCollector is the equivalent table collector as the RocksDB
// DeleteRangeTblPropCollector. Pebble does not require it because Pebble will
// prioritize its own compactions of range tombstones.
type pebbleDeleteRangeCollector struct{}

func (pebbleDeleteRangeCollector) Add(_ pebble.InternalKey, _ []byte) error {
	return nil
}

func (pebbleDeleteRangeCollector) Finish(_ map[string]string) error {
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
		Comparer:                    EngineComparer,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    maxConcurrentCompactions,
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
		Merger:                      MVCCMerger,
		TablePropertyCollectors:     PebbleTablePropertyCollectors,
	}
	// Automatically flush 10s after the first range tombstone is added to a
	// memtable. This ensures that we can reclaim space even when there's no
	// activity on the database generating flushes.
	opts.Experimental.DeleteRangeFlushDelay = 10 * time.Second
	// Enable deletion pacing. This helps prevent disk slowness events on some
	// SSDs, that kick off an expensive GC if a lot of files are deleted at
	// once.
	opts.Experimental.MinDeletionRate = 128 << 20 // 128 MB

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

	// Set disk health check interval to min(5s, maxSyncDurationDefault). This
	// is mostly to ease testing; the default of 5s is too infrequent to test
	// conveniently. See the disk-stalled roachtest for an example of how this
	// is used.
	diskHealthCheckInterval := 5 * time.Second
	if diskHealthCheckInterval.Seconds() > maxSyncDurationDefault.Seconds() {
		diskHealthCheckInterval = maxSyncDurationDefault
	}
	// Instantiate a file system with disk health checking enabled. This FS wraps
	// vfs.Default, and can be wrapped for encryption-at-rest.
	opts.FS = vfs.WithDiskHealthChecks(vfs.Default, diskHealthCheckInterval,
		func(name string, duration time.Duration) {
			opts.EventListener.DiskSlow(pebble.DiskSlowInfo{
				Path:     name,
				Duration: duration,
			})
		})
	return opts
}

type pebbleLogger struct {
	ctx   context.Context
	depth int
}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	log.Storage.InfofDepth(l.ctx, l.depth, format, args...)
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.Storage.FatalfDepth(l.ctx, l.depth, format, args...)
}

// PebbleConfig holds all configuration parameters and knobs used in setting up
// a new Pebble instance.
type PebbleConfig struct {
	// StorageConfig contains storage configs for all storage engines.
	// A non-nil cluster.Settings must be provided in the StorageConfig for a
	// Pebble instance that will be used to write intents.
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

	closed  bool
	path    string
	auxDir  string
	maxSize int64
	attrs   roachpb.Attributes
	// settings must be non-nil if this Pebble instance will be used to write
	// intents.
	settings      *cluster.Settings
	statsHandler  EncryptionStatsHandler
	fileRegistry  *PebbleFileRegistry
	eventListener *pebble.EventListener

	// Stats updated by pebble.EventListener invocations, and returned in
	// GetStats. Updated and retrieved atomically.
	diskSlowCount, diskStallCount uint64

	// Relevant options copied over from pebble.Options.
	fs     vfs.FS
	logger pebble.Logger

	wrappedIntentWriter intentDemuxWriter
}

var _ Engine = &Pebble{}

// NewEncryptedEnvFunc creates an encrypted environment and returns the vfs.FS to use for reading
// and writing data. This should be initialized by calling engineccl.Init() before calling
// NewPebble(). The optionBytes is a binary serialized baseccl.EncryptionOptions, so that non-CCL
// code does not depend on CCL code.
var NewEncryptedEnvFunc func(fs vfs.FS, fr *PebbleFileRegistry, dbDir string, readOnly bool, optionBytes []byte) (vfs.FS, EncryptionStatsHandler, error)

// ResolveEncryptedEnvOptions fills in cfg.Opts.FS with an encrypted vfs if this
// store has encryption-at-rest enabled. Also returns the associated file
// registry and EncryptionStatsHandler.
func ResolveEncryptedEnvOptions(
	cfg *PebbleConfig,
) (*PebbleFileRegistry, EncryptionStatsHandler, error) {
	fileRegistry := &PebbleFileRegistry{FS: cfg.Opts.FS, DBDir: cfg.Dir, ReadOnly: cfg.Opts.ReadOnly}
	if cfg.UseFileRegistry {
		if err := fileRegistry.Load(); err != nil {
			return nil, nil, err
		}
	} else {
		if err := fileRegistry.checkNoRegistryFile(); err != nil {
			return nil, nil, fmt.Errorf("encryption was used on this store before, but no encryption flags " +
				"specified. You need a CCL build and must fully specify the --enterprise-encryption flag")
		}
		fileRegistry = nil
	}

	var statsHandler EncryptionStatsHandler
	if len(cfg.ExtraOptions) > 0 {
		// Encryption is enabled.
		if !cfg.UseFileRegistry {
			return nil, nil, fmt.Errorf("file registry is needed to support encryption")
		}
		if NewEncryptedEnvFunc == nil {
			return nil, nil, fmt.Errorf("encryption is enabled but no function to create the encrypted env")
		}
		var err error
		cfg.Opts.FS, statsHandler, err =
			NewEncryptedEnvFunc(cfg.Opts.FS, fileRegistry, cfg.Dir, cfg.Opts.ReadOnly, cfg.ExtraOptions)
		if err != nil {
			return nil, nil, err
		}
	}
	return fileRegistry, statsHandler, nil
}

// NewPebble creates a new Pebble instance, at the specified path.
func NewPebble(ctx context.Context, cfg PebbleConfig) (*Pebble, error) {
	// pebble.Open also calls EnsureDefaults, but only after doing a clone. Call
	// EnsureDefaults beforehand so we have a matching cfg here for when we save
	// cfg.FS and cfg.ReadOnly later on.
	if cfg.Opts == nil {
		cfg.Opts = DefaultPebbleOptions()
	}
	cfg.Opts.EnsureDefaults()
	cfg.Opts.Cleaner = pebble.ArchiveCleaner{}
	cfg.Opts.ErrorIfNotExists = cfg.MustExist
	if settings := cfg.Settings; settings != nil {
		cfg.Opts.WALMinSyncInterval = func() time.Duration {
			return minWALSyncInterval.Get(&settings.SV)
		}
	}

	auxDir := cfg.Opts.FS.PathJoin(cfg.Dir, base.AuxiliaryDir)
	if err := cfg.Opts.FS.MkdirAll(auxDir, 0755); err != nil {
		return nil, err
	}

	fileRegistry, statsHandler, err := ResolveEncryptedEnvOptions(&cfg)
	if err != nil {
		return nil, err
	}

	// The context dance here is done so that we have a clean context without
	// timeouts that has a copy of the log tags.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	logCtx = logtags.AddTag(logCtx, "pebble", nil)
	cfg.Opts.Logger = pebbleLogger{
		ctx:   logCtx,
		depth: 1,
	}
	cfg.Opts.EventListener = pebble.MakeLoggingEventListener(pebbleLogger{
		ctx:   logCtx,
		depth: 2, // skip over the EventListener stack frame
	})
	p := &Pebble{
		path:         cfg.Dir,
		auxDir:       auxDir,
		maxSize:      cfg.MaxSize,
		attrs:        cfg.Attrs,
		settings:     cfg.Settings,
		statsHandler: statsHandler,
		fileRegistry: fileRegistry,
		fs:           cfg.Opts.FS,
		logger:       cfg.Opts.Logger,
	}
	p.connectEventMetrics(ctx, &cfg.Opts.EventListener)
	p.eventListener = &cfg.Opts.EventListener
	p.wrappedIntentWriter = wrapIntentWriter(ctx, p, cfg.Settings, true /* isLongLived */)

	db, err := pebble.Open(cfg.StorageConfig.Dir, cfg.Opts)
	if err != nil {
		return nil, err
	}
	p.db = db

	return p, nil
}

func newPebbleInMem(
	ctx context.Context, attrs roachpb.Attributes, cacheSize int64, settings *cluster.Settings,
) *Pebble {
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
				MaxSize:  512 << 20, /* 512 MiB */
				Settings: settings,
			},
			Opts: opts,
		})
	if err != nil {
		panic(err)
	}
	return db
}

func (p *Pebble) connectEventMetrics(ctx context.Context, eventListener *pebble.EventListener) {
	oldDiskSlow := eventListener.DiskSlow

	eventListener.DiskSlow = func(info pebble.DiskSlowInfo) {
		oldDiskSlow(info)
		maxSyncDuration := maxSyncDurationDefault
		fatalOnExceeded := maxSyncDurationFatalOnExceededDefault
		if p.settings != nil {
			maxSyncDuration = MaxSyncDuration.Get(&p.settings.SV)
			fatalOnExceeded = MaxSyncDurationFatalOnExceeded.Get(&p.settings.SV)
		}
		if info.Duration.Seconds() >= maxSyncDuration.Seconds() {
			atomic.AddUint64(&p.diskStallCount, 1)
			// Note that the below log messages go to the main cockroach log, not
			// the pebble-specific log.
			if fatalOnExceeded {
				log.Fatalf(ctx, "disk stall detected: pebble unable to write to %s in %.2f seconds",
					info.Path, redact.Safe(info.Duration.Seconds()))
			} else {
				log.Errorf(ctx, "disk stall detected: pebble unable to write to %s in %.2f seconds",
					info.Path, redact.Safe(info.Duration.Seconds()))
			}
			return
		}
		atomic.AddUint64(&p.diskSlowCount, 1)
	}
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
	_ = p.db.Close()
}

// Closed implements the Engine interface.
func (p *Pebble) Closed() bool {
	return p.closed
}

// ExportMVCCToSst is part of the engine.Reader interface.
func (p *Pebble) ExportMVCCToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	useTBI bool,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	r := wrapReader(p)
	// Doing defer r.Free() does not inline.
	b, summary, k, err := pebbleExportToSst(r, startKey, endKey, startTS, endTS, exportAllRevisions, targetSize,
		maxSize, useTBI)
	r.Free()
	return b, summary, k, err
}

// MVCCGet implements the Engine interface.
func (p *Pebble) MVCCGet(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	r := wrapReader(p)
	// Doing defer r.Free() does not inline.
	v, err := r.MVCCGet(key)
	r.Free()
	return v, err
}

func (p *Pebble) rawGet(key []byte) ([]byte, error) {
	ret, closer, err := p.db.Get(key)
	if closer != nil {
		retCopy := make([]byte, len(ret))
		copy(retCopy, ret)
		ret = retCopy
		closer.Close()
	}
	if errors.Is(err, pebble.ErrNotFound) || len(ret) == 0 {
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

// MVCCGetProto implements the Engine interface.
func (p *Pebble) MVCCGetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pebbleGetProto(p, key, msg)
}

// MVCCIterate implements the Engine interface.
func (p *Pebble) MVCCIterate(
	start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(r, start, end, iterKind, f)
		r.Free()
		return err
	}
	return iterateOnReader(p, start, end, iterKind, f)
}

// NewMVCCIterator implements the Engine interface.
func (p *Pebble) NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter := r.NewMVCCIterator(iterKind, opts)
		r.Free()
		if util.RaceEnabled {
			iter = wrapInUnsafeIter(iter)
		}
		return iter
	}
	iter := MVCCIterator(newPebbleIterator(p.db, nil, opts))
	if iter == nil {
		panic("couldn't create a new iterator")
	}
	if util.RaceEnabled {
		iter = wrapInUnsafeIter(iter)
	}
	return iter
}

// NewEngineIterator implements the Engine interface.
func (p *Pebble) NewEngineIterator(opts IterOptions) EngineIterator {
	iter := newPebbleIterator(p.db, nil, opts)
	if iter == nil {
		panic("couldn't create a new iterator")
	}
	return iter
}

// ConsistentIterators implements the Engine interface.
func (p *Pebble) ConsistentIterators() bool {
	return false
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

// ClearMVCC implements the Engine interface.
func (p *Pebble) ClearMVCC(key MVCCKey) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return p.clear(key)
}

// ClearUnversioned implements the Engine interface.
func (p *Pebble) ClearUnversioned(key roachpb.Key) error {
	return p.clear(MVCCKey{Key: key})
}

// ClearIntent implements the Engine interface.
func (p *Pebble) ClearIntent(
	key roachpb.Key, state PrecedingIntentState, txnDidNotUpdateMeta bool, txnUUID uuid.UUID,
) (int, error) {
	_, separatedIntentCountDelta, err :=
		p.wrappedIntentWriter.ClearIntent(key, state, txnDidNotUpdateMeta, txnUUID, nil)
	return separatedIntentCountDelta, err
}

// ClearEngineKey implements the Engine interface.
func (p *Pebble) ClearEngineKey(key EngineKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Delete(key.Encode(), pebble.Sync)
}

func (p *Pebble) clear(key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Delete(EncodeKey(key), pebble.Sync)
}

// SingleClearEngineKey implements the Engine interface.
func (p *Pebble) SingleClearEngineKey(key EngineKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.SingleDelete(key.Encode(), pebble.Sync)
}

// ClearRawRange implements the Engine interface.
func (p *Pebble) ClearRawRange(start, end roachpb.Key) error {
	return p.clearRange(MVCCKey{Key: start}, MVCCKey{Key: end})
}

// ClearMVCCRangeAndIntents implements the Engine interface.
func (p *Pebble) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	_, err := p.wrappedIntentWriter.ClearMVCCRangeAndIntents(start, end, nil)
	return err

}

// ClearMVCCRange implements the Engine interface.
func (p *Pebble) ClearMVCCRange(start, end MVCCKey) error {
	return p.clearRange(start, end)
}

func (p *Pebble) clearRange(start, end MVCCKey) error {
	bufStart := EncodeKey(start)
	bufEnd := EncodeKey(end)
	return p.db.DeleteRange(bufStart, bufEnd, pebble.Sync)
}

// ClearIterRange implements the Engine interface.
func (p *Pebble) ClearIterRange(iter MVCCIterator, start, end roachpb.Key) error {
	// Write all the tombstones in one batch.
	batch := p.NewUnindexedBatch(true /* writeOnly */)
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

// PutMVCC implements the Engine interface.
func (p *Pebble) PutMVCC(key MVCCKey, value []byte) error {
	if key.Timestamp.IsEmpty() {
		panic("PutMVCC timestamp is empty")
	}
	return p.put(key, value)
}

// PutUnversioned implements the Engine interface.
func (p *Pebble) PutUnversioned(key roachpb.Key, value []byte) error {
	return p.put(MVCCKey{Key: key}, value)
}

// PutIntent implements the Engine interface.
func (p *Pebble) PutIntent(
	ctx context.Context,
	key roachpb.Key,
	value []byte,
	state PrecedingIntentState,
	txnDidNotUpdateMeta bool,
	txnUUID uuid.UUID,
) (int, error) {

	_, separatedIntentCountDelta, err :=
		p.wrappedIntentWriter.PutIntent(ctx, key, value, state, txnDidNotUpdateMeta, txnUUID, nil)
	return separatedIntentCountDelta, err
}

// PutEngineKey implements the Engine interface.
func (p *Pebble) PutEngineKey(key EngineKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Set(key.Encode(), value, pebble.Sync)
}

// SafeToWriteSeparatedIntents implements the Engine interface.
func (p *Pebble) SafeToWriteSeparatedIntents(ctx context.Context) (bool, error) {
	// This is not fast. Pebble should not be used by writers that want
	// performance. They should use pebbleBatch.
	return p.wrappedIntentWriter.safeToWriteSeparatedIntents(ctx)
}

// IsSeparatedIntentsEnabledForTesting implements the Engine interface.
func (p *Pebble) IsSeparatedIntentsEnabledForTesting(ctx context.Context) bool {
	return !p.settings.Version.ActiveVersionOrEmpty(ctx).Less(
		clusterversion.ByKey(clusterversion.SeparatedIntents)) && SeparatedIntentsEnabled.Get(&p.settings.SV)
}

func (p *Pebble) put(key MVCCKey, value []byte) error {
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

// GetMetrics implements the Engine interface.
func (p *Pebble) GetMetrics() (*Metrics, error) {
	m := p.db.Metrics()

	// Aggregate compaction stats across levels.
	var ingestedBytes, compactedBytesRead, compactedBytesWritten, numSSTables int64
	for _, lm := range m.Levels {
		ingestedBytes += int64(lm.BytesIngested)
		compactedBytesRead += int64(lm.BytesRead)
		compactedBytesWritten += int64(lm.BytesCompacted)
		numSSTables += lm.NumFiles
	}

	return &Metrics{
		BlockCacheHits:                 m.BlockCache.Hits,
		BlockCacheMisses:               m.BlockCache.Misses,
		BlockCacheUsage:                m.BlockCache.Size,
		BlockCachePinnedUsage:          0,
		BloomFilterPrefixChecked:       m.Filter.Hits + m.Filter.Misses,
		BloomFilterPrefixUseful:        m.Filter.Hits,
		DiskSlowCount:                  int64(atomic.LoadUint64(&p.diskSlowCount)),
		DiskStallCount:                 int64(atomic.LoadUint64(&p.diskStallCount)),
		MemtableTotalSize:              int64(m.MemTable.Size),
		Flushes:                        m.Flush.Count,
		FlushedBytes:                   int64(m.Levels[0].BytesFlushed),
		Compactions:                    m.Compact.Count,
		IngestedBytes:                  ingestedBytes,
		CompactedBytesRead:             compactedBytesRead,
		CompactedBytesWritten:          compactedBytesWritten,
		TableReadersMemEstimate:        m.TableCache.Size,
		PendingCompactionBytesEstimate: int64(m.Compact.EstimatedDebt),
		L0FileCount:                    m.Levels[0].NumFiles,
		L0SublevelCount:                int64(m.Levels[0].Sublevels),
		ReadAmplification:              int64(m.ReadAmp()),
		NumSSTables:                    numSSTables,
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
		rv.FileRegistry, err = protoutil.Marshal(p.fileRegistry.getRegistryCopy())
		if err != nil {
			return nil, err
		}
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
	activeKeyID, err := p.statsHandler.GetActiveDataKeyID()
	if err != nil {
		return nil, err
	}

	m := p.db.Metrics()
	stats.TotalFiles = 3 /* CURRENT, MANIFEST, OPTIONS */
	stats.TotalFiles += uint64(m.WAL.Files + m.Table.ZombieCount + m.WAL.ObsoleteFiles + m.Table.ObsoleteCount)
	stats.TotalBytes = m.WAL.Size + m.Table.ZombieSize + m.Table.ObsoleteSize
	for _, l := range m.Levels {
		stats.TotalFiles += uint64(l.NumFiles)
		stats.TotalBytes += uint64(l.Size)
	}

	sstSizes := make(map[pebble.FileNum]uint64)
	sstInfos, err := p.db.SSTables()
	if err != nil {
		return nil, err
	}
	for _, ssts := range sstInfos {
		for _, sst := range ssts {
			sstSizes[sst.FileNum] = sst.Size
		}
	}

	for filePath, entry := range fr.Files {
		keyID, err := p.statsHandler.GetKeyIDFromSettings(entry.EncryptionSettings)
		if err != nil {
			return nil, err
		}
		if len(keyID) == 0 {
			keyID = "plain"
		}
		if keyID != activeKeyID {
			continue
		}
		stats.ActiveKeyFiles++

		filename := p.fs.PathBase(filePath)
		numStr := strings.TrimSuffix(filename, ".sst")
		if len(numStr) == len(filename) {
			continue // not a sstable
		}
		u, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing filename %q", errors.Safe(filename))
		}
		stats.ActiveKeyBytes += sstSizes[pebble.FileNum(u)]
	}
	return stats, nil
}

// GetAuxiliaryDir implements the Engine interface.
func (p *Pebble) GetAuxiliaryDir() string {
	return p.auxDir
}

// NewBatch implements the Engine interface.
func (p *Pebble) NewBatch() Batch {
	return newPebbleBatch(p.db, p.db.NewIndexedBatch(), false /* writeOnly */, p.settings)
}

// NewReadOnly implements the Engine interface.
func (p *Pebble) NewReadOnly() ReadWriter {
	// TODO(sumeer): a sync.Pool for pebbleReadOnly would save on allocations
	// for the underlying pebbleIterators.
	return &pebbleReadOnly{
		parent: p,
		// Defensively set reusable=true. One has to be careful about this since
		// an accidental false value would cause these iterators, that are value
		// members of pebbleReadOnly, to be put in the pebbleIterPool.
		prefixIter:       pebbleIterator{reusable: true},
		normalIter:       pebbleIterator{reusable: true},
		prefixEngineIter: pebbleIterator{reusable: true},
		normalEngineIter: pebbleIterator{reusable: true},
	}
}

// NewUnindexedBatch implements the Engine interface.
func (p *Pebble) NewUnindexedBatch(writeOnly bool) Batch {
	return newPebbleBatch(p.db, p.db.NewBatch(), writeOnly, p.settings)
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

// Remove implements the FS interface.
func (p *Pebble) Remove(filename string) error {
	return p.fs.Remove(filename)
}

// RemoveAll implements the Engine interface.
func (p *Pebble) RemoveAll(dir string) error {
	return p.fs.RemoveAll(dir)
}

// Link implements the FS interface.
func (p *Pebble) Link(oldname, newname string) error {
	return p.fs.Link(oldname, newname)
}

var _ fs.FS = &Pebble{}

// Create implements the FS interface.
func (p *Pebble) Create(name string) (fs.File, error) {
	return p.fs.Create(name)
}

// CreateWithSync implements the FS interface.
func (p *Pebble) CreateWithSync(name string, bytesPerSync int) (fs.File, error) {
	f, err := p.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: bytesPerSync}), nil
}

// Open implements the FS interface.
func (p *Pebble) Open(name string) (fs.File, error) {
	return p.fs.Open(name)
}

// OpenDir implements the FS interface.
func (p *Pebble) OpenDir(name string) (fs.File, error) {
	return p.fs.OpenDir(name)
}

// Rename implements the FS interface.
func (p *Pebble) Rename(oldname, newname string) error {
	return p.fs.Rename(oldname, newname)
}

// MkdirAll implements the FS interface.
func (p *Pebble) MkdirAll(name string) error {
	return p.fs.MkdirAll(name, 0755)
}

// RemoveDir implements the FS interface.
func (p *Pebble) RemoveDir(name string) error {
	return p.fs.Remove(name)
}

// List implements the FS interface.
func (p *Pebble) List(name string) ([]string, error) {
	dirents, err := p.fs.List(name)
	sort.Strings(dirents)
	return dirents, err
}

// Stat implements the FS interface.
func (p *Pebble) Stat(name string) (os.FileInfo, error) {
	return p.fs.Stat(name)
}

// CreateCheckpoint implements the Engine interface.
func (p *Pebble) CreateCheckpoint(dir string) error {
	return p.db.Checkpoint(dir)
}

type pebbleReadOnly struct {
	parent *Pebble
	// The iterator reuse optimization in pebbleReadOnly is for servicing a
	// BatchRequest, such that the iterators get reused across different
	// requests in the batch.
	// Reuse iterators for {normal,prefix} x {MVCCKey,EngineKey} iteration. We
	// need separate iterators for EngineKey and MVCCKey iteration since
	// iterators that make separated locks/intents look as interleaved need to
	// use both simultaneously.
	// When the first iterator is initialized, the underlying *pebble.Iterator
	// is stashed in iter, so that subsequent iterator initialization can use
	// Iterator.Clone to use the same underlying engine state. This relies on
	// the fact that all pebbleIterators created here are marked as reusable,
	// which causes pebbleIterator.Close to not close iter. iter will be closed
	// when pebbleReadOnly.Close is called.
	//
	// TODO(sumeer): The lazy iterator creation is insufficient to address
	// issues like https://github.com/cockroachdb/cockroach/issues/55461.
	// We could create the pebble.Iterator eagerly, since a caller using
	// pebbleReadOnly is eventually going to create one anyway. But we
	// already have different behaviors in different Reader implementations
	// (see Reader.ConsistentIterators) that callers don't pay attention
	// to, and adding another such difference could be a source of bugs.
	// See https://github.com/cockroachdb/cockroach/pull/58515#pullrequestreview-563993424
	// for more discussion.
	prefixIter       pebbleIterator
	normalIter       pebbleIterator
	prefixEngineIter pebbleIterator
	normalEngineIter pebbleIterator
	iter             cloneableIter
	closed           bool
}

var _ ReadWriter = &pebbleReadOnly{}

func (p *pebbleReadOnly) Close() {
	if p.closed {
		panic("closing an already-closed pebbleReadOnly")
	}
	p.closed = true
	// Setting iter to nil is sufficient since it will be closed by one of the
	// subsequent destroy calls.
	p.iter = nil
	p.prefixIter.destroy()
	p.normalIter.destroy()
	p.prefixEngineIter.destroy()
	p.normalEngineIter.destroy()
}

func (p *pebbleReadOnly) Closed() bool {
	return p.closed
}

// ExportMVCCToSst is part of the engine.Reader interface.
func (p *pebbleReadOnly) ExportMVCCToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	useTBI bool,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	r := wrapReader(p)
	// Doing defer r.Free() does not inline.
	b, summary, k, err := pebbleExportToSst(
		r, startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, useTBI)
	r.Free()
	return b, summary, k, err
}

func (p *pebbleReadOnly) MVCCGet(key MVCCKey) ([]byte, error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	return p.parent.MVCCGet(key)
}

func (p *pebbleReadOnly) rawGet(key []byte) ([]byte, error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	return p.parent.rawGet(key)
}

func (p *pebbleReadOnly) MVCCGetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	return p.parent.MVCCGetProto(key, msg)
}

func (p *pebbleReadOnly) MVCCIterate(
	start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error,
) error {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(r, start, end, iterKind, f)
		r.Free()
		return err
	}
	return iterateOnReader(p, start, end, iterKind, f)
}

// NewMVCCIterator implements the Engine interface.
func (p *pebbleReadOnly) NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter := r.NewMVCCIterator(iterKind, opts)
		r.Free()
		if util.RaceEnabled {
			iter = wrapInUnsafeIter(iter)
		}
		return iter
	}

	if !opts.MinTimestampHint.IsEmpty() {
		// MVCCIterators that specify timestamp bounds cannot be cached.
		iter := MVCCIterator(newPebbleIterator(p.parent.db, nil, opts))
		if util.RaceEnabled {
			iter = wrapInUnsafeIter(iter)
		}
		return iter
	}

	iter := &p.normalIter
	if opts.Prefix {
		iter = &p.prefixIter
	}
	if iter.inuse {
		panic("iterator already in use")
	}
	// Ensures no timestamp hints etc.
	checkOptionsForIterReuse(opts)

	if iter.iter != nil {
		iter.setBounds(opts.LowerBound, opts.UpperBound)
	} else {
		iter.init(p.parent.db, p.iter, opts)
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		iter.reusable = true
	}

	iter.inuse = true
	var rv MVCCIterator = iter
	if util.RaceEnabled {
		rv = wrapInUnsafeIter(rv)
	}
	return rv
}

// NewEngineIterator implements the Engine interface.
func (p *pebbleReadOnly) NewEngineIterator(opts IterOptions) EngineIterator {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	iter := &p.normalEngineIter
	if opts.Prefix {
		iter = &p.prefixEngineIter
	}
	if iter.inuse {
		panic("iterator already in use")
	}
	// Ensures no timestamp hints etc.
	checkOptionsForIterReuse(opts)

	if iter.iter != nil {
		iter.setBounds(opts.LowerBound, opts.UpperBound)
	} else {
		iter.init(p.parent.db, p.iter, opts)
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		iter.reusable = true
	}

	iter.inuse = true
	return iter
}

// checkOptionsForIterReuse checks that the options are appropriate for
// iterators that are reusable, and panics if not. This includes disallowing
// any timestamp hints.
func checkOptionsForIterReuse(opts IterOptions) {
	if !opts.MinTimestampHint.IsEmpty() || !opts.MaxTimestampHint.IsEmpty() {
		panic("iterator with timestamp hints cannot be reused")
	}
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}
}

// ConsistentIterators implements the Engine interface.
func (p *pebbleReadOnly) ConsistentIterators() bool {
	return true
}

// Writer methods are not implemented for pebbleReadOnly. Ideally, the code
// could be refactored so that a Reader could be supplied to evaluateBatch

// Writer is the write interface to an engine's data.
func (p *pebbleReadOnly) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCC(key MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearUnversioned(key roachpb.Key) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearIntent(
	key roachpb.Key, state PrecedingIntentState, txnDidNotUpdateMeta bool, txnUUID uuid.UUID,
) (int, error) {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearEngineKey(key EngineKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) SingleClearEngineKey(key EngineKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearRawRange(start, end roachpb.Key) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCRange(start, end MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearIterRange(iter MVCCIterator, start, end roachpb.Key) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) Merge(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutMVCC(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutUnversioned(key roachpb.Key, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutIntent(
	ctx context.Context,
	key roachpb.Key,
	value []byte,
	state PrecedingIntentState,
	txnDidNotUpdateMeta bool,
	txnUUID uuid.UUID,
) (int, error) {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutEngineKey(key EngineKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) SafeToWriteSeparatedIntents(context.Context) (bool, error) {
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

// ExportMVCCToSst is part of the engine.Reader interface.
func (p *pebbleSnapshot) ExportMVCCToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	useTBI bool,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	r := wrapReader(p)
	// Doing defer r.Free() does not inline.
	b, summary, k, err := pebbleExportToSst(
		r, startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, useTBI)
	r.Free()
	return b, summary, k, err
}

// Get implements the Reader interface.
func (p *pebbleSnapshot) MVCCGet(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	r := wrapReader(p)
	// Doing defer r.Free() does not inline.
	v, err := r.MVCCGet(key)
	r.Free()
	return v, err
}

func (p *pebbleSnapshot) rawGet(key []byte) ([]byte, error) {
	ret, closer, err := p.snapshot.Get(key)
	if closer != nil {
		retCopy := make([]byte, len(ret))
		copy(retCopy, ret)
		ret = retCopy
		closer.Close()
	}
	if errors.Is(err, pebble.ErrNotFound) || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// MVCCGetProto implements the Reader interface.
func (p *pebbleSnapshot) MVCCGetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pebbleGetProto(p, key, msg)
}

// MVCCIterate implements the Reader interface.
func (p *pebbleSnapshot) MVCCIterate(
	start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(r, start, end, iterKind, f)
		r.Free()
		return err
	}
	return iterateOnReader(p, start, end, iterKind, f)
}

// NewMVCCIterator implements the Reader interface.
func (p *pebbleSnapshot) NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter := r.NewMVCCIterator(iterKind, opts)
		r.Free()
		if util.RaceEnabled {
			iter = wrapInUnsafeIter(iter)
		}
		return iter
	}
	iter := MVCCIterator(newPebbleIterator(p.snapshot, nil, opts))
	if util.RaceEnabled {
		iter = wrapInUnsafeIter(iter)
	}
	return iter
}

// NewEngineIterator implements the Reader interface.
func (p pebbleSnapshot) NewEngineIterator(opts IterOptions) EngineIterator {
	return newPebbleIterator(p.snapshot, nil, opts)
}

// ConsistentIterators implements the Reader interface.
func (p pebbleSnapshot) ConsistentIterators() bool {
	return true
}

// pebbleGetProto uses Reader.MVCCGet, so it not as efficient as a function
// that can unmarshal without copying bytes. But we don't care about
// efficiency, since this is used to implement Reader.MVCCGetProto, which is
// deprecated and only used in tests.
func pebbleGetProto(
	reader Reader, key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	val, err := reader.MVCCGet(key)
	if err != nil || val == nil {
		return false, 0, 0, err
	}
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	if msg != nil {
		err = protoutil.Unmarshal(val, msg)
	}
	return true, keyBytes, valBytes, err
}

func pebbleExportToSst(
	reader Reader,
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	useTBI bool,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	sstFile := &MemFile{}
	sstWriter := MakeBackupSSTWriter(sstFile)
	defer sstWriter.Close()

	var rows RowCounter
	iter := NewMVCCIncrementalIterator(
		reader,
		MVCCIncrementalIterOptions{
			EndKey:                              endKey,
			EnableTimeBoundIteratorOptimization: useTBI,
			StartTime:                           startTS,
			EndTime:                             endTS,
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
			if unsafeKey.Timestamp.IsEmpty() {
				// This should never be an intent since the incremental iterator returns
				// an error when encountering intents.
				if err := sstWriter.PutUnversioned(unsafeKey.Key, unsafeValue); err != nil {
					return nil, roachpb.BulkOpSummary{}, nil, errors.Wrapf(err, "adding key %s", unsafeKey)
				}
			} else {
				if err := sstWriter.PutMVCC(unsafeKey, unsafeValue); err != nil {
					return nil, roachpb.BulkOpSummary{}, nil, errors.Wrapf(err, "adding key %s", unsafeKey)
				}
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
