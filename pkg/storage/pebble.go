// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/pebbleiter"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/crlib/fifo"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/replay"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	humanize "github.com/dustin/go-humanize"
)

// UseEFOS controls whether uses of pebble Snapshots should use
// EventuallyFileOnlySnapshots instead. This reduces write-amp with the main
// tradeoff being higher space-amp. Note that UseExciseForSnapshot, if true,
// effectively causes EventuallyFileOnlySnapshots to be used as well.
//
// Note: Do NOT read this setting directly. Use ShouldUseEFOS() instead.
var UseEFOS = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.experimental.eventually_file_only_snapshots.enabled",
	"set to false to disable eventually-file-only-snapshots (kv.snapshot_receiver.excise.enabled must also be false)",
	metamorphic.ConstantWithTestBool(
		"storage.experimental.eventually_file_only_snapshots.enabled", true), /* defaultValue */
	settings.WithPublic)

// UseExciseForSnapshots controls whether virtual-sstable-based excises should
// be used instead of range deletions for clearing out replica contents as part
// of a rebalance/recovery snapshot application. Applied on the receiver side.
// Note that setting this setting to true also effectively causes UseEFOS above
// to become true. This interaction is why this setting is defined in the
// storage package even though it mostly affects KV.
var UseExciseForSnapshots = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.snapshot_receiver.excise.enabled",
	"set to false to disable excises in place of range deletions for KV snapshots",
	metamorphic.ConstantWithTestBool(
		"kv.snapshot_receiver.excise.enabled", true), /* defaultValue */
	settings.WithPublic,
)

// IngestSplitEnabled controls whether ingest-time splitting is enabled in
// Pebble. This feature allows for existing sstables to be split into multiple
// virtual sstables at ingest time if that allows for an ingestion sstable to go
// into a lower level than it would otherwise be in. No keys are masked with
// this split; it only happens if there are no keys in that existing sstable
// in the span of the incoming sstable.
var IngestSplitEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.ingest_split.enabled",
	"set to false to disable ingest-time splitting that lowers write-amplification",
	metamorphic.ConstantWithTestBool(
		"storage.ingest_split.enabled", true), /* defaultValue */
	settings.WithPublic,
)

// ColumnarBlocksEnabled controls whether columnar-blocks are enabled in Pebble.
var ColumnarBlocksEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"storage.columnar_blocks.enabled",
	"set to true to enable columnar-blocks to store KVs in a columnar format",
	metamorphic.ConstantWithTestBool(
		"storage.columnar_blocks.enabled", true /* defaultValue */),
	settings.WithPublic,
)

// deleteCompactionsCanExcise controls whether delete compactions can
// apply rangedels/rangekeydels on sstables they partially apply to, through
// an excise operation, instead of just applying the rangedels/rangekeydels
// that fully delete sstables.
var deleteCompactionsCanExcise = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"storage.delete_compaction_excise.enabled",
	"set to false to direct Pebble to not partially excise sstables in delete-only compactions",
	metamorphic.ConstantWithTestBool(
		"storage.delete_compaction_excise.enabled", true), /* defaultValue */
	settings.WithPublic)

// IngestAsFlushable controls whether ingested sstables that overlap the
// memtable may be lazily ingested: written to the WAL and enqueued in the list
// of flushables (eg, memtables, large batches and now lazily-ingested
// sstables). This only affects sstables that are ingested in the future. If a
// sstable was already lazily ingested but not flushed, a crash and subsequent
// recovery will still enqueue the sstables as flushable when the ingest's WAL
// entry is replayed.
//
// This cluster setting will be removed in a subsequent release.
var IngestAsFlushable = settings.RegisterBoolSetting(
	settings.ApplicationLevel, // used to init temp storage in virtual cluster servers
	"storage.ingest_as_flushable.enabled",
	"set to true to enable lazy ingestion of sstables",
	metamorphic.ConstantWithTestBool(
		"storage.ingest_as_flushable.enabled", true))

// MinCapacityForBulkIngest is the fraction of remaining store capacity
// under which bulk ingestion requests are rejected.
var MinCapacityForBulkIngest = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.bulk_io_write.min_capacity_remaining_fraction",
	"remaining store capacity fraction below which bulk ingestion requests are rejected",
	0.05,
	settings.FloatInRange(0.04, 0.3),
	settings.WithPublic,
)

// BlockLoadConcurrencyLimit controls the maximum number of outstanding
// filesystem read operations for loading sstable blocks. This limit is a
// last-resort queueing mechanism to avoid memory issues or running against the
// Go OS system threads limit (see runtime.SetMaxThreads() with default value
// 10,000).
//
// The limit is distributed evenly between all stores (rounding up). This is to
// provide isolation between the stores - we don't want one bad disk blocking
// other stores.
var BlockLoadConcurrencyLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel, // used by temp storage as well
	"storage.block_load.node_max_active",
	"maximum number of outstanding sstable block reads per host",
	7500,
	settings.IntInRange(1, 9000),
)

// readaheadModeInformed controls the pebble.ReadaheadConfig.Informed setting.
//
// Note that the setting is taken into account when a table enters the Pebble
// table cache; it can take a while for an updated setting to take effect.
var readaheadModeInformed = settings.RegisterEnumSetting(
	settings.ApplicationLevel, // used by temp storage as well
	"storage.readahead_mode.informed",
	"the readahead mode for operations which are known to read through large chunks of data; "+
		"sys-readahead performs explicit prefetching via the readahead syscall; "+
		"fadv-sequential lets the OS perform prefetching via fadvise(FADV_SEQUENTIAL)",
	"fadv-sequential",
	map[objstorageprovider.ReadaheadMode]string{
		objstorageprovider.NoReadahead:       "off",
		objstorageprovider.SysReadahead:      "sys-readahead",
		objstorageprovider.FadviseSequential: "fadv-sequential",
	},
)

// readaheadModeSpeculative controls the pebble.ReadaheadConfig.Speculative setting.
//
// Note that the setting is taken into account when a table enters the Pebble
// table cache; it can take a while for an updated setting to take effect.
var readaheadModeSpeculative = settings.RegisterEnumSetting(
	settings.ApplicationLevel, // used by temp storage as well
	"storage.readahead_mode.speculative",
	"the readahead mode that is used automatically when sequential reads are detected; "+
		"sys-readahead performs explicit prefetching via the readahead syscall; "+
		"fadv-sequential starts with explicit prefetching via the readahead syscall then automatically "+
		"switches to OS-driven prefetching via fadvise(FADV_SEQUENTIAL)",
	"fadv-sequential",
	map[objstorageprovider.ReadaheadMode]string{
		objstorageprovider.NoReadahead:       "off",
		objstorageprovider.SysReadahead:      "sys-readahead",
		objstorageprovider.FadviseSequential: "fadv-sequential",
	},
)

// CompressionAlgorithm is an enumeration of available compression algorithms
// available.
type compressionAlgorithm int64

const (
	compressionAlgorithmSnappy compressionAlgorithm = 1
	compressionAlgorithmZstd   compressionAlgorithm = 2
	compressionAlgorithmNone   compressionAlgorithm = 3
)

// String implements fmt.Stringer for CompressionAlgorithm.
func (c compressionAlgorithm) String() string {
	switch c {
	case compressionAlgorithmSnappy:
		return "snappy"
	case compressionAlgorithmZstd:
		return "zstd"
	case compressionAlgorithmNone:
		return "none"
	default:
		panic(errors.Errorf("unknown compression type: %d", c))
	}
}

// RegisterCompressionAlgorithmClusterSetting is a helper to register an enum
// cluster setting with the given name, description and default value.
func RegisterCompressionAlgorithmClusterSetting(
	name settings.InternalKey, desc string, defaultValue compressionAlgorithm,
) *settings.EnumSetting[compressionAlgorithm] {
	return settings.RegisterEnumSetting(
		// NB: We can't use settings.SystemOnly today because we may need to read the
		// value from within a tenant building an sstable for AddSSTable.
		settings.SystemVisible, name,
		desc,
		// TODO(jackson): Consider using a metamorphic constant here, but many tests
		// will need to override it because they depend on a deterministic sstable
		// size.
		defaultValue.String(),
		map[compressionAlgorithm]string{
			compressionAlgorithmSnappy: compressionAlgorithmSnappy.String(),
			compressionAlgorithmZstd:   compressionAlgorithmZstd.String(),
			compressionAlgorithmNone:   compressionAlgorithmNone.String(),
		},
		settings.WithPublic,
	)
}

// CompressionAlgorithmStorage determines the compression algorithm used to
// compress data blocks when writing sstables for use in a Pebble store (written
// directly, or constructed for ingestion on a remote store via AddSSTable).
// Users should call getCompressionAlgorithm with the cluster setting, rather
// than calling Get directly.
var CompressionAlgorithmStorage = RegisterCompressionAlgorithmClusterSetting(
	"storage.sstable.compression_algorithm",
	`determines the compression algorithm to use when compressing sstable data blocks for use in a Pebble store;`,
	compressionAlgorithmSnappy, // Default.
)

// CompressionAlgorithmBackupStorage determines the compression algorithm used
// to compress data blocks when writing sstables that contain backup row data
// storage. Users should call getCompressionAlgorithm with the cluster setting,
// rather than calling Get directly.
var CompressionAlgorithmBackupStorage = RegisterCompressionAlgorithmClusterSetting(
	"storage.sstable.compression_algorithm_backup_storage",
	`determines the compression algorithm to use when compressing sstable data blocks for backup row data storage;`,
	compressionAlgorithmSnappy, // Default.
)

// CompressionAlgorithmBackupTransport determines the compression algorithm used
// to compress data blocks when writing sstables that will be immediately
// iterated and will never need to touch disk. These sstables typically have
// much larger blocks and benefit from compression. However, this compression
// algorithm may be different to the one used when writing out the sstables for
// remote storage. Users should call getCompressionAlgorithm with the cluster
// setting, rather than calling Get directly.
var CompressionAlgorithmBackupTransport = RegisterCompressionAlgorithmClusterSetting(
	"storage.sstable.compression_algorithm_backup_transport",
	`determines the compression algorithm to use when compressing sstable data blocks for backup transport;`,
	compressionAlgorithmSnappy, // Default.
)

func getCompressionAlgorithm(
	ctx context.Context,
	settings *cluster.Settings,
	setting *settings.EnumSetting[compressionAlgorithm],
) pebble.Compression {
	switch setting.Get(&settings.SV) {
	case compressionAlgorithmSnappy:
		return pebble.SnappyCompression
	case compressionAlgorithmZstd:
		return pebble.ZstdCompression
	case compressionAlgorithmNone:
		return pebble.NoCompression
	default:
		return pebble.DefaultCompression
	}
}

var walFailoverUnhealthyOpThreshold = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"storage.wal_failover.unhealthy_op_threshold",
	"the latency of a WAL write considered unhealthy and triggers a failover to a secondary WAL location",
	100*time.Millisecond,
	settings.WithPublic,
)

// TODO(ssd): This could be SystemOnly but we currently init pebble
// engines for temporary storage. Temporary engines shouldn't really
// care about download compactions, but they do currently simply
// because of code organization.
var concurrentDownloadCompactions = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"storage.max_download_compaction_concurrency",
	"the maximum number of concurrent download compactions",
	8,
	settings.IntWithMinimum(1),
)

// ShouldUseEFOS returns true if either of the UseEFOS or UseExciseForSnapshots
// cluster settings are enabled, and EventuallyFileOnlySnapshots must be used
// to guarantee snapshot-like semantics.
func ShouldUseEFOS(settings *settings.Values) bool {
	return UseEFOS.Get(settings) || UseExciseForSnapshots.Get(settings)
}

// EngineComparer is a pebble.Comparer object that implements MVCC-specific
// comparator settings for use with Pebble.
var EngineComparer = func() pebble.Comparer {
	// We use the pebble/cockroachkvs implementation, but we override the
	// FormatKey method.
	c := cockroachkvs.Comparer

	c.FormatKey = func(k []byte) fmt.Formatter {
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
	}
	// TODO(jackson): Consider overriding ValidateKey and using the stricter
	// EngineKey.Validate. Today some tests create lock-table keys without the
	// lock table prefix and these test keys fail EngineKey.Validate.
	return c
}()

// KeySchemas holds the set of KeySchemas understandable by CockroachDB.
var KeySchemas = []*pebble.KeySchema{&cockroachkvs.KeySchema}

// TODO(jackson): We need to rethink uses of DefaultKeySchema when we introduce
// a new key schema.

// DefaultKeySchema is the name of the default key schema.
var DefaultKeySchema = cockroachkvs.KeySchema.Name

// MVCCMerger is a pebble.Merger object that implements the merge operator used
// by Cockroach.
var MVCCMerger = &pebble.Merger{
	Name: "cockroach_merge_operator",
	Merge: func(_, value []byte) (pebble.ValueMerger, error) {
		merger := NewMVCCValueMerger()
		err := merger.MergeNewer(value)
		if err != nil {
			return nil, err
		}
		return merger, nil
	},
}

var _ sstable.BlockIntervalSuffixReplacer = MVCCBlockIntervalSuffixReplacer{}

type MVCCBlockIntervalSuffixReplacer struct{}

func (MVCCBlockIntervalSuffixReplacer) ApplySuffixReplacement(
	interval sstable.BlockInterval, newSuffix []byte,
) (sstable.BlockInterval, error) {
	synthDecoded, err := DecodeMVCCTimestampSuffix(newSuffix)
	if err != nil {
		return sstable.BlockInterval{}, errors.AssertionFailedf("could not decode synthetic suffix")
	}
	synthDecodedWalltime := uint64(synthDecoded.WallTime)
	// The returned bound includes the synthetic suffix, regardless of its logical
	// component.
	return sstable.BlockInterval{Lower: synthDecodedWalltime, Upper: synthDecodedWalltime + 1}, nil
}

type pebbleIntervalMapper struct{}

var _ sstable.IntervalMapper = pebbleIntervalMapper{}

// MapPointKey is part of the sstable.IntervalMapper interface.
func (pebbleIntervalMapper) MapPointKey(
	key pebble.InternalKey, value []byte,
) (sstable.BlockInterval, error) {
	return mapSuffixToInterval(key.UserKey)
}

// MapRangeKey is part of the sstable.IntervalMapper interface.
func (pebbleIntervalMapper) MapRangeKeys(span sstable.Span) (sstable.BlockInterval, error) {
	var res sstable.BlockInterval
	for _, k := range span.Keys {
		i, err := mapSuffixToInterval(k.Suffix)
		if err != nil {
			return sstable.BlockInterval{}, err
		}
		res.UnionWith(i)
	}
	return res, nil
}

// mapSuffixToInterval maps the suffix of a key to a timestamp interval.
// The buffer can be an entire key or just the suffix.
func mapSuffixToInterval(b []byte) (sstable.BlockInterval, error) {
	if len(b) == 0 {
		return sstable.BlockInterval{}, nil
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(b[len(b)-1])
	if versionLen == 0 {
		// This is not an MVCC key that we can collect.
		return sstable.BlockInterval{}, nil
	}
	// prefixPartEnd points to the sentinel byte, unless this is a bare suffix, in
	// which case the index is -1.
	prefixPartEnd := len(b) - 1 - versionLen
	// Sanity check: the index should be >= -1. Additionally, if the index is >=
	// 0, it should point to the sentinel byte, as this is a full EngineKey.
	if prefixPartEnd < -1 || (prefixPartEnd >= 0 && b[prefixPartEnd] != sentinel) {
		return sstable.BlockInterval{}, errors.Errorf("invalid key %s", roachpb.Key(b).String())
	}
	// We don't need the last byte (the version length).
	versionLen--
	// Only collect if this looks like an MVCC timestamp.
	if versionLen == engineKeyVersionWallTimeLen ||
		versionLen == engineKeyVersionWallAndLogicalTimeLen ||
		versionLen == engineKeyVersionWallLogicalAndSyntheticTimeLen {
		// INVARIANT: -1 <= prefixPartEnd < len(b) - 1.
		// Version consists of the bytes after the sentinel and before the length.
		ts := binary.BigEndian.Uint64(b[prefixPartEnd+1:])
		return sstable.BlockInterval{Lower: ts, Upper: ts + 1}, nil
	}
	return sstable.BlockInterval{}, nil
}

const mvccWallTimeIntervalCollector = "MVCCTimeInterval"

var _ pebble.BlockPropertyFilterMask = (*mvccWallTimeIntervalRangeKeyMask)(nil)

type mvccWallTimeIntervalRangeKeyMask struct {
	sstable.BlockIntervalFilter
}

// SetSuffix implements the pebble.BlockPropertyFilterMask interface.
func (m *mvccWallTimeIntervalRangeKeyMask) SetSuffix(suffix []byte) error {
	if len(suffix) == 0 {
		// This is currently impossible, because the only range key Cockroach
		// writes today is the MVCC Delete Range that's always suffixed.
		return nil
	}
	ts, err := DecodeMVCCTimestampSuffix(suffix)
	if err != nil {
		return err
	}
	m.BlockIntervalFilter.SetInterval(uint64(ts.WallTime), math.MaxUint64)
	return nil
}

// PebbleBlockPropertyCollectors is the list of functions to construct
// BlockPropertyCollectors.
var PebbleBlockPropertyCollectors = []func() pebble.BlockPropertyCollector{
	func() pebble.BlockPropertyCollector {
		return sstable.NewBlockIntervalCollector(
			mvccWallTimeIntervalCollector,
			pebbleIntervalMapper{},
			MVCCBlockIntervalSuffixReplacer{},
		)
	},
}

// MinimumSupportedFormatVersion is the version that provides features that the
// Cockroach code relies on unconditionally (like range keys). New stores are by
// default created with this version. It should correspond to the minimum
// supported binary version.
const MinimumSupportedFormatVersion = pebble.FormatColumnarBlocks

// DefaultPebbleOptions returns the default pebble options.
func DefaultPebbleOptions() *pebble.Options {
	opts := &pebble.Options{
		Comparer:   &EngineComparer,
		FS:         vfs.Default,
		KeySchema:  DefaultKeySchema,
		KeySchemas: sstable.MakeKeySchemas(KeySchemas...),
		// A value of 2 triggers a compaction when there is 1 sub-level.
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 1000,
		LBaseMaxBytes:         64 << 20, // 64 MB
		Levels:                make([]pebble.LevelOptions, 7),
		// NB: Options.MaxConcurrentCompactions may be overidden in NewPebble to
		// allow overriding the max at runtime through
		// Engine.SetCompactionConcurrency.
		MaxConcurrentCompactions:    getMaxConcurrentCompactions,
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
		Merger:                      MVCCMerger,
		BlockPropertyCollectors:     PebbleBlockPropertyCollectors,
		FormatMajorVersion:          MinimumSupportedFormatVersion,
	}
	opts.Experimental.L0CompactionConcurrency = l0SubLevelCompactionConcurrency
	// Automatically flush 10s after the first range tombstone is added to a
	// memtable. This ensures that we can reclaim space even when there's no
	// activity on the database generating flushes.
	opts.FlushDelayDeleteRange = 10 * time.Second
	// Automatically flush 10s after the first range key is added to a memtable.
	// This ensures that range keys are quickly flushed, allowing use of lazy
	// combined iteration within Pebble.
	opts.FlushDelayRangeKey = 10 * time.Second
	// Enable deletion pacing. This helps prevent disk slowness events on some
	// SSDs, that kick off an expensive GC if a lot of files are deleted at
	// once.
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB
	opts.Experimental.ShortAttributeExtractor = shortAttributeExtractorForValues
	opts.Experimental.RequiredInPlaceValueBound = pebble.UserKeyPrefixBound{
		Lower: EncodeMVCCKey(MVCCKey{Key: keys.LocalRangeLockTablePrefix}),
		Upper: EncodeMVCCKey(MVCCKey{Key: keys.LocalRangeLockTablePrefix.PrefixEnd()}),
	}
	// Disable multi-level compaction heuristic for now. See #134423
	// for why this was disabled, and what needs to be changed to reenable it.
	// This issue tracks re-enablement: https://github.com/cockroachdb/pebble/issues/4139
	opts.Experimental.MultiLevelCompactionHeuristic = pebble.NoMultiLevel{}

	opts.Experimental.UserKeyCategories = userKeyCategories

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

	// These size classes are a subset of available size classes in jemalloc[1].
	// The size classes are used by Pebble for determining target block sizes for
	// flushes, with the goal of reducing internal fragmentation. There are many
	// more size classes that could be included, however, sstable blocks have a
	// target block size of 32KiB, a minimum size threshold of ~19.6KiB and are
	// unlikely to exceed 128KiB.
	//
	// [1] https://jemalloc.net/jemalloc.3.html#size_classes
	opts.AllocatorSizeClasses = []int{
		16384,
		20480, 24576, 28672, 32768,
		40960, 49152, 57344, 65536,
		81920, 98304, 114688, 131072,
	}

	return opts
}

func shortAttributeExtractorForValues(
	key []byte, keyPrefixLen int, value []byte,
) (pebble.ShortAttribute, error) {
	suffixLen := len(key) - keyPrefixLen
	const lockTableSuffixLen = engineKeyVersionLockTableLen + sentinelLen
	if suffixLen == engineKeyNoVersion || suffixLen == lockTableSuffixLen {
		// Not a versioned MVCC value.
		return 0, nil
	}
	isTombstone, err := EncodedMVCCValueIsTombstone(value)
	if err != nil {
		return 0, err
	}
	if isTombstone {
		return 1, nil
	}
	return 0, nil
}

// engineConfig holds all configuration parameters and knobs used in setting up
// a new storage engine.
type engineConfig struct {
	attrs roachpb.Attributes
	// ballastSize is the amount reserved by a ballast file for manual
	// out-of-disk recovery.
	ballastSize int64
	// env holds the initialized virtual filesystem that the Engine should use.
	env *fs.Env
	// maxSize is used for calculating free space and making rebalancing
	// decisions. The zero value indicates that there is no absolute maximum size.
	maxSize storeSize
	// If true, creating the instance fails if the target directory does not hold
	// an initialized instance.
	//
	// Makes no sense for in-memory instances.
	mustExist bool
	// pebble specific options.
	opts *pebble.Options
	// remoteStorageFactory is used to pass the ExternalStorage factory.
	remoteStorageFactory *cloud.EarlyBootExternalStorageAccessor
	// settings instance for cluster-wide knobs. Must not be nil.
	settings *cluster.Settings
	// sharedStorage is a cloud.ExternalStorage that can be used by all Pebble
	// stores on this node and on other nodes to store sstables.
	sharedStorage cloud.ExternalStorage

	// beforeClose is a slice of functions to be invoked before the engine is closed.
	beforeClose []func(*Pebble)
	// afterClose is a slice of functions to be invoked after the engine is closed.
	afterClose []func()

	// diskMonitor is used to output a disk trace when a stall is detected.
	diskMonitor *disk.Monitor

	// DiskWriteStatsCollector is used to categorically track disk write metrics
	// across all Pebble stores on this node.
	DiskWriteStatsCollector *vfs.DiskWriteStatsCollector

	// blockConcurrencyLimitDivisor is used to calculate the block load
	// concurrency limit: it is the current valuer of the
	// BlockLoadConcurrencyLimit setting divided by this value. It should be set
	// to the number of stores.
	//
	// This is necessary because we want separate limiters per stores (we don't
	// want one bad disk to block other stores)
	//
	// A value of 0 disables the limit.
	blockConcurrencyLimitDivisor int
}

// Pebble is a wrapper around a Pebble database instance.
type Pebble struct {
	atomic struct {
		// compactionConcurrency is the current compaction concurrency set on
		// the Pebble store. The compactionConcurrency option in the Pebble
		// Options struct is a closure which will return
		// Pebble.atomic.compactionConcurrency.
		//
		// This mechanism allows us to change the Pebble compactionConcurrency
		// on the fly without restarting Pebble.
		compactionConcurrency uint64
	}

	cfg         engineConfig
	db          *pebble.DB
	closed      bool
	auxDir      string
	ballastPath string
	properties  roachpb.StoreProperties

	// Stats updated by pebble.EventListener invocations, and returned in
	// GetMetrics. Updated and retrieved atomically.
	writeStallCount                  int64
	writeStallDuration               time.Duration
	writeStallStartNanos             int64
	diskSlowCount                    int64
	diskStallCount                   int64
	singleDelInvariantViolationCount int64
	singleDelIneffectualCount        int64
	sharedBytesRead                  int64
	sharedBytesWritten               int64
	iterStats                        struct {
		syncutil.Mutex
		AggregatedIteratorStats
	}
	batchCommitStats struct {
		syncutil.Mutex
		AggregatedBatchCommitStats
	}
	diskWriteStatsCollector *vfs.DiskWriteStatsCollector
	// Relevant options copied over from pebble.Options.
	logCtx        context.Context
	logger        pebble.LoggerAndTracer
	eventListener *pebble.EventListener
	mu            struct {
		// This mutex is the lowest in any lock ordering.
		syncutil.Mutex
		flushCompletedCallback func()
	}
	asyncDone sync.WaitGroup

	// minVersion is the minimum CockroachDB version that can open this store.
	minVersion roachpb.Version

	storeIDPebbleLog *base.StoreIDContainer
	replayer         *replay.WorkloadCollector
	diskSlowFunc     atomic.Pointer[func(vfs.DiskSlowInfo)]
	lowDiskSpaceFunc atomic.Pointer[func(pebble.LowDiskSpaceInfo)]

	singleDelLogEvery log.EveryN
}

// WorkloadCollector implements an workloadCollectorGetter and returns the
// workload collector stored on Pebble. This method is invoked following a
// successful cast of an Engine to a `workloadCollectorGetter` type. This method
// allows for pebble exclusive functionality to be used without modifying the
// Engine interface.
func (p *Pebble) WorkloadCollector() *replay.WorkloadCollector {
	return p.replayer
}

var _ Engine = &Pebble{}

// WorkloadCollectorEnabled specifies if the workload collector will be enabled
var WorkloadCollectorEnabled = envutil.EnvOrDefaultBool("COCKROACH_STORAGE_WORKLOAD_COLLECTOR", false)

// SetCompactionConcurrency will return the previous compaction concurrency.
func (p *Pebble) SetCompactionConcurrency(n uint64) uint64 {
	prevConcurrency := atomic.SwapUint64(&p.atomic.compactionConcurrency, n)
	return prevConcurrency
}

// RegisterDiskSlowCallback registers a callback that will be run when a write
// operation on the disk has been seen to be slow. Only one handler can be
// registered per Pebble instance.
func (p *Pebble) RegisterDiskSlowCallback(f func(vfs.DiskSlowInfo)) {
	p.diskSlowFunc.Store(&f)
}

// RegisterLowDiskSpaceCallback registers a callback that will be run when a a
// disk is running out of space. Only one handler can be registered per Pebble
// instance.
func (p *Pebble) RegisterLowDiskSpaceCallback(f func(info pebble.LowDiskSpaceInfo)) {
	p.lowDiskSpaceFunc.Store(&f)
}

// AdjustCompactionConcurrency adjusts the compaction concurrency up or down by
// the passed delta, down to a minimum of 1.
func (p *Pebble) AdjustCompactionConcurrency(delta int64) uint64 {
	for {
		current := atomic.LoadUint64(&p.atomic.compactionConcurrency)
		adjusted := int64(current) + delta
		if adjusted < 1 {
			adjusted = 1
		}
		if atomic.CompareAndSwapUint64(&p.atomic.compactionConcurrency, current, uint64(adjusted)) {
			return uint64(adjusted)
		}
	}
}

// SetStoreID adds the store id to pebble logs.
func (p *Pebble) SetStoreID(ctx context.Context, storeID int32) error {
	if p == nil {
		return nil
	}
	p.storeIDPebbleLog.Set(ctx, storeID)
	// Note that SetCreatorID only does something if remote storage is configured
	// in the pebble options.
	if storeID != base.TempStoreID {
		if err := p.db.SetCreatorID(uint64(storeID)); err != nil {
			return err
		}
	}
	return nil
}

// GetStoreID returns to configured store ID.
func (p *Pebble) GetStoreID() (int32, error) {
	if p == nil {
		return 0, errors.AssertionFailedf("GetStoreID requires non-nil Pebble")
	}
	if p.storeIDPebbleLog == nil {
		return 0, errors.AssertionFailedf("GetStoreID requires an initialized store ID container")
	}
	storeID := p.storeIDPebbleLog.Get()
	if storeID == 0 {
		return 0, errors.AssertionFailedf("GetStoreID must be called after calling SetStoreID")
	}
	return storeID, nil
}

func (p *Pebble) Download(ctx context.Context, span roachpb.Span, copy bool) error {
	const copySpanName, rewriteSpanName = "pebble.Download", "pebble.DownloadRewrite"
	spanName := rewriteSpanName
	if copy {
		spanName = copySpanName
	}
	ctx, sp := tracing.ChildSpan(ctx, spanName)
	defer sp.Finish()
	if p == nil {
		return nil
	}
	downloadSpan := pebble.DownloadSpan{
		StartKey:               EncodeMVCCKey(MVCCKey{Key: span.Key}),
		EndKey:                 EncodeMVCCKey(MVCCKey{Key: span.EndKey}),
		ViaBackingFileDownload: copy,
	}
	return p.db.Download(ctx, []pebble.DownloadSpan{downloadSpan})
}

type remoteStorageAdaptor struct {
	p       *Pebble
	ctx     context.Context
	factory *cloud.EarlyBootExternalStorageAccessor
}

func (r remoteStorageAdaptor) CreateStorage(locator remote.Locator) (remote.Storage, error) {
	es, err := r.factory.OpenURL(r.ctx, string(locator))
	return &externalStorageWrapper{p: r.p, ctx: r.ctx, es: es}, err
}

// ConfigureForSharedStorage is used to configure a pebble Options for shared
// storage.
var ConfigureForSharedStorage func(opts *pebble.Options, storage remote.Storage) error

// newPebble creates a new Pebble instance, at the specified path.
// Do not use directly (except in test); use Open instead.
//
// Direct users of NewPebble: cfs.opts.{Logger,LoggerAndTracer} must not be
// set.
func newPebble(ctx context.Context, cfg engineConfig) (p *Pebble, err error) {
	if cfg.opts == nil {
		cfg.opts = DefaultPebbleOptions()
	} else {
		// Open also causes DefaultPebbleOptions before calling NewPebble, so we
		// are tolerant of Logger being set to pebble.DefaultLogger.
		if cfg.opts.Logger != nil && cfg.opts.Logger != pebble.DefaultLogger {
			return nil, errors.AssertionFailedf("Options.Logger is set to unexpected value")
		}
		// Clone the given options so that we are free to modify them.
		cfg.opts = cfg.opts.Clone()
	}
	if cfg.opts.FormatMajorVersion < MinimumSupportedFormatVersion {
		return nil, errors.AssertionFailedf(
			"FormatMajorVersion is %d, should be at least %d",
			cfg.opts.FormatMajorVersion, MinimumSupportedFormatVersion,
		)
	}
	cfg.opts.FS = cfg.env
	cfg.opts.Lock = cfg.env.DirectoryLock
	cfg.opts.ErrorIfNotExists = cfg.mustExist
	for i := range cfg.opts.Levels {
		cfg.opts.Levels[i].Compression = func() block.Compression {
			return getCompressionAlgorithm(ctx, cfg.settings, CompressionAlgorithmStorage)
		}
	}

	if cfg.opts.MaxConcurrentDownloads == nil {
		cfg.opts.MaxConcurrentDownloads = func() int {
			return int(concurrentDownloadCompactions.Get(&cfg.settings.SV))
		}
	}

	cfg.opts.EnsureDefaults()

	// The context dance here is done so that we have a clean context without
	// timeouts that has a copy of the log tags.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	// The store id, could not necessarily be determined when this function
	// is called. Therefore, we use a container for the store id.
	storeIDContainer := &base.StoreIDContainer{}
	logCtx = logtags.AddTag(logCtx, "s", storeIDContainer)
	logCtx = logtags.AddTag(logCtx, "pebble", nil)

	cfg.opts.Local.ReadaheadConfig = objstorageprovider.NewReadaheadConfig()
	updateReadaheadFn := func(ctx context.Context) {
		cfg.opts.Local.ReadaheadConfig.Set(
			readaheadModeInformed.Get(&cfg.settings.SV),
			readaheadModeSpeculative.Get(&cfg.settings.SV),
		)
	}
	updateReadaheadFn(context.Background())
	readaheadModeInformed.SetOnChange(&cfg.settings.SV, updateReadaheadFn)
	readaheadModeSpeculative.SetOnChange(&cfg.settings.SV, updateReadaheadFn)

	cfg.opts.WALMinSyncInterval = func() time.Duration {
		return minWALSyncInterval.Get(&cfg.settings.SV)
	}
	cfg.opts.Experimental.EnableValueBlocks = func() bool { return true }
	cfg.opts.Experimental.DisableIngestAsFlushable = func() bool {
		// Disable flushable ingests if shared storage is enabled. This is because
		// flushable ingests currently do not support Excise operations.
		//
		// TODO(bilal): Remove the first part of this || statement when
		// https://github.com/cockroachdb/pebble/issues/2676 is completed, or when
		// Pebble has better guards against this.
		return cfg.sharedStorage != nil || !IngestAsFlushable.Get(&cfg.settings.SV)
	}
	cfg.opts.Experimental.IngestSplit = func() bool {
		return IngestSplitEnabled.Get(&cfg.settings.SV)
	}
	cfg.opts.Experimental.EnableColumnarBlocks = func() bool {
		return ColumnarBlocksEnabled.Get(&cfg.settings.SV)
	}
	cfg.opts.Experimental.EnableDeleteOnlyCompactionExcises = func() bool {
		return deleteCompactionsCanExcise.Get(&cfg.settings.SV)
	}

	auxDir := cfg.opts.FS.PathJoin(cfg.env.Dir, base.AuxiliaryDir)
	if !cfg.env.IsReadOnly() {
		if err := cfg.opts.FS.MkdirAll(auxDir, 0755); err != nil {
			return nil, err
		}
	}
	ballastPath := base.EmergencyBallastFile(cfg.env.PathJoin, cfg.env.Dir)

	if d := int64(cfg.blockConcurrencyLimitDivisor); d != 0 {
		val := (BlockLoadConcurrencyLimit.Get(&cfg.settings.SV) + d - 1) / d
		cfg.opts.LoadBlockSema = fifo.NewSemaphore(val)
		BlockLoadConcurrencyLimit.SetOnChange(&cfg.settings.SV, func(ctx context.Context) {
			cfg.opts.LoadBlockSema.UpdateCapacity((BlockLoadConcurrencyLimit.Get(&cfg.settings.SV) + d - 1) / d)
		})
	}

	cfg.opts.Logger = nil // Defensive, since LoggerAndTracer will be used.
	if cfg.opts.LoggerAndTracer == nil {
		cfg.opts.LoggerAndTracer = pebbleLogger{
			ctx:   logCtx,
			depth: 1,
		}
	}
	// Else, already have a LoggerAndTracer. This only occurs in unit tests.

	// Establish the emergency ballast if we can. If there's not sufficient
	// disk space, the ballast will be reestablished from Capacity when the
	// store's capacity is queried periodically.
	if !cfg.opts.ReadOnly {
		du, err := cfg.env.UnencryptedFS.GetDiskUsage(cfg.env.Dir)
		// If the FS is an in-memory FS, GetDiskUsage returns
		// vfs.ErrUnsupported and we skip ballast creation.
		if err != nil && !errors.Is(err, vfs.ErrUnsupported) {
			return nil, errors.Wrap(err, "retrieving disk usage")
		} else if err == nil {
			resized, err := maybeEstablishBallast(cfg.env.UnencryptedFS, ballastPath, cfg.ballastSize, du)
			if err != nil {
				return nil, errors.Wrap(err, "resizing ballast")
			}
			if resized {
				cfg.opts.LoggerAndTracer.Infof("resized ballast %s to size %s",
					ballastPath, humanizeutil.IBytes(cfg.ballastSize))
			}
		}
	}

	p = &Pebble{
		cfg:                     cfg,
		auxDir:                  auxDir,
		ballastPath:             ballastPath,
		properties:              computeStoreProperties(ctx, cfg),
		logger:                  cfg.opts.LoggerAndTracer,
		logCtx:                  logCtx,
		storeIDPebbleLog:        storeIDContainer,
		replayer:                replay.NewWorkloadCollector(cfg.env.Dir),
		singleDelLogEvery:       log.Every(5 * time.Minute),
		diskWriteStatsCollector: cfg.DiskWriteStatsCollector,
	}

	// MaxConcurrentCompactions can be set by multiple sources, but all the
	// sources will eventually call NewPebble. So, we override
	// cfg.opts.MaxConcurrentCompactions to a closure which will return
	// Pebble.atomic.compactionConcurrency. This will allow us to both honor
	// the compactions concurrency which has already been set and allow us
	// to update the compactionConcurrency on the fly by changing the
	// Pebble.atomic.compactionConcurrency variable.
	p.atomic.compactionConcurrency = uint64(cfg.opts.MaxConcurrentCompactions())
	cfg.opts.MaxConcurrentCompactions = func() int {
		return int(atomic.LoadUint64(&p.atomic.compactionConcurrency))
	}

	// NB: The ordering of the event listeners passed to TeeEventListener is
	// deliberate. The listener returned by makeMetricEtcEventListener is
	// responsible for crashing the process if a DiskSlow event indicates the
	// disk is stalled. While the logging subsystem should also be robust to
	// stalls and crash the process if unable to write logs, there's less risk
	// to sequencing the crashing listener first.
	//
	// For the same reason, make the logging call asynchronous for DiskSlow events.
	// This prevents slow logging calls during a disk slow/stall event from holding
	// up Pebble's internal disk health checking, and better obeys the
	// EventListener contract for not having any functions block or take a while to
	// run. Creating goroutines is acceptable given their low cost, and the low
	// write concurrency to Pebble's FS (Pebble compactions + flushes + SQL
	// spilling to disk). If the maximum concurrency of DiskSlow events increases
	// significantly in the future, we can improve the logic here by queueing up
	// most of the logging work (except for the Fatalf call), and have it be done
	// by a single goroutine.
	// TODO(jackson): Refactor this indirection; there's no need the DiskSlow
	// callback needs to go through the EventListener and this structure is
	// confusing.
	cfg.env.RegisterOnDiskSlow(func(info pebble.DiskSlowInfo) {
		el := cfg.opts.EventListener
		p.async(func() { el.DiskSlow(info) })
	})
	el := pebble.TeeEventListener(
		p.makeMetricEtcEventListener(logCtx),
		pebble.MakeLoggingEventListener(pebbleLogger{
			ctx:   logCtx,
			depth: 2, // skip over the EventListener stack frame
		}),
	)

	p.eventListener = &el
	cfg.opts.EventListener = &el

	// If both cfg.sharedStorage and cfg.remoteStorageFactory are set, CRDB uses
	// cfg.sharedStorage. Note that eventually we will enable using both at the
	// same time, but we don't have the right abstractions in place to do that
	// today.
	//
	// We prefer cfg.sharedStorage, since the Locator -> Storage mapping contained
	// in it is needed for CRDB to function properly.
	if cfg.sharedStorage != nil {
		esWrapper := &externalStorageWrapper{p: p, es: cfg.sharedStorage, ctx: logCtx}
		if ConfigureForSharedStorage == nil {
			return nil, errors.New("shared storage requires CCL features")
		}
		if err := ConfigureForSharedStorage(cfg.opts, esWrapper); err != nil {
			return nil, errors.Wrap(err, "error when configuring shared storage")
		}
	} else {
		if cfg.remoteStorageFactory != nil {
			cfg.opts.Experimental.RemoteStorage = remoteStorageAdaptor{p: p, ctx: logCtx, factory: cfg.remoteStorageFactory}
		}
	}

	// Read the current store cluster version.
	storeClusterVersion, minVerFileExists, err := getMinVersion(p.cfg.env.UnencryptedFS, cfg.env.Dir)
	if err != nil {
		return nil, err
	}
	if minVerFileExists {
		// Avoid running a binary too new for this store. This is what you'd catch
		// if, say, you restarted directly from v21.2 into v22.2 (bumping the min
		// version) without going through v22.1 first.
		//
		// Note that "going through" above means that v22.1 successfully upgrades
		// all existing stores. If v22.1 crashes half-way through the startup
		// sequence (so now some stores have v21.2, but others v22.1) you are
		// expected to run v22.1 again (hopefully without the crash this time) which
		// would then rewrite all the stores.
		if v := cfg.settings.Version; storeClusterVersion.Less(v.MinSupportedVersion()) {
			if storeClusterVersion.Major < clusterversion.DevOffset && v.LatestVersion().Major >= clusterversion.DevOffset {
				return nil, errors.Errorf(
					"store last used with cockroach non-development version v%s "+
						"cannot be opened by development version v%s",
					storeClusterVersion, v.LatestVersion(),
				)
			}
			return nil, errors.Errorf(
				"store last used with cockroach version v%s "+
					"is too old for running version v%s (which requires data from v%s or later)",
				storeClusterVersion, v.LatestVersion(), v.MinSupportedVersion(),
			)
		}
		cfg.opts.ErrorIfNotExists = true
	} else {
		if cfg.opts.ErrorIfNotExists || cfg.opts.ReadOnly {
			// Make sure the message is not confusing if the store does exist but
			// there is no min version file.
			filename := p.cfg.env.UnencryptedFS.PathJoin(cfg.env.Dir, MinVersionFilename)
			return nil, errors.Errorf(
				"pebble: database %q does not exist (missing required file %q)",
				cfg.env.Dir, filename,
			)
		}
		// If there is no min version file, there should be no store. If there is
		// one, it's either 1) a store from a very old version (which we don't want
		// to open) or 2) an empty store that was created from a previous bootstrap
		// attempt that failed right before writing out the min version file. We set
		// a flag to disallow the open in case 1.
		cfg.opts.ErrorIfNotPristine = true
	}

	if WorkloadCollectorEnabled {
		p.replayer.Attach(cfg.opts)
	}

	db, err := pebble.Open(cfg.env.Dir, cfg.opts)
	if err != nil {
		// Decorate the errors caused by the flags we set above.
		if minVerFileExists && errors.Is(err, pebble.ErrDBDoesNotExist) {
			err = errors.Wrap(err, "min version file exists but store doesn't")
		}
		if !minVerFileExists && errors.Is(err, pebble.ErrDBNotPristine) {
			err = errors.Wrap(err, "store has no min-version file; this can "+
				"happen if the store was created by an old CockroachDB version that is no "+
				"longer supported")
		}
		return nil, err
	}
	p.db = db

	if !minVerFileExists {
		storeClusterVersion = cfg.settings.Version.ActiveVersionOrEmpty(ctx).Version
		if storeClusterVersion == (roachpb.Version{}) {
			// If there is no active version, use the minimum supported version.
			storeClusterVersion = cfg.settings.Version.MinSupportedVersion()
		}
	}

	// The storage engine performs its own internal migrations
	// through the setting of the store cluster version. When
	// storage's min version is set, SetMinVersion writes to disk to
	// commit to the new store cluster version. Then it idempotently
	// applies any internal storage engine migrations necessitated
	// or enabled by the new store cluster version. If we crash
	// after committing the new store cluster version but before
	// applying the internal migrations, we're left in an in-between
	// state.
	//
	// To account for this, after the engine is open,
	// unconditionally set the min cluster version again. If any
	// storage engine state has not been updated, the call to
	// SetMinVersion will update it.  If all storage engine state is
	// already updated, SetMinVersion is a noop.
	if err := p.SetMinVersion(storeClusterVersion); err != nil {
		p.Close()
		return nil, err
	}

	return p, nil
}

var userKeyCategories = pebble.MakeUserKeyCategories(
	EngineComparer.Compare,
	category("local-1", keys.LocalRangeIDPrefix.AsRawKey()),
	category("rangeid", keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd()),
	category("local-2", keys.LocalRangePrefix),
	category("range", keys.LocalRangePrefix.PrefixEnd()),
	category("local-3", keys.LocalRangeLockTablePrefix),
	category("lock", keys.LocalRangeLockTablePrefix.PrefixEnd()),
	category("local-4", keys.LocalPrefix.PrefixEnd()),
	category("meta", keys.MetaMax),
	category("system", keys.SystemMax),
	category("tenant", nil),
)

func category(name string, upperBound roachpb.Key) pebble.UserKeyCategory {
	if upperBound == nil {
		return pebble.UserKeyCategory{Name: name}
	}
	ek := EngineKey{Key: upperBound}
	return pebble.UserKeyCategory{Name: name, UpperBound: ek.Encode()}
}

// async launches the provided function in a new goroutine. It uses a wait group
// to synchronize with (*Pebble).Close to ensure all launched goroutines have
// exited before Close returns.
func (p *Pebble) async(fn func()) {
	p.asyncDone.Add(1)
	go func() {
		defer p.asyncDone.Done()
		fn()
	}()
}

// writePreventStartupFile creates a file that will prevent nodes from automatically restarting after
// experiencing sstable corruption.
func (p *Pebble) writePreventStartupFile(ctx context.Context, corruptionError error) {
	auxDir := p.GetAuxiliaryDir()
	path := base.PreventedStartupFile(auxDir)

	preventStartupMsg := fmt.Sprintf(`ATTENTION:

  this node is terminating because of sstable corruption.
	Corruption may be a consequence of a hardware error.

	Error: %s

  A file preventing this node from restarting was placed at:
  %s`, corruptionError.Error(), path)

	if err := fs.WriteFile(p.cfg.env.UnencryptedFS, path, []byte(preventStartupMsg), fs.UnspecifiedWriteCategory); err != nil {
		log.Warningf(ctx, "%v", err)
	}
}

func (p *Pebble) makeMetricEtcEventListener(ctx context.Context) pebble.EventListener {
	return pebble.EventListener{
		BackgroundError: func(err error) {
			if errors.Is(err, pebble.ErrCorruption) {
				p.writePreventStartupFile(ctx, err)
				log.Fatalf(ctx, "local corruption detected: %v", err)
			}
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			atomic.AddInt64(&p.writeStallCount, 1)
			startNanos := timeutil.Now().UnixNano()
			atomic.StoreInt64(&p.writeStallStartNanos, startNanos)
		},
		WriteStallEnd: func() {
			startNanos := atomic.SwapInt64(&p.writeStallStartNanos, 0)
			if startNanos == 0 {
				// Should not happen since these callbacks are registered when Pebble
				// is opened, but just in case we miss the WriteStallBegin, lets not
				// corrupt the metric.
				return
			}
			stallDuration := timeutil.Now().UnixNano() - startNanos
			if stallDuration < 0 {
				return
			}
			atomic.AddInt64((*int64)(&p.writeStallDuration), stallDuration)
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			maxSyncDuration := fs.MaxSyncDuration.Get(&p.cfg.settings.SV)
			fatalOnExceeded := fs.MaxSyncDurationFatalOnExceeded.Get(&p.cfg.settings.SV)
			if info.Duration.Seconds() >= maxSyncDuration.Seconds() {
				atomic.AddInt64(&p.diskStallCount, 1)
				// Note that the below log messages go to the main cockroach log, not
				// the pebble-specific log.
				//
				// Run non-fatal log.* calls in separate goroutines as they could block
				// if the logging device is also slow/stalling, preventing pebble's disk
				// health checking from functioning correctly. See the comment in
				// pebble.EventListener on why it's important for this method to return
				// quickly.
				if fatalOnExceeded {
					// The write stall may prevent the process from exiting. If
					// the process won't exit, we can at least terminate all our
					// RPC connections first.
					//
					// See pkg/cli.runStart for where this function is hooked
					// up.
					log.MakeProcessUnavailable()

					if p.cfg.diskMonitor != nil {
						log.Fatalf(ctx, "disk stall detected: %s\n%s", info, p.cfg.diskMonitor.LogTrace())
					} else {
						log.Fatalf(ctx, "disk stall detected: %s", info)
					}
				} else {
					if p.cfg.diskMonitor != nil {
						p.async(func() {
							log.Errorf(ctx, "disk stall detected: %s\n%s", info, p.cfg.diskMonitor.LogTrace())
						})
					} else {
						p.async(func() { log.Errorf(ctx, "disk stall detected: %s", info) })
					}
				}
				return
			}
			atomic.AddInt64(&p.diskSlowCount, 1)
			// Call any custom handlers registered for disk slowness.
			if fn := p.diskSlowFunc.Load(); fn != nil {
				(*fn)(info)
			}
		},
		FlushEnd: func(info pebble.FlushInfo) {
			if info.Err != nil {
				return
			}
			p.mu.Lock()
			cb := p.mu.flushCompletedCallback
			p.mu.Unlock()
			if cb != nil {
				cb()
			}
		},
		LowDiskSpace: func(info pebble.LowDiskSpaceInfo) {
			if fn := p.lowDiskSpaceFunc.Load(); fn != nil {
				(*fn)(info)
			}
		},
		PossibleAPIMisuse: func(info pebble.PossibleAPIMisuseInfo) {
			switch info.Kind {
			case pebble.IneffectualSingleDelete:
				if p.singleDelLogEvery.ShouldLog() {
					log.Infof(p.logCtx, "possible ineffectual SingleDel on key %s", roachpb.Key(info.UserKey))
				}
				atomic.AddInt64(&p.singleDelIneffectualCount, 1)

			case pebble.NondeterministicSingleDelete:
				if p.singleDelLogEvery.ShouldLog() {
					log.Infof(p.logCtx, "possible nondeterministic SingleDel on key %s", roachpb.Key(info.UserKey))
				}
				atomic.AddInt64(&p.singleDelInvariantViolationCount, 1)
			}
		},
	}
}

// Env implements Engine.
func (p *Pebble) Env() *fs.Env { return p.cfg.env }

func (p *Pebble) String() string {
	dir := p.cfg.env.Dir
	if dir == "" {
		dir = "<in-mem>"
	}
	attrs := p.cfg.attrs.String()
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
	for _, closeFunc := range p.cfg.beforeClose {
		closeFunc(p)
	}

	p.closed = true

	// Wait for any asynchronous goroutines to exit.
	p.asyncDone.Wait()

	handleErr := func(err error) {
		if err == nil {
			return
		}
		// Allow unclean close in production builds for now. We refrain from
		// Fatal-ing on an unclean close because Cockroach opens and closes
		// ephemeral engines at time, and an error in those codepaths should not
		// fatal the process.
		//
		// TODO(jackson): Propagate the error to call sites without fataling:
		// This is tricky, because the Reader interface requires Close return
		// nothing.
		if buildutil.CrdbTestBuild {
			log.Fatalf(p.logCtx, "error during engine close: %s\n", err)
		} else {
			log.Errorf(p.logCtx, "error during engine close: %s\n", err)
		}
	}

	handleErr(p.db.Close())
	if p.cfg.env != nil {
		p.cfg.env.Close()
		p.cfg.env = nil
	}
	if p.cfg.diskMonitor != nil {
		p.cfg.diskMonitor.Close()
		p.cfg.diskMonitor = nil
	}
	for _, closeFunc := range p.cfg.afterClose {
		closeFunc()
	}
}

// aggregateIterStats is propagated to all of an engine's iterators, aggregating
// iterator stats when an iterator is closed or its stats are reset. These
// aggregated stats are exposed through GetMetrics.
func (p *Pebble) aggregateIterStats(stats IteratorStats) {
	p.iterStats.Lock()
	defer p.iterStats.Unlock()
	p.iterStats.BlockBytes += stats.Stats.InternalStats.BlockBytes
	p.iterStats.BlockBytesInCache += stats.Stats.InternalStats.BlockBytesInCache
	p.iterStats.BlockReadDuration += stats.Stats.InternalStats.BlockReadDuration
	p.iterStats.ExternalSeeks += stats.Stats.ForwardSeekCount[pebble.InterfaceCall] + stats.Stats.ReverseSeekCount[pebble.InterfaceCall]
	p.iterStats.ExternalSteps += stats.Stats.ForwardStepCount[pebble.InterfaceCall] + stats.Stats.ReverseStepCount[pebble.InterfaceCall]
	p.iterStats.InternalSeeks += stats.Stats.ForwardSeekCount[pebble.InternalIterCall] + stats.Stats.ReverseSeekCount[pebble.InternalIterCall]
	p.iterStats.InternalSteps += stats.Stats.ForwardStepCount[pebble.InternalIterCall] + stats.Stats.ReverseStepCount[pebble.InternalIterCall]
}

func (p *Pebble) aggregateBatchCommitStats(stats BatchCommitStats) {
	p.batchCommitStats.Lock()
	p.batchCommitStats.Count++
	p.batchCommitStats.TotalDuration += stats.TotalDuration
	p.batchCommitStats.SemaphoreWaitDuration += stats.SemaphoreWaitDuration
	p.batchCommitStats.WALQueueWaitDuration += stats.WALQueueWaitDuration
	p.batchCommitStats.MemTableWriteStallDuration += stats.MemTableWriteStallDuration
	p.batchCommitStats.L0ReadAmpWriteStallDuration += stats.L0ReadAmpWriteStallDuration
	p.batchCommitStats.WALRotationDuration += stats.WALRotationDuration
	p.batchCommitStats.CommitWaitDuration += stats.CommitWaitDuration
	p.batchCommitStats.Unlock()
}

// Closed implements the Engine interface.
func (p *Pebble) Closed() bool {
	return p.closed
}

// MVCCIterate implements the Engine interface.
func (p *Pebble) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// NewMVCCIterator implements the Engine interface.
func (p *Pebble) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter, err := newPebbleIterator(ctx, p.db, opts, StandardDurability, p)
	if err != nil {
		return nil, err
	}
	return maybeWrapInUnsafeIter(iter), nil
}

// NewEngineIterator implements the Engine interface.
func (p *Pebble) NewEngineIterator(ctx context.Context, opts IterOptions) (EngineIterator, error) {
	return newPebbleIterator(ctx, p.db, opts, StandardDurability, p)
}

// ScanInternal implements the Engine interface.
func (p *Pebble) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	rawLower := EngineKey{Key: lower}.Encode()
	rawUpper := EngineKey{Key: upper}.Encode()
	// TODO(sumeer): set category.
	return p.db.ScanInternal(ctx, block.CategoryUnknown, rawLower, rawUpper, visitPointKey,
		visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// ConsistentIterators implements the Engine interface.
func (p *Pebble) ConsistentIterators() bool {
	return false
}

// PinEngineStateForIterators implements the Engine interface.
func (p *Pebble) PinEngineStateForIterators(fs.ReadCategory) error {
	return errors.AssertionFailedf(
		"PinEngineStateForIterators must not be called when ConsistentIterators returns false")
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
func (p *Pebble) ClearMVCC(key MVCCKey, opts ClearOptions) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return p.clear(key, opts)
}

// ClearUnversioned implements the Engine interface.
func (p *Pebble) ClearUnversioned(key roachpb.Key, opts ClearOptions) error {
	return p.clear(MVCCKey{Key: key}, opts)
}

// ClearEngineKey implements the Engine interface.
func (p *Pebble) ClearEngineKey(key EngineKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	if !opts.ValueSizeKnown {
		return p.db.Delete(key.Encode(), pebble.Sync)
	}
	return p.db.DeleteSized(key.Encode(), opts.ValueSize, pebble.Sync)
}

func (p *Pebble) clear(key MVCCKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	if !opts.ValueSizeKnown {
		return p.db.Delete(EncodeMVCCKey(key), pebble.Sync)
	}
	// Use DeleteSized to propagate the value size.
	return p.db.DeleteSized(EncodeMVCCKey(key), opts.ValueSize, pebble.Sync)
}

// SingleClearEngineKey implements the Engine interface.
func (p *Pebble) SingleClearEngineKey(key EngineKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.SingleDelete(key.Encode(), pebble.Sync)
}

// ClearRawRange implements the Engine interface.
func (p *Pebble) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	startRaw, endRaw := EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode()
	if pointKeys {
		if err := p.db.DeleteRange(startRaw, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	if rangeKeys {
		if err := p.db.RangeKeyDelete(startRaw, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

// ClearMVCCRange implements the Engine interface.
func (p *Pebble) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	// Write all the tombstones in one batch.
	batch := p.NewUnindexedBatch()
	defer batch.Close()

	if err := batch.ClearMVCCRange(start, end, pointKeys, rangeKeys); err != nil {
		return err
	}
	return batch.Commit(true)
}

// ClearMVCCVersions implements the Engine interface.
func (p *Pebble) ClearMVCCVersions(start, end MVCCKey) error {
	return p.db.DeleteRange(EncodeMVCCKey(start), EncodeMVCCKey(end), pebble.Sync)
}

// ClearMVCCIteratorRange implements the Engine interface.
func (p *Pebble) ClearMVCCIteratorRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	// Write all the tombstones in one batch.
	batch := p.NewUnindexedBatch()
	defer batch.Close()

	if err := batch.ClearMVCCIteratorRange(start, end, pointKeys, rangeKeys); err != nil {
		return err
	}
	return batch.Commit(true)
}

// ClearMVCCRangeKey implements the Engine interface.
func (p *Pebble) ClearMVCCRangeKey(rangeKey MVCCRangeKey) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	// If the range key holds an encoded timestamp as it was read from storage,
	// write the tombstone to clear it using the same encoding of the timestamp.
	// See #129592.
	if len(rangeKey.EncodedTimestampSuffix) > 0 {
		return p.ClearEngineRangeKey(
			rangeKey.StartKey, rangeKey.EndKey, rangeKey.EncodedTimestampSuffix)
	}
	return p.ClearEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp))
}

// PutMVCCRangeKey implements the Engine interface.
func (p *Pebble) PutMVCCRangeKey(rangeKey MVCCRangeKey, value MVCCValue) error {
	// NB: all MVCC APIs currently assume all range keys are range tombstones.
	if !value.IsTombstone() {
		return errors.New("range keys can only be MVCC range tombstones")
	}
	valueRaw, err := EncodeMVCCValue(value)
	if err != nil {
		return errors.Wrapf(err, "failed to encode MVCC value for range key %s", rangeKey)
	}
	return p.PutRawMVCCRangeKey(rangeKey, valueRaw)
}

// PutRawMVCCRangeKey implements the Engine interface.
func (p *Pebble) PutRawMVCCRangeKey(rangeKey MVCCRangeKey, value []byte) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return p.PutEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp), value)
}

// Merge implements the Engine interface.
func (p *Pebble) Merge(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Merge(EncodeMVCCKey(key), value, pebble.Sync)
}

// PutMVCC implements the Engine interface.
func (p *Pebble) PutMVCC(key MVCCKey, value MVCCValue) error {
	if key.Timestamp.IsEmpty() {
		panic("PutMVCC timestamp is empty")
	}
	encValue, err := EncodeMVCCValue(value)
	if err != nil {
		return err
	}
	return p.put(key, encValue)
}

// PutRawMVCC implements the Engine interface.
func (p *Pebble) PutRawMVCC(key MVCCKey, value []byte) error {
	if key.Timestamp.IsEmpty() {
		panic("PutRawMVCC timestamp is empty")
	}
	return p.put(key, value)
}

// PutUnversioned implements the Engine interface.
func (p *Pebble) PutUnversioned(key roachpb.Key, value []byte) error {
	return p.put(MVCCKey{Key: key}, value)
}

// PutEngineKey implements the Engine interface.
func (p *Pebble) PutEngineKey(key EngineKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Set(key.Encode(), value, pebble.Sync)
}

func (p *Pebble) put(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Set(EncodeMVCCKey(key), value, pebble.Sync)
}

// PutEngineRangeKey implements the Engine interface.
func (p *Pebble) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	return p.db.RangeKeySet(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, value, pebble.Sync)
}

// ClearEngineRangeKey implements the Engine interface.
func (p *Pebble) ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error {
	return p.db.RangeKeyUnset(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, pebble.Sync)
}

// LogData implements the Engine interface.
func (p *Pebble) LogData(data []byte) error {
	return p.db.LogData(data, pebble.Sync)
}

// LogLogicalOp implements the Engine interface.
func (p *Pebble) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// LocalTimestampsEnabled controls whether local timestamps are written in MVCC
// values. A true setting is also gated on clusterversion.LocalTimestamps. After
// all nodes in a cluster are at or beyond clusterversion.LocalTimestamps,
// different nodes will see the version state transition at different times.
// Nodes that have not yet seen the transition may remove the local timestamp
// from an intent that has one during intent resolution. This will not cause
// problems.
//
// TODO(nvanbenschoten): remove this cluster setting and its associated plumbing
// when removing the cluster version, once we're confident in the efficacy and
// stability of local timestamps.
var LocalTimestampsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.transaction.local_timestamps.enabled",
	"if enabled, MVCC keys will be written with local timestamps",
	true,
)

func shouldWriteLocalTimestamps(ctx context.Context, settings *cluster.Settings) bool {
	return LocalTimestampsEnabled.Get(&settings.SV)
}

// ShouldWriteLocalTimestamps implements the Writer interface.
func (p *Pebble) ShouldWriteLocalTimestamps(ctx context.Context) bool {
	// This is not fast. Pebble should not be used by writers that want
	// performance. They should use pebbleBatch.
	return shouldWriteLocalTimestamps(ctx, p.cfg.settings)
}

// Attrs implements the Engine interface.
func (p *Pebble) Attrs() roachpb.Attributes {
	return p.cfg.attrs
}

// Properties implements the Engine interface.
func (p *Pebble) Properties() roachpb.StoreProperties {
	return p.properties
}

// Capacity implements the Engine interface.
func (p *Pebble) Capacity() (roachpb.StoreCapacity, error) {
	dir := p.cfg.env.Dir
	if dir != "" {
		var err error
		// Eval directory if it is a symbolic links.
		if dir, err = filepath.EvalSymlinks(dir); err != nil {
			return roachpb.StoreCapacity{}, err
		}
	}
	du, err := p.cfg.env.UnencryptedFS.GetDiskUsage(dir)
	if errors.Is(err, vfs.ErrUnsupported) {
		// This is an in-memory instance. Pretend we're empty since we
		// don't know better and only use this for testing. Using any
		// part of the actual file system here can throw off allocator
		// rebalancing in a hard-to-trace manner. See #7050.
		return roachpb.StoreCapacity{
			Capacity:  p.cfg.maxSize.bytes,
			Available: p.cfg.maxSize.bytes,
		}, nil
	} else if err != nil {
		return roachpb.StoreCapacity{}, err
	}

	if du.TotalBytes > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(du.TotalBytes), humanizeutil.IBytes(math.MaxInt64))
	}
	if du.AvailBytes > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(du.AvailBytes), humanizeutil.IBytes(math.MaxInt64))
	}
	fsuTotal := int64(du.TotalBytes)
	fsuAvail := int64(du.AvailBytes)

	// If the emergency ballast isn't appropriately sized, try to resize it.
	// This is a no-op if the ballast is already sized or if there's not
	// enough available capacity to resize it. Capacity is called periodically
	// by the kvserver, and that drives the automatic resizing of the ballast.
	if !p.cfg.env.IsReadOnly() {
		resized, err := maybeEstablishBallast(p.cfg.env.UnencryptedFS, p.ballastPath, p.cfg.ballastSize, du)
		if err != nil {
			return roachpb.StoreCapacity{}, errors.Wrap(err, "resizing ballast")
		}
		if resized {
			p.logger.Infof("resized ballast %s to size %s",
				p.ballastPath, humanizeutil.IBytes(p.cfg.ballastSize))
			du, err = p.cfg.env.UnencryptedFS.GetDiskUsage(dir)
			if err != nil {
				return roachpb.StoreCapacity{}, err
			}
		}
	}

	// Pebble has detailed accounting of its own disk space usage, and it's
	// incrementally updated which helps avoid O(# files) work here.
	m := p.db.Metrics()
	totalUsedBytes := int64(m.DiskSpaceUsage())

	// We don't have incremental accounting of the disk space usage of files
	// in the auxiliary directory. Walk the auxiliary directory and all its
	// subdirectories, adding to the total used bytes.
	if errOuter := filepath.Walk(p.auxDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// This can happen if CockroachDB removes files out from under us -
			// just keep going to get the best estimate we can.
			if oserror.IsNotExist(err) {
				return nil
			}
			// Special-case: if the store-dir is configured using the root of some fs,
			// e.g. "/mnt/db", we might have special fs-created files like lost+found
			// that we can't read, so just ignore them rather than crashing.
			if oserror.IsPermission(err) && filepath.Base(path) == "lost+found" {
				return nil
			}
			return err
		}
		if path == p.ballastPath {
			// Skip the ballast. Counting it as used is likely to confuse
			// users, and it's more akin to space that is just unavailable
			// like disk space often restricted to a root user.
			return nil
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
	if ((p.cfg.maxSize.bytes == 0 || p.cfg.maxSize.bytes >= fsuTotal) && p.cfg.maxSize.percent == 0) || p.cfg.env.Dir == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
			Used:      totalUsedBytes,
		}, nil
	}

	maxSize := p.cfg.maxSize.bytes
	if p.cfg.maxSize.percent != 0 {
		maxSize = int64(float64(fsuTotal) * p.cfg.maxSize.percent)
	}

	available := maxSize - totalUsedBytes
	if available > fsuAvail {
		available = fsuAvail
	}
	if available < 0 {
		available = 0
	}

	return roachpb.StoreCapacity{
		Capacity:  maxSize,
		Available: available,
		Used:      totalUsedBytes,
	}, nil
}

// Flush implements the Engine interface.
func (p *Pebble) Flush() error {
	return p.db.Flush()
}

// GetMetrics implements the Engine interface.
func (p *Pebble) GetMetrics() Metrics {
	m := Metrics{
		Metrics:                          p.db.Metrics(),
		WriteStallCount:                  atomic.LoadInt64(&p.writeStallCount),
		WriteStallDuration:               time.Duration(atomic.LoadInt64((*int64)(&p.writeStallDuration))),
		DiskSlowCount:                    atomic.LoadInt64(&p.diskSlowCount),
		DiskStallCount:                   atomic.LoadInt64(&p.diskStallCount),
		SingleDelInvariantViolationCount: atomic.LoadInt64(&p.singleDelInvariantViolationCount),
		SingleDelIneffectualCount:        atomic.LoadInt64(&p.singleDelIneffectualCount),
		SharedStorageReadBytes:           atomic.LoadInt64(&p.sharedBytesRead),
		SharedStorageWriteBytes:          atomic.LoadInt64(&p.sharedBytesWritten),
	}
	if sema := p.cfg.opts.LoadBlockSema; sema != nil {
		semaStats := sema.Stats()
		m.BlockLoadConcurrencyLimit = semaStats.Capacity
		m.BlockLoadsInProgress = semaStats.Outstanding
		m.BlockLoadsQueued = semaStats.NumHadToWait
	}
	p.iterStats.Lock()
	m.Iterator = p.iterStats.AggregatedIteratorStats
	p.iterStats.Unlock()
	p.batchCommitStats.Lock()
	m.BatchCommitStats = p.batchCommitStats.AggregatedBatchCommitStats
	p.batchCommitStats.Unlock()
	if p.diskWriteStatsCollector != nil {
		m.DiskWriteStats = p.diskWriteStatsCollector.GetStats()
	}
	return m
}

// GetPebbleOptions implements the Engine interface.
func (p *Pebble) GetPebbleOptions() *pebble.Options {
	return p.cfg.opts
}

// GetEncryptionRegistries implements the Engine interface.
func (p *Pebble) GetEncryptionRegistries() (*fs.EncryptionRegistries, error) {
	rv := &fs.EncryptionRegistries{}
	var err error
	if p.cfg.env.Encryption != nil {
		rv.KeyRegistry, err = p.cfg.env.Encryption.StatsHandler.GetDataKeysRegistry()
		if err != nil {
			return nil, err
		}
	}
	if p.cfg.env.Registry != nil {
		rv.FileRegistry, err = protoutil.Marshal(p.cfg.env.Registry.GetRegistrySnapshot())
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}

// GetEnvStats implements the Engine interface.
func (p *Pebble) GetEnvStats() (*fs.EnvStats, error) {
	// TODO(sumeer): make the stats complete. There are no bytes stats. The TotalFiles is missing
	// files that are not in the registry (from before encryption was enabled).
	stats := &fs.EnvStats{}
	if p.cfg.env.Encryption == nil {
		return stats, nil
	}
	stats.EncryptionType = p.cfg.env.Encryption.StatsHandler.GetActiveStoreKeyType()
	var err error
	stats.EncryptionStatus, err = p.cfg.env.Encryption.StatsHandler.GetEncryptionStatus()
	if err != nil {
		return nil, err
	}
	fr := p.cfg.env.Registry.GetRegistrySnapshot()
	activeKeyID, err := p.cfg.env.Encryption.StatsHandler.GetActiveDataKeyID()
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
		keyID, err := p.cfg.env.Encryption.StatsHandler.GetKeyIDFromSettings(entry.EncryptionSettings)
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

		filename := p.cfg.env.PathBase(filePath)
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

	// Ensure that encryption percentage does not exceed 100%.
	frFileLen := uint64(len(fr.Files))
	if stats.TotalFiles < frFileLen {
		stats.TotalFiles = frFileLen
	}

	if stats.TotalBytes < stats.ActiveKeyBytes {
		stats.TotalBytes = stats.ActiveKeyBytes
	}

	return stats, nil
}

// GetAuxiliaryDir implements the Engine interface.
func (p *Pebble) GetAuxiliaryDir() string {
	return p.auxDir
}

// NewBatch implements the Engine interface.
func (p *Pebble) NewBatch() Batch {
	return newPebbleBatch(p.db, p.db.NewIndexedBatch(), p.cfg.settings, p, p)
}

// NewReader implements the Engine interface.
func (p *Pebble) NewReader(durability DurabilityRequirement) Reader {
	return newPebbleReadOnly(p, durability)
}

// NewReadOnly implements the Engine interface.
func (p *Pebble) NewReadOnly(durability DurabilityRequirement) ReadWriter {
	return newPebbleReadOnly(p, durability)
}

// NewUnindexedBatch implements the Engine interface.
func (p *Pebble) NewUnindexedBatch() Batch {
	return newPebbleBatch(p.db, p.db.NewBatch(), p.cfg.settings, p, p)
}

// NewWriteBatch implements the Engine interface.
func (p *Pebble) NewWriteBatch() WriteBatch {
	return newWriteBatch(p.db, p.db.NewBatch(), p.cfg.settings, p, p)
}

// NewSnapshot implements the Engine interface.
func (p *Pebble) NewSnapshot() Reader {
	return &pebbleSnapshot{
		snapshot: p.db.NewSnapshot(),
		parent:   p,
	}
}

// NewEventuallyFileOnlySnapshot implements the Engine interface.
func (p *Pebble) NewEventuallyFileOnlySnapshot(keyRanges []roachpb.Span) EventuallyFileOnlyReader {
	engineKeyRanges := make([]pebble.KeyRange, len(keyRanges))
	for i := range keyRanges {
		engineKeyRanges[i].Start = EngineKey{Key: keyRanges[i].Key}.Encode()
		engineKeyRanges[i].End = EngineKey{Key: keyRanges[i].EndKey}.Encode()
	}
	efos := p.db.NewEventuallyFileOnlySnapshot(engineKeyRanges)
	return &pebbleEFOS{
		efos:      efos,
		parent:    p,
		keyRanges: keyRanges,
	}
}

// IngestLocalFiles implements the Engine interface.
func (p *Pebble) IngestLocalFiles(ctx context.Context, paths []string) error {
	return p.db.Ingest(ctx, paths)
}

// IngestLocalFilesWithStats implements the Engine interface.
func (p *Pebble) IngestLocalFilesWithStats(
	ctx context.Context, paths []string,
) (pebble.IngestOperationStats, error) {
	return p.db.IngestWithStats(ctx, paths)
}

// IngestAndExciseFiles implements the Engine interface.
func (p *Pebble) IngestAndExciseFiles(
	ctx context.Context,
	paths []string,
	shared []pebble.SharedSSTMeta,
	external []pebble.ExternalFile,
	exciseSpan roachpb.Span,
	sstsContainExciseTombstone bool,
) (pebble.IngestOperationStats, error) {
	rawSpan := pebble.KeyRange{
		Start: EngineKey{Key: exciseSpan.Key}.Encode(),
		End:   EngineKey{Key: exciseSpan.EndKey}.Encode(),
	}
	return p.db.IngestAndExcise(ctx, paths, shared, external, rawSpan)
}

// IngestExternalFiles implements the Engine interface.
func (p *Pebble) IngestExternalFiles(
	ctx context.Context, external []pebble.ExternalFile,
) (pebble.IngestOperationStats, error) {
	return p.db.IngestExternalFiles(ctx, external)
}

// PreIngestDelay implements the Engine interface.
func (p *Pebble) PreIngestDelay(ctx context.Context) {
	preIngestDelay(ctx, p, p.cfg.settings)
}

// GetTableMetrics implements the Engine interface.
func (p *Pebble) GetTableMetrics(start, end roachpb.Key) ([]enginepb.SSTableMetricsInfo, error) {
	filterOpt := pebble.WithKeyRangeFilter(
		EncodeMVCCKey(MVCCKey{Key: start}),
		EncodeMVCCKey(MVCCKey{Key: end}),
	)
	tableInfo, err := p.db.SSTables(filterOpt, pebble.WithProperties(), pebble.WithApproximateSpanBytes())
	if err != nil {
		return []enginepb.SSTableMetricsInfo{}, err
	}

	var totalTables int
	for _, info := range tableInfo {
		totalTables += len(info)
	}

	var metricsInfo []enginepb.SSTableMetricsInfo

	for level, sstableInfos := range tableInfo {
		for _, sstableInfo := range sstableInfos {
			marshalTableInfo, err := json.Marshal(sstableInfo)
			if err != nil {
				return []enginepb.SSTableMetricsInfo{}, err
			}
			metricsInfo = append(metricsInfo, enginepb.SSTableMetricsInfo{
				Level:                int32(level),
				TableID:              uint64(sstableInfo.TableInfo.FileNum),
				TableInfoJSON:        marshalTableInfo,
				ApproximateSpanBytes: sstableInfo.ApproximateSpanBytes,
			})
		}
	}
	return metricsInfo, nil
}

// ScanStorageInternalKeys implements the Engine interface.
func (p *Pebble) ScanStorageInternalKeys(
	start, end roachpb.Key, megabytesPerSecond int64,
) ([]enginepb.StorageInternalKeysMetrics, error) {
	stats, err := p.db.ScanStatistics(context.TODO(), start, end, pebble.ScanStatisticsOptions{LimitBytesPerSecond: 1000000 * megabytesPerSecond})
	if err != nil {
		return []enginepb.StorageInternalKeysMetrics{}, err
	}
	setMetricsFromStats := func(
		level int, stats *pebble.KeyStatistics, m *enginepb.StorageInternalKeysMetrics) {
		*m = enginepb.StorageInternalKeysMetrics{
			Level:                       int32(level),
			SnapshotPinnedKeys:          uint64(stats.SnapshotPinnedKeys),
			SnapshotPinnedKeysBytes:     stats.SnapshotPinnedKeysBytes,
			PointKeyDeleteCount:         uint64(stats.KindsCount[pebble.InternalKeyKindDelete]),
			PointKeySetCount:            uint64(stats.KindsCount[pebble.InternalKeyKindSet]),
			RangeDeleteCount:            uint64(stats.KindsCount[pebble.InternalKeyKindRangeDelete]),
			RangeKeySetCount:            uint64(stats.KindsCount[pebble.InternalKeyKindRangeKeySet]),
			RangeKeyDeleteCount:         uint64(stats.KindsCount[pebble.InternalKeyKindRangeKeyDelete]),
			PointKeyDeleteIsLatestCount: uint64(stats.LatestKindsCount[pebble.InternalKeyKindDelete]),
			PointKeySetIsLatestCount:    uint64(stats.LatestKindsCount[pebble.InternalKeyKindSet]),
		}
	}
	var metrics []enginepb.StorageInternalKeysMetrics
	for level := 0; level < 7; level++ {
		var m enginepb.StorageInternalKeysMetrics
		setMetricsFromStats(level, &stats.Levels[level], &m)
		metrics = append(metrics, m)
	}
	var m enginepb.StorageInternalKeysMetrics
	setMetricsFromStats(-1 /* level */, &stats.Accumulated, &m)
	metrics = append(metrics, m)
	return metrics, nil
}

// ApproximateDiskBytes implements the Engine interface.
func (p *Pebble) ApproximateDiskBytes(
	from, to roachpb.Key,
) (bytes, remoteBytes, externalBytes uint64, _ error) {
	fromEncoded := EngineKey{Key: from}.Encode()
	toEncoded := EngineKey{Key: to}.Encode()
	bytes, remoteBytes, externalBytes, err := p.db.EstimateDiskUsageByBackingType(fromEncoded, toEncoded)
	if err != nil {
		return 0, 0, 0, err
	}
	return bytes, remoteBytes, externalBytes, nil
}

// Compact implements the Engine interface.
func (p *Pebble) Compact() error {
	return p.db.Compact(nil, EncodeMVCCKey(MVCCKeyMax), true /* parallel */)
}

// CompactRange implements the Engine interface.
func (p *Pebble) CompactRange(start, end roachpb.Key) error {
	// TODO(jackson): Consider changing Engine.CompactRange's signature to take
	// in EngineKeys so that it's unambiguous that the arguments have already
	// been encoded as engine keys. We do need to encode these keys in protocol
	// buffers when they're sent over the wire during the
	// crdb_internal.compact_engine_span builtin. Maybe we should have a
	// roachpb.Key equivalent for EngineKey so we don't lose that type
	// information?
	if ek, ok := DecodeEngineKey(start); !ok || ek.Validate() != nil {
		return errors.Errorf("invalid start key: %q", start)
	}
	if ek, ok := DecodeEngineKey(end); !ok || ek.Validate() != nil {
		return errors.Errorf("invalid end key: %q", end)
	}
	return p.db.Compact(start, end, true /* parallel */)
}

// RegisterFlushCompletedCallback implements the Engine interface.
func (p *Pebble) RegisterFlushCompletedCallback(cb func()) {
	p.mu.Lock()
	p.mu.flushCompletedCallback = cb
	p.mu.Unlock()
}

func checkpointSpansNote(spans []roachpb.Span) []byte {
	note := "CRDB spans:\n"
	for _, span := range spans {
		note += span.String() + "\n"
	}
	return []byte(note)
}

// CreateCheckpoint implements the Engine interface.
func (p *Pebble) CreateCheckpoint(dir string, spans []roachpb.Span) error {
	opts := []pebble.CheckpointOption{
		pebble.WithFlushedWAL(),
	}
	if l := len(spans); l > 0 {
		s := make([]pebble.CheckpointSpan, 0, l)
		for _, span := range spans {
			s = append(s, pebble.CheckpointSpan{
				Start: EngineKey{Key: span.Key}.Encode(),
				End:   EngineKey{Key: span.EndKey}.Encode(),
			})
		}
		opts = append(opts, pebble.WithRestrictToSpans(s))
	}
	if err := p.db.Checkpoint(dir, opts...); err != nil {
		return err
	}

	// Write out the min version file.
	if err := writeMinVersionFile(p.cfg.env.UnencryptedFS, dir, p.MinVersion()); err != nil {
		return errors.Wrapf(err, "writing min version file for checkpoint")
	}

	// TODO(#90543, cockroachdb/pebble#2285): move spans info to Pebble manifest.
	if len(spans) > 0 {
		if err := fs.SafeWriteToFile(
			p.cfg.env, dir, p.cfg.env.PathJoin(dir, "checkpoint.txt"),
			checkpointSpansNote(spans),
			fs.UnspecifiedWriteCategory,
		); err != nil {
			return err
		}
	}

	return nil
}

// pebbleFormatVersionMap maps cluster versions to the corresponding pebble
// format version. For a given cluster version, the entry with the latest
// version that is not newer than the given version is chosen.
//
// This map needs to include the "final" version for each supported previous
// release, and the in-development versions of the current release and the
// previous one (for experimental version skipping during upgrade).
//
// Pebble has a concept of format major versions, similar to cluster versions.
// Backwards incompatible changes to Pebble's on-disk format are gated behind
// new format major versions. Bumping the storage engine's format major version
// is tied to a CockroachDB cluster version.
//
// Format major versions and cluster versions both only ratchet upwards. Here we
// map the persisted cluster version to the corresponding format major version,
// ratcheting Pebble's format major version if necessary.
//
// The pebble versions are advanced when nodes enter the "fence" version for the
// named cluster version, if there is one, so that if *any* node moves into the
// named version, it can be assumed all *nodes* have ratcheted to the pebble
// version associated with it, since they did so during the fence version.
var pebbleFormatVersionMap = map[clusterversion.Key]pebble.FormatMajorVersion{
	clusterversion.V24_3: pebble.FormatColumnarBlocks,
}

// pebbleFormatVersionKeys contains the keys in the map above, in descending order.
var pebbleFormatVersionKeys []clusterversion.Key = func() []clusterversion.Key {
	versionKeys := make([]clusterversion.Key, 0, len(pebbleFormatVersionMap))
	for k := range pebbleFormatVersionMap {
		versionKeys = append(versionKeys, k)
	}
	// Sort the keys in reverse order.
	sort.Slice(versionKeys, func(i, j int) bool {
		return versionKeys[i] > versionKeys[j]
	})
	return versionKeys
}()

// pebbleFormatVersion finds the most recent pebble format version supported by
// the given cluster version.
func pebbleFormatVersion(clusterVersion roachpb.Version) pebble.FormatMajorVersion {
	// pebbleFormatVersionKeys are sorted in descending order; find the first one
	// that is not newer than clusterVersion.
	for _, k := range pebbleFormatVersionKeys {
		if clusterVersion.AtLeast(k.Version().FenceVersion()) {
			return pebbleFormatVersionMap[k]
		}
	}
	// This should never happen in production. But we tolerate tests creating
	// imaginary older versions; we must still use the earliest supported
	// format.
	return MinimumSupportedFormatVersion
}

// SetMinVersion implements the Engine interface.
func (p *Pebble) SetMinVersion(version roachpb.Version) error {
	p.minVersion = version

	if p.cfg.env.IsReadOnly() {
		// Don't make any on-disk changes.
		return nil
	}

	// NB: SetMinVersion must be idempotent. It may called multiple
	// times with the same version.

	// Writing the min version file commits this storage engine to the
	// provided cluster version.
	if err := writeMinVersionFile(p.cfg.env.UnencryptedFS, p.cfg.env.Dir, version); err != nil {
		return err
	}

	// Set the shared object creator ID .
	if storeID := p.storeIDPebbleLog.Get(); storeID != 0 && storeID != base.TempStoreID {
		if err := p.db.SetCreatorID(uint64(storeID)); err != nil {
			return err
		}
	}

	formatVers := pebbleFormatVersion(version)
	if p.db.FormatMajorVersion() < formatVers {
		if err := p.db.RatchetFormatMajorVersion(formatVers); err != nil {
			return errors.Wrap(err, "ratcheting format major version")
		}
	}
	return nil
}

// MinVersion implements the Engine interface.
func (p *Pebble) MinVersion() roachpb.Version {
	return p.minVersion
}

// BufferedSize implements the Engine interface.
func (p *Pebble) BufferedSize() int {
	return 0
}

// ConvertFilesToBatchAndCommit implements the Engine interface.
func (p *Pebble) ConvertFilesToBatchAndCommit(
	_ context.Context, paths []string, clearedSpans []roachpb.Span,
) error {
	files := make([]sstable.ReadableFile, len(paths))
	closeFiles := func() {
		for i := range files {
			if files[i] != nil {
				files[i].Close()
			}
		}
	}
	for i, fileName := range paths {
		f, err := p.cfg.env.Open(fileName)
		if err != nil {
			closeFiles()
			return err
		}
		files[i] = f
	}
	iter, err := NewSSTEngineIterator(
		[][]sstable.ReadableFile{files},
		IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: roachpb.KeyMin,
			UpperBound: roachpb.KeyMax,
		})
	if err != nil {
		// TODO(sumeer): we don't call closeFiles() since in the error case some
		// of the files may be closed. See the code in
		// https://github.com/cockroachdb/pebble/blob/master/external_iterator.go#L104-L113
		// which closes the opened readers. At this point in the code we don't
		// know which files are already closed. The callee needs to be fixed to
		// not close any of the files or close all the files in the error case.
		// The natural behavior would be to not close any file. Fix this in
		// Pebble, and then adjust the code here if needed.
		return err
	}
	defer iter.Close()

	batch := p.NewWriteBatch()
	for i := range clearedSpans {
		err :=
			batch.ClearRawRange(clearedSpans[i].Key, clearedSpans[i].EndKey, true, true)
		if err != nil {
			return err
		}
	}
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: roachpb.KeyMin})
	for valid {
		hasPoint, hasRange := iter.HasPointAndRange()
		if hasPoint {
			var k EngineKey
			if k, err = iter.UnsafeEngineKey(); err != nil {
				break
			}
			var v []byte
			if v, err = iter.UnsafeValue(); err != nil {
				break
			}
			if err = batch.PutEngineKey(k, v); err != nil {
				break
			}
		}
		if hasRange && iter.RangeKeyChanged() {
			var rangeBounds roachpb.Span
			if rangeBounds, err = iter.EngineRangeBounds(); err != nil {
				break
			}
			rangeKeys := iter.EngineRangeKeys()
			for i := range rangeKeys {
				if err = batch.PutEngineRangeKey(rangeBounds.Key, rangeBounds.EndKey, rangeKeys[i].Version,
					rangeKeys[i].Value); err != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		valid, err = iter.NextEngineKey()
	}
	if err != nil {
		batch.Close()
		return err
	}
	return batch.Commit(true)
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
	// When the first iterator is initialized, or when
	// PinEngineStateForIterators is called (whichever happens first), the
	// underlying *pebble.Iterator is stashed in iter, so that subsequent
	// iterator initialization can use Iterator.Clone to use the same underlying
	// engine state. This relies on the fact that all pebbleIterators created
	// here are marked as reusable, which causes pebbleIterator.Close to not
	// close iter. iter will be closed when pebbleReadOnly.Close is called.
	prefixIter       pebbleIterator
	normalIter       pebbleIterator
	prefixEngineIter pebbleIterator
	normalEngineIter pebbleIterator

	iter       pebbleiter.Iterator
	iterUsed   bool // avoids cloning after PinEngineStateForIterators()
	durability DurabilityRequirement
	closed     bool
}

var _ ReadWriter = &pebbleReadOnly{}

var pebbleReadOnlyPool = sync.Pool{
	New: func() interface{} {
		return &pebbleReadOnly{
			// Defensively set reusable=true. One has to be careful about this since
			// an accidental false value would cause these iterators, that are value
			// members of pebbleReadOnly, to be put in the pebbleIterPool.
			prefixIter:       pebbleIterator{reusable: true},
			normalIter:       pebbleIterator{reusable: true},
			prefixEngineIter: pebbleIterator{reusable: true},
			normalEngineIter: pebbleIterator{reusable: true},
		}
	},
}

// Instantiates a new pebbleReadOnly.
func newPebbleReadOnly(parent *Pebble, durability DurabilityRequirement) *pebbleReadOnly {
	p := pebbleReadOnlyPool.Get().(*pebbleReadOnly)
	// When p is a reused pebbleReadOnly from the pool, the iter fields preserve
	// the original reusable=true that was set above in pebbleReadOnlyPool.New(),
	// and some buffers that are safe to reuse. Everything else has been reset by
	// pebbleIterator.destroy().
	*p = pebbleReadOnly{
		parent:           parent,
		prefixIter:       p.prefixIter,
		normalIter:       p.normalIter,
		prefixEngineIter: p.prefixEngineIter,
		normalEngineIter: p.normalEngineIter,
		durability:       durability,
	}
	return p
}

func (p *pebbleReadOnly) Close() {
	if p.closed {
		panic("closing an already-closed pebbleReadOnly")
	}
	p.closed = true
	if p.iter != nil && !p.iterUsed {
		err := p.iter.Close()
		if err != nil {
			panic(err)
		}
	}

	// Setting iter to nil is sufficient since it will be closed by one of the
	// subsequent destroy calls.
	p.iter = nil
	p.prefixIter.destroy()
	p.normalIter.destroy()
	p.prefixEngineIter.destroy()
	p.normalEngineIter.destroy()
	p.durability = StandardDurability

	pebbleReadOnlyPool.Put(p)
}

func (p *pebbleReadOnly) Closed() bool {
	return p.closed
}

func (p *pebbleReadOnly) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// NewMVCCIterator implements the Engine interface.
func (p *pebbleReadOnly) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter := &p.normalIter
	if opts.Prefix {
		iter = &p.prefixIter
	}
	if iter.inuse {
		return newPebbleIteratorByCloning(ctx, CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, p.durability), nil
	}

	if iter.iter != nil {
		iter.setOptions(ctx, opts, p.durability)
	} else {
		if err := iter.initReuseOrCreate(
			ctx, p.parent.db, p.iter, p.iterUsed, opts, p.durability, p.parent); err != nil {
			return nil, err
		}
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
		iter.reusable = true
	}

	iter.inuse = true
	return maybeWrapInUnsafeIter(iter), nil
}

// NewEngineIterator implements the Engine interface.
func (p *pebbleReadOnly) NewEngineIterator(
	ctx context.Context, opts IterOptions,
) (EngineIterator, error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	iter := &p.normalEngineIter
	if opts.Prefix {
		iter = &p.prefixEngineIter
	}
	if iter.inuse {
		return newPebbleIteratorByCloning(ctx, CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, p.durability), nil
	}

	if iter.iter != nil {
		iter.setOptions(ctx, opts, p.durability)
	} else {
		err := iter.initReuseOrCreate(
			ctx, p.parent.db, p.iter, p.iterUsed, opts, p.durability, p.parent)
		if err != nil {
			return nil, err
		}
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
		iter.reusable = true
	}

	iter.inuse = true
	return iter, nil
}

// ConsistentIterators implements the Engine interface.
func (p *pebbleReadOnly) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Engine interface.
func (p *pebbleReadOnly) PinEngineStateForIterators(readCategory fs.ReadCategory) error {
	if p.iter == nil {
		o := &pebble.IterOptions{Category: readCategory.PebbleCategory()}
		if p.durability == GuaranteedDurability {
			o.OnlyReadGuaranteedDurable = true
		}
		iter, err := p.parent.db.NewIter(o)
		if err != nil {
			return err
		}
		p.iter = pebbleiter.MaybeWrap(iter)
		// NB: p.iterUsed == false avoids cloning this in NewMVCCIterator(), since
		// we've just created it.
	}
	return nil
}

// ScanInternal implements the Reader interface.
func (p *pebbleReadOnly) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	return p.parent.ScanInternal(ctx, lower, upper, visitPointKey, visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// Writer methods are not implemented for pebbleReadOnly. Ideally, the code
// could be refactored so that a Reader could be supplied to evaluateBatch

// Writer is the write interface to an engine's data.

func (p *pebbleReadOnly) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCC(key MVCCKey, opts ClearOptions) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearUnversioned(key roachpb.Key, opts ClearOptions) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearEngineKey(key EngineKey, opts ClearOptions) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) SingleClearEngineKey(key EngineKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCVersions(start, end MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutMVCCRangeKey(MVCCRangeKey, MVCCValue) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutRawMVCCRangeKey(MVCCRangeKey, []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutEngineRangeKey(roachpb.Key, roachpb.Key, []byte, []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearEngineRangeKey(roachpb.Key, roachpb.Key, []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCRangeKey(MVCCRangeKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) Merge(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutMVCC(key MVCCKey, value MVCCValue) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutRawMVCC(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutUnversioned(key roachpb.Key, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutEngineKey(key EngineKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) LogData(data []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	panic("not implemented")
}

func (p *pebbleReadOnly) ShouldWriteLocalTimestamps(ctx context.Context) bool {
	panic("not implemented")
}

func (p *pebbleReadOnly) BufferedSize() int {
	panic("not implemented")
}

// pebbleSnapshot represents a snapshot created using Pebble.NewSnapshot().
type pebbleSnapshot struct {
	snapshot *pebble.Snapshot
	parent   *Pebble
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

// MVCCIterate implements the Reader interface.
func (p *pebbleSnapshot) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// NewMVCCIterator implements the Reader interface.
func (p *pebbleSnapshot) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter, err := newPebbleIterator(ctx, p.snapshot, opts, StandardDurability, p.parent)
	if err != nil {
		return nil, err
	}
	return maybeWrapInUnsafeIter(MVCCIterator(iter)), nil
}

// NewEngineIterator implements the Reader interface.
func (p pebbleSnapshot) NewEngineIterator(
	ctx context.Context, opts IterOptions,
) (EngineIterator, error) {
	return newPebbleIterator(ctx, p.snapshot, opts, StandardDurability, p.parent)
}

// ConsistentIterators implements the Reader interface.
func (p pebbleSnapshot) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Reader interface.
func (p *pebbleSnapshot) PinEngineStateForIterators(fs.ReadCategory) error {
	// Snapshot already pins state, so nothing to do.
	return nil
}

// ScanInternal implements the Reader interface.
func (p *pebbleSnapshot) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	rawLower := EngineKey{Key: lower}.Encode()
	rawUpper := EngineKey{Key: upper}.Encode()
	// TODO(sumeer): set category.
	return p.snapshot.ScanInternal(ctx, block.CategoryUnknown, rawLower, rawUpper, visitPointKey,
		visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// pebbleEFOS represents an eventually file-only snapshot created using
// NewEventuallyFileOnlySnapshot.
type pebbleEFOS struct {
	efos      *pebble.EventuallyFileOnlySnapshot
	parent    *Pebble
	keyRanges []roachpb.Span
	closed    bool
}

var _ EventuallyFileOnlyReader = &pebbleEFOS{}

// Close implements the Reader interface.
func (p *pebbleEFOS) Close() {
	_ = p.efos.Close()
	p.closed = true
}

// Closed implements the Reader interface.
func (p *pebbleEFOS) Closed() bool {
	return p.closed
}

// MVCCIterate implements the Reader interface.
func (p *pebbleEFOS) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// WaitForFileOnly implements the EventuallyFileOnlyReader interface.
func (p *pebbleEFOS) WaitForFileOnly(
	ctx context.Context, gracePeriodBeforeFlush time.Duration,
) error {
	return p.efos.WaitForFileOnlySnapshot(ctx, gracePeriodBeforeFlush)
}

// NewMVCCIterator implements the Reader interface.
func (p *pebbleEFOS) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	// Check if the bounds fall within the EFOS' keyRanges. We can only do this
	// check for non-prefix iterators as prefix iterators often don't specify
	// any bounds.
	if !opts.Prefix {
		if opts.LowerBound == nil || opts.UpperBound == nil {
			return nil, errors.AssertionFailedf("cannot create iterators on EFOS without bounds")
		}
		var found bool
		boundSpan := roachpb.Span{Key: opts.LowerBound, EndKey: opts.UpperBound}
		for i := range p.keyRanges {
			if p.keyRanges[i].Contains(boundSpan) {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.AssertionFailedf("iterator bounds exceed eventually-file-only-snapshot key ranges: %s", boundSpan.String())
		}
	}
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter, err := newPebbleIterator(ctx, p.efos, opts, StandardDurability, p.parent)
	if err != nil {
		return nil, err
	}
	return maybeWrapInUnsafeIter(MVCCIterator(iter)), nil
}

// NewEngineIterator implements the Reader interface.
func (p *pebbleEFOS) NewEngineIterator(
	ctx context.Context, opts IterOptions,
) (EngineIterator, error) {
	return newPebbleIterator(ctx, p.efos, opts, StandardDurability, p.parent)
}

// ConsistentIterators implements the Reader interface.
func (p *pebbleEFOS) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Reader interface.
func (p *pebbleEFOS) PinEngineStateForIterators(fs.ReadCategory) error {
	// Snapshot already pins state, so nothing to do.
	return nil
}

// ScanInternal implements the Reader interface.
func (p *pebbleEFOS) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	rawLower := EngineKey{Key: lower}.Encode()
	rawUpper := EngineKey{Key: upper}.Encode()
	// TODO(sumeer): set category.
	return p.efos.ScanInternal(ctx, block.CategoryUnknown, rawLower, rawUpper, visitPointKey,
		visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// ExceedMaxSizeError is the error returned when an export request
// fails due the export size exceeding the budget. This can be caused
// by large KVs that have many revisions.
type ExceedMaxSizeError struct {
	reached int64
	maxSize uint64
}

var _ error = &ExceedMaxSizeError{}

func (e *ExceedMaxSizeError) Error() string {
	return fmt.Sprintf("export size (%d bytes) exceeds max size (%d bytes)", e.reached, e.maxSize)
}
