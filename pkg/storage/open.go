// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
)

// A ConfigOption may be passed to Open to configure the storage engine.
type ConfigOption func(cfg *engineConfig) error

// CombineOptions combines many options into one.
func CombineOptions(opts ...ConfigOption) ConfigOption {
	return func(cfg *engineConfig) error {
		for _, opt := range opts {
			if err := opt(cfg); err != nil {
				return err
			}
		}
		return nil
	}
}

// MustExist configures an engine to error on Open if the target directory
// does not contain an initialized store.
var MustExist ConfigOption = func(cfg *engineConfig) error {
	cfg.mustExist = true
	return nil
}

// DisableAutomaticCompactions configures an engine to be opened with disabled
// automatic compactions. Used primarily for debugCompactCmd.
var DisableAutomaticCompactions ConfigOption = func(cfg *engineConfig) error {
	cfg.opts.DisableAutomaticCompactions = true
	return nil
}

// ForceWriterParallelism configures an engine to be opened with disabled
// automatic compactions. Used primarily for debugCompactCmd.
var ForceWriterParallelism ConfigOption = func(cfg *engineConfig) error {
	cfg.opts.Experimental.ForceWriterParallelism = true
	return nil
}

// ForTesting configures the engine for use in testing. It may randomize some
// config options to improve test coverage.
var ForTesting ConfigOption = func(cfg *engineConfig) error {
	cfg.beforeClose = append(cfg.beforeClose, func(p *Pebble) {
		m := p.db.Metrics()
		if m.Keys.MissizedTombstonesCount > 0 {
			// A missized tombstone is a Pebble DELSIZED tombstone that encodes
			// the wrong size of the value it deletes. This kind of tombstone is
			// written when ClearOptions.ValueSizeKnown=true. If this assertion
			// failed, something might be awry in the code clearing the key. Are
			// we feeding the wrong value length to ValueSize?
			panic(errors.AssertionFailedf("expected to find 0 missized tombstones; found %d", m.Keys.MissizedTombstonesCount))
		}
	})
	return nil
}

// Attributes configures the engine's attributes.
func Attributes(attrs roachpb.Attributes) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.attrs = attrs
		return nil
	}
}

// storeSize configures the maximum allowable size for a store.
// Can be specified either as a percentage of total capacity or
// an absolute byte size; if both are specified, the percentage takes
// precedence.
type storeSize struct {
	bytes   int64
	percent float64
}

// MaxSizeBytes ets the intended maximum store size as an absolute byte
// value. MaxSizeBytes is used for calculating free space and making rebalancing
// decisions.
func MaxSizeBytes(size int64) ConfigOption {
	return maxSize(storeSize{bytes: size})
}

// MaxSizePercent ets the intended maximum store size as the specified percentage
// of total capacity. MaxSizePercent is used for calculating free space and making
// rebalancing decisions.
func MaxSizePercent(percent float64) ConfigOption {
	return maxSize(storeSize{percent: percent})
}

// maxSize sets the intended maximum store size. MaxSize is used for
// calculating free space and making rebalancing decisions. Either an
// absolute size or a percentage of total capacity can be specified;
// if both are specified, the percentage is used.
func maxSize(size storeSize) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.maxSize = size
		return nil
	}
}

// BlockSize sets the engine block size, primarily for testing purposes.
func BlockSize(size int) ConfigOption {
	return func(cfg *engineConfig) error {
		for i := range cfg.opts.Levels {
			cfg.opts.Levels[i].BlockSize = size
			cfg.opts.Levels[i].IndexBlockSize = size
		}
		return nil
	}
}

// TargetFileSize sets the target file size across all levels of the LSM,
// primarily for testing purposes.
func TargetFileSize(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		for i := range cfg.opts.Levels {
			cfg.opts.Levels[i].TargetFileSize = size
		}
		return nil
	}
}

// MaxWriterConcurrency sets the concurrency of the sstable Writers. A concurrency
// of 0 implies no parallelism in the Writer, and a concurrency of 1 or more implies
// parallelism in the Writer. Currently, there's no difference between a concurrency
// of 1 or more.
func MaxWriterConcurrency(concurrency int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.Experimental.MaxWriterConcurrency = concurrency
		return nil
	}
}

// MaxOpenFiles sets the maximum number of files an engine should open.
func MaxOpenFiles(count int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.MaxOpenFiles = count
		return nil
	}

}

// CacheSize configures the size of the block cache. Note that this option is
// ignored if Caches() is also used.
func CacheSize(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.CacheSize = size
		return nil
	}
}

// Caches sets the block and file caches. Useful when multiple stores share
// the same caches.
func Caches(cache *pebble.Cache, fileCache *pebble.FileCache) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.Cache = cache
		cfg.opts.FileCache = fileCache
		return nil
	}
}

// BallastSize sets the amount reserved by a ballast file for manual
// out-of-disk recovery.
func BallastSize(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.ballastSize = size
		return nil
	}
}

// SharedStorage enables use of shared storage (experimental).
func SharedStorage(sharedStorage cloud.ExternalStorage) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.sharedStorage = sharedStorage
		if cfg.sharedStorage != nil && cfg.opts.FormatMajorVersion < pebble.FormatMinForSharedObjects {
			cfg.opts.FormatMajorVersion = pebble.FormatMinForSharedObjects
		}
		return nil
	}
}

// SecondaryCache enables use of a secondary cache to store shared objects.
func SecondaryCache(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.Experimental.SecondaryCacheSizeBytes = size
		return nil
	}
}

// RemoteStorageFactory enables use of remote storage (experimental).
func RemoteStorageFactory(accessor *cloud.EarlyBootExternalStorageAccessor) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.remoteStorageFactory = accessor
		return nil
	}
}

// MaxConcurrentCompactions configures the maximum number of concurrent
// compactions an Engine will execute.
func MaxConcurrentCompactions(n int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.MaxConcurrentCompactions = func() int { return n }
		return nil
	}
}

// MaxConcurrentDownloads configures the maximum number of concurrent
// download compactions an Engine will execute.
func MaxConcurrentDownloads(n int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.MaxConcurrentDownloads = func() int { return n }
		return nil
	}
}

// LBaseMaxBytes configures the maximum number of bytes for LBase.
func LBaseMaxBytes(v int64) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.opts.LBaseMaxBytes = v
		return nil
	}
}

func noopConfigOption(*engineConfig) error {
	return nil
}

func errConfigOption(err error) func(*engineConfig) error {
	return func(*engineConfig) error { return err }
}

func makeExternalWALDir(
	engineCfg *engineConfig,
	externalDir storagepb.ExternalPath,
	defaultFS vfs.FS,
	diskWriteStats disk.WriteStatsManager,
) (wal.Dir, error) {
	// If the store is encrypted, we require that all the WAL failover dirs also
	// be encrypted so that the user doesn't accidentally leak data unencrypted
	// onto the filesystem.
	if engineCfg.env.Encryption != nil && externalDir.Encryption == nil {
		return wal.Dir{}, errors.Newf("must provide --enterprise-encryption flag for %q, used as WAL failover path for encrypted store %q",
			externalDir.Path, engineCfg.env.Dir)
	}
	if engineCfg.env.Encryption == nil && externalDir.Encryption != nil {
		return wal.Dir{}, errors.Newf("must provide --enterprise-encryption flag for store %q, specified WAL failover path %q is encrypted",
			engineCfg.env.Dir, externalDir.Path)
	}
	env, err := fs.InitEnv(context.Background(), defaultFS, externalDir.Path, fs.EnvConfig{
		RW:                engineCfg.env.RWMode(),
		EncryptionOptions: externalDir.Encryption,
	}, diskWriteStats)
	if err != nil {
		return wal.Dir{}, err
	}
	engineCfg.afterClose = append(engineCfg.afterClose, env.Close)
	return wal.Dir{
		FS:      env,
		Dirname: externalDir.Path,
	}, nil
}

// WALFailover configures automatic failover of the engine's write-ahead log to
// another volume in the event the WAL becomes blocked on a write that does not
// complete within a reasonable duration.
func WALFailover(
	walCfg storagepb.WALFailover,
	storeEnvs fs.Envs,
	defaultFS vfs.FS,
	diskWriteStats disk.WriteStatsManager,
) ConfigOption {
	// The set of options available in single-store versus multi-store
	// configurations vary. This is in part due to the need to store the multiple
	// stores' WALs separately. When WALFailoverExplicitPath is provided, we have
	// no stable store identifier available to disambiguate the WALs of multiple
	// stores. Note that the store ID is not known when a store is first opened.
	if len(storeEnvs) == 1 {
		switch walCfg.Mode {
		case storagepb.WALFailoverMode_DEFAULT, storagepb.WALFailoverMode_AMONG_STORES:
			return noopConfigOption
		case storagepb.WALFailoverMode_DISABLED:
			// Check if the user provided an explicit previous path. If they did, they
			// were previously using WALFailoverExplicitPath and are now disabling it.
			// We need to add the explicilt path to WALRecoveryDirs.
			if walCfg.PrevPath.IsSet() {
				return func(cfg *engineConfig) error {
					walDir, err := makeExternalWALDir(cfg, walCfg.PrevPath, defaultFS, diskWriteStats)
					if err != nil {
						return err
					}
					cfg.opts.WALRecoveryDirs = append(cfg.opts.WALRecoveryDirs, walDir)
					return nil
				}
			}
			// No PrevPath was provided. The user may be simply expressing their
			// intent to not run with WAL failover, regardless of any future default
			// values. If WAL failover was previously enabled, Open will error when it
			// notices the OPTIONS file encodes a WAL failover secondary that was not
			// provided to Options.WALRecoveryDirs.
			return noopConfigOption
		case storagepb.WALFailoverMode_EXPLICIT_PATH:
			// The user has provided an explicit path to which we should fail over WALs.
			return func(cfg *engineConfig) error {
				walDir, err := makeExternalWALDir(cfg, walCfg.Path, defaultFS, diskWriteStats)
				if err != nil {
					return err
				}
				cfg.opts.WALFailover = makePebbleWALFailoverOptsForDir(cfg.settings, walDir)
				if walCfg.PrevPath.IsSet() {
					walDir, err := makeExternalWALDir(cfg, walCfg.PrevPath, defaultFS, diskWriteStats)
					if err != nil {
						return err
					}
					cfg.opts.WALRecoveryDirs = append(cfg.opts.WALRecoveryDirs, walDir)
				}
				return nil
			}
		default:
			panic("unreachable")
		}
	}

	switch walCfg.Mode {
	case storagepb.WALFailoverMode_DEFAULT:
		// If the user specified no WAL failover setting, we default to disabling WAL
		// failover and assume that the previous process did not have WAL failover
		// enabled (so there's no need to populate Options.WALRecoveryDirs). If an
		// operator had WAL failover enabled and now wants to disable it, they must
		// explicitly set --wal-failover=disabled for the next process.
		return noopConfigOption
	case storagepb.WALFailoverMode_DISABLED:
		// Check if the user provided an explicit previous path; that's unsupported
		// in multi-store configurations.
		if walCfg.PrevPath.IsSet() {
			return errConfigOption(errors.Newf("storage: cannot use explicit prev_path --wal-failover option with multiple stores"))
		}
		// No PrevPath was provided, implying that the user previously was using
		// WALFailoverAmongStores.

		// Fallthrough
	case storagepb.WALFailoverMode_EXPLICIT_PATH:
		// Not supported for multi-store configurations.
		return errConfigOption(errors.Newf("storage: cannot use explicit path --wal-failover option with multiple stores"))
	case storagepb.WALFailoverMode_AMONG_STORES:
		// Fallthrough
	default:
		panic("unreachable")
	}

	// Either
	// 1. mode == WALFailoverAmongStores
	//   or
	// 2. mode == WALFailoverDisabled and the user previously was using
	//    WALFailoverAmongStores, so we should build the deterministic store pairing
	//    to determine which WALRecoveryDirs to pass to which engines.
	//
	// For each store, we need to determine which store is its secondary for the
	// purpose of WALs. Even if failover is disabled, it's possible that it wasn't
	// when the previous process ran, and the secondary's wal dir may have WALs
	// that need to be replayed.
	//
	// To assign secondaries, we sort by path and dictate that the next store in
	// the slice is the secondary. Note that in-memory stores may not have unique
	// paths, in which case we fall back to using the ordering of the store flags
	// (which falls out of the use of a stable sort).
	//
	// TODO(jackson): Using the path is a simple way to assign secondaries, but
	// it's not resilient to changing between absolute and relative paths,
	// introducing symlinks, etc. Since we have the fs.Envs already available, we
	// could peek into the data directories, find the most recent OPTIONS file and
	// parse out the previous secondary if any. If we had device nos and inodes
	// available, we could deterministically sort by those instead.
	sortedEnvs := slices.Clone(storeEnvs)
	slices.SortStableFunc(sortedEnvs, func(a, b *fs.Env) int {
		return cmp.Compare(a.Dir, b.Dir)
	})

	indexOfEnv := func(e *fs.Env) (int, bool) {
		for i := range sortedEnvs {
			if sortedEnvs[i] == e {
				return i, true
			}
		}
		return 0, false
	}
	return func(cfg *engineConfig) error {
		// Find the Env being opened in the slice of sorted envs.
		idx, ok := indexOfEnv(cfg.env)
		if !ok {
			panic(errors.AssertionFailedf("storage: opening a store with an unrecognized filesystem Env (dir=%s)", cfg.env.Dir))
		}
		// Ensure that either all the stores are encrypted, or none are.
		for _, storeEnv := range sortedEnvs {
			if (storeEnv.Encryption == nil) != (cfg.env.Encryption == nil) {
				return errors.Newf("storage: must provide --enterprise-encryption flag for all stores or none if using WAL failover")
			}
		}

		failoverIdx := (idx + 1) % len(sortedEnvs)
		secondaryEnv := sortedEnvs[failoverIdx]

		// Ref once to ensure the secondary Env isn't closed before this Engine has
		// been closed if the secondary's corresponding Engine is closed first.
		secondaryEnv.Ref()
		cfg.afterClose = append(cfg.afterClose, secondaryEnv.Close)

		secondary := wal.Dir{
			FS: secondaryEnv,
			// Use auxiliary/wals-among-stores within the other stores directory.
			Dirname: secondaryEnv.PathJoin(secondaryEnv.Dir, base.AuxiliaryDir, "wals-among-stores"),
		}
		if walCfg.Mode == storagepb.WALFailoverMode_AMONG_STORES {
			cfg.opts.WALFailover = makePebbleWALFailoverOptsForDir(cfg.settings, secondary)
			return nil
		}
		// mode == WALFailoverDisabled
		cfg.opts.WALRecoveryDirs = append(cfg.opts.WALRecoveryDirs, secondary)
		return nil
	}
}

func makePebbleWALFailoverOptsForDir(
	settings *cluster.Settings, dir wal.Dir,
) *pebble.WALFailoverOptions {
	return &pebble.WALFailoverOptions{
		Secondary: dir,
		FailoverOptions: wal.FailoverOptions{
			// Leave most the options to their defaults, but
			// UnhealthyOperationLatencyThreshold should be pulled from the
			// cluster setting.
			UnhealthyOperationLatencyThreshold: func() (time.Duration, bool) {
				return walFailoverUnhealthyOpThreshold.Get(&settings.SV), true
			},
		},
	}
}

// PebbleOptions contains Pebble-specific options in the same format as a
// Pebble OPTIONS file. For example:
// [Options]
// delete_range_flush_delay=2s
// flush_split_bytes=4096
func PebbleOptions(pebbleOptions string, parseHooks *pebble.ParseHooks) ConfigOption {
	return func(cfg *engineConfig) error {
		return cfg.opts.Parse(pebbleOptions, parseHooks)
	}
}

// DiskMonitor configures a monitor to track disk stats.
func DiskMonitor(diskMonitor *disk.Monitor) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.diskMonitor = diskMonitor
		return nil
	}
}

// DiskWriteStatsCollector configures an engine to categorically track disk write stats.
func DiskWriteStatsCollector(dsc *vfs.DiskWriteStatsCollector) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.DiskWriteStatsCollector = dsc
		return nil
	}
}

// BlockConcurrencyLimitDivisor sets the divisor used to calculate the block
// load concurrency limit: the current value of the BlockLoadConcurrencyLimit
// setting divided by the divisor. It should be set to the number of stores.
//
// A value of 0 disables the limiter.
func BlockConcurrencyLimitDivisor(d int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.blockConcurrencyLimitDivisor = d
		return nil
	}
}

// If enables the given option if enable is true.
func If(enable bool, opt ConfigOption) ConfigOption {
	if enable {
		return opt
	}
	return func(cfg *engineConfig) error { return nil }
}

// InMemory re-exports fs.InMemory.
//
// TODO(jackson): Update callers to use fs.InMemory directly.
var InMemory = fs.InMemory

// Open opens a new Pebble storage engine, reading and writing data to the
// provided fs.Env, configured with the provided options.
//
// If successful, the returned Engine takes ownership over the provided fs.Env's
// reference. When the Engine is closed, the fs.Env is closed once too. If the
// Env must be retained beyond the Engine's lifetime, the caller should Ref() it
// first.
func Open(
	ctx context.Context, env *fs.Env, settings *cluster.Settings, opts ...ConfigOption,
) (*Pebble, error) {
	if settings == nil {
		return nil, errors.AssertionFailedf("Open requires non-nil *cluster.Settings")
	}
	var cfg engineConfig
	cfg.env = env
	cfg.settings = settings
	cfg.opts = DefaultPebbleOptions()
	cfg.opts.FS = env
	cfg.opts.ReadOnly = env.IsReadOnly()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			// Run after-close hooks if there are any. This ensures we
			// release any references to fs.Envs that would've been held by
			// the engine if it had been successfully opened.
			for _, f := range cfg.afterClose {
				f()
			}
			return nil, err
		}
	}
	p, err := newPebble(ctx, cfg)
	if err != nil {
		// Run after-close hooks if there are any. This ensures we
		// release any references to fs.Envs that would've been held by
		// the engine if it had been successfully opened.
		for _, f := range cfg.afterClose {
			f()
		}
		return nil, err
	}
	return p, nil
}
