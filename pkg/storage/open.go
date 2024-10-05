// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
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

// ReadOnly configures an engine to be opened in read-only mode.
var ReadOnly ConfigOption = func(cfg *engineConfig) error {
	cfg.Opts.ReadOnly = true
	return nil
}

// MustExist configures an engine to error on Open if the target directory
// does not contain an initialized store.
var MustExist ConfigOption = func(cfg *engineConfig) error {
	cfg.MustExist = true
	return nil
}

// DisableAutomaticCompactions configures an engine to be opened with disabled
// automatic compactions. Used primarily for debugCompactCmd.
var DisableAutomaticCompactions ConfigOption = func(cfg *engineConfig) error {
	cfg.Opts.DisableAutomaticCompactions = true
	return nil
}

// ForceWriterParallelism configures an engine to be opened with disabled
// automatic compactions. Used primarily for debugCompactCmd.
var ForceWriterParallelism ConfigOption = func(cfg *engineConfig) error {
	cfg.Opts.Experimental.ForceWriterParallelism = true
	return nil
}

// ForTesting configures the engine for use in testing. It may randomize some
// config options to improve test coverage.
var ForTesting ConfigOption = func(cfg *engineConfig) error {
	cfg.onClose = append(cfg.onClose, func(p *Pebble) {
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
		cfg.Attrs = attrs
		return nil
	}
}

// MaxSizeBytes ets the intended maximum store size as an absolute byte
// value. MaxSizeBytes is used for calculating free space and making rebalancing
// decisions.
func MaxSizeBytes(size int64) ConfigOption {
	return maxSize(base.StoreSize{Bytes: size})
}

// MaxSizePercent ets the intended maximum store size as the specified percentage
// of total capacity. MaxSizePercent is used for calculating free space and making
// rebalancing decisions.
func MaxSizePercent(percent float64) ConfigOption {
	return maxSize(base.StoreSize{Percent: percent})
}

// maxSize sets the intended maximum store size. MaxSize is used for
// calculating free space and making rebalancing decisions. Either an
// absolute size or a percentage of total capacity can be specified;
// if both are specified, the percentage is used.
func maxSize(size base.StoreSize) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.MaxSize = size
		return nil
	}
}

// BlockSize sets the engine block size, primarily for testing purposes.
func BlockSize(size int) ConfigOption {
	return func(cfg *engineConfig) error {
		for i := range cfg.Opts.Levels {
			cfg.Opts.Levels[i].BlockSize = size
			cfg.Opts.Levels[i].IndexBlockSize = size
		}
		return nil
	}
}

// TargetFileSize sets the target file size across all levels of the LSM,
// primarily for testing purposes.
func TargetFileSize(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		for i := range cfg.Opts.Levels {
			cfg.Opts.Levels[i].TargetFileSize = size
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
		cfg.Opts.Experimental.MaxWriterConcurrency = concurrency
		return nil
	}
}

// MaxOpenFiles sets the maximum number of files an engine should open.
func MaxOpenFiles(count int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Opts.MaxOpenFiles = count
		return nil
	}

}

// CacheSize configures the size of the block cache.
func CacheSize(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.cacheSize = &size
		return nil
	}
}

// Caches sets the block and table caches. Useful when multiple stores share
// the same caches.
func Caches(cache *pebble.Cache, tableCache *pebble.TableCache) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Opts.Cache = cache
		cfg.Opts.TableCache = tableCache
		return nil
	}
}

// BallastSize sets the amount reserved by a ballast file for manual
// out-of-disk recovery.
func BallastSize(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.BallastSize = size
		return nil
	}
}

// SharedStorage enables use of shared storage (experimental).
func SharedStorage(sharedStorage cloud.ExternalStorage) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.SharedStorage = sharedStorage
		return nil
	}
}

// SecondaryCache enables use of a secondary cache to store shared objects.
func SecondaryCache(size int64) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Opts.Experimental.SecondaryCacheSizeBytes = size
		return nil
	}
}

// RemoteStorageFactory enables use of remote storage (experimental).
func RemoteStorageFactory(accessor *cloud.ExternalStorageAccessor) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.RemoteStorageFactory = accessor
		return nil
	}
}

// MaxConcurrentCompactions configures the maximum number of concurrent
// compactions an Engine will execute.
func MaxConcurrentCompactions(n int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Opts.MaxConcurrentCompactions = func() int { return n }
		return nil
	}
}

// PebbleOptions contains Pebble-specific options in the same format as a
// Pebble OPTIONS file. For example:
// [Options]
// delete_range_flush_delay=2s
// flush_split_bytes=4096
func PebbleOptions(pebbleOptions string, parseHooks *pebble.ParseHooks) ConfigOption {
	return func(cfg *engineConfig) error {
		return cfg.Opts.Parse(pebbleOptions, parseHooks)
	}
}

// EncryptionAtRest configures an engine to use encryption-at-rest. It is used
// for configuring in-memory engines, which are used in tests. It is not safe
// to modify the given slice afterwards as it is captured by reference.
func EncryptionAtRest(encryptionOptions []byte) ConfigOption {
	return func(cfg *engineConfig) error {
		if len(encryptionOptions) > 0 {
			cfg.UseFileRegistry = true
			cfg.EncryptionOptions = encryptionOptions
		}
		return nil
	}
}

// Hook configures a hook to initialize additional storage options. It's used
// to initialize encryption-at-rest details in CCL builds.
func Hook(hookFunc func(*base.StorageConfig) error) ConfigOption {
	return func(cfg *engineConfig) error {
		if hookFunc == nil {
			return nil
		}
		return hookFunc(&cfg.PebbleConfig.StorageConfig)
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

// A Location describes where the storage engine's data will be written. A
// Location may be in-memory or on the filesystem.
type Location struct {
	dir string
	fs  vfs.FS
}

// MakeLocation constructs a Location from a directory and a vfs.FS. Typically
// callers should prefer `Filesystem` or `InMemory` rather than directly
// invoking MakeLocation.
func MakeLocation(dir string, fs vfs.FS) Location {
	return Location{dir: dir, fs: fs}
}

// Filesystem constructs a Location that instructs the storage engine to read
// and store data on the filesystem in the provided directory.
func Filesystem(dir string) Location {
	return Location{
		dir: dir,
		fs:  vfs.Default,
	}
}

// InMemory constructs a Location that instructs the storage engine to store
// data in-memory.
func InMemory() Location {
	return Location{
		dir: "",
		fs:  vfs.NewMem(),
	}
}

type engineConfig struct {
	PebbleConfig
	// cacheSize is stored separately so that we can avoid constructing the
	// PebbleConfig.Opts.Cache until the call to Open. A Cache is created with
	// a ref count of 1, so creating the Cache during execution of
	// ConfigOption makes it too easy to leak a cache.
	cacheSize *int64
}

// Open opens a new Pebble storage engine, reading and writing data to the
// provided Location, configured with the provided options.
func Open(
	ctx context.Context, loc Location, settings *cluster.Settings, opts ...ConfigOption,
) (*Pebble, error) {
	var cfg engineConfig
	cfg.Dir = loc.dir
	cfg.Settings = settings
	cfg.Opts = DefaultPebbleOptions()
	cfg.Opts.FS = loc.fs
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.cacheSize != nil && cfg.Opts.Cache == nil {
		cfg.Opts.Cache = pebble.NewCache(*cfg.cacheSize)
		defer cfg.Opts.Cache.Unref()
	}
	p, err := NewPebble(ctx, cfg.PebbleConfig)
	if err != nil {
		return nil, err
	}
	return p, nil
}
