// Copyright 2021 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	return nil
}

// ForStickyEngineTesting is similar to ForTesting but leaves separated
// intents as enabled since we cannot ensure consistency in the test setup
// between what the KV layer thinks and what the engine does in terms of
// writing separated intents. Since our optimizations are for the case where
// we know there are only separated intents, this sidesteps any test issues
// due to inconsistencies.
var ForStickyEngineTesting ConfigOption = func(cfg *engineConfig) error {
	return nil
}

// Attributes configures the engine's attributes.
func Attributes(attrs roachpb.Attributes) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Attrs = attrs
		return nil
	}
}

// MaxSize sets the intended maximum store size. MaxSize is used for
// calculating free space and making rebalancing decisions.
func MaxSize(size int64) ConfigOption {
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

// MaxConcurrentCompactions configures the maximum number of concurrent
// compactions an Engine will execute.
func MaxConcurrentCompactions(n int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Opts.MaxConcurrentCompactions = func() int { return n }
		return nil
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

// LatestReleaseFormatMajorVersion opens the database already upgraded to the
// latest release's format major version.
var LatestReleaseFormatMajorVersion ConfigOption = func(cfg *engineConfig) error {
	// TODO(jackson): Tie the below to the mapping in SetMinVersion.
	cfg.PebbleConfig.Opts.FormatMajorVersion = pebble.FormatPrePebblev1Marked // v22.2
	return nil
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
	if cfg.cacheSize != nil {
		cfg.Opts.Cache = pebble.NewCache(*cfg.cacheSize)
		defer cfg.Opts.Cache.Unref()
	}
	p, err := NewPebble(ctx, cfg.PebbleConfig)
	if err != nil {
		return nil, err
	}
	// Set the active cluster version, ensuring the engine's format
	// major version is ratcheted sufficiently high to match the
	// settings cluster version.
	if v := settings.Version.ActiveVersionOrEmpty(ctx).Version; v != (roachpb.Version{}) {
		if err := p.SetMinVersion(v); err != nil {
			p.Close()
			return nil, err
		}
	}
	return p, nil
}
