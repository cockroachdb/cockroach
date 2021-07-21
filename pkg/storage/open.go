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

// MaxOpenFiles sets the maximum number of files an engine should open.
func MaxOpenFiles(count int) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Opts.MaxOpenFiles = count
		return nil
	}

}

// Settings sets the cluster settings to use.
func Settings(settings *cluster.Settings) ConfigOption {
	return func(cfg *engineConfig) error {
		cfg.Settings = settings
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

// SettingsForTesting configures the engine's cluster settings for an engine
// used in testing. It may randomize some cluster settings to improve test
// coverage.
func SettingsForTesting() ConfigOption {
	return Settings(makeRandomSettingsForSeparatedIntents())
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
		// fs is left nil intentionally, so that it will be left as the
		// default of vfs.Default wrapped in vfs.WithDiskHealthChecks
		// (initialized by DefaultPebbleOptions).
		// TODO(jackson): Refactor to make it harder to accidentially remove
		// disk health checks by setting your own VFS in a call to NewPebble.
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
func Open(ctx context.Context, loc Location, opts ...ConfigOption) (*Pebble, error) {
	var cfg engineConfig
	cfg.Dir = loc.dir
	cfg.Opts = DefaultPebbleOptions()
	if loc.fs != nil {
		cfg.Opts.FS = loc.fs
	}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.cacheSize != nil {
		cfg.Opts.Cache = pebble.NewCache(*cfg.cacheSize)
		defer cfg.Opts.Cache.Unref()
	}
	return NewPebble(ctx, cfg.PebbleConfig)
}
