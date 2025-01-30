// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// NewTempEngine creates a new engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewTempEngine(
	ctx context.Context,
	tempStorage base.TempStorageConfig,
	storeSpec base.StoreSpec,
	diskWriteStats disk.WriteStatsManager,
) (diskmap.Factory, vfs.FS, error) {
	return NewPebbleTempEngine(ctx, tempStorage, storeSpec, diskWriteStats)
}

type pebbleTempEngine struct {
	db        *pebble.DB
	closeFunc func()
}

// Close implements the diskmap.Factory interface.
func (r *pebbleTempEngine) Close() {
	if err := r.db.Close(); err != nil {
		log.Fatalf(context.TODO(), "%v", err)
	}
	r.closeFunc()
}

// NewSortedDiskMap implements the diskmap.Factory interface.
func (r *pebbleTempEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
	return newPebbleMap(r.db, false /* allowDuplications */)
}

// NewSortedDiskMultiMap implements the diskmap.Factory interface.
func (r *pebbleTempEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return newPebbleMap(r.db, true /* allowDuplicates */)
}

// NewPebbleTempEngine creates a new Pebble engine for DistSQL processors to use
// when the working set is larger than can be stored in memory.
func NewPebbleTempEngine(
	ctx context.Context,
	tempStorage base.TempStorageConfig,
	storeSpec base.StoreSpec,
	diskWriteStats disk.WriteStatsManager,
) (diskmap.Factory, vfs.FS, error) {
	return newPebbleTempEngine(ctx, tempStorage, storeSpec, diskWriteStats)
}

func newPebbleTempEngine(
	ctx context.Context,
	tempStorage base.TempStorageConfig,
	storeSpec base.StoreSpec,
	diskWriteStats disk.WriteStatsManager,
) (*pebbleTempEngine, vfs.FS, error) {
	var baseFS vfs.FS
	var dir string
	var cacheSize int64 = 128 << 20 // 128 MiB, arbitrary, but not "too big"
	if tempStorage.InMemory {
		cacheSize = 8 << 20 // 8 MiB, smaller for in-memory, still non-zero
		baseFS = vfs.NewMem()
	} else {
		baseFS = vfs.Default
		dir = tempStorage.Path
	}
	env, err := fs.InitEnv(ctx, baseFS, dir, fs.EnvConfig{
		RW: fs.ReadWrite,
		// Adopt the encryption options of the provided store spec so that
		// temporary data is encrypted if the store is encrypted.
		EncryptionOptions: storeSpec.EncryptionOptions,
	}, diskWriteStats)
	if err != nil {
		return nil, nil, err
	}

	var statsCollector *vfs.DiskWriteStatsCollector
	if diskWriteStats != nil && !tempStorage.InMemory {
		statsCollector, err = diskWriteStats.GetOrCreateCollector(dir)
		if err != nil {
			return nil, nil, errors.Wrap(err, "retrieving stats collector")
		}
	}

	p, err := Open(ctx, env,
		tempStorage.Settings,
		CacheSize(cacheSize),
		func(cfg *engineConfig) error {
			// The Pebble temp engine does not use MVCC Encoding. Instead, the
			// caller-provided key is used as-is (with the prefix prepended). See
			// pebbleMap.makeKey and pebbleMap.makeKeyWithSequence on how this works.
			// Use the default bytes.Compare-like comparer.
			cfg.opts.Comparer = pebble.DefaultComparer
			cfg.opts.KeySchemas = nil
			cfg.opts.KeySchema = ""
			cfg.opts.DisableWAL = true
			cfg.opts.Experimental.UserKeyCategories = pebble.UserKeyCategories{}
			cfg.opts.BlockPropertyCollectors = nil
			cfg.opts.EnableSQLRowSpillMetrics = true
			cfg.DiskWriteStatsCollector = statsCollector
			return nil
		},
	)
	if err != nil {
		return nil, nil, err
	}

	// Set store ID for the pebble engine. We are not using shared storage for
	// temp stores so this cannot error out.
	_ = p.SetStoreID(ctx, base.TempStoreID)
	return &pebbleTempEngine{
		db:        p.db,
		closeFunc: env.Close,
	}, env, nil
}
