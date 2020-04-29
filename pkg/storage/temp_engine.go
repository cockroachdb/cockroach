// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// NewTempEngine creates a new engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewTempEngine(
	ctx context.Context,
	engine enginepb.EngineType,
	tempStorage base.TempStorageConfig,
	storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	switch engine {
	case enginepb.EngineTypeTeePebbleRocksDB:
		fallthrough
	case enginepb.EngineTypeDefault, enginepb.EngineTypePebble:
		return NewPebbleTempEngine(ctx, tempStorage, storeSpec)
	case enginepb.EngineTypeRocksDB:
		return NewRocksDBTempEngine(tempStorage, storeSpec)
	}
	panic(fmt.Sprintf("unknown engine type: %d", engine))
}

type rocksDBTempEngine struct {
	db *RocksDB
}

// Close implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) Close() {
	r.db.Close()
}

// NewSortedDiskMap implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
	return newRocksDBMap(r.db, false /* allowDuplications */)
}

// NewSortedDiskMultiMap implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return newRocksDBMap(r.db, true /* allowDuplicates */)
}

// storageConfigFromTempStorageConfigAndStoreSpec creates a base.StorageConfig
// used by both the RocksDB and Pebble temp engines from the given arguments.
func storageConfigFromTempStorageConfigAndStoreSpec(
	config base.TempStorageConfig, spec base.StoreSpec,
) base.StorageConfig {
	return base.StorageConfig{
		Attrs: roachpb.Attributes{},
		Dir:   config.Path,
		// MaxSize doesn't matter for temp storage - it's not enforced in any way.
		MaxSize:         0,
		UseFileRegistry: spec.UseFileRegistry,
		ExtraOptions:    spec.ExtraOptions,
	}
}

// NewRocksDBTempEngine creates a new RocksDB engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewRocksDBTempEngine(
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
		db := newRocksDBInMem(roachpb.Attributes{} /* attrs */, 0 /* cacheSize */)
		return &rocksDBTempEngine{db: db}, db, nil
	}

	cfg := RocksDBConfig{
		StorageConfig: storageConfigFromTempStorageConfigAndStoreSpec(tempStorage, storeSpec),
		MaxOpenFiles:  128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	defer rocksDBCache.Release()
	db, err := NewRocksDB(cfg, rocksDBCache)
	if err != nil {
		return nil, nil, err
	}

	return &rocksDBTempEngine{db: db}, db, nil
}

type pebbleTempEngine struct {
	db *pebble.DB
}

// Close implements the diskmap.Factory interface.
func (r *pebbleTempEngine) Close() {
	err := r.db.Close()
	if err != nil {
		log.Fatalf(context.TODO(), "%v", err)
	}
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
	ctx context.Context, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	return newPebbleTempEngine(ctx, tempStorage, storeSpec)
}

var pebbleTempEngineTablePropertyCollectors = []func() pebble.TablePropertyCollector{
	func() pebble.TablePropertyCollector { return &pebbleDeleteRangeCollector{} },
}

func newPebbleTempEngine(
	ctx context.Context, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (*pebbleTempEngine, fs.FS, error) {
	// Default options as copied over from pebble/cmd/pebble/db.go
	opts := DefaultPebbleOptions()
	// Pebble doesn't currently support 0-size caches, so use a 128MB cache for
	// now.
	opts.Cache = pebble.NewCache(128 << 20)
	defer opts.Cache.Unref()

	// The Pebble temp engine does not use MVCC Encoding. Instead, the
	// caller-provided key is used as-is (with the prefix prepended). See
	// pebbleMap.makeKey and pebbleMap.makeKeyWithSequence on how this works.
	// Use the default bytes.Compare-like comparer.
	opts.Comparer = pebble.DefaultComparer
	opts.DisableWAL = true
	opts.TablePropertyCollectors = pebbleTempEngineTablePropertyCollectors

	storageConfig := storageConfigFromTempStorageConfigAndStoreSpec(tempStorage, storeSpec)
	if tempStorage.InMemory {
		opts.FS = vfs.NewMem()
		storageConfig.Dir = ""
	}

	p, err := NewPebble(
		ctx,
		PebbleConfig{
			StorageConfig: storageConfig,
			Opts:          opts,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return &pebbleTempEngine{db: p.db}, p, nil
}
