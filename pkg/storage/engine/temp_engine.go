// Copyright 2017 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// TestStorageEngine represents an engine type for testing.
var TestStorageEngine enginepb.EngineType

func init() {
	_ = TestStorageEngine.Set(envutil.EnvOrDefaultString("COCKROACH_TEST_STORAGE_ENGINE", "rocksdb"))
}

// NewTempEngine creates a new engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewTempEngine(
	engine enginepb.EngineType, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, error) {
	if engine == enginepb.EngineTypePebble {
		return NewPebbleTempEngine(tempStorage, storeSpec)
	}

	return NewRocksDBTempEngine(tempStorage, storeSpec)
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

// NewRocksDBTempEngine creates a new RocksDB engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewRocksDBTempEngine(
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
		db := newRocksDBInMem(roachpb.Attributes{} /* attrs */, 0 /* cacheSize */).RocksDB
		return &rocksDBTempEngine{db: db}, nil
	}

	cfg := RocksDBConfig{
		StorageConfig: base.StorageConfig{
			Attrs: roachpb.Attributes{},
			Dir:   tempStorage.Path,
			// MaxSize doesn't matter for temp storage - it's not enforced in any
			// way.
			MaxSize:         0,
			UseFileRegistry: storeSpec.UseFileRegistry,
			ExtraOptions:    storeSpec.ExtraOptions,
		},
		MaxOpenFiles: 128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	defer rocksDBCache.Release()
	db, err := NewRocksDB(cfg, rocksDBCache)
	if err != nil {
		return nil, err
	}

	return &rocksDBTempEngine{db: db}, nil
}

type pebbleTempEngine struct {
	db *pebble.DB
}

// Close implements the diskmap.Factory interface.
func (r *pebbleTempEngine) Close() {
	err := r.db.Close()
	if err != nil {
		log.Fatal(context.TODO(), err)
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
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, error) {
	// Default options as copied over from pebble/cmd/pebble/db.go
	opts := DefaultPebbleOptions()
	// Pebble doesn't currently support 0-size caches, so use a 128MB cache for
	// now.
	opts.Cache = pebble.NewCache(128 << 20)
	// The Pebble temp engine does not use MVCC Encoding. Instead, the
	// caller-provided key is used as-is (with the prefix prepended). See
	// pebbleMap.makeKey and pebbleMap.makeKeyWithSequence on how this works.
	// Use the default bytes.Compare-like comparer.
	opts.Comparer = pebble.DefaultComparer
	opts.DisableWAL = true
	opts.TablePropertyCollectors = nil

	path := tempStorage.Path
	if tempStorage.InMemory {
		opts.FS = vfs.NewMem()
		path = ""
	}

	p, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &pebbleTempEngine{db: p}, nil
}
