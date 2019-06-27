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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
)

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

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory.
func NewTempEngine(
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
		db := NewInMem(roachpb.Attributes{} /* attrs */, 0 /* cacheSize */).RocksDB
		return &rocksDBTempEngine{db: db}, nil
	}

	cfg := RocksDBConfig{
		Attrs: roachpb.Attributes{},
		Dir:   tempStorage.Path,
		// MaxSizeBytes doesn't matter for temp storage - it's not
		// enforced in any way.
		MaxSizeBytes:    0,
		MaxOpenFiles:    128, // TODO(arjun): Revisit this.
		UseFileRegistry: storeSpec.UseFileRegistry,
		ExtraOptions:    storeSpec.ExtraOptions,
	}
	cache := NewRocksDBCache(0)
	db, err := NewRocksDB(cfg, cache)
	if err != nil {
		return nil, err
	}

	return &rocksDBTempEngine{db: db}, nil
}
