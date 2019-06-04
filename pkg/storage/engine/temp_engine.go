// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package engine

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// NewTempEngine creates a new engine for DistSQL processors to use when the
// working set is larger than can be stored in memory.
func NewTempEngine(
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (MapProvidingEngine, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
		return NewInMem(roachpb.Attributes{} /* attrs */, 0 /* cacheSize */), nil
	}

	rocksDBCfg := RocksDBConfig{
		Attrs: roachpb.Attributes{},
		Dir:   tempStorage.Path,
		// MaxSizeBytes doesn't matter for temp storage - it's not
		// enforced in any way.
		MaxSizeBytes:    0,
		MaxOpenFiles:    128, // TODO(arjun): Revisit this.
		UseFileRegistry: storeSpec.UseFileRegistry,
		ExtraOptions:    storeSpec.ExtraOptions,
	}
	rocksDBCache := NewRocksDBCache(0)
	rocksdb, err := NewRocksDB(rocksDBCfg, rocksDBCache)
	if err != nil {
		return nil, err
	}

	return rocksdb, nil
}
