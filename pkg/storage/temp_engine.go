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
	"io"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// NewTempEngine creates a new engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewTempEngine(
	ctx context.Context, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, vfs.FS, error) {
	return NewPebbleTempEngine(ctx, tempStorage, storeSpec)
}

type pebbleTempEngine struct {
	db     *pebble.DB
	closer io.Closer
}

// Close implements the diskmap.Factory interface.
func (r *pebbleTempEngine) Close() {
	if err := r.db.Close(); err != nil {
		log.Fatalf(context.TODO(), "%v", err)
	}
	if err := r.closer.Close(); err != nil {
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
) (diskmap.Factory, vfs.FS, error) {
	return newPebbleTempEngine(ctx, tempStorage, storeSpec)
}

func newPebbleTempEngine(
	ctx context.Context, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (*pebbleTempEngine, vfs.FS, error) {
	var loc Location
	var cacheSize int64 = 128 << 20 // 128 MiB, arbitrary, but not "too big"
	if tempStorage.InMemory {
		cacheSize = 8 << 20 // 8 MiB, smaller for in-memory, still non-zero
		loc = InMemory()
	} else {
		loc = Filesystem(tempStorage.Path)
	}

	p, err := Open(ctx, loc,
		tempStorage.Settings,
		CacheSize(cacheSize),
		func(cfg *engineConfig) error {
			cfg.EncryptionOptions = storeSpec.EncryptionOptions

			// The Pebble temp engine does not use MVCC Encoding. Instead, the
			// caller-provided key is used as-is (with the prefix prepended). See
			// pebbleMap.makeKey and pebbleMap.makeKeyWithSequence on how this works.
			// Use the default bytes.Compare-like comparer.
			cfg.Opts.Comparer = pebble.DefaultComparer
			cfg.Opts.DisableWAL = true
			cfg.Opts.Experimental.KeyValidationFunc = nil
			cfg.Opts.BlockPropertyCollectors = nil
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
		db:     p.db,
		closer: p.closer,
	}, p, nil
}
