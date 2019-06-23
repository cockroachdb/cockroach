// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// InMem wraps RocksDB and configures it for in-memory only storage.
type InMem struct {
	*RocksDB
}

// NewInMem allocates and returns a new, opened InMem engine.
// The caller must call the engine's Close method when the engine is no longer
// needed.
//
// FIXME(tschottdorf): make the signature similar to NewRocksDB (require a cfg).
func NewInMem(attrs roachpb.Attributes, cacheSize int64) InMem {
	cache := NewRocksDBCache(cacheSize)
	// The cache starts out with a refcount of one, and creating the engine
	// from it adds another refcount, at which point we release one of them.
	defer cache.Release()

	// TODO(bdarnell): The hard-coded 512 MiB is wrong; see
	// https://github.com/cockroachdb/cockroach/issues/16750
	rdb, err := newMemRocksDB(attrs, cache, 512<<20 /* MaxSizeBytes: 512 MiB */)
	if err != nil {
		panic(err)
	}
	db := InMem{RocksDB: rdb}
	return db
}

var _ Engine = InMem{}
