// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package engine

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// InMem wraps RocksDB and configures it for in-memory only storage.
type InMem struct {
	*RocksDB
}

// NewInMem allocates and returns a new, opened InMem engine.
// The caller must call the engine's Close method when the engine is no longer
// needed.
func NewInMem(attrs roachpb.Attributes, cacheSize int64) InMem {
	cache := NewRocksDBCache(cacheSize)
	// The cache starts out with a refcount of one, and creating the engine
	// from it adds another refcount, at which point we release one of them.
	defer cache.Release()

	rdb, err := newMemRocksDB(attrs, cache, 512<<20 /* 512 MB */)
	if err != nil {
		panic(err)
	}
	db := InMem{RocksDB: rdb}
	return db
}

var _ Engine = InMem{}
