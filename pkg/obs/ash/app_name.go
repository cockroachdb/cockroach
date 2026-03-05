// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// maxAppNameMapSize is the maximum number of entries allowed in the
// app name map. When this limit is reached, new entries are dropped
// and a warning is logged periodically.
const maxAppNameMapSize = 10000

// appNameMap stores the mapping from app name ID (uint64) to app name
// string. This map is node-local and is populated when app names are
// first seen. sync.Map is used because the access pattern is
// read-heavy (sampler reads on every tick) with infrequent writes
// (only when a new app name is first seen).
var appNameMap sync.Map // uint64 -> string

// appNameMapSize tracks the number of entries in appNameMap. Used to
// enforce maxAppNameMapSize without iterating the sync.Map.
var appNameMapSize atomic.Int64

// appNameMapFullLog rate-limits the warning logged when the map is at
// capacity.
var appNameMapFullLog = log.Every(time.Minute)

// hashAppName computes a uint64 FNV hash of the app name string.
func hashAppName(appName string) uint64 {
	fnv := util.MakeFNV64()
	for _, b := range []byte(appName) {
		fnv.Add(uint64(b))
	}
	return fnv.Sum()
}

// GetOrStoreAppNameID hashes the app name string to an ID and stores
// the mapping in the node-local map if it doesn't already exist.
// Returns the ID. This is the primary entry point for statement
// execution paths that know the app name string.
func GetOrStoreAppNameID(appName string) uint64 {
	if appName == "" {
		return 0
	}
	id := hashAppName(appName)
	// Use Load first to avoid the allocation from boxing appName
	// into interface{} on every call to LoadOrStore. The common
	// case (app name already seen) hits Load and returns without
	// allocating.
	if _, ok := appNameMap.Load(id); ok {
		return id
	}
	if appNameMapSize.Load() >= maxAppNameMapSize {
		logAppNameMapFull()
		return id
	}
	if _, loaded := appNameMap.LoadOrStore(id, appName); !loaded {
		appNameMapSize.Add(1)
	}
	return id
}

// StoreAppNameMapping stores a single app name ID to string mapping.
// This is used when the ID is already known (e.g., received in a
// SetupFlowRequest from a remote node) and we only need to populate
// the local map without re-hashing the app name.
func StoreAppNameMapping(id uint64, name string) {
	if id == 0 || name == "" {
		return
	}
	if _, ok := appNameMap.Load(id); ok {
		return
	}
	if appNameMapSize.Load() >= maxAppNameMapSize {
		logAppNameMapFull()
		return
	}
	if _, loaded := appNameMap.LoadOrStore(id, name); !loaded {
		appNameMapSize.Add(1)
	}
}

// logAppNameMapFull logs a rate-limited warning when the app name map
// is at capacity.
func logAppNameMapFull() {
	if appNameMapFullLog.ShouldLog() {
		log.Ops.Warningf(
			context.Background(),
			"ASH app name map is at capacity (%d entries); "+
				"new app names will not be recorded",
			maxAppNameMapSize,
		)
	}
}

// GetAppName looks up the app name string for the given ID in the
// node-local map. Returns the name and true if found, or empty
// string and false on cache miss.
func GetAppName(id uint64) (string, bool) {
	if id == 0 {
		return "", false
	}
	val, ok := appNameMap.Load(id)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// GetAllAppNameMappings returns a copy of all app name ID to string
// mappings. Used by the AppNameMappings RPC to serve the full map
// to requesting nodes.
func GetAllAppNameMappings() map[uint64]string {
	result := make(map[uint64]string)
	appNameMap.Range(func(key, value any) bool {
		result[key.(uint64)] = value.(string)
		return true
	})
	return result
}
