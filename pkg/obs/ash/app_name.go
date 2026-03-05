// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// maxAppNameMapSize is the maximum number of entries allowed in the
// app name map. When this limit is reached, new entries are dropped
// and a warning is logged periodically.
const maxAppNameMapSize = 10000

// appNameMap stores the mapping from app name ID (uint64) to app name
// string. This map is node-local and is populated when app names are
// first seen. syncutil.Map is used because the access pattern is
// read-heavy (sampler reads on every tick) with infrequent writes
// (only when a new app name is first seen).
var appNameMap syncutil.Map[uint64, string]

// appNameMapSize tracks the number of entries in appNameMap. Used to
// enforce maxAppNameMapSize without iterating the syncutil.Map.
var appNameMapSize atomic.Int64

// appNameMapFullLog rate-limits the warning logged when the map is at
// capacity.
var appNameMapFullLog = log.Every(time.Minute)

// hashAppName computes a uint64 FNV hash of the app name string.
func hashAppName(appName string) uint64 {
	fnv := util.MakeFNV64()
	for i := 0; i < len(appName); i++ {
		fnv.Add(uint64(appName[i]))
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
	if _, ok := appNameMap.Load(id); ok {
		return id
	}
	storeAppNameMapping(id, appName)
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
	storeAppNameMapping(id, name)
}

// storeAppNameMapping is the slow path for storing a new app name
// mapping. It is split out from the callers so that taking &name does
// not cause the string parameter to escape to the heap on the fast
// path (Load hit).
//
//go:noinline
func storeAppNameMapping(id uint64, name string) {
	if appNameMapSize.Load() >= maxAppNameMapSize {
		logAppNameMapFull()
		return
	}
	if _, loaded := appNameMap.LoadOrStore(id, &name); !loaded {
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
	return *val, true
}

// GetAllAppNameMappings returns a copy of all app name ID to string
// mappings. Used by the AppNameMappings RPC to serve the full map
// to requesting nodes.
func GetAllAppNameMappings() map[uint64]string {
	result := make(map[uint64]string)
	appNameMap.Range(func(key uint64, value *string) bool {
		result[key] = *value
		return true
	})
	return result
}
