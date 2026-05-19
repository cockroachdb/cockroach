// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// ExecutionCacheEnabled gates all execution-cache reads and writes. When
// false, PutExecution returns 0 and GetExecution returns the zero value.
var ExecutionCacheEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"obs.execution_cache.enabled",
	"if set, ASH execution-attribute enrichment is enabled",
	true,
)

// ExecutionCacheLimit caps the number of entries in the per-node
// execution cache. Eviction is FIFO when this limit is exceeded.
var ExecutionCacheLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"obs.execution_cache.limit",
	"maximum number of entries in the ASH execution cache",
	50000,
	settings.NonNegativeInt,
)

// globalExecutionCache is a process-wide singleton. We use a single
// cache across tenants for the same reason as appNameMap (see comments
// in app_name.go).
var globalExecutionCache atomic.Pointer[ExecutionCache]

// getGlobalExecutionCache lazily initializes the singleton at the limit
// defined by the cluster setting. The size is fixed at first use;
// changing the setting at runtime takes effect for newly-built caches
// only. (POC limitation; sufficient for a demo.)
func getGlobalExecutionCache() *ExecutionCache {
	if c := globalExecutionCache.Load(); c != nil {
		return c
	}
	c := NewExecutionCache(int(ExecutionCacheLimit.Default()))
	if !globalExecutionCache.CompareAndSwap(nil, c) {
		return globalExecutionCache.Load()
	}
	return c
}
