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
	"enable ASH execution-attribute enrichment",
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

// globalExecutionCache is a process-wide singleton. A single
// cross-tenant cache is acceptable because (1) shared-process
// multi-tenancy with many in-process tenants is not a production
// configuration, and (2) the ASH sampler is itself process-wide and
// would otherwise have to dial each tenant's server to resolve attrs.
var globalExecutionCache atomic.Pointer[ExecutionCache]

// getGlobalExecutionCache lazily initializes the singleton at the
// registered default of ExecutionCacheLimit. Cluster-setting overrides
// of the limit do not resize the cache at runtime (POC limitation).
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
