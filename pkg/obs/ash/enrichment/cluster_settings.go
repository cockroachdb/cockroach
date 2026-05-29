// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment

import "github.com/cockroachdb/cockroach/pkg/settings"

// Enabled gates the entire ASH enrichment subsystem. When false, both
// PutExecution and GetExecution are no-ops, no goroutines do work on
// behalf of enrichment, and the resolution RPC returns an empty
// response. Defaults to false during 26.3 rollout.
var Enabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"obs.ash.enrichment.enabled",
	"if set, the ASH enrichment subsystem records per-execution "+
		"attributes and asynchronously enriches collected samples",
	false,
	settings.WithPublic)

// CacheLimit bounds the number of per-execution entries retained in the
// enrichment cache. When the limit is exceeded, the oldest entries are
// evicted (FIFO). The cache only needs to retain entries long enough for
// downstream samplers and remote enricher RPCs to read them, so retention
// is expected to be short. Setting this to 0 disables the cache entirely
// (PutExecution and GetExecution become no-ops).
var CacheLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"obs.ash.enrichment_cache.limit",
	"maximum number of per-execution entries retained in the ASH "+
		"enrichment cache (set to 0 to disable)",
	65536,
	settings.WithPublic)
