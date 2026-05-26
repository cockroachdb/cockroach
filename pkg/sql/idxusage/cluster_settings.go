// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idxusage

import "github.com/cockroachdb/cockroach/pkg/settings"

// Enable determines whether to collect per-index usage statistics.
var Enable = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.metrics.index_usage_stats.enabled", "collect per index usage statistics", true, /* defaultValue */
	settings.WithPublic)
