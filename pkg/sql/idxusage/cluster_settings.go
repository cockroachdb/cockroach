// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxusage

import "github.com/cockroachdb/cockroach/pkg/settings"

// Enable determines whether to collect per-index usage statistics.
var Enable = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.metrics.index_usage_stats.enabled", "collect per index usage statistics", true, /* defaultValue */
	settings.WithPublic)
