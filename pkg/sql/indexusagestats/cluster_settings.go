// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexusagestats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Enable determines whether to collect per-index usage statistics.
var Enable = settings.RegisterBoolSetting(
	"sql.metrics.index_usage_stats.enabled", "collect per index usage statistics", true, /* defaultValue */
).WithPublic()

// ClearInterval determines the interval that index usage
// statistics is being reset.
var ClearInterval = settings.RegisterDurationSetting(
	"sql.metrics.index_usage_stats.reset_interval",
	"interval controlling how often index usage statistics should be reset",
	time.Hour, /* defaultValue */
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()
