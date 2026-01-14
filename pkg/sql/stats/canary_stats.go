// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import "github.com/cockroachdb/cockroach/pkg/settings"

// CanaryFraction controls the probabilistic sampling rate for queries
// participating in the canary statistics rollout feature.
//
// This cluster-level setting determines what fraction of queries will use
// "canary statistics" (newly collected stats within their canary window)
// versus "stable statistics" (previously proven stats). For example, a value
// of 0.2 means 20% of queries will test canary stats while 80% use stable stats.
//
// The selection is atomic per query: if a query is chosen for canary evaluation,
// it will use canary statistics for ALL tables it references (where available).
// A query never uses a mix of canary and stable statistics.
var CanaryFraction = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.stats.canary_fraction",
	"probability that table statistics will use canary mode instead of stable mode for query planning [0.0-1.0]",
	0,
	settings.Fraction,
	settings.WithVisibility(settings.Reserved),
)
