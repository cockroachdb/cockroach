// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// FeatureDeniedByFeatureFlagCounter is a counter that is incremented every time a feature is
// denied via the feature flag cluster setting, for example. feature.schema_change.enabled = FALSE.
var FeatureDeniedByFeatureFlagCounter = telemetry.GetCounterOnce("sql.feature_flag_denied")
