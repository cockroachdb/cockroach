// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// FeatureDeniedByFeatureFlagCounter is a counter that is incremented every time a feature is
// denied via the feature flag cluster setting, for example. feature.schema_change.enabled = FALSE.
var FeatureDeniedByFeatureFlagCounter = telemetry.GetCounterOnce("sql.feature_flag_denied")
