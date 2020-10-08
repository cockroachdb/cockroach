// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantrate

import "github.com/cockroachdb/cockroach/pkg/settings/cluster"

// OverrideSettingsWithRateLimits utilizes LimitConfigs from the values stored in the
// settings.
func OverrideSettingsWithRateLimits(settings *cluster.Settings, rl LimitConfigs) {
	readRequestRateLimit.Override(&settings.SV, float64(rl.ReadRequests.Rate))
	readRequestBurstLimit.Override(&settings.SV, rl.ReadRequests.Burst)
	writeRequestRateLimit.Override(&settings.SV, float64(rl.WriteRequests.Rate))
	writeRequestBurstLimit.Override(&settings.SV, rl.WriteRequests.Burst)
	readRateLimit.Override(&settings.SV, int64(rl.ReadBytes.Rate))
	readBurstLimit.Override(&settings.SV, rl.ReadBytes.Burst)
	writeRateLimit.Override(&settings.SV, int64(rl.WriteBytes.Rate))
	writeBurstLimit.Override(&settings.SV, rl.WriteBytes.Burst)
}
