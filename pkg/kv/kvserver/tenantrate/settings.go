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

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/tenantcostmodel"
)

// Config contains the configuration of the rate limiter.
//
// We limit the rate of KV operations using the tenant cost model which can
// map these operations to "Request Units".
type Config struct {
	// Rate defines the "sustained" rate limit in Request Units per second.
	Rate float64
	// Burst defines the "burst" limit in Request Units. Unused units accumulate
	// up to this limit.
	Burst float64

	CostModel tenantcostmodel.Config
}

// Settings for the rate limiter. These determine the values for a Config,
// though not directly (the settings have user-friendlier units).
//
// The settings are designed so that there is one important "knob" to turn:
// kv.tenant_rate_limiter.rate_limit.
//
// The burst limit setting is defined as a multiplier of the rate (i.e. in
// seconds), so it doesn't need to be adjusted in concert with the rate.
var (
	ruRateLimit = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.rate_limit",
		"per-tenant rate limit in Request Units per second",
		200,
		settings.PositiveFloat,
	)

	ruBurstLimitSeconds = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.burst_limit_seconds",
		"per-tenant burst limit as a multiplier of the rate",
		10,
		settings.PositiveFloat,
	)

	// List of config settings, used to set up "on change" notifiers.
	configSettings = [...]settings.WritableSetting{
		ruRateLimit,
		ruBurstLimitSeconds,
	}
)

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	rate := ruRateLimit.Get(sv)
	return Config{
		Rate:      rate,
		Burst:     rate * ruBurstLimitSeconds.Get(sv),
		CostModel: tenantcostmodel.ConfigFromSettings(sv),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	rate := ruRateLimit.Default()
	return Config{
		Rate:      rate,
		Burst:     rate * ruBurstLimitSeconds.Default(),
		CostModel: tenantcostmodel.DefaultConfig(),
	}
}
