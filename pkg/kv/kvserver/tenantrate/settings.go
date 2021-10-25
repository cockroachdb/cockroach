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
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
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

	// ReadRequestUnits is the baseline cost of a read, in KV Compute Units.
	ReadRequestUnits float64
	// ReadUnitsPerByte is the size-dependent cost of a read, in KV Compute Units
	// per byte.
	ReadUnitsPerByte float64
	// WriteRequestUnits is the baseline cost of a write, in KV Compute Units.
	WriteRequestUnits float64
	// WriteUnitsPerByte is the size-dependent cost of a write, in KV Compute
	// Units per byte.
	WriteUnitsPerByte float64
}

// Settings for the rate limiter. These determine the values for a Config,
// though not directly (the settings have user-friendlier units).
//
// The settings are designed so that there is one important "knob" to turn:
// kv.tenant_rate_limiter.rate_limit.
//
// The burst limit setting is defined as a multiplier of the rate (i.e. in
// seconds), so it doesn't need to be adjusted in concert with the rate.
//
// These settings have analogs in the tenantcostmodel settings but the values
// are different; the purpose here is to estimate CPU usage on the KV node,
// whereas the tenant cost model is about pricing and takes into consideration
// more costs (like network transfers, IOs).
var (
	// kvcuRateLimit was initially set to an absolute value in KV Compute Units/s,
	// with the intention of throttling free tier tenants.
	//
	// We now use it to disallow a single tenant from harnessing a large fraction
	// of a KV node, in order to avoid very significant fluctuations in
	// performance depending on what other tenants are using the same KV node.
	// In this mode, a value of -200 means that we allow 200 KV Compute Units/s
	// per CPU, or roughly 20% of the machine (by design 1 RU roughly maps to 1
	// CPU-millisecond).
	kvcuRateLimit = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.rate_limit",
		"per-tenant rate limit in KV Compute Units per second if positive, "+
			"or KV Compute Units per second per CPU if negative",
		-200,
		func(v float64) error {
			if v == 0 {
				return errors.New("cannot set to zero value")
			}
			return nil
		},
	)

	kvcuBurstLimitSeconds = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.burst_limit_seconds",
		"per-tenant burst limit as a multiplier of the rate",
		10,
		settings.PositiveFloat,
	)

	readRequestCost = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.read_request_cost",
		"base cost of a read request in KV Compute Units",
		0.7,
		settings.PositiveFloat,
	)

	readCostPerMB = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.read_cost_per_megabyte",
		"cost of a read in KV Compute Units per MB",
		10.0,
		settings.PositiveFloat,
	)

	writeRequestCost = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.write_request_cost",
		"base cost of a write request in KV Compute Units",
		1.0,
		settings.PositiveFloat,
	)

	writeCostPerMB = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.write_cost_per_megabyte",
		"cost of a write in KV Compute Units per MB",
		400.0,
		settings.PositiveFloat,
	)

	// List of config settings, used to set up "on change" notifiers.
	configSettings = [...]settings.WritableSetting{
		kvcuRateLimit,
		kvcuBurstLimitSeconds,
		readRequestCost,
		readCostPerMB,
		writeRequestCost,
		writeCostPerMB,
	}
)

// absoluteRateFromConfigValue returns an absolute rate (in KVCU/s) from a value
// of the kv.tenant_rate_limiter.rate_limit setting.
func absoluteRateFromConfigValue(value float64) float64 {
	if value < 0 {
		// We use GOMAXPROCS instead of NumCPU because the former could be adjusted
		// based on cgroup limits (see cgroups.AdjustMaxProcs).
		return -value * float64(runtime.GOMAXPROCS(0))
	}
	return value
}

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	rate := absoluteRateFromConfigValue(kvcuRateLimit.Get(sv))
	return Config{
		Rate:              rate,
		Burst:             rate * kvcuBurstLimitSeconds.Get(sv),
		ReadRequestUnits:  readRequestCost.Get(sv),
		ReadUnitsPerByte:  readCostPerMB.Get(sv) / (1024 * 1024),
		WriteRequestUnits: writeRequestCost.Get(sv),
		WriteUnitsPerByte: writeCostPerMB.Get(sv) / (1024 * 1024),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	rate := absoluteRateFromConfigValue(kvcuRateLimit.Default())
	return Config{
		Rate:              rate,
		Burst:             rate * kvcuBurstLimitSeconds.Default(),
		ReadRequestUnits:  readRequestCost.Default(),
		ReadUnitsPerByte:  readCostPerMB.Default() / (1024 * 1024),
		WriteRequestUnits: writeRequestCost.Default(),
		WriteUnitsPerByte: writeCostPerMB.Default() / (1024 * 1024),
	}
}
