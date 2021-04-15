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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// Config contains the configuration of the rate limiter.
//
// We limit the rate in terms of "KV Compute Units". The configuration contains
// the rate and burst limits for KVCUs, as well as factors that define a "cost
// mode" for calculating the number of KVCUs for a read or write request.
//
// Specifically, the cost model is a linear function combining a fixed
// pre-request cost and a size-dependent (per-byte) cost.
//
// For a read:
//   KVCUs = ReadRequestUnits + <size of read> * ReadUnitsPerByte
// For a write:
//   KVCUs = WriteRequestUnits + <size of write) * WriteUnitsPerByte
//
type Config struct {
	// Rate defines the "sustained" rate limit in KV Compute Units per second.
	Rate float64
	// Burst defines the "burst" limit in KV Compute Units. Unused units
	// accumulate up to this limit.
	Burst float64

	// ReadRequestUnits is the baseline cost of a read, in KV Compute Units.
	ReadRequestUnits float64
	// ReadRequestUnits is the size-dependent cost of a read, in KV Compute Units
	// per byte.
	ReadUnitsPerByte float64
	// WriteRequestUnits is the baseline cost of a write, in KV Compute Units.
	WriteRequestUnits float64
	// WriteRequestUnits is the size-dependent cost of a write, in KV Compute
	// Units per byte.
	WriteUnitsPerByte float64
}

// Settings for the rate limiter. These determine the values for a Config,
// though not directly (the settings have user-friendlier units).
//
// The settings are designed so that there is one important "knob" to turn:
// kv.tenant_rate_limiter.rate_limit.
//
// The rest of the settings are meant to be changed rarely. Note that the burst
// limit setting is defined as a multiplier of the rate (i.e. in seconds), so
// it doesn't need to be adjusted in concert with the rate.
//
// The settings that determine the KV Request Units (the "cost model") are set
// based on experiments, where 1000 KV Request Units correspond to one CPU
// second.
var (
	kvcuRateLimit = settings.RegisterFloatSetting(
		"kv.tenant_rate_limiter.rate_limit",
		"per-tenant rate limit in KV Compute Units per second",
		200,
		settings.PositiveFloat,
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

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(st *cluster.Settings) Config {
	const perMBToPerByte = float64(1) / (1024 * 1024)
	var c Config
	c.Rate = kvcuRateLimit.Get(&st.SV)
	c.Burst = c.Rate * kvcuBurstLimitSeconds.Get(&st.SV)
	c.ReadRequestUnits = readRequestCost.Get(&st.SV)
	c.ReadUnitsPerByte = readCostPerMB.Get(&st.SV) * perMBToPerByte
	c.WriteRequestUnits = writeRequestCost.Get(&st.SV)
	c.WriteUnitsPerByte = writeCostPerMB.Get(&st.SV) * perMBToPerByte
	return c
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	return Config{
		Rate:              kvcuRateLimit.Default(),
		Burst:             kvcuRateLimit.Default() * kvcuBurstLimitSeconds.Default(),
		ReadRequestUnits:  readRequestCost.Default(),
		ReadUnitsPerByte:  readCostPerMB.Default() / (1024 * 1024),
		WriteRequestUnits: writeRequestCost.Default(),
		WriteUnitsPerByte: writeCostPerMB.Default() / (1024 * 1024),
	}
}
