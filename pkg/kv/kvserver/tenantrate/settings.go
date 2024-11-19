// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantrate

import (
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Config contains the configuration of the rate limiter.
//
// We limit the rate of KV operations using the tenant cost model which can
// map these operations to "KV Compute Units". Note that KV Compute Units are
// not the same thing as Request Units. One KV Compute Unit is equivalent to one
// millisecond of CPU. Request Units measure other resources besides CPU (e.g.
// egress and IOPS). Also, Request Units correspond to the retail price of
// resources rather than the underlying cost of Cloud Provider resources (i.e.
// they include markup). For example, one KV Compute Unit might correspond to 2
// Request Units.
type Config struct {
	// Rate defines the "sustained" rate limit in KV Compute Units per second.
	Rate float64
	// Burst defines the "burst" limit in KV Compute Units. Unused units
	// accumulate up to this limit.
	Burst float64

	// ReadBatchUnits is the baseline cost of a read batch, in KV Compute Units.
	ReadBatchUnits float64
	// ReadRequestUnits is the baseline cost of one read request in a batch, in
	// KV Compute Units.
	ReadRequestUnits float64
	// ReadUnitsPerByte is the cost of a reading a byte in KV Compute Units.
	ReadUnitsPerByte float64
	// WriteBatchUnits is the baseline cost of a write batch, in KV Compute Units.
	WriteBatchUnits float64
	// WriteRequestUnits is the baseline cost of one write request in a batch,
	// in KV Compute Units.
	WriteRequestUnits float64
	// WriteUnitsPerByte is the cost of writing a byte in KV Compute Units.
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
	// KVCURateLimit specifies the limit of KV Compute Units / second that a
	// single tenant is allowed to use on an individual KV node. This is
	// intended to prevent a single tenant from harnessing a large fraction of a
	// KV node, in order to avoid very significant fluctuations in performance
	// depending on what other tenants are using the same KV node. By design, 1
	// RU roughly maps to 1 CPU-millisecond.
	//
	// If the value is positive, then this setting is interpreted as an absolute
	// limit of KVCU / second, so that a value of 1000 would limit CPU
	// consumption for a tenant to ~1 vCPU.
	//
	// If the value is negative, then this setting is interpreted as a relative
	// percentage of total CPU available. For example -200 means that we allow
	// 200 KV Compute Units / second per vCPU, or roughly 20% of the machine.
	KVCURateLimit = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.rate_limit",
		"per-tenant rate limit in KV Compute Units per second if positive, "+
			"or KV Compute Units per second per CPU if negative",
		-400,
		settings.NonZeroFloat,
	)

	kvcuBurstLimitSeconds = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.burst_limit_seconds",
		"per-tenant burst limit as a multiplier of the rate",
		10,
		settings.NonNegativeFloat,
	)

	readBatchCost = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.read_batch_cost",
		"base cost of a read batch in KV Compute Units",
		0.567,
		settings.NonNegativeFloat,
	)

	readRequestCost = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.read_request_cost",
		"base cost of a read request in KV Compute Units",
		0.0275,
		settings.NonNegativeFloat,
	)

	readCostPerMiB = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.read_cost_per_mebibyte",
		"cost of a read in KV Compute Units per MiB",
		12.77,
		settings.NonNegativeFloat,
	)

	writeBatchCost = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.write_batch_cost",
		"base cost of a write batch in KV Compute Units",
		0.328,
		settings.NonNegativeFloat,
	)

	writeRequestCost = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.write_request_cost",
		"base cost of a write request in KV Compute Units",
		0.314,
		settings.NonNegativeFloat,
	)

	writeCostPerMiB = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"kv.tenant_rate_limiter.write_cost_per_megabyte",
		"cost of a write in KV Compute Units per MiB",
		43.95,
		settings.NonNegativeFloat,
	)

	// List of config settings, used to set up "on change" notifiers.
	configSettings = [...]settings.NonMaskedSetting{
		KVCURateLimit,
		kvcuBurstLimitSeconds,
		readBatchCost,
		readRequestCost,
		readCostPerMiB,
		writeBatchCost,
		writeRequestCost,
		writeCostPerMiB,
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
	rate := absoluteRateFromConfigValue(KVCURateLimit.Get(sv))
	return Config{
		Rate:              rate,
		Burst:             rate * kvcuBurstLimitSeconds.Get(sv),
		ReadBatchUnits:    readBatchCost.Get(sv),
		ReadRequestUnits:  readRequestCost.Get(sv),
		ReadUnitsPerByte:  readCostPerMiB.Get(sv) / (1024 * 1024),
		WriteBatchUnits:   writeBatchCost.Get(sv),
		WriteRequestUnits: writeRequestCost.Get(sv),
		WriteUnitsPerByte: writeCostPerMiB.Get(sv) / (1024 * 1024),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	rate := absoluteRateFromConfigValue(KVCURateLimit.Default())
	return Config{
		Rate:              rate,
		Burst:             rate * kvcuBurstLimitSeconds.Default(),
		ReadBatchUnits:    readBatchCost.Default(),
		ReadRequestUnits:  readRequestCost.Default(),
		ReadUnitsPerByte:  readCostPerMiB.Default() / (1024 * 1024),
		WriteBatchUnits:   writeBatchCost.Default(),
		WriteRequestUnits: writeRequestCost.Default(),
		WriteUnitsPerByte: writeCostPerMiB.Default() / (1024 * 1024),
	}
}
