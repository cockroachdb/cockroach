// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcostmodel

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Settings for the cost model parameters. These determine the values for a
// Config, though not directly (some settings have user-friendlier units).
//
// The KV operation parameters are set based on experiments, where 1000 Request
// Units correspond to one CPU second of usage on the host cluster.
//
// TODO(radu): these settings are not currently used on the tenant side; there,
// only the defaults are used. Ideally, the tenant would always get the values
// from the host cluster.
var (
	readRequestCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.kv_read_request_cost",
		"base cost of a read request in Request Units",
		0.6993,
		settings.PositiveFloat,
	)

	readCostPerMB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.kv_read_cost_per_megabyte",
		"cost of a read in Request Units per MB",
		107.6563,
		settings.PositiveFloat,
	)

	writeRequestCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.kv_write_request_cost",
		"base cost of a write request in Request Units",
		5.7733,
		settings.PositiveFloat,
	)

	writeCostPerMB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.kv_write_cost_per_megabyte",
		"cost of a write in Request Units per MB",
		2026.3021,
		settings.PositiveFloat,
	)

	podCPUSecondCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.pod_cpu_second_cost",
		"cost of a CPU-second on the tenant POD in Request Units",
		1000.0,
		settings.PositiveFloat,
	)

	pgwireEgressCostPerMB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.pgwire_egress_cost_per_megabyte",
		"cost of client <-> SQL ingress/egress per MB",
		878.9063,
		settings.PositiveFloat,
	)

	// List of config settings, used by SetOnChange.
	configSettings = [...]settings.NonMaskedSetting{
		readRequestCost,
		readCostPerMB,
		writeRequestCost,
		writeCostPerMB,
		podCPUSecondCost,
		pgwireEgressCostPerMB,
	}
)

const perMBToPerByte = float64(1) / (1024 * 1024)

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	return Config{
		KVReadRequest:    RU(readRequestCost.Get(sv)),
		KVReadByte:       RU(readCostPerMB.Get(sv) * perMBToPerByte),
		KVWriteRequest:   RU(writeRequestCost.Get(sv)),
		KVWriteByte:      RU(writeCostPerMB.Get(sv) * perMBToPerByte),
		PodCPUSecond:     RU(podCPUSecondCost.Get(sv)),
		PGWireEgressByte: RU(pgwireEgressCostPerMB.Get(sv) * perMBToPerByte),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	return Config{
		KVReadRequest:    RU(readRequestCost.Default()),
		KVReadByte:       RU(readCostPerMB.Default() * perMBToPerByte),
		KVWriteRequest:   RU(writeRequestCost.Default()),
		KVWriteByte:      RU(writeCostPerMB.Default() * perMBToPerByte),
		PodCPUSecond:     RU(podCPUSecondCost.Default()),
		PGWireEgressByte: RU(pgwireEgressCostPerMB.Default() * perMBToPerByte),
	}
}

// SetOnChange installs a callback that is run whenever a cost model cluster
// setting changes.
//
// It calls SetOnChange on the relevant cluster settings.
func SetOnChange(sv *settings.Values, fn func(context.Context)) {
	for _, s := range configSettings {
		s.SetOnChange(sv, fn)
	}
}

var _ = SetOnChange
var _ = ConfigFromSettings
