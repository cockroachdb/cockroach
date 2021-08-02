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
var (
	readRequestCost = settings.RegisterFloatSetting(
		"tenant_cost_model.kv_read_request_cost",
		"base cost of a read request in Request Units",
		0.7,
		settings.PositiveFloat,
	)

	readCostPerMB = settings.RegisterFloatSetting(
		"tenant_cost_model.kv_read_cost_per_megabyte",
		"cost of a read in Request Units per MB",
		10.0,
		settings.PositiveFloat,
	)

	writeRequestCost = settings.RegisterFloatSetting(
		"tenant_cost_model.kv_write_request_cost",
		"base cost of a write request in Request Units",
		1.0,
		settings.PositiveFloat,
	)

	writeCostPerMB = settings.RegisterFloatSetting(
		"tenant_cost_model.kv_write_cost_per_megabyte",
		"cost of a write in Request Units per MB",
		400.0,
		settings.PositiveFloat,
	)

	podCPUSecondCost = settings.RegisterFloatSetting(
		"tenant_cost_model.pod_cpu_second_cost",
		"cost of a CPU-second on the tenant POD in Request Units",
		1000.0,
		settings.PositiveFloat,
	)

	// List of config settings, used by SetOnChange.
	configSettings = [...]settings.WritableSetting{
		readRequestCost,
		readCostPerMB,
		writeRequestCost,
		writeCostPerMB,
		podCPUSecondCost,
	}
)

const perMBToPerByte = float64(1) / (1024 * 1024)

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	return Config{
		KVReadRequest:  RU(readRequestCost.Get(sv)),
		KVReadByte:     RU(readCostPerMB.Get(sv) * perMBToPerByte),
		KVWriteRequest: RU(writeRequestCost.Get(sv)),
		KVWriteByte:    RU(writeCostPerMB.Get(sv) * perMBToPerByte),
		PodCPUSecond:   RU(podCPUSecondCost.Get(sv)),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	return Config{
		KVReadRequest:  RU(readRequestCost.Default()),
		KVReadByte:     RU(readCostPerMB.Default() * perMBToPerByte),
		KVWriteRequest: RU(writeRequestCost.Default()),
		KVWriteByte:    RU(writeCostPerMB.Default() * perMBToPerByte),
		PodCPUSecond:   RU(podCPUSecondCost.Default()),
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
