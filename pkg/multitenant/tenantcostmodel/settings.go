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
	readBatchCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.read_batch_cost",
		"base cost of a read batch in Request Units",
		0.3333,
		settings.PositiveFloat,
	)

	readRequestCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.read_request_cost",
		"base cost of a read request in Request Units",
		0.3333,
		settings.PositiveFloat,
	)

	readPayloadCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.read_payload_cost_per_mebibyte",
		"cost of a read payload in Request Units per MiB",
		16,
		settings.PositiveFloat,
	)

	writeBatchCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.write_batch_cost",
		"base cost of a write batch in Request Units",
		1,
		settings.PositiveFloat,
	)

	writeRequestCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.write_request_cost",
		"base cost of a write request in Request Units",
		1,
		settings.PositiveFloat,
	)

	writePayloadCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.write_payload_cost_per_mebibyte",
		"cost of a write payload in Request Units per MiB",
		1024,
		settings.PositiveFloat,
	)

	sqlCPUSecondCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.sql_cpu_second_cost",
		"cost of a CPU-second in SQL pods in Request Units",
		333.3333,
		settings.PositiveFloat,
	)

	pgwireEgressCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.pgwire_egress_cost_per_mebibyte",
		"cost of client <-> SQL ingress/egress per MiB",
		1024,
		settings.PositiveFloat,
	)

	// List of config settings, used by SetOnChange.
	configSettings = [...]settings.NonMaskedSetting{
		readBatchCost,
		readRequestCost,
		readPayloadCostPerMiB,
		writeBatchCost,
		writeRequestCost,
		writePayloadCostPerMiB,
		sqlCPUSecondCost,
		pgwireEgressCostPerMiB,
	}
)

const perMiBToPerByte = float64(1) / (1024 * 1024)

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	return Config{
		KVReadBatch:      RU(readBatchCost.Get(sv)),
		KVReadRequest:    RU(readRequestCost.Get(sv)),
		KVReadByte:       RU(readPayloadCostPerMiB.Get(sv) * perMiBToPerByte),
		KVWriteBatch:     RU(writeBatchCost.Get(sv)),
		KVWriteRequest:   RU(writeRequestCost.Get(sv)),
		KVWriteByte:      RU(writePayloadCostPerMiB.Get(sv) * perMiBToPerByte),
		PodCPUSecond:     RU(sqlCPUSecondCost.Get(sv)),
		PGWireEgressByte: RU(pgwireEgressCostPerMiB.Get(sv) * perMiBToPerByte),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	return Config{
		KVReadBatch:      RU(readBatchCost.Default()),
		KVReadRequest:    RU(readRequestCost.Default()),
		KVReadByte:       RU(readPayloadCostPerMiB.Default() * perMiBToPerByte),
		KVWriteBatch:     RU(writeBatchCost.Default()),
		KVWriteRequest:   RU(writeRequestCost.Default()),
		KVWriteByte:      RU(writePayloadCostPerMiB.Default() * perMiBToPerByte),
		PodCPUSecond:     RU(sqlCPUSecondCost.Default()),
		PGWireEgressByte: RU(pgwireEgressCostPerMiB.Default() * perMiBToPerByte),
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
