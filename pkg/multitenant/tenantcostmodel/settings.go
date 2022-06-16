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
	ReadBatchCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.read_batch_cost",
		"base cost of a read batch in Request Units",
		0.50,
		settings.NonNegativeFloat,
	)

	ReadRequestCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.read_request_cost",
		"base cost of a read request in Request Units",
		0.125,
		settings.NonNegativeFloat,
	)

	ReadPayloadCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.read_payload_cost_per_mebibyte",
		"cost of a read payload in Request Units per MiB",
		16,
		settings.NonNegativeFloat,
	)

	WriteBatchCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.write_batch_cost",
		"base cost of a write batch in Request Units",
		1,
		settings.NonNegativeFloat,
	)

	WriteRequestCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.write_request_cost",
		"base cost of a write request in Request Units",
		1,
		settings.NonNegativeFloat,
	)

	WritePayloadCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.write_payload_cost_per_mebibyte",
		"cost of a write payload in Request Units per MiB",
		1024,
		settings.NonNegativeFloat,
	)

	SQLCPUSecondCost = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.sql_cpu_second_cost",
		"cost of a CPU-second in SQL pods in Request Units",
		333.3333,
		settings.NonNegativeFloat,
	)

	PgwireEgressCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.pgwire_egress_cost_per_mebibyte",
		"cost of client <-> SQL ingress/egress per MiB",
		1024,
		settings.NonNegativeFloat,
	)

	ExternalIOEgressCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.external_io_egress_per_mebibyte",
		"cost of a write to external storage in Request Units per MiB",
		1024,
		settings.NonNegativeFloat,
	)

	ExternalIOIngressCostPerMiB = settings.RegisterFloatSetting(
		settings.TenantReadOnly,
		"tenant_cost_model.external_io_ingress_per_mebibyte",
		"cost of a read from external storage in Request Units per MiB",
		0,
		settings.NonNegativeFloat,
	)

	// List of config settings, used by SetOnChange.
	configSettings = [...]settings.NonMaskedSetting{
		ReadBatchCost,
		ReadRequestCost,
		ReadPayloadCostPerMiB,
		WriteBatchCost,
		WriteRequestCost,
		WritePayloadCostPerMiB,
		SQLCPUSecondCost,
		PgwireEgressCostPerMiB,
		ExternalIOEgressCostPerMiB,
		ExternalIOIngressCostPerMiB,
	}
)

const perMiBToPerByte = float64(1) / (1024 * 1024)

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	return Config{
		KVReadBatch:           RU(ReadBatchCost.Get(sv)),
		KVReadRequest:         RU(ReadRequestCost.Get(sv)),
		KVReadByte:            RU(ReadPayloadCostPerMiB.Get(sv) * perMiBToPerByte),
		KVWriteBatch:          RU(WriteBatchCost.Get(sv)),
		KVWriteRequest:        RU(WriteRequestCost.Get(sv)),
		KVWriteByte:           RU(WritePayloadCostPerMiB.Get(sv) * perMiBToPerByte),
		PodCPUSecond:          RU(SQLCPUSecondCost.Get(sv)),
		PGWireEgressByte:      RU(PgwireEgressCostPerMiB.Get(sv) * perMiBToPerByte),
		ExternalIOIngressByte: RU(ExternalIOIngressCostPerMiB.Get(sv) * perMiBToPerByte),
		ExternalIOEgressByte:  RU(ExternalIOEgressCostPerMiB.Get(sv) * perMiBToPerByte),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	return Config{
		KVReadBatch:           RU(ReadBatchCost.Default()),
		KVReadRequest:         RU(ReadRequestCost.Default()),
		KVReadByte:            RU(ReadPayloadCostPerMiB.Default() * perMiBToPerByte),
		KVWriteBatch:          RU(WriteBatchCost.Default()),
		KVWriteRequest:        RU(WriteRequestCost.Default()),
		KVWriteByte:           RU(WritePayloadCostPerMiB.Default() * perMiBToPerByte),
		PodCPUSecond:          RU(SQLCPUSecondCost.Default()),
		PGWireEgressByte:      RU(PgwireEgressCostPerMiB.Default() * perMiBToPerByte),
		ExternalIOIngressByte: RU(ExternalIOEgressCostPerMiB.Default() * perMiBToPerByte),
		ExternalIOEgressByte:  RU(ExternalIOIngressCostPerMiB.Default() * perMiBToPerByte),
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
