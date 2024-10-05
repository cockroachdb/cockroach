// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostmodel

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
		settings.SystemVisible,
		"tenant_cost_model.read_batch_cost",
		"base cost of a read batch in Request Units",
		0.50,
		settings.NonNegativeFloat,
	)

	ReadRequestCost = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.read_request_cost",
		"base cost of a read request in Request Units",
		0.125,
		settings.NonNegativeFloat,
	)

	ReadPayloadCostPerMiB = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.read_payload_cost_per_mebibyte",
		"cost of a read payload in Request Units per MiB",
		16,
		settings.NonNegativeFloat,
	)

	WriteBatchCost = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.write_batch_cost",
		"base cost of a write batch in Request Units",
		1,
		settings.NonNegativeFloat,
	)

	WriteRequestCost = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.write_request_cost",
		"base cost of a write request in Request Units",
		1,
		settings.NonNegativeFloat,
	)

	WritePayloadCostPerMiB = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.write_payload_cost_per_mebibyte",
		"cost of a write payload in Request Units per MiB",
		1024,
		settings.NonNegativeFloat,
	)

	SQLCPUSecondCost = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.sql_cpu_second_cost",
		"cost of a CPU-second in SQL pods in Request Units",
		333.3333,
		settings.NonNegativeFloat,
	)

	PgwireEgressCostPerMiB = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.pgwire_egress_cost_per_mebibyte",
		"cost of client <-> SQL ingress/egress per MiB",
		1024,
		settings.NonNegativeFloat,
	)

	ExternalIOEgressCostPerMiB = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.external_io_egress_per_mebibyte",
		"cost of a write to external storage in Request Units per MiB",
		1024,
		settings.NonNegativeFloat,
	)

	ExternalIOIngressCostPerMiB = settings.RegisterFloatSetting(
		settings.SystemVisible,
		"tenant_cost_model.external_io_ingress_per_mebibyte",
		"cost of a read from external storage in Request Units per MiB",
		0,
		settings.NonNegativeFloat,
	)

	CrossRegionNetworkCostSetting = settings.RegisterStringSetting(
		settings.SystemVisible,
		"tenant_cost_model.cross_region_network_cost",
		"network cost table for cross-region traffic",
		"",
		settings.WithValidateString(validateRegionalCostMultiplierTableSetting),
		settings.WithReportable(true),
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
		CrossRegionNetworkCostSetting,
	}
)

func validateRegionalCostMultiplierTableSetting(values *settings.Values, tableStr string) error {
	_, err := NewNetworkCostTable(tableStr)
	return err
}

// networkCostTableSetting is the json structure of the
// 'tenant_cost_model.cross_region_network_cost' setting.
type networkCostTableSetting struct {
	RegionPairs []regionCostEntry `json:"regionPairs"`
}

// regionCostEntry contains the network transfer cost for a single source ->
// destination region pair.
type regionCostEntry struct {
	NetworkPath
	// Cost is how many RUs should be charged for each logical byte of transfer.
	Cost float64 `json:"cost"`
}

// NetworkPath describes a source region -> destination region network flow.
type NetworkPath struct {
	// FromRegion is the egress region for the network traffic.
	FromRegion string `json:"fromRegion"`
	// ToRegion is the ingress region for the network traffic.
	ToRegion string `json:"toRegion"`
}

// NetworkCostTable describes the cost of network bandwidith between pairs of
// region.
type NetworkCostTable struct {
	// Matrix contains the source -> destination network cost for pairs of
	// regions.
	Matrix map[NetworkPath]NetworkCost `json:",omitempty"`
}

// NewNetworkCostTable parses the setting value, validates the setting, then
// returns a form that is programatically usable.
func NewNetworkCostTable(setting string) (*NetworkCostTable, error) {
	if setting == "" {
		return nil, nil
	}

	var jsonObj networkCostTableSetting
	if err := json.Unmarshal([]byte(setting), &jsonObj); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal json")
	}

	// Convert the setting into a hash table and validate the individual entries
	table := newEmptyCostTable()
	for i, entry := range jsonObj.RegionPairs {
		if entry.FromRegion == "" {
			return nil, errors.Newf("entry %d is missing 'fromRegion'", i)
		}
		if entry.ToRegion == "" {
			return nil, errors.Newf("entry %d is missing 'toRegion'", i)
		}
		if entry.FromRegion == entry.ToRegion {
			return nil, errors.Newf("'%s' contains an entry for itself. The cost for intra-region traffic should always be zero.", entry.FromRegion)
		}
		if entry.Cost < 0 {
			return nil, errors.Newf("network cost for '%s' -> '%s' must not be negative", entry.FromRegion, entry.ToRegion)
		}
		path := NetworkPath{
			FromRegion: entry.FromRegion,
			ToRegion:   entry.ToRegion,
		}
		table.Matrix[path] = NetworkCost(entry.Cost)
	}

	// Verify the table is complete
	regionSet := map[string]struct{}{}
	for path := range table.Matrix {
		regionSet[path.FromRegion] = struct{}{}
		regionSet[path.ToRegion] = struct{}{}
	}
	for from := range regionSet {
		for to := range regionSet {
			_, ok := table.Matrix[NetworkPath{
				FromRegion: from,
				ToRegion:   to,
			}]
			if from != to && !ok {
				return nil, errors.Newf("the network cost table is missing from region '%s' to region '%s'", from, to)
			}
		}
	}

	return table, nil
}

func newEmptyCostTable() *NetworkCostTable {
	return &NetworkCostTable{Matrix: make(map[NetworkPath]NetworkCost)}
}

const perMiBToPerByte = float64(1) / (1024 * 1024)

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	tableStr := CrossRegionNetworkCostSetting.Get(sv)

	networkTable, err := NewNetworkCostTable(tableStr)
	if err != nil {
		// This should not happen unless someone manually updates the settings
		// table, bypassing the validation.
		log.Errorf(
			context.Background(),
			"failed to parse the network cost table %q: err=%v",
			tableStr,
			err,
		)
	}
	if networkTable == nil {
		networkTable = newEmptyCostTable()
	}

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
		NetworkCostTable:      *networkTable,
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
		NetworkCostTable:      *newEmptyCostTable(),
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
