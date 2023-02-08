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
	"encoding/json"
	"sort"

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

	RegionalCostMultiplierTableSetting = func() *settings.StringSetting {
		s := settings.RegisterValidatedStringSetting(
			settings.TenantReadOnly,
			"tenant_cost_model.regional_cost_multiplier_table",
			"cost multiplier table for cross-region traffic in compacted form",
			"",
			validateRegionalCostMultiplierTableSetting,
		)
		s.SetReportable(true)
		return s
	}()

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
		RegionalCostMultiplierTableSetting,
	}
)

func validateRegionalCostMultiplierTableSetting(values *settings.Values, tableStr string) error {
	_, err := parseRegionalCostMultiplierTableSetting(tableStr)
	return err
}

func parseRegionalCostMultiplierTableSetting(
	tableStr string,
) (*RegionalCostMultiplierCompactTable, error) {
	var compact RegionalCostMultiplierCompactTable
	if tableStr == "" {
		return &compact, nil
	}
	if err := json.Unmarshal([]byte(tableStr), &compact); err != nil {
		return nil, err
	}
	if err := compact.Validate(); err != nil {
		return nil, err
	}
	return &compact, nil
}

// RegionalCostMultiplierCompactTable describes the cost multiplier table for
// cross-region transfers in compacted form. This model assumes that the cost
// is the same for both directions.
type RegionalCostMultiplierCompactTable struct {
	// Regions represents the list of region names. The indexes of the regions
	// will be used alongside with the matrix.
	Regions []string `json:"regions,omitempty"`
	// Matrix represents the compacted version of the regional cost multiplier
	// table. The compacted version only takes everything above the diagonal
	// matrix of the table, including itself.
	//
	// Consider the following cost multiplier table:
	//     | a | b | c
	//   --------------
	//   a | 1 | 1 | 2
	//   b | 1 | 1 | 2
	//   c | 2 | 2 | 1
	//
	// That can be represented as:
	// - Regions=[a, b, c]
	// - Matrix=[[1, 1, 2], [1, 2], [1]]
	Matrix [][]RUMultiplier `json:"matrix,omitempty"`
}

// Validate returns nil if the compacted cost multiplier table is valid, or
// an error otherwise.
func (ct *RegionalCostMultiplierCompactTable) Validate() error {
	if len(ct.Regions) != len(ct.Matrix) {
		return errors.Newf(
			"unexpected number of rows: found %d regions, but %d rows in matrix",
			len(ct.Regions),
			len(ct.Matrix),
		)
	}
	expectedNumCols := len(ct.Regions)
	for _, row := range ct.Matrix {
		if len(row) != expectedNumCols {
			return errors.New("malformed cost multiplier matrix")
		}
		for _, val := range row {
			if val < 0 {
				return errors.New("values must be non-negative")
			}
		}
		expectedNumCols--
	}
	return nil
}

// RegionalCostMultiplierTable describes the cost multiplier table for
// cross-region transfers. This model assumes that the cost is the same for both
// directions.
type RegionalCostMultiplierTable struct {
	// Matrix represents the expanded version of the regional cost multiplier
	// table.
	Matrix map[string]map[string]RUMultiplier `json:",omitempty"`
}

// CostMultiplier returns the cost multiplier for transfers between regions r1
// and r2. If the mapping could not be found, a multiplier of 1 is assumed.
func (et *RegionalCostMultiplierTable) CostMultiplier(r1, r2 string) RUMultiplier {
	if et.Matrix == nil {
		return RUMultiplier(1)
	}
	t2, ok := et.Matrix[r1]
	if !ok || t2 == nil {
		return RUMultiplier(1)
	}
	val, ok2 := t2[r2]
	if !ok2 {
		return RUMultiplier(1)
	}
	return val
}

// Validate returns nil if the expanded cost multiplier table is valid, or
// an error otherwise.
func (et *RegionalCostMultiplierTable) Validate() error {
	if et.Matrix == nil {
		return nil
	}
	hasValue := func(fromRegion, toRegion string, val RUMultiplier) bool {
		inner, ok := et.Matrix[fromRegion]
		if !ok {
			return false
		}
		v, ok2 := inner[toRegion]
		if !ok2 {
			return false
		}
		return v == val
	}
	for fromRegion, row := range et.Matrix {
		if row == nil {
			return errors.New("inner matrix cannot be nil")
		}
		if len(row) != len(et.Matrix) {
			return errors.Newf("number of rows and columns do not match: rows=%d, cols=%d",
				len(et.Matrix), len(row))
		}
		for toRegion, val := range row {
			if !hasValue(fromRegion, toRegion, val) || !hasValue(toRegion, fromRegion, val) {
				return errors.New("table values are invalid")
			}
			if val < 0 {
				return errors.New("table values must be non-negative")
			}
		}
	}
	return nil
}

// MakeRegionalCostMultiplierTableFromCompact creates an expanded regional cost
// multiplier table from its compacted form. This assumes that the input
// compacted table is valid.
func MakeRegionalCostMultiplierTableFromCompact(
	compact *RegionalCostMultiplierCompactTable,
) RegionalCostMultiplierTable {
	idxToRegion := make(map[int]string)
	for idx, region := range compact.Regions {
		idxToRegion[idx] = region
	}
	matrix := make(map[string]map[string]RUMultiplier)
	linkRegion := func(a, b string, val RUMultiplier) {
		if _, ok := matrix[a]; !ok {
			matrix[a] = make(map[string]RUMultiplier)
		}
		matrix[a][b] = val
	}
	for fromIdx, row := range compact.Matrix {
		fromRegion := idxToRegion[fromIdx]
		toIdx := fromIdx
		for _, multiplier := range row {
			toRegion := idxToRegion[toIdx]
			linkRegion(fromRegion, toRegion, multiplier)
			if fromIdx != toIdx {
				linkRegion(toRegion, fromRegion, multiplier)
			}
			toIdx++
		}
	}
	return RegionalCostMultiplierTable{Matrix: matrix}
}

// MakeRegionalCostMultiplierCompactTableFromExpanded creates a compacted version
// of the regional cost multiplier table from its expanded form. This assumes
// that the input expanded table is valid.
func MakeRegionalCostMultiplierCompactTableFromExpanded(
	expanded *RegionalCostMultiplierTable,
) RegionalCostMultiplierCompactTable {
	regions := make([]string, 0, len(expanded.Matrix))
	for key := range expanded.Matrix {
		regions = append(regions, key)
	}

	// Sort for deterministic ordering.
	sort.Strings(regions)

	// Build empty diagonal matrix table.
	matrix := make([][]RUMultiplier, len(expanded.Matrix))
	numCols := len(matrix)
	for i := 0; i < len(matrix); i++ {
		matrix[i] = make([]RUMultiplier, numCols)
		numCols--
	}

	// Fill the table.
	for i := 0; i < len(matrix); i++ {
		fromIdx, toIdx := i, i
		for j := 0; j < len(matrix[i]); j++ {
			fromRegion, toRegion := regions[fromIdx], regions[toIdx]
			matrix[i][j] = expanded.Matrix[fromRegion][toRegion]
			toIdx++
		}
	}
	return RegionalCostMultiplierCompactTable{Regions: regions, Matrix: matrix}
}

const perMiBToPerByte = float64(1) / (1024 * 1024)

// ConfigFromSettings constructs a Config using the cluster setting values.
func ConfigFromSettings(sv *settings.Values) Config {
	tableStr := RegionalCostMultiplierTableSetting.Get(sv)
	compact, err := parseRegionalCostMultiplierTableSetting(tableStr)
	if err != nil {
		// This should not happen unless someone manually updates the settings
		// table, bypassing the validation.
		log.Errorf(
			context.Background(),
			"failed to parse regional cost RU multiplier table %q: err=%v; defaulting to no multipliers",
			tableStr,
			err,
		)
		compact = &RegionalCostMultiplierCompactTable{}
	}
	return Config{
		KVReadBatch:                    RU(ReadBatchCost.Get(sv)),
		KVReadRequest:                  RU(ReadRequestCost.Get(sv)),
		KVReadByte:                     RU(ReadPayloadCostPerMiB.Get(sv) * perMiBToPerByte),
		KVWriteBatch:                   RU(WriteBatchCost.Get(sv)),
		KVWriteRequest:                 RU(WriteRequestCost.Get(sv)),
		KVWriteByte:                    RU(WritePayloadCostPerMiB.Get(sv) * perMiBToPerByte),
		PodCPUSecond:                   RU(SQLCPUSecondCost.Get(sv)),
		PGWireEgressByte:               RU(PgwireEgressCostPerMiB.Get(sv) * perMiBToPerByte),
		ExternalIOIngressByte:          RU(ExternalIOIngressCostPerMiB.Get(sv) * perMiBToPerByte),
		ExternalIOEgressByte:           RU(ExternalIOEgressCostPerMiB.Get(sv) * perMiBToPerByte),
		KVInterRegionRUMultiplierTable: MakeRegionalCostMultiplierTableFromCompact(compact),
	}
}

// DefaultConfig returns the configuration that corresponds to the default
// setting values.
func DefaultConfig() Config {
	tableStr := RegionalCostMultiplierTableSetting.Default()
	compact, err := parseRegionalCostMultiplierTableSetting(tableStr)
	if err != nil {
		// This should not happen unless someone manually updates the settings
		// table, bypassing the validation.
		log.Errorf(
			context.Background(),
			"failed to parse regional cost RU multiplier table %q: err=%v; defaulting to no multipliers",
			tableStr,
			err,
		)
		compact = &RegionalCostMultiplierCompactTable{}
	}
	return Config{
		KVReadBatch:                    RU(ReadBatchCost.Default()),
		KVReadRequest:                  RU(ReadRequestCost.Default()),
		KVReadByte:                     RU(ReadPayloadCostPerMiB.Default() * perMiBToPerByte),
		KVWriteBatch:                   RU(WriteBatchCost.Default()),
		KVWriteRequest:                 RU(WriteRequestCost.Default()),
		KVWriteByte:                    RU(WritePayloadCostPerMiB.Default() * perMiBToPerByte),
		PodCPUSecond:                   RU(SQLCPUSecondCost.Default()),
		PGWireEgressByte:               RU(PgwireEgressCostPerMiB.Default() * perMiBToPerByte),
		ExternalIOIngressByte:          RU(ExternalIOEgressCostPerMiB.Default() * perMiBToPerByte),
		ExternalIOEgressByte:           RU(ExternalIOIngressCostPerMiB.Default() * perMiBToPerByte),
		KVInterRegionRUMultiplierTable: MakeRegionalCostMultiplierTableFromCompact(compact),
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
var _ = MakeRegionalCostMultiplierCompactTableFromExpanded
