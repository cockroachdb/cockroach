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

import "github.com/cockroachdb/cockroach/pkg/settings"

// SettingValues is a struct that can be populated from test files, via YAML.
type SettingValues struct {
	Rate  float64
	Burst float64

	Read  Factors
	Write Factors
}

// Factors for reads and writes.
type Factors struct {
	Base    float64
	PerByte float64
}

// OverrideSettings sets the cluster setting according to the given
// settingValues.
//
// Uninitialized (zero) values are ignored.
func OverrideSettings(sv *settings.Values, vals SettingValues) {
	override := func(setting *settings.FloatSetting, val float64) {
		if val != 0 {
			setting.Override(sv, val)
		}
	}
	override(kvcuRateLimit, vals.Rate)
	override(kvcuBurstLimitSeconds, vals.Burst/kvcuRateLimit.Get(sv))

	override(readRequestCost, vals.Read.Base)
	override(readCostPerMB, vals.Read.PerByte*1024*1024)
	override(writeRequestCost, vals.Write.Base)
	override(writeCostPerMB, vals.Write.PerByte*1024*1024)
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
