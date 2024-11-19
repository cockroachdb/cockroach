// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostmodel

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestParseRegionalCostMultiplierTableSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name  string
		input string
		err   string
	}{
		{
			name:  "empty",
			input: "",
		},
		{
			name:  "empty_json",
			input: `{"regionPairs":[]}`,
		},
		{
			name: "valid",
			input: `{"regionPairs": [
				{"fromRegion": "us-central1", "toRegion": "us-west1", "cost": 10},
				{"fromRegion": "us-west1", "toRegion": "us-central1", "cost": 20}
			]}`,
		},
		{
			name:  "malformed",
			input: "testing",
			err:   "invalid character",
		},
		{
			name: "entry_missing_from_region",
			input: `{"regionPairs": [
				{"toRegion": "us-central1", "cost": 0}
			]}`,
			err: "entry 0 is missing 'fromRegion'",
		},
		{
			name: "entry_missing_to_region",
			input: `{"regionPairs": [
				{"fromRegion": "us-central1", "cost": 0}
			]}`,
			err: "entry 0 is missing 'toRegion'",
		},
		{
			name: "loopback_entry",
			input: `{"regionPairs": [
				{"fromRegion": "us-central1", "toRegion": "us-central1", "cost": 0}
			]}`,
			err: "'us-central1' contains an entry for itself",
		},
		{
			name: "entry_negative_cost",
			input: `{"regionPairs": [
				{"fromRegion": "us-central1", "toRegion": "us-west1", "cost": 10},
				{"fromRegion": "us-west1", "toRegion": "us-central1", "cost": -20}
			]}`,
			err: "must not be negative",
		},
		{
			name: "missing_region",
			input: `{"regionPairs": [
				{"fromRegion": "us-central1", "toRegion": "us-west1", "cost": 0}
			]}`,
			err: "missing from region 'us-west1' to region 'us-central1'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CrossRegionNetworkCostSetting.Validate(nil, tc.input)
			if tc.err != "" {
				require.Regexp(t, tc.err, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegionalCostMultiplierTable_CostMultiplier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	table, err := NewNetworkCostTable(`
		{"regionPairs": [
				{"fromRegion": "us-central1", "toRegion": "us-west1", "cost": 10},
				{"fromRegion": "us-west1", "toRegion": "us-central1", "cost": 20}
		]}
	`)
	require.NoError(t, err)

	cost, found := table.Matrix[NetworkPath{
		FromRegion: "us-central1",
		ToRegion:   "us-west1",
	}]
	require.True(t, found)
	require.Equal(t, cost, NetworkCost(10))

	cost, found = table.Matrix[NetworkPath{
		FromRegion: "us-west1",
		ToRegion:   "us-central1",
	}]
	require.True(t, found)
	require.Equal(t, cost, NetworkCost(20))

	cost = table.Matrix[NetworkPath{
		FromRegion: "does-not-exist",
		ToRegion:   "also-does-not-exist",
	}]
	require.Equal(t, cost, NetworkCost(0))
}

func TestEstimatedCPUSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name    string
		jsonStr string
		err     string
	}{
		{
			name:    "empty string",
			jsonStr: ``,
		},
		{
			name:    "invalid JSON",
			jsonStr: `{"foo":`,
			err:     "unexpected end of JSON input",
		},
		{
			name: "valid JSON",
			jsonStr: func() string {
				bytes, err := json.Marshal(&DefaultEstimatedCPUModel)
				require.NoError(t, err)
				return string(bytes)
			}(),
		},
		{
			name:    "mismatched ReadRequestCost field lengths",
			jsonStr: `{"ReadRequestCost": {"BatchSize": [], "CPUPerRequest": [1]}}`,
			err:     "estimated_cpu model lookup arrays cannot have different lengths",
		},
		{
			name:    "mismatched ReadBytesCost field lengths",
			jsonStr: `{"ReadBytesCost": {"PayloadSize": [1], "CPUPerByte": [1, 2]}}`,
			err:     "estimated_cpu model lookup arrays cannot have different lengths",
		},
		{
			name:    "mismatched WriteBatchCost field lengths",
			jsonStr: `{"WriteBatchCost": {"RatePerNode": [1, 2], "CPUPerRequest": [1]}}`,
			err:     "estimated_cpu model lookup arrays cannot have different lengths",
		},
		{
			name:    "mismatched WriteRequestCost field lengths",
			jsonStr: `{"WriteRequestCost": {"BatchSize": [], "CPUPerRequest": [1]}}`,
			err:     "estimated_cpu model lookup arrays cannot have different lengths",
		},
		{
			name:    "mismatched WriteBytesCost field lengths",
			jsonStr: `{"WriteBytesCost": {"PayloadSize": [1], "CPUPerByte": []}}`,
			err:     "estimated_cpu model lookup arrays cannot have different lengths",
		},
		{
			name: "Background CPU Amortization cannot be zero",
			jsonStr: `{"WriteBytesCost": {"PayloadSize": [1], "CPUPerByte": [2]},
						"BackgroundCPU": {"Amount": 0.5, "Amortization": 0}}`,
			err: "estimated_cpu model cannot use zero or negative CPU amortization",
		},
		{
			name: "Background CPU Amortization cannot be negative",
			jsonStr: `{"WriteBytesCost": {"PayloadSize": [1], "CPUPerByte": [2]},
						"BackgroundCPU": {"Amount": 0.5, "Amortization": -5}}`,
			err: "estimated_cpu model cannot use zero or negative CPU amortization",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := EstimatedCPUCostSetting.Validate(nil, tc.jsonStr)
			if tc.err != "" {
				require.Regexp(t, tc.err, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
