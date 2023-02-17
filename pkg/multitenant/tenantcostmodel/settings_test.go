// Copyright 2023 The Cockroach Authors.
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
			name:  "malformed",
			input: "testing",
			err:   "invalid character",
		},
		{
			name:  "valid_json",
			input: `{"regions":["asia-southeast1"],"matrix":[[1,1.5,2.6],[1,3.5],[1]]}`,
			err:   "unexpected number of rows",
		},
		{
			name:  "empty_json",
			input: `{"regions":[],"matrix":[]}`,
		},
		{
			name:  "valid",
			input: `{"regions":["us-east1","eu-central1","asia-southeast1"],"matrix":[[1,1.5,2.6],[1,3.5],[1]]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table, err := parseRegionalCostMultiplierTableSetting(tc.input)
			if tc.err != "" {
				require.Regexp(t, tc.err, err.Error())
			} else {
				require.NoError(t, err)
				require.NotNil(t, table)
			}
		})
	}
}

func TestRegionalCostMultiplierCompactTable_Validate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name  string
		table *RegionalCostMultiplierCompactTable
		err   string
	}{
		{
			name: "mismatched_rows",
			table: &RegionalCostMultiplierCompactTable{
				Regions: []string{"a"},
			},
			err: "unexpected number of rows: found 1 regions, but 0 rows in matrix",
		},
		{
			name: "invalid_matrix_start",
			table: &RegionalCostMultiplierCompactTable{
				Regions: []string{"a"},
				Matrix: [][]RUMultiplier{
					{1, 2},
				},
			},
			err: "malformed cost multiplier matrix",
		},
		{
			name: "invalid_matrix_end",
			table: &RegionalCostMultiplierCompactTable{
				Regions: []string{"a", "b", "c"},
				Matrix: [][]RUMultiplier{
					{1, 2, 3},
					{2, 3},
					{},
				},
			},
			err: "malformed cost multiplier matrix",
		},
		{
			name: "negative values",
			table: &RegionalCostMultiplierCompactTable{
				Regions: []string{"a", "b", "c"},
				Matrix: [][]RUMultiplier{
					{2, -3, 4},
					{3, 4},
					{4},
				},
			},
			err: "values must be non-negative",
		},
		{
			name: "valid",
			table: &RegionalCostMultiplierCompactTable{
				Regions: []string{"a", "b", "c", "d"},
				Matrix: [][]RUMultiplier{
					{1, 2, 3, 4},
					{2, 3, 4},
					{3, 4},
					{0},
				},
			},
		},
		{
			name:  "valid_empty",
			table: &RegionalCostMultiplierCompactTable{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.table.Validate()
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegionalCostMultiplierTable_CostMultiplier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name     string
		table    *RegionalCostMultiplierTable
		r1       string
		r2       string
		expected RUMultiplier
	}{
		{
			name:     "nil_matrix",
			table:    &RegionalCostMultiplierTable{},
			r1:       "a",
			r2:       "b",
			expected: 1,
		},
		{
			name: "no_row_key",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{},
			},
			r1:       "a",
			r2:       "b",
			expected: 1,
		},
		{
			name: "nil_row",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": nil,
				},
			},
			r1:       "a",
			r2:       "b",
			expected: 1,
		},
		{
			name: "no_col_key",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": {},
				},
			},
			r1:       "a",
			r2:       "b",
			expected: 1,
		},
		{
			name: "found",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": {
						"b": 100,
					},
				},
			},
			r1:       "a",
			r2:       "b",
			expected: 100,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.table.CostMultiplier(tc.r1, tc.r2))
		})
	}
}

func TestRegionalCostMultiplierTable_Validate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name  string
		table *RegionalCostMultiplierTable
		err   string
	}{
		{
			name:  "nil_matrix",
			table: &RegionalCostMultiplierTable{},
		},
		{
			name: "nil_inner_matrix",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": nil,
				},
			},
			err: "inner matrix cannot be nil",
		},
		{
			name: "more_rows_than_cols",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": {
						"b": 100,
					},
					"b": {
						"a": 100,
					},
				},
			},
			err: "number of rows and columns do not match: rows=2, cols=1",
		},
		{
			name: "more_cols_than_rows",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": {
						"b": 100,
						"c": 101,
					},
				},
			},
			err: "number of rows and columns do not match: rows=1, cols=2",
		},
		{
			name: "not_symmetrical",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": {
						"a": 10,
						"b": 100,
					},
					"b": {
						"a": 40,
						"b": 10,
					},
				},
			},
			err: "table values are invalid",
		},
		{
			name: "negative_values",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": {
						"a": -10,
						"b": 100,
					},
					"b": {
						"a": 100,
						"b": -20,
					},
				},
			},
			err: "table values must be non-negative",
		},
		{
			name: "valid",
			table: &RegionalCostMultiplierTable{
				Matrix: map[string]map[string]RUMultiplier{
					"a": {
						"a": 10,
						"b": 55,
						"c": 66,
					},
					"b": {
						"a": 55,
						"b": 10,
						"c": 77,
					},
					"c": {
						"a": 66,
						"b": 77,
						"c": 10,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.table.Validate()
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMakeRegionalCostMultiplierTableFromCompact(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("empty", func(t *testing.T) {
		compact := &RegionalCostMultiplierCompactTable{}
		require.NoError(t, compact.Validate())

		table := MakeRegionalCostMultiplierTableFromCompact(compact)
		expectedTable := RegionalCostMultiplierTable{
			Matrix: map[string]map[string]RUMultiplier{},
		}
		require.Equal(t, expectedTable, table)
		require.NoError(t, table.Validate())
	})

	t.Run("single region", func(t *testing.T) {
		compact := &RegionalCostMultiplierCompactTable{
			Regions: []string{"us-east1"},
			Matrix:  [][]RUMultiplier{{1.9}},
		}
		require.NoError(t, compact.Validate())

		table := MakeRegionalCostMultiplierTableFromCompact(compact)
		expectedTable := RegionalCostMultiplierTable{
			Matrix: map[string]map[string]RUMultiplier{
				"us-east1": {
					"us-east1": 1.9,
				},
			},
		}
		require.Equal(t, expectedTable, table)
		require.NoError(t, table.Validate())
	})

	t.Run("multiple regions", func(t *testing.T) {
		compact := &RegionalCostMultiplierCompactTable{
			Regions: []string{"us-east1", "eu-central1", "asia-southeast1"},
			Matrix: [][]RUMultiplier{
				{1, 1.5, 2.6},
				{1, 3.5},
				{1},
			},
		}
		require.NoError(t, compact.Validate())

		table := MakeRegionalCostMultiplierTableFromCompact(compact)
		expectedTable := RegionalCostMultiplierTable{
			Matrix: map[string]map[string]RUMultiplier{
				"us-east1": {
					"us-east1":        1,
					"eu-central1":     1.5,
					"asia-southeast1": 2.6,
				},
				"eu-central1": {
					"us-east1":        1.5,
					"eu-central1":     1,
					"asia-southeast1": 3.5,
				},
				"asia-southeast1": {
					"us-east1":        2.6,
					"eu-central1":     3.5,
					"asia-southeast1": 1,
				},
			},
		}
		require.Equal(t, expectedTable, table)
		require.NoError(t, table.Validate())
	})
}

func TestMakeRegionalCostMultiplierCompactTableFromExpanded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("empty", func(t *testing.T) {
		expanded := &RegionalCostMultiplierTable{}
		require.NoError(t, expanded.Validate())

		table := MakeRegionalCostMultiplierCompactTableFromExpanded(expanded)
		expectedTable := RegionalCostMultiplierCompactTable{
			Regions: []string{},
			Matrix:  [][]RUMultiplier{},
		}
		require.Equal(t, expectedTable, table)
		require.NoError(t, table.Validate())
	})

	t.Run("single region", func(t *testing.T) {
		expanded := &RegionalCostMultiplierTable{
			Matrix: map[string]map[string]RUMultiplier{
				"eu-central1": {
					"eu-central1": 42,
				},
			},
		}
		require.NoError(t, expanded.Validate())

		table := MakeRegionalCostMultiplierCompactTableFromExpanded(expanded)
		expectedTable := RegionalCostMultiplierCompactTable{
			Regions: []string{"eu-central1"},
			Matrix:  [][]RUMultiplier{{42}},
		}
		require.Equal(t, expectedTable, table)
		require.NoError(t, table.Validate())
	})

	t.Run("multiple regions", func(t *testing.T) {
		expanded := &RegionalCostMultiplierTable{
			Matrix: map[string]map[string]RUMultiplier{
				"us-east1": {
					"us-east1":        1,
					"eu-central1":     1.5,
					"asia-southeast1": 2.6,
				},
				"eu-central1": {
					"us-east1":        1.5,
					"eu-central1":     1,
					"asia-southeast1": 3.5,
				},
				"asia-southeast1": {
					"us-east1":        2.6,
					"eu-central1":     3.5,
					"asia-southeast1": 1,
				},
			},
		}
		require.NoError(t, expanded.Validate())

		table := MakeRegionalCostMultiplierCompactTableFromExpanded(expanded)
		expectedTable := RegionalCostMultiplierCompactTable{
			Regions: []string{"asia-southeast1", "eu-central1", "us-east1"},
			Matrix:  [][]RUMultiplier{{1, 3.5, 2.6}, {1, 1.5}, {1}},
		}
		require.Equal(t, expectedTable, table)
		require.NoError(t, table.Validate())
	})
}
