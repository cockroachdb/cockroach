// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterSetCreation(t *testing.T) {
	tests := []struct {
		name               string
		filterSet          *filtertypes.FilterSet
		expectedFilters    int
		expectedLogic      filtertypes.LogicOperator
		expectedFirstField string
		expectedFirstValue interface{}
	}{
		{
			name:            "nil filter set",
			filterSet:       nil,
			expectedFilters: 0,
		},
		{
			name:            "empty filter set",
			filterSet:       filters.NewFilterSet(),
			expectedFilters: 0,
			expectedLogic:   filtertypes.LogicAnd,
		},
		{
			name: "single state filter",
			filterSet: filters.NewFilterSet().
				AddFilter("State", filtertypes.OpEqual, "pending"),
			expectedFilters:    1,
			expectedLogic:      filtertypes.LogicAnd,
			expectedFirstField: "State",
			expectedFirstValue: "pending",
		},
		{
			name: "multiple filters with AND logic",
			filterSet: filters.NewFilterSet().
				AddFilter("State", filtertypes.OpEqual, "running").
				AddFilter("Type", filtertypes.OpEqual, "backup"),
			expectedFilters:    2,
			expectedLogic:      filtertypes.LogicAnd,
			expectedFirstField: "State",
			expectedFirstValue: "running",
		},
		{
			name: "complex filters with OR logic",
			filterSet: filters.NewFilterSet().
				AddFilter("CreationDatetime", filtertypes.OpGreater, "2024-01-01").
				AddFilter("State", filtertypes.OpIn, []string{"pending", "running"}).
				SetLogic(filtertypes.LogicOr),
			expectedFilters:    2,
			expectedLogic:      filtertypes.LogicOr,
			expectedFirstField: "CreationDatetime",
			expectedFirstValue: "2024-01-01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectedFilters == 0 {
				if tt.filterSet == nil {
					assert.Nil(t, tt.filterSet)
				} else {
					assert.True(t, tt.filterSet.IsEmpty())
				}
				return
			}

			require.NotNil(t, tt.filterSet)
			assert.Len(t, tt.filterSet.Filters, tt.expectedFilters)
			assert.Equal(t, tt.expectedLogic, tt.filterSet.Logic)

			if tt.expectedFilters > 0 {
				assert.Equal(t, tt.expectedFirstField, tt.filterSet.Filters[0].Field)
				assert.Equal(t, tt.expectedFirstValue, tt.filterSet.Filters[0].Value)
			}
		})
	}
}

func TestTaskFilteringExamples(t *testing.T) {
	tests := []struct {
		name      string
		filterSet *filtertypes.FilterSet
		valid     bool
	}{
		{
			name: "filter by state and type",
			filterSet: filters.NewFilterSet().
				AddFilter("State", filtertypes.OpEqual, "pending").
				AddFilter("Type", filtertypes.OpEqual, "backup"),
			valid: true,
		},
		{
			name: "filter by creation date range",
			filterSet: filters.NewFilterSet().
				AddFilter("CreationDatetime", filtertypes.OpGreaterEq, "2024-01-01").
				AddFilter("CreationDatetime", filtertypes.OpLess, "2024-02-01"),
			valid: true,
		},
		{
			name: "filter by multiple states with OR",
			filterSet: filters.NewFilterSet().
				AddFilter("State", filtertypes.OpIn, []string{"pending", "running"}).
				SetLogic(filtertypes.LogicOr),
			valid: true,
		},
		{
			name: "complex filter with mixed operators",
			filterSet: filters.NewFilterSet().
				AddFilter("State", filtertypes.OpNotIn, []string{"failed", "cancelled"}).
				AddFilter("CreationDatetime", filtertypes.OpGreater, "2024-01-01").
				AddFilter("Type", filtertypes.OpLike, "backup"),
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.filterSet.Validate()
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
