// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stripe

import (
	"testing"
	"time"

	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/stretchr/testify/assert"
)

// Test struct to verify validation works with FilterValue
type TestFilters struct {
	State FilterValue[string] `stripe:"state" validate:"omitempty,oneof=pending running done failed"`
	Type  FilterValue[string] `stripe:"type" validate:"omitempty,alphanum,max=50"`
}

func TestStripeQueryBinding_Validation(t *testing.T) {
	binding := NewStripeQueryBinding()

	tests := []struct {
		name        string
		queryParams map[string][]string
		expectError bool
		description string
	}{
		{
			name: "valid state values",
			queryParams: map[string][]string{
				"state[in]": {"pending,running,done"},
			},
			expectError: false,
			description: "should accept valid state values in array",
		},
		{
			name: "valid single state value",
			queryParams: map[string][]string{
				"state": {"pending"},
			},
			expectError: false,
			description: "should accept valid single state value",
		},
		{
			name: "invalid state value",
			queryParams: map[string][]string{
				"state": {"invalid"},
			},
			expectError: true,
			description: "should reject invalid state value",
		},
		{
			name: "invalid state in array",
			queryParams: map[string][]string{
				"state[in]": {"pending,invalid,done"},
			},
			expectError: true,
			description: "should reject array containing invalid state value",
		},
		{
			name: "valid type",
			queryParams: map[string][]string{
				"type": {"test123"},
			},
			expectError: false,
			description: "should accept valid alphanumeric type",
		},
		{
			name: "invalid type (too long)",
			queryParams: map[string][]string{
				"type": {"this_is_a_very_long_type_name_that_exceeds_fifty_characters_limit"},
			},
			expectError: true,
			description: "should reject type that exceeds 50 characters",
		},
		{
			name:        "empty values",
			queryParams: map[string][]string{},
			expectError: false,
			description: "should accept empty query params",
		},
		{
			name: "unknown field",
			queryParams: map[string][]string{
				"unknown_field": {"some_value"},
			},
			expectError: true,
			description: "should reject unknown field names",
		},
		{
			name: "unknown field with operator",
			queryParams: map[string][]string{
				"unknown_field[gte]": {"2024-01-01"},
			},
			expectError: true,
			description: "should reject unknown field names with operators",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filters TestFilters
			err := binding.BindQuery(&filters, tt.queryParams)

			if tt.expectError && err == nil {
				t.Errorf("Test '%s': %s - Expected error but got none", tt.name, tt.description)
			}
			if !tt.expectError && err != nil {
				t.Errorf("Test '%s': %s - Expected no error but got: %v", tt.name, tt.description, err)
			}
		})
	}
}

func TestFilterValue_ToFilterSet(t *testing.T) {
	tests := []struct {
		name     string
		filters  TestFilters
		expected []struct {
			field    string
			operator filtertypes.FilterOperator
			value    interface{}
		}
	}{
		{
			name: "IN operator with comma-separated values",
			filters: TestFilters{
				State: FilterValue[string]{
					Value:    "pending,running",
					Operator: filtertypes.OpIn,
				},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpIn, []string{"pending", "running"}},
			},
		},
		{
			name: "Equal operator with single value",
			filters: TestFilters{
				State: FilterValue[string]{
					Value:    "pending",
					Operator: filtertypes.OpEqual,
				},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpEqual, "pending"},
			},
		},
		{
			name: "String field with equal operator",
			filters: TestFilters{
				Type: FilterValue[string]{
					Value:    "test",
					Operator: filtertypes.OpEqual,
				},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"Type", filtertypes.OpEqual, "test"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the actual ToFilterSet function
			filterSet := ToFilterSet(tt.filters)

			if len(filterSet.Filters) != len(tt.expected) {
				t.Errorf("Expected %d filters, got %d", len(tt.expected), len(filterSet.Filters))
				return
			}

			for i, expected := range tt.expected {
				filter := filterSet.Filters[i]
				if filter.Field != expected.field || filter.Operator != expected.operator {
					t.Errorf("Filter %d incorrect: expected field=%s operator=%s, got field=%s operator=%s",
						i, expected.field, expected.operator, filter.Field, filter.Operator)
				}

				// Check value with type consideration
				if !equalValues(filter.Value, expected.value) {
					t.Errorf("Filter %d value incorrect: expected %v (%T), got %v (%T)",
						i, expected.value, expected.value, filter.Value, filter.Value)
				}
			}
		})
	}
}

// equalValues compares two values for equality, handling type differences
func equalValues(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle slice comparison
	if aSlice, ok := a.([]string); ok {
		if bSlice, ok := b.([]string); ok {
			if len(aSlice) != len(bSlice) {
				return false
			}
			for i, v := range aSlice {
				if v != bSlice[i] {
					return false
				}
			}
			return true
		}
		return false
	}

	// Handle string comparison
	if aStr, ok := a.(string); ok {
		if bStr, ok := b.(string); ok {
			return aStr == bStr
		}
		return false
	}

	return a == b
}

func TestToFilterSet(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected []struct {
			field    string
			operator filtertypes.FilterOperator
			value    interface{}
		}
	}{
		{
			name: "empty struct",
			input: struct {
				Name FilterValue[string] `stripe:"name"`
			}{},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{},
		},
		{
			name: "single field with value",
			input: struct {
				Name FilterValue[string] `stripe:"name"`
			}{
				Name: FilterValue[string]{
					Value:    "test",
					Operator: filtertypes.OpEqual,
				},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"Name", filtertypes.OpEqual, "test"},
			},
		},
		{
			name: "multiple fields with mixed empty/filled",
			input: struct {
				Name  FilterValue[string]   `stripe:"name"`
				Count FilterValue[int]      `stripe:"count"`
				Tags  FilterValue[[]string] `stripe:"tags"`
			}{
				Name: FilterValue[string]{
					Value:    "test",
					Operator: filtertypes.OpEqual,
				},
				Count: FilterValue[int]{}, // Empty
				Tags: FilterValue[[]string]{
					Value:    []string{"tag1", "tag2"},
					Operator: filtertypes.OpIn,
				},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"Name", filtertypes.OpEqual, "test"},
				{"Tags", filtertypes.OpIn, []string{"tag1", "tag2"}},
			},
		},
		{
			name: "non-FilterValue fields ignored",
			input: struct {
				Name    FilterValue[string] `stripe:"name"`
				Regular string              `stripe:"regular"`
			}{
				Name: FilterValue[string]{
					Value:    "test",
					Operator: filtertypes.OpEqual,
				},
				Regular: "ignored",
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"Name", filtertypes.OpEqual, "test"},
			},
		},
		{
			name: "fields without stripe tags ignored",
			input: struct {
				Name   FilterValue[string] `stripe:"name"`
				Hidden FilterValue[string]
			}{
				Name: FilterValue[string]{
					Value:    "test",
					Operator: filtertypes.OpEqual,
				},
				Hidden: FilterValue[string]{
					Value:    "hidden",
					Operator: filtertypes.OpEqual,
				},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"Name", filtertypes.OpEqual, "test"},
			},
		},
		{
			name: "pointer to struct",
			input: &struct {
				Name FilterValue[string] `stripe:"name"`
			}{
				Name: FilterValue[string]{
					Value:    "test",
					Operator: filtertypes.OpEqual,
				},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"Name", filtertypes.OpEqual, "test"},
			},
		},
		{
			name: "real world usage - tasks InputGetAllDTO",
			input: struct {
				Type             FilterValue[string]    `stripe:"type"`
				State            FilterValue[string]    `stripe:"state"`
				CreationDatetime FilterValue[time.Time] `stripe:"creation_datetime"`
			}{
				Type: FilterValue[string]{
					Value:    "backup",
					Operator: filtertypes.OpEqual,
				},
				State: FilterValue[string]{
					Value:    "pending,running",
					Operator: filtertypes.OpIn,
				},
				// CreationDatetime left empty to test mixed scenarios
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"Type", filtertypes.OpEqual, "backup"},
				{"State", filtertypes.OpIn, []string{"pending", "running"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToFilterSet(tt.input)

			assert.Len(t, result.Filters, len(tt.expected), "Number of filters should match")

			for i, expectedFilter := range tt.expected {
				assert.Equal(t, expectedFilter.field, result.Filters[i].Field, "Field name should match")
				assert.Equal(t, expectedFilter.operator, result.Filters[i].Operator, "Operator should match")
				assert.Equal(t, expectedFilter.value, result.Filters[i].Value, "Value should match")
			}
		})
	}
}

func TestSmartFilterValueBehavior(t *testing.T) {
	// Test the complete smart FilterValue workflow:
	// 1. Parse query parameters into FilterValue[string] fields
	// 2. Convert to FilterSet using ToFilterSet() which handles IN/NOT_IN conversion

	tests := []struct {
		name        string
		queryParams map[string][]string
		expected    []struct {
			field    string
			operator filtertypes.FilterOperator
			value    interface{}
		}
	}{
		{
			name: "single value uses equal operator",
			queryParams: map[string][]string{
				"state": {"pending"},
				"type":  {"backup"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpEqual, "pending"},
				{"Type", filtertypes.OpEqual, "backup"},
			},
		},
		{
			name: "IN operator with comma-separated values gets converted to slice",
			queryParams: map[string][]string{
				"state[in]": {"pending,running,done"},
				"type":      {"backup"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpIn, []string{"pending", "running", "done"}},
				{"Type", filtertypes.OpEqual, "backup"},
			},
		},
		{
			name: "NOT_IN operator with comma-separated values gets converted to slice",
			queryParams: map[string][]string{
				"state[not_in]": {"failed,done"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpNotIn, []string{"failed", "done"}},
			},
		},
		{
			name: "other operators work with single values",
			queryParams: map[string][]string{
				"state[ne]": {"failed"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpNotEqual, "failed"},
			},
		},
	}

	binding := NewStripeQueryBinding()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Bind query parameters to struct
			var filters TestFilters
			err := binding.BindQuery(&filters, tt.queryParams)
			assert.NoError(t, err, "Query binding should succeed")

			// Step 2: Convert to FilterSet
			filterSet := ToFilterSet(filters)

			// Step 3: Verify the results
			assert.Len(t, filterSet.Filters, len(tt.expected), "Number of filters should match")

			for i, expectedFilter := range tt.expected {
				filter := filterSet.Filters[i]
				assert.Equal(t, expectedFilter.field, filter.Field, "Field name should match")
				assert.Equal(t, expectedFilter.operator, filter.Operator, "Operator should match")
				assert.Equal(t, expectedFilter.value, filter.Value, "Value should match")
			}
		})
	}
}

// TestCustomStringTypes tests the handling of custom string types (like TaskState)
// This ensures the fixes for binding custom types work correctly
func TestCustomStringTypes(t *testing.T) {
	// Using a local type to simulate tasks.TaskState
	type CustomState string

	const (
		StatePending CustomState = "pending"
		StateRunning CustomState = "running"
		StateDone    CustomState = "done"
		StateFailed  CustomState = "failed"
	)

	type CustomTypeFilters struct {
		State FilterValue[CustomState] `stripe:"state"`
		Type  FilterValue[string]      `stripe:"type"`
	}

	tests := []struct {
		name        string
		queryParams map[string][]string
		expected    []struct {
			field    string
			operator filtertypes.FilterOperator
			value    interface{}
		}
	}{
		{
			name: "simple custom string type query",
			queryParams: map[string][]string{
				"state": {"done"},
				"type":  {"test"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpEqual, StateDone},
				{"Type", filtertypes.OpEqual, "test"},
			},
		},
		{
			name: "bracket notation with single custom type value",
			queryParams: map[string][]string{
				"state[in]": {"done"},
				"type":      {"test"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpIn, []CustomState{StateDone}},
				{"Type", filtertypes.OpEqual, "test"},
			},
		},
		{
			name: "bracket notation with multiple custom type values",
			queryParams: map[string][]string{
				"state[in]": {"pending,running,done"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpIn, []CustomState{StatePending, StateRunning, StateDone}},
			},
		},
		{
			name: "bracket notation with NOT_IN and custom types",
			queryParams: map[string][]string{
				"state[nin]": {"failed,done"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpNotIn, []CustomState{StateFailed, StateDone}},
			},
		},
		{
			name: "mixed simple and complex filters with custom types",
			queryParams: map[string][]string{
				"type":      {"backup"},
				"state[ne]": {"failed"},
			},
			expected: []struct {
				field    string
				operator filtertypes.FilterOperator
				value    interface{}
			}{
				{"State", filtertypes.OpNotEqual, StateFailed},
				{"Type", filtertypes.OpEqual, "backup"},
			},
		},
	}

	binding := NewStripeQueryBinding()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Bind query parameters to struct
			var filters CustomTypeFilters
			err := binding.BindQuery(&filters, tt.queryParams)
			assert.NoError(t, err, "Query binding should succeed")

			// Step 2: Convert to FilterSet
			filterSet := ToFilterSet(filters)

			// Step 3: Verify the results
			assert.Len(t, filterSet.Filters, len(tt.expected), "Number of filters should match")

			// Find each expected filter and verify it
			for _, expectedFilter := range tt.expected {
				found := false
				for _, actualFilter := range filterSet.Filters {
					if actualFilter.Field == expectedFilter.field {
						assert.Equal(t, expectedFilter.operator, actualFilter.Operator, "Operator should match for field %s", expectedFilter.field)
						assert.Equal(t, expectedFilter.value, actualFilter.Value, "Value should match for field %s", expectedFilter.field)
						found = true
						break
					}
				}
				assert.True(t, found, "Expected filter for field %s not found", expectedFilter.field)
			}
		})
	}
}

// TestBracketNotationEdgeCases tests edge cases for bracket notation parsing
func TestBracketNotationEdgeCases(t *testing.T) {
	type EdgeCaseFilters struct {
		State FilterValue[string] `stripe:"state"`
	}

	tests := []struct {
		name        string
		queryParams map[string][]string
		expectError bool
		expected    interface{}
	}{
		{
			name: "empty value in bracket notation",
			queryParams: map[string][]string{
				"state[in]": {""},
			},
			expectError: false,
			expected:    []string{""}, // Empty string should still create a slice with one empty element
		},
		{
			name: "whitespace trimming in bracket notation",
			queryParams: map[string][]string{
				"state[in]": {" pending , running , done "},
			},
			expectError: false,
			expected:    []string{"pending", "running", "done"}, // Should trim whitespace
		},
		{
			name: "single comma-separated value with trailing comma",
			queryParams: map[string][]string{
				"state[in]": {"pending,"},
			},
			expectError: false,
			expected:    []string{"pending", ""}, // Trailing comma creates empty element
		},
	}

	binding := NewStripeQueryBinding()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filters EdgeCaseFilters
			err := binding.BindQuery(&filters, tt.queryParams)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			filterSet := ToFilterSet(filters)

			if len(filterSet.Filters) > 0 {
				assert.Equal(t, tt.expected, filterSet.Filters[0].Value)
			}
		})
	}
}
