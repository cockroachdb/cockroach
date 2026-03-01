// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"testing"

	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseSCIMFilter_EmptyFilter tests that empty filter returns empty FilterSet
func TestParseSCIMFilter_EmptyFilter(t *testing.T) {
	filterSet, err := ParseSCIMFilter("")

	require.NoError(t, err)
	assert.True(t, filterSet.IsEmpty())
	assert.Len(t, filterSet.Filters, 0)
}

// TestParseSCIMFilter_SimpleEquality tests simple equality filters
func TestParseSCIMFilter_SimpleEquality(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectField string
		expectOp    filtertypes.FilterOperator
		expectValue interface{}
	}{
		{
			name:        "userName eq string",
			input:       `userName eq "john@example.com"`,
			expectField: "userName",
			expectOp:    filtertypes.OpEqual,
			expectValue: "john@example.com",
		},
		{
			name:        "active eq true",
			input:       `active eq true`,
			expectField: "active",
			expectOp:    filtertypes.OpEqual,
			expectValue: true,
		},
		{
			name:        "active eq false",
			input:       `active eq false`,
			expectField: "active",
			expectOp:    filtertypes.OpEqual,
			expectValue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet, err := ParseSCIMFilter(tt.input)
			require.NoError(t, err)
			require.Len(t, filterSet.Filters, 1)

			assert.Equal(t, tt.expectField, filterSet.Filters[0].Field)
			assert.Equal(t, tt.expectOp, filterSet.Filters[0].Operator)
			assert.Equal(t, tt.expectValue, filterSet.Filters[0].Value)
		})
	}
}

// TestParseSCIMFilter_ComparisonOperators tests all SCIM comparison operators
func TestParseSCIMFilter_ComparisonOperators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expectOp filtertypes.FilterOperator
	}{
		{
			name:     "not equal (ne)",
			input:    `status ne "inactive"`,
			expectOp: filtertypes.OpNotEqual,
		},
		{
			name:     "contains (co)",
			input:    `displayName co "john"`,
			expectOp: filtertypes.OpContains,
		},
		{
			name:     "starts with (sw)",
			input:    `userName sw "admin"`,
			expectOp: filtertypes.OpStartsWith,
		},
		{
			name:     "ends with (ew)",
			input:    `email ew "@example.com"`,
			expectOp: filtertypes.OpEndsWith,
		},
		{
			name:     "greater than (gt)",
			input:    `age gt 30`,
			expectOp: filtertypes.OpGreater,
		},
		{
			name:     "greater than or equal (ge)",
			input:    `age ge 18`,
			expectOp: filtertypes.OpGreaterEq,
		},
		{
			name:     "less than (lt)",
			input:    `score lt 100`,
			expectOp: filtertypes.OpLess,
		},
		{
			name:     "less than or equal (le)",
			input:    `score le 50`,
			expectOp: filtertypes.OpLessEq,
		},
		{
			name:     "present (pr)",
			input:    `title pr`,
			expectOp: filtertypes.OpPresent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet, err := ParseSCIMFilter(tt.input)
			require.NoError(t, err)
			require.Len(t, filterSet.Filters, 1)

			assert.Equal(t, tt.expectOp, filterSet.Filters[0].Operator)
		})
	}
}

// TestParseSCIMFilter_LogicalAnd tests AND logical expression
func TestParseSCIMFilter_LogicalAnd(t *testing.T) {
	filterSet, err := ParseSCIMFilter(`userName eq "john" and active eq true`)

	require.NoError(t, err)
	require.Len(t, filterSet.Filters, 2)
	assert.Equal(t, filtertypes.LogicAnd, filterSet.Logic)

	// First filter
	assert.Equal(t, "userName", filterSet.Filters[0].Field)
	assert.Equal(t, filtertypes.OpEqual, filterSet.Filters[0].Operator)
	assert.Equal(t, "john", filterSet.Filters[0].Value)

	// Second filter
	assert.Equal(t, "active", filterSet.Filters[1].Field)
	assert.Equal(t, filtertypes.OpEqual, filterSet.Filters[1].Operator)
	assert.Equal(t, true, filterSet.Filters[1].Value)
}

// TestParseSCIMFilter_LogicalOr tests OR logical expression
func TestParseSCIMFilter_LogicalOr(t *testing.T) {
	filterSet, err := ParseSCIMFilter(`status eq "active" or status eq "pending"`)

	require.NoError(t, err)
	require.Len(t, filterSet.Filters, 2)
	assert.Equal(t, filtertypes.LogicOr, filterSet.Logic)

	// Both filters should have same field with different values
	assert.Equal(t, "status", filterSet.Filters[0].Field)
	assert.Equal(t, "active", filterSet.Filters[0].Value)
	assert.Equal(t, "status", filterSet.Filters[1].Field)
	assert.Equal(t, "pending", filterSet.Filters[1].Value)
}

// TestParseSCIMFilter_SubAttribute tests sub-attribute path (dot notation)
func TestParseSCIMFilter_SubAttribute(t *testing.T) {
	filterSet, err := ParseSCIMFilter(`name.familyName eq "Smith"`)

	require.NoError(t, err)
	require.Len(t, filterSet.Filters, 1)
	assert.Equal(t, "name.familyName", filterSet.Filters[0].Field)
	assert.Equal(t, filtertypes.OpEqual, filterSet.Filters[0].Operator)
	assert.Equal(t, "Smith", filterSet.Filters[0].Value)
}

// TestParseSCIMFilter_NotExpression tests NOT expression with simple negation
func TestParseSCIMFilter_NotExpression(t *testing.T) {
	// NOT on equality should become not equal
	filterSet, err := ParseSCIMFilter(`not (active eq true)`)

	require.NoError(t, err)
	require.Len(t, filterSet.Filters, 1)
	assert.Equal(t, "active", filterSet.Filters[0].Field)
	assert.Equal(t, filtertypes.OpNotEqual, filterSet.Filters[0].Operator)
	assert.Equal(t, true, filterSet.Filters[0].Value)
}

// TestParseSCIMFilter_NotExpression_ComparisonNegation tests negation of comparison operators
func TestParseSCIMFilter_NotExpression_ComparisonNegation(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expectedOp filtertypes.FilterOperator
		originalOp string
	}{
		{
			name:       "not less than becomes greater or equal",
			input:      `not (age lt 18)`,
			expectedOp: filtertypes.OpGreaterEq,
		},
		{
			name:       "not greater than becomes less or equal",
			input:      `not (age gt 65)`,
			expectedOp: filtertypes.OpLessEq,
		},
		{
			name:       "not less or equal becomes greater",
			input:      `not (score le 50)`,
			expectedOp: filtertypes.OpGreater,
		},
		{
			name:       "not greater or equal becomes less",
			input:      `not (score ge 100)`,
			expectedOp: filtertypes.OpLess,
		},
		{
			name:       "not not equal becomes equal",
			input:      `not (status ne "active")`,
			expectedOp: filtertypes.OpEqual,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet, err := ParseSCIMFilter(tt.input)
			require.NoError(t, err)
			require.Len(t, filterSet.Filters, 1)
			assert.Equal(t, tt.expectedOp, filterSet.Filters[0].Operator)
		})
	}
}

// TestParseSCIMFilter_NotExpression_ComplexUnsupported tests that NOT on complex expressions fails
func TestParseSCIMFilter_NotExpression_ComplexUnsupported(t *testing.T) {
	// NOT on a compound expression is not supported
	_, err := ParseSCIMFilter(`not (userName eq "john" and active eq true)`)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOT operator on complex expressions not yet supported")
}

// TestParseSCIMFilter_NotExpression_CannotNegate tests operators that cannot be negated
func TestParseSCIMFilter_NotExpression_CannotNegate(t *testing.T) {
	// contains, starts with, ends with cannot be negated
	tests := []string{
		`not (displayName co "john")`,
		`not (userName sw "admin")`,
		`not (email ew "@example.com")`,
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := ParseSCIMFilter(input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot negate")
		})
	}
}

// TestParseSCIMFilter_InvalidSyntax tests that invalid filter syntax returns error
func TestParseSCIMFilter_InvalidSyntax(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "missing value",
			input: `userName eq`,
		},
		{
			name:  "invalid operator",
			input: `userName == "john"`,
		},
		{
			name:  "completely invalid",
			input: `this is not a valid filter`,
		},
		{
			name:  "unbalanced quotes",
			input: `userName eq "john`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSCIMFilter(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "failed to parse SCIM filter")
		})
	}
}

// TestParseSCIMFilter_MultipleLogicalOperators tests chained logical operators
func TestParseSCIMFilter_MultipleLogicalOperators(t *testing.T) {
	// Note: SCIM filter parser handles operator precedence
	filterSet, err := ParseSCIMFilter(`status eq "active" and role eq "admin" and department eq "engineering"`)

	require.NoError(t, err)
	require.Len(t, filterSet.Filters, 3)
	assert.Equal(t, filtertypes.LogicAnd, filterSet.Logic)
}

// TestParseSCIMFilter_CaseInsensitiveOperators tests that operators are case-insensitive
func TestParseSCIMFilter_CaseInsensitiveOperators(t *testing.T) {
	tests := []struct {
		input    string
		expectOp filtertypes.FilterOperator
	}{
		{`userName EQ "john"`, filtertypes.OpEqual},
		{`userName Eq "john"`, filtertypes.OpEqual},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			filterSet, err := ParseSCIMFilter(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filterSet)
			require.Len(t, filterSet.Filters, 1)
			assert.Equal(t, tt.expectOp, filterSet.Filters[0].Operator)
		})
	}
}

// TestParseSCIMFilter_QuotedStrings tests various quoted string formats
func TestParseSCIMFilter_QuotedStrings(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectValue string
	}{
		{
			name:        "simple string",
			input:       `displayName eq "John Doe"`,
			expectValue: "John Doe",
		},
		{
			name:        "empty string",
			input:       `note eq ""`,
			expectValue: "",
		},
		{
			name:        "string with special chars",
			input:       `email eq "user+test@example.com"`,
			expectValue: "user+test@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet, err := ParseSCIMFilter(tt.input)
			require.NoError(t, err)
			require.Len(t, filterSet.Filters, 1)
			assert.Equal(t, tt.expectValue, filterSet.Filters[0].Value)
		})
	}
}

// TestParseSCIMFilter_NumericValues tests parsing of numeric values
func TestParseSCIMFilter_NumericValues(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "integer",
			input: `count gt 10`,
		},
		{
			name:  "negative integer",
			input: `balance lt -100`,
		},
		{
			name:  "decimal",
			input: `price le 99.99`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterSet, err := ParseSCIMFilter(tt.input)
			require.NoError(t, err)
			require.Len(t, filterSet.Filters, 1)
			// Value should be parsed (exact type depends on parser implementation)
			assert.NotNil(t, filterSet.Filters[0].Value)
		})
	}
}

// TestParseSCIMFilter_Grouping tests parenthesized expressions
func TestParseSCIMFilter_Grouping(t *testing.T) {
	filterSet, err := ParseSCIMFilter(`(status eq "active") and (role eq "admin")`)

	require.NoError(t, err)
	require.Len(t, filterSet.Filters, 2)
	assert.Equal(t, filtertypes.LogicAnd, filterSet.Logic)
}
