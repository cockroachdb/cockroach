// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"reflect"

	"github.com/cockroachdb/errors"
)

// FilterOperator represents the type of comparison operation to perform.
type FilterOperator string

const (
	// OpEqual checks for equality (=)
	OpEqual FilterOperator = "eq"
	// OpNotEqual checks for inequality (!=)
	OpNotEqual FilterOperator = "ne"
	// OpLess checks if field value is less than filter value (<)
	OpLess FilterOperator = "lt"
	// OpLessEq checks if field value is less than or equal to filter value (<=)
	OpLessEq FilterOperator = "le"
	// OpGreater checks if field value is greater than filter value (>)
	OpGreater FilterOperator = "gt"
	// OpGreaterEq checks if field value is greater than or equal to filter value (>=)
	OpGreaterEq FilterOperator = "ge"
	// OpIn checks if field value is in a list of values (IN)
	OpIn FilterOperator = "in"
	// OpNotIn checks if field value is not in a list of values (NOT IN)
	OpNotIn FilterOperator = "not_in"
	// OpLike checks if field value matches a pattern (LIKE for SQL, contains for memory)
	OpLike FilterOperator = "like"
	// OpNotLike checks if field value does not match a pattern (NOT LIKE for SQL, !contains for memory)
	OpNotLike FilterOperator = "not_like"
)

// LogicOperator represents how multiple filters should be combined.
type LogicOperator string

const (
	// LogicAnd requires all filters to match (AND)
	LogicAnd LogicOperator = "and"
	// LogicOr requires at least one filter to match (OR)
	LogicOr LogicOperator = "or"
)

// FieldFilter represents a single filter condition on a field.
type FieldFilter struct {
	// Field is the name of the field to filter on
	Field string `json:"field"`
	// Operator is the comparison operator to use
	Operator FilterOperator `json:"operator"`
	// Value is the value to compare against (can be single value or slice for IN/NOT_IN)
	Value interface{} `json:"value"`
}

// FilterSet represents a collection of filters with a logic operator.
type FilterSet struct {
	// Filters is the list of individual field filters
	Filters []FieldFilter `json:"filters"`
	// Logic determines how filters are combined (default: AND)
	Logic LogicOperator `json:"logic"`
}

// AddFilter adds a new filter to the FilterSet.
// For IN/NOT_IN operators, value should be a slice.
// For other operators on slice values, the first element will be extracted automatically.
func (fs *FilterSet) AddFilter(
	field string, operator FilterOperator, value interface{},
) *FilterSet {
	// Handle operator-specific value processing
	processedValue := value

	// If the value is a slice and the operator is not IN/NOT_IN, extract the first element
	if operator != OpIn && operator != OpNotIn {
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Slice && v.Len() > 0 {
			// For equality and comparison operators, use the first element
			processedValue = v.Index(0).Interface()
		}
	}

	fs.Filters = append(fs.Filters, FieldFilter{
		Field:    field,
		Operator: operator,
		Value:    processedValue,
	})
	return fs
}

// SetLogic sets the logic operator for combining filters.
func (fs *FilterSet) SetLogic(logic LogicOperator) *FilterSet {
	fs.Logic = logic
	return fs
}

// IsEmpty returns true if the FilterSet has no filters.
func (fs *FilterSet) IsEmpty() bool {
	return len(fs.Filters) == 0
}

// Validate validates the FilterSet for consistency and supported operations.
func (fs *FilterSet) Validate() error {
	if fs == nil {
		return nil
	}

	for i, filter := range fs.Filters {
		if err := filter.Validate(); err != nil {
			return errors.Wrapf(err, "filter %d invalid", i)
		}
	}

	if fs.Logic != LogicAnd && fs.Logic != LogicOr {
		return errors.Newf("invalid logic operator: %s", fs.Logic)
	}

	return nil
}

// Validate validates a single FieldFilter.
func (ff *FieldFilter) Validate() error {
	if ff.Field == "" {
		return errors.New("field name cannot be empty")
	}

	// Validate operator
	switch ff.Operator {
	case OpEqual, OpNotEqual, OpLess, OpLessEq, OpGreater, OpGreaterEq, OpLike, OpNotLike:
		// These operators require a single value
		if ff.Value == nil {
			return errors.Newf("operator %s requires a value", ff.Operator)
		}
	case OpIn, OpNotIn:
		// These operators require a slice/array value
		if ff.Value == nil {
			return errors.Newf("operator %s requires a list of values", ff.Operator)
		}
		// Check if value is a slice or array
		v := reflect.ValueOf(ff.Value)
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return errors.Newf("operator %s requires a slice/array value, got %T", ff.Operator, ff.Value)
		}
		if v.Len() == 0 {
			return errors.Newf("operator %s requires at least one value", ff.Operator)
		}
	default:
		return errors.Newf("unsupported operator: %s", ff.Operator)
	}

	return nil
}
