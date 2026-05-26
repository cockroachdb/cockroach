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
	// SCIM-specific operators
	// OpContains checks if field value contains the filter value (SCIM "co")
	OpContains FilterOperator = "co"
	// OpStartsWith checks if field value starts with the filter value (SCIM "sw")
	OpStartsWith FilterOperator = "sw"
	// OpEndsWith checks if field value ends with the filter value (SCIM "ew")
	OpEndsWith FilterOperator = "ew"
	// OpPresent checks if field is present and not null/zero (SCIM "pr")
	OpPresent FilterOperator = "pr"
)

// LogicOperator represents how multiple filters should be combined.
type LogicOperator string

const (
	// LogicAnd requires all filters to match (AND)
	LogicAnd LogicOperator = "and"
	// LogicOr requires at least one filter to match (OR)
	LogicOr LogicOperator = "or"
)

// SortOrder represents the direction of sorting
type SortOrder string

const (
	// SortAscending sorts results in ascending order
	SortAscending SortOrder = "ascending"
	// SortDescending sorts results in descending order
	SortDescending SortOrder = "descending"
)

// PaginationParams represents pagination configuration (SCIM-style, offset-based)
type PaginationParams struct {
	// StartIndex is the 1-based index of the first result (SCIM spec compliant)
	// Default: 1
	StartIndex int `json:"startIndex"`

	// Count is the maximum number of results to return per page
	// Default: -1 (unlimited)
	// -1 means no pagination
	Count int `json:"count"`
}

// GetOffset returns the 0-indexed offset for database queries
func (pp *PaginationParams) GetOffset() int {
	if pp.StartIndex <= 1 {
		return 0
	}
	return pp.StartIndex - 1
}

// GetLimit returns the limit for database queries
// Returns -1 for unlimited (no LIMIT clause)
func (pp *PaginationParams) GetLimit() int {
	return pp.Count
}

// SortParams represents sorting configuration (SCIM-style)
type SortParams struct {
	// SortBy is the attribute to sort by (supports dot notation: "name.givenName")
	SortBy string `json:"sortBy"`

	// SortOrder is the sort direction (ascending or descending)
	// Default: ascending
	SortOrder SortOrder `json:"sortOrder"`
}

// FieldFilter represents a single filter condition on a field.
type FieldFilter struct {
	// Field is the name of the field to filter on
	Field string `json:"field"`
	// Operator is the comparison operator to use
	Operator FilterOperator `json:"operator"`
	// Value is the value to compare against (can be single value or slice for IN/NOT_IN)
	Value interface{} `json:"value"`
}

// FilterSet represents a collection of filters with pagination and sorting
type FilterSet struct {
	// Filters is the list of individual field filters
	Filters []FieldFilter `json:"filters"`
	// Logic determines how filters are combined (default: AND)
	Logic LogicOperator `json:"logic"`

	// SubGroups holds nested FilterSets that are evaluated as
	// self-contained units and combined with the parent's Logic.
	SubGroups []FilterSet `json:"sub_groups,omitempty"`

	// Pagination controls result pagination (optional)
	Pagination *PaginationParams `json:"pagination,omitempty"`

	// Sort controls result ordering (optional)
	Sort *SortParams `json:"sort,omitempty"`
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

// AddSubGroup appends a nested FilterSet as a sub-group.
// Sub-groups are evaluated as self-contained units, then combined
// with the parent's Logic operator.
func (fs *FilterSet) AddSubGroup(sg FilterSet) *FilterSet {
	fs.SubGroups = append(fs.SubGroups, sg)
	return fs
}

// NestFiltersAsSubGroup moves the current flat Filters into a new
// sub-group (preserving the current Logic), then clears flat Filters.
//
// This is useful when wrapping user-provided filters into a group
// before adding mandatory filters at the top level:
//
//	fs.NestFiltersAsSubGroup()
//	fs.SetLogic(LogicAnd)
//	fs.AddFilter("UserID", OpEqual, principalID)
//	// Result: user_id = X AND (user's original filters)
//
// If there are no flat Filters, this is a no-op.
func (fs *FilterSet) NestFiltersAsSubGroup() *FilterSet {
	if len(fs.Filters) == 0 {
		return fs
	}
	subGroup := FilterSet{
		Filters: fs.Filters,
		Logic:   fs.Logic,
	}
	fs.Filters = nil
	fs.SubGroups = append(fs.SubGroups, subGroup)
	return fs
}

// SetLogic sets the logic operator for combining filters.
func (fs *FilterSet) SetLogic(logic LogicOperator) *FilterSet {
	fs.Logic = logic
	return fs
}

// SetPagination sets pagination parameters on the FilterSet
func (fs *FilterSet) SetPagination(startIndex, count int) *FilterSet {
	fs.Pagination = &PaginationParams{
		StartIndex: startIndex,
		Count:      count,
	}
	return fs
}

// SetSort sets sorting parameters on the FilterSet
func (fs *FilterSet) SetSort(sortBy string, sortOrder SortOrder) *FilterSet {
	fs.Sort = &SortParams{
		SortBy:    sortBy,
		SortOrder: sortOrder,
	}
	return fs
}

// IsEmpty returns true if the FilterSet has no filters.
func (fs *FilterSet) IsEmpty() bool {
	return len(fs.Filters) == 0 && len(fs.SubGroups) == 0
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

	// Validate logic operator - use filters.NewFilterSet() to get the default LogicAnd
	if fs.Logic != LogicAnd && fs.Logic != LogicOr {
		return errors.Newf("invalid logic operator: %q (use filters.NewFilterSet() for default LogicAnd)", fs.Logic)
	}

	// Validate pagination (if specified)
	if fs.Pagination != nil {
		if fs.Pagination.StartIndex < 1 {
			return errors.New("startIndex must be >= 1")
		}
		// Count can be -1 for unlimited, or positive for limited
		if fs.Pagination.Count < -1 || fs.Pagination.Count == 0 {
			return errors.New("count must be > 0 or -1 for unlimited")
		}
	}

	// Validate sorting (if specified)
	if fs.Sort != nil {
		if fs.Sort.SortBy == "" {
			return errors.New("sortBy cannot be empty when sort is specified")
		}
		if fs.Sort.SortOrder != SortAscending && fs.Sort.SortOrder != SortDescending {
			return errors.Newf("invalid sortOrder: %s (must be 'ascending' or 'descending')", fs.Sort.SortOrder)
		}
	}

	for i, sg := range fs.SubGroups {
		if err := sg.Validate(); err != nil {
			return errors.Wrapf(err, "sub_group %d invalid", i)
		}
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
	case OpEqual, OpNotEqual, OpLess, OpLessEq, OpGreater, OpGreaterEq, OpLike, OpNotLike, OpContains, OpStartsWith, OpEndsWith:
		// These operators require a single value
		if ff.Value == nil {
			return errors.Newf("operator %s requires a value", ff.Operator)
		}
	case OpPresent:
		// Present operator doesn't require a value (it checks for field existence)
		// Value is ignored if provided
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
