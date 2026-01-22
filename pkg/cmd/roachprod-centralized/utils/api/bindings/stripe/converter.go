// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stripe

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
)

// TypeConverter handles type conversion from string values to target types
type TypeConverter struct{}

// NewTypeConverter creates a new type converter
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// SetValueByType sets a value based on its type, handling special cases for IN/NOT_IN operators
func (tc *TypeConverter) SetValueByType(
	fieldValue reflect.Value, queryValue string, operator filtertypes.FilterOperator,
) error {
	fieldType := fieldValue.Type()

	// Handle slice types - both for IN/NOT_IN operators AND for single values that should become single-element slices
	if fieldType.Kind() == reflect.Slice {
		// For IN/NOT_IN operators, split by comma
		if operator == filtertypes.OpIn || operator == filtertypes.OpNotIn {
			return tc.setSliceValue(fieldValue, queryValue)
		} else {
			// For other operators on slice fields, create a single-element slice
			return tc.setSingleElementSlice(fieldValue, queryValue)
		}
	}

	// For single-type fields with IN/NOT_IN operators, we need special handling
	if operator == filtertypes.OpIn || operator == filtertypes.OpNotIn {
		// Store the comma-separated values as a single string
		// ToFilterSet() will handle the splitting when creating the filter
		fieldValue.SetString(queryValue)
		return nil
	}

	// Handle single values
	return tc.SetSimpleValue(fieldValue, queryValue)
}

// setSliceValue sets a slice value from comma-separated query string
func (tc *TypeConverter) setSliceValue(fieldValue reflect.Value, queryValue string) error {
	values := strings.Split(queryValue, ",")

	slice := reflect.MakeSlice(fieldValue.Type(), len(values), len(values))

	for i, val := range values {
		val = strings.TrimSpace(val)
		elemValue := slice.Index(i)

		if err := tc.SetSimpleValue(elemValue, val); err != nil {
			return errors.Wrapf(err, "failed to set slice element %d", i)
		}
	}

	fieldValue.Set(slice)
	return nil
}

// setSingleElementSlice sets a slice field with a single element (for non-IN/NOT_IN operators on slice fields)
func (tc *TypeConverter) setSingleElementSlice(fieldValue reflect.Value, queryValue string) error {
	slice := reflect.MakeSlice(fieldValue.Type(), 1, 1)
	elemValue := slice.Index(0)

	if err := tc.SetSimpleValue(elemValue, queryValue); err != nil {
		return errors.Wrapf(err, "failed to set single slice element")
	}

	fieldValue.Set(slice)
	return nil
}

// SetSimpleValue sets a simple value (string, int, time.Time, etc.)
func (tc *TypeConverter) SetSimpleValue(fieldValue reflect.Value, value string) error {
	switch fieldValue.Kind() {
	case reflect.String:
		// Handle both regular strings and custom string types (like TaskState)
		if fieldValue.Type() == reflect.TypeOf("") {
			// Regular string
			fieldValue.SetString(value)
		} else {
			// Custom string type - create a value of the correct type
			customStringValue := reflect.New(fieldValue.Type()).Elem()
			customStringValue.SetString(value)
			fieldValue.Set(customStringValue)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid integer value: %s", value)
		}
		fieldValue.SetInt(intVal)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uintVal, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid unsigned integer value: %s", value)
		}
		fieldValue.SetUint(uintVal)
	case reflect.Float32, reflect.Float64:
		floatVal, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid float value: %s", value)
		}
		fieldValue.SetFloat(floatVal)
	case reflect.Bool:
		boolVal, err := strconv.ParseBool(value)
		if err != nil {
			return errors.Wrapf(err, "invalid boolean value: %s", value)
		}
		fieldValue.SetBool(boolVal)
	default:
		// Handle time.Time and other special types
		if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
			timeVal, err := time.Parse(time.RFC3339, value)
			if err != nil {
				// Try parsing as date only
				timeVal, err = time.Parse("2006-01-02", value)
				if err != nil {
					return errors.Wrapf(err, "invalid time value: %s", value)
				}
			}
			fieldValue.Set(reflect.ValueOf(timeVal))
		} else {
			return errors.Newf("unsupported field type: %s", fieldValue.Type())
		}
	}

	return nil
}

// IsZeroValue checks if a reflect.Value is a zero value
func (tc *TypeConverter) IsZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Slice, reflect.Array:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	default:
		return reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
	}
}
