// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stripe

import (
	"net/http"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/gin-gonic/gin/binding"
)

// FilterValue represents a typed query parameter value paired with a filter operator.
// This enables sophisticated filtering through URL query parameters while maintaining type safety.
type FilterValue[T any] struct {
	Value    T
	Operator filtertypes.FilterOperator
}

// IsEmpty returns true if the FilterValue has no value set
func (fv FilterValue[T]) IsEmpty() bool {
	var zero T
	return reflect.DeepEqual(fv.Value, zero)
}

// StripeQueryBinding implements Gin's Binding interface to parse Stripe-style query parameters.
// This binding supports complex filtering operations through URL query parameters using
// bracket notation similar to Stripe's API (e.g., "filter[status]=active&filter[type]=premium").
type StripeQueryBinding struct {
	parser    *QueryParser
	validator *Validator
}

// NewStripeQueryBinding creates a new StripeQueryBinding with all components
func NewStripeQueryBinding() *StripeQueryBinding {
	return &StripeQueryBinding{
		parser:    NewQueryParser(),
		validator: NewValidator(),
	}
}

// StripeQuery is a global instance of StripeQueryBinding for use with Gin's ShouldBindWith
var StripeQuery = NewStripeQueryBinding()

// Name returns the name of the binding
func (StripeQueryBinding) Name() string {
	return "stripe-query"
}

// Bind parses Stripe-style query parameters into struct fields
func (b StripeQueryBinding) Bind(req *http.Request, obj interface{}) error {
	values := req.URL.Query()
	return b.BindQuery(obj, values)
}

// BindQuery parses query parameters into the given object
func (b StripeQueryBinding) BindQuery(obj interface{}, values map[string][]string) error {
	if err := b.parser.ParseStripeQuery(obj, values); err != nil {
		return err
	}

	// Run validation using the standard validator
	if binding.Validator != nil {
		if err := binding.Validator.ValidateStruct(obj); err != nil {
			return err
		}

		// Additional validation for FilterValue fields
		if err := b.validator.ValidateFilterValues(obj); err != nil {
			return err
		}
	} else {
		// If no validator is available, still try our custom validation
		if err := b.validator.ValidateFilterValues(obj); err != nil {
			return err
		}
	}

	return nil
}

// ToFilterSet converts any struct with FilterValue fields to a filters.FilterSet.
// It uses reflection to automatically process all FilterValue[T] fields in the struct,
// using their stripe tag values as field names. This eliminates the need to manually implement
// ToFilterSet for every DTO with FilterValue fields.
//
// Example usage:
//
//	type MyDTO struct {
//	    Name  FilterValue[string]   `stripe:"name"`
//	    Count FilterValue[int]      `stripe:"count"`
//	}
//
//	dto := MyDTO{...}
//	filterSet := ToFilterSet(dto)  // Uses "name" and "count" as field names
func ToFilterSet(obj interface{}) filtertypes.FilterSet {
	filterSet := filters.NewFilterSet()

	objValue := reflect.ValueOf(obj)
	if objValue.Kind() == reflect.Ptr {
		objValue = objValue.Elem()
	}

	if objValue.Kind() != reflect.Struct {
		return *filterSet // Return empty if not a struct
	}

	objType := objValue.Type()

	for i := range objType.NumField() {
		field := objType.Field(i)
		fieldValue := objValue.Field(i)

		// Skip unexported fields
		if !fieldValue.CanInterface() {
			continue
		}

		// Check if this field has FilterValue methods (duck typing approach)
		isEmpty := fieldValue.MethodByName("IsEmpty")
		if !isEmpty.IsValid() {
			continue // Not a FilterValue type
		}

		// Get the stripe tag (still needed for query binding validation)
		stripeTag := field.Tag.Get("stripe")
		if stripeTag == "" || stripeTag == "-" {
			continue
		}

		// Check if the FilterValue is empty
		results := isEmpty.Call(nil)
		if len(results) > 0 && results[0].Bool() {
			continue // Skip empty FilterValues
		}

		// Get the Operator and Value fields
		operatorField := fieldValue.FieldByName("Operator")
		valueField := fieldValue.FieldByName("Value")

		if !operatorField.IsValid() || !valueField.IsValid() {
			continue
		}

		// Extract the operator (should be filters.FilterOperator)
		operator, ok := operatorField.Interface().(filtertypes.FilterOperator)
		if !ok {
			continue
		}

		// Get the value and handle IN/NOT_IN conversion
		value := valueField.Interface()

		// For IN/NOT_IN operators on single-type fields, convert comma-separated string to slice
		if (operator == filtertypes.OpIn || operator == filtertypes.OpNotIn) && valueField.Kind() == reflect.String {
			stringValue := valueField.String()
			if stringValue != "" {
				// Split the comma-separated values and convert to appropriate slice type
				values := strings.Split(stringValue, ",")

				// If this is a custom string type (not regular string), preserve that type
				if valueField.Type() != reflect.TypeOf("") {
					// Create a slice of the custom type
					customTypeSlice := reflect.MakeSlice(reflect.SliceOf(valueField.Type()), len(values), len(values))
					for i, val := range values {
						trimmedVal := strings.TrimSpace(val)
						// Create a value of the custom type
						customValue := reflect.New(valueField.Type()).Elem()
						customValue.SetString(trimmedVal)
						customTypeSlice.Index(i).Set(customValue)
					}
					value = customTypeSlice.Interface()
				} else {
					// Regular string type
					convertedValues := make([]string, len(values))
					for i, val := range values {
						convertedValues[i] = strings.TrimSpace(val)
					}
					value = convertedValues
				}
			}
		}

		// Add the filter using the Go field name (not the stripe tag).
		// This keeps FilterSet API-agnostic. The repository layer translates
		// field names to storage format (db tags for SQL, field names for memory).
		filterSet.AddFilter(field.Name, operator, value)
	}

	return *filterSet
}
