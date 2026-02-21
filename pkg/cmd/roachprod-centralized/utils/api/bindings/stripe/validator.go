// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stripe

import (
	"fmt"
	"reflect"
	"strings"

	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
	"github.com/go-playground/validator/v10"
)

// Validator handles validation of FilterValue fields and struct validation
type Validator struct {
	converter *TypeConverter
	validator *validator.Validate
}

// NewValidator creates a new validator
func NewValidator() *Validator {
	return &Validator{
		converter: NewTypeConverter(),
		validator: validator.New(),
	}
}

// ValidateFilterValues validates the Value field inside FilterValue types using the original validate tag
func (v *Validator) ValidateFilterValues(obj interface{}) error {
	objValue := reflect.ValueOf(obj)
	if objValue.Kind() == reflect.Ptr {
		objValue = objValue.Elem()
	}

	if objValue.Kind() != reflect.Struct {
		return nil
	}

	objType := objValue.Type()

	for i := range objType.NumField() {
		field := objType.Field(i)
		fieldValue := objValue.Field(i)

		// Check if this is a FilterValue type
		valueField := fieldValue.FieldByName("Value")
		if !valueField.IsValid() {
			continue
		}

		// Get the validation tag from the parent field
		validateTag := field.Tag.Get("validate")
		if validateTag == "" {
			continue
		}

		// Skip empty FilterValue fields - check if Value is zero
		if v.converter.IsZeroValue(valueField) {
			continue
		}

		// Get the operator to determine validation strategy
		operatorField := fieldValue.FieldByName("Operator")
		if !operatorField.IsValid() {
			continue
		}

		// Extract the operator value
		operator, ok := operatorField.Interface().(filtertypes.FilterOperator)
		if !ok {
			continue
		}

		// For IN/NOT_IN operators on string fields, validate each comma-separated value individually
		if (operator == filtertypes.OpIn || operator == filtertypes.OpNotIn) && valueField.Kind() == reflect.String {
			stringValue := valueField.String()
			if stringValue != "" {
				values := strings.Split(stringValue, ",")
				for _, val := range values {
					val = strings.TrimSpace(val)
					if err := v.validateSingleValue(val, validateTag, field.Name); err != nil {
						return err
					}
				}
			}
		} else {
			// For other operators or non-string fields, validate normally
			if err := v.validateSingleValue(valueField.Interface(), validateTag, field.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateSingleValue validates a single value using the given validation tag
func (v *Validator) validateSingleValue(value interface{}, validateTag, fieldName string) error {
	// Create a temporary struct for validation with the same tag
	valueType := reflect.TypeOf(value)
	tempStructType := reflect.StructOf([]reflect.StructField{
		{
			Name: "Value",
			Type: valueType,
			Tag:  reflect.StructTag(fmt.Sprintf("validate:\"%s\"", validateTag)),
		},
	})

	tempStruct := reflect.New(tempStructType).Elem()
	tempStruct.FieldByName("Value").Set(reflect.ValueOf(value))

	// Use validator to validate the temporary struct
	if err := v.validator.Struct(tempStruct.Addr().Interface()); err != nil {
		// Clean up the error message to show the actual field name instead of "Value[0]"
		return v.formatValidationError(fieldName, err)
	}

	return nil
}

// formatValidationError formats validator errors to show clean field names instead of internal "Value[0]" references
func (v *Validator) formatValidationError(fieldName string, err error) error {
	errMsg := err.Error()

	// Replace "Key: 'Value[0]' Error:Field validation for 'Value[0]'" with clean field name
	// Handle single value case: remove [0] reference entirely
	if strings.Contains(errMsg, "Value[0]") {
		// For single values, we want a clean message without array notation
		cleanFieldRef := fmt.Sprintf("Field validation for '%s'", fieldName)
		errMsg = strings.ReplaceAll(errMsg, "Key: 'Value[0]' Error:Field validation for 'Value[0]'", cleanFieldRef)

		cleanValueRef := fmt.Sprintf("'%s'", fieldName)
		errMsg = strings.ReplaceAll(errMsg, "'Value[0]'", cleanValueRef)
	} else if strings.Contains(errMsg, "Value[") {
		// For multiple values, still use the field name but keep array notation meaningful
		// This handles cases like Value[1], Value[2], etc.
		cleanValuePrefix := fmt.Sprintf("'%s[", fieldName)
		errMsg = strings.ReplaceAll(errMsg, "'Value[", cleanValuePrefix)
	}

	// Also handle the case where it just says "Key: 'Value' Error:Field validation for 'Value'"
	if strings.Contains(errMsg, "Key: 'Value' Error:Field validation for 'Value'") {
		cleanFieldRef := fmt.Sprintf("Field validation for '%s'", fieldName)
		errMsg = strings.ReplaceAll(errMsg, "Key: 'Value' Error:Field validation for 'Value'", cleanFieldRef)
	}

	cleanValueRef := fmt.Sprintf("'%s'", fieldName)
	errMsg = strings.ReplaceAll(errMsg, "'Value'", cleanValueRef)

	return errors.Newf("validation failed for field %s: %s", fieldName, errMsg)
}
