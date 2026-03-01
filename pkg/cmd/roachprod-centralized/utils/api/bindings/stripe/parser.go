// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stripe

import (
	"reflect"
	"strings"

	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
)

// QueryParser handles parsing of Stripe-style query parameters
type QueryParser struct {
	converter *TypeConverter
}

// NewQueryParser creates a new query parser
func NewQueryParser() *QueryParser {
	return &QueryParser{
		converter: NewTypeConverter(),
	}
}

// ParseStripeQuery parses Stripe-style query parameters into struct fields
func (p *QueryParser) ParseStripeQuery(obj interface{}, values map[string][]string) error {
	objValue := reflect.ValueOf(obj)
	if objValue.Kind() == reflect.Ptr {
		objValue = objValue.Elem()
	}

	if objValue.Kind() != reflect.Struct {
		return errors.New("binding element must be a struct")
	}

	objType := objValue.Type()

	// First, collect all valid field names from the struct
	validFields := make(map[string]bool)
	for i := range objType.NumField() {
		field := objType.Field(i)
		stripeTag := field.Tag.Get("stripe")
		if stripeTag != "" && stripeTag != "-" {
			validFields[stripeTag] = true
		}
	}

	// Reserved query parameters that should not be validated as filter fields
	// These are handled separately for pagination and sorting
	reservedParams := map[string]bool{
		"count":      true,
		"startIndex": true,
		"sortBy":     true,
		"sortOrder":  true,
	}

	// Check for unknown fields in query parameters
	for queryParam := range values {
		fieldName := p.extractFieldName(queryParam)
		// Skip validation for reserved pagination/sorting parameters
		if reservedParams[fieldName] {
			continue
		}
		if !validFields[fieldName] {
			return errors.Newf("Field validation for '%s' failed on unknown field", fieldName)
		}
	}

	// Validate sortBy value if present
	if sortByValues, exists := values["sortBy"]; exists && len(sortByValues) > 0 {
		sortByField := sortByValues[0]
		if !validFields[sortByField] {
			return errors.Newf("Field validation for 'sortBy' failed: '%s' is not a valid field", sortByField)
		}
	}

	// Parse known fields
	for i := range objType.NumField() {
		field := objType.Field(i)
		fieldValue := objValue.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		stripeTag := field.Tag.Get("stripe")
		if stripeTag == "" || stripeTag == "-" {
			continue
		}

		if err := p.setFieldFromQuery(fieldValue, stripeTag, values); err != nil {
			return errors.Wrapf(err, "failed to set field %s", field.Name)
		}
	}

	return nil
}

// extractFieldName extracts the field name from a query parameter, removing operator brackets
// Examples: "state" -> "state", "state[in]" -> "state", "created_at[gte]" -> "created_at"
func (p *QueryParser) extractFieldName(queryParam string) string {
	if bracketIndex := strings.Index(queryParam, "["); bracketIndex > 0 {
		return queryParam[:bracketIndex]
	}
	return queryParam
}

// setFieldFromQuery sets a struct field from query parameters
func (p *QueryParser) setFieldFromQuery(
	fieldValue reflect.Value, fieldName string, values map[string][]string,
) error {
	// Check if this is a FilterValue type by looking for Value and Operator fields
	valueField := fieldValue.FieldByName("Value")
	operatorField := fieldValue.FieldByName("Operator")

	if valueField.IsValid() && operatorField.IsValid() && fieldValue.Type().Kind() == reflect.Struct {
		// This is a FilterValue type
		return p.setFilterValue(fieldValue, fieldName, values)
	}

	// Handle regular fields (fallback for non-FilterValue types)
	if queryValues, exists := values[fieldName]; exists && len(queryValues) > 0 {
		return p.converter.SetSimpleValue(fieldValue, queryValues[0])
	}

	return nil
}

// setFilterValue sets a FilterValue field from query parameters
func (p *QueryParser) setFilterValue(
	fieldValue reflect.Value, fieldName string, values map[string][]string,
) error {
	valueField := fieldValue.FieldByName("Value")
	operatorField := fieldValue.FieldByName("Operator")

	// Look for both simple and bracket notation
	var queryValue string
	var operator filtertypes.FilterOperator = filtertypes.OpEqual

	// Check for bracket notation first (e.g., "state[in]", "priority[gte]")
	for key, vals := range values {
		if len(vals) == 0 {
			continue
		}

		if strings.HasPrefix(key, fieldName+"[") && strings.HasSuffix(key, "]") {
			// Extract operator from bracket notation
			bracketStart := strings.Index(key, "[")
			bracketEnd := strings.LastIndex(key, "]")

			if bracketStart > 0 && bracketEnd > bracketStart {
				operatorStr := key[bracketStart+1 : bracketEnd]
				queryValue = vals[0]

				var err error
				operator, err = p.parseOperator(operatorStr)
				if err != nil {
					return err
				}
				break
			}
		}
	}

	// Check for simple field name if no bracket notation found
	if queryValue == "" {
		if vals, exists := values[fieldName]; exists && len(vals) > 0 {
			queryValue = vals[0]
			operator = filtertypes.OpEqual
		}
	}

	// If no value found, leave the FilterValue empty
	if queryValue == "" {
		return nil
	}

	// Set the operator
	operatorField.Set(reflect.ValueOf(operator))

	// Set the value based on the Value field type
	return p.converter.SetValueByType(valueField, queryValue, operator)
}

// parseOperator converts string operator to FilterOperator
func (p *QueryParser) parseOperator(operatorStr string) (filtertypes.FilterOperator, error) {
	switch operatorStr {
	case "eq":
		return filtertypes.OpEqual, nil
	case "ne", "neq":
		return filtertypes.OpNotEqual, nil
	case "gt":
		return filtertypes.OpGreater, nil
	case "gte":
		return filtertypes.OpGreaterEq, nil
	case "lt":
		return filtertypes.OpLess, nil
	case "lte":
		return filtertypes.OpLessEq, nil
	case "in":
		return filtertypes.OpIn, nil
	case "not_in", "nin":
		return filtertypes.OpNotIn, nil
	case "like":
		return filtertypes.OpLike, nil
	case "not_like":
		return filtertypes.OpNotLike, nil
	default:
		return "", errors.Newf("unsupported operator: %s", operatorStr)
	}
}
