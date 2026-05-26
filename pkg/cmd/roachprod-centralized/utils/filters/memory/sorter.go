// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
	"github.com/iancoleman/strcase"
)

// SortByField sorts a slice in-place using reflection based on SortParams.
// The slice elements can be any type - the function will use reflection to access fields.
// Field names in sortParams should be in snake_case and will be converted to PascalCase for lookup.
// If defaultField is provided and the sort field is unknown, it will be used as fallback.
func SortByField[T any](slice []T, sortParams *types.SortParams, defaultField string) error {
	if sortParams == nil {
		return nil
	}

	ascending := sortParams.SortOrder == types.SortAscending
	fieldToSort := sortParams.SortBy
	if fieldToSort == "" && defaultField != "" {
		fieldToSort = defaultField
	}

	// Convert snake_case to PascalCase for Go struct field lookup
	pascalFieldName := strcase.ToCamel(fieldToSort)

	slices.SortFunc(slice, func(i, j T) int {
		iVal := reflect.ValueOf(i)
		jVal := reflect.ValueOf(j)

		// Get field values using reflection
		iFieldValue, err := getFieldValueReflect(iVal, pascalFieldName)
		if err != nil {
			// If field not found, use default field if provided
			if defaultField != "" {
				defaultPascal := strcase.ToCamel(defaultField)
				iFieldValue, _ = getFieldValueReflect(iVal, defaultPascal)
				jFieldValue, _ := getFieldValueReflect(jVal, defaultPascal)
				return compareValues(iFieldValue, jFieldValue, ascending)
			}
			return 0
		}

		jFieldValue, err := getFieldValueReflect(jVal, pascalFieldName)
		if err != nil {
			return 0
		}

		return compareValues(iFieldValue, jFieldValue, ascending)
	})

	return nil
}

// getFieldValueReflect extracts a field value from a reflected value using getter methods or direct field access
func getFieldValueReflect(val reflect.Value, fieldName string) (interface{}, error) {
	// Try getter method first (e.g., GetCreationDatetime)
	methodName := "Get" + fieldName
	method := val.MethodByName(methodName)
	if method.IsValid() {
		results := method.Call(nil)
		if len(results) > 0 {
			return results[0].Interface(), nil
		}
	}

	// Handle pointer types
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		elemVal := val.Elem()
		method := elemVal.MethodByName(methodName)
		if method.IsValid() {
			results := method.Call(nil)
			if len(results) > 0 {
				return results[0].Interface(), nil
			}
		}

		// Try struct field on dereferenced value
		if elemVal.Kind() == reflect.Struct {
			field := elemVal.FieldByName(fieldName)
			if field.IsValid() {
				return field.Interface(), nil
			}
		}
	}

	// Handle struct fields directly
	if val.Kind() == reflect.Struct {
		field := val.FieldByName(fieldName)
		if field.IsValid() {
			return field.Interface(), nil
		}
	}

	// Handle interface types
	if val.Kind() == reflect.Interface && !val.IsNil() {
		concreteVal := val.Elem()
		method := concreteVal.MethodByName(methodName)
		if method.IsValid() {
			results := method.Call(nil)
			if len(results) > 0 {
				return results[0].Interface(), nil
			}
		}
	}

	return nil, errors.Newf("field %s not found", fieldName)
}

// compareValues compares two values and returns -1, 0, or 1
func compareValues(a, b interface{}, ascending bool) int {
	// Handle time.Time
	if aTime, ok := a.(time.Time); ok {
		if bTime, ok := b.(time.Time); ok {
			cmp := aTime.Compare(bTime)
			if ascending {
				return cmp
			}
			return -cmp
		}
	}

	// Handle strings (case-insensitive)
	if aStr, ok := a.(string); ok {
		if bStr, ok := b.(string); ok {
			cmp := strings.Compare(strings.ToLower(aStr), strings.ToLower(bStr))
			if ascending {
				return cmp
			}
			return -cmp
		}
	}

	// Handle booleans
	if aBool, ok := a.(bool); ok {
		if bBool, ok := b.(bool); ok {
			var cmp int
			if aBool == bBool {
				cmp = 0
			} else if aBool {
				cmp = 1
			} else {
				cmp = -1
			}
			if ascending {
				return cmp
			}
			return -cmp
		}
	}

	// Handle comparable types (int, float, etc.) using reflection
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	if aVal.Kind() >= reflect.Int && aVal.Kind() <= reflect.Float64 &&
		bVal.Kind() >= reflect.Int && bVal.Kind() <= reflect.Float64 {

		aFloat := toFloat64(aVal)
		bFloat := toFloat64(bVal)

		var cmp int
		if aFloat < bFloat {
			cmp = -1
		} else if aFloat > bFloat {
			cmp = 1
		} else {
			cmp = 0
		}

		if ascending {
			return cmp
		}
		return -cmp
	}

	// Default: try string comparison
	aStr := reflect.ValueOf(a).String()
	bStr := reflect.ValueOf(b).String()
	cmp := strings.Compare(strings.ToLower(aStr), strings.ToLower(bStr))
	if ascending {
		return cmp
	}
	return -cmp
}

// toFloat64 converts numeric reflect values to float64
func toFloat64(val reflect.Value) float64 {
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(val.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(val.Uint())
	case reflect.Float32, reflect.Float64:
		return val.Float()
	default:
		return 0
	}
}

// ApplyPagination applies pagination to a slice and returns the paginated slice.
// The original slice is not modified. Returns an empty slice if offset is beyond the slice length.
// If limit is -1, returns all elements from offset onwards.
func ApplyPagination[T any](slice []T, pagination *types.PaginationParams) []T {
	if pagination == nil {
		return slice
	}

	offset := pagination.GetOffset()
	limit := pagination.GetLimit()

	// Handle offset beyond results
	if offset >= len(slice) {
		return []T{}
	}

	// If limit is -1, return all from offset
	if limit == -1 {
		return slice[offset:]
	}

	// Calculate end index
	end := offset + limit
	if end > len(slice) {
		end = len(slice)
	}

	return slice[offset:end]
}
