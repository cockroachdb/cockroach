// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
	"github.com/iancoleman/strcase"
)

// FilterEvaluator helps evaluate filters against in-memory objects.
type FilterEvaluator struct {
	filteredType reflect.Type
}

// NewFilterEvaluator creates a new memory filter evaluator.
func NewFilterEvaluatorWithTypeHint(filteredType reflect.Type) *FilterEvaluator {
	return &FilterEvaluator{filteredType: filteredType}
}

// Evaluate checks if an object matches the given FilterSet.
func (mfe *FilterEvaluator) Evaluate(obj interface{}, fs *types.FilterSet) (bool, error) {
	if fs == nil || fs.IsEmpty() {
		return true, nil
	}

	if err := fs.Validate(); err != nil {
		return false, errors.Wrap(err, "invalid filter set")
	}

	objValue := reflect.ValueOf(obj)
	if objValue.Kind() == reflect.Ptr {
		objValue = objValue.Elem()
	}

	return mfe.evaluateGroup(objValue, fs)
}

// evaluateGroup evaluates all Filters and SubGroups in a FilterSet,
// combining results with short-circuit evaluation.
func (mfe *FilterEvaluator) evaluateGroup(
	objValue reflect.Value, fs *types.FilterSet,
) (bool, error) {
	isOr := fs.Logic == types.LogicOr

	for i, filter := range fs.Filters {
		match, err := mfe.evaluateFilter(objValue, filter)
		if err != nil {
			return false, errors.Wrapf(err, "filter %d", i)
		}
		if isOr && match {
			return true, nil
		}
		if !isOr && !match {
			return false, nil
		}
	}

	for i, sg := range fs.SubGroups {
		match, err := mfe.evaluateGroup(objValue, &sg)
		if err != nil {
			return false, errors.Wrapf(err, "sub_group %d", i)
		}
		if isOr && match {
			return true, nil
		}
		if !isOr && !match {
			return false, nil
		}
	}

	// OR with parts but none matched -> false. Everything else -> true.
	if isOr && (len(fs.Filters) > 0 || len(fs.SubGroups) > 0) {
		return false, nil
	}
	return true, nil
}

// evaluateFilter evaluates a single filter against an object.
func (mfe *FilterEvaluator) evaluateFilter(
	objValue reflect.Value, filter types.FieldFilter,
) (bool, error) {
	fieldValue, err := mfe.getFieldValue(objValue, filter.Field)
	if err != nil {
		return false, err
	}

	switch filter.Operator {
	case types.OpEqual:
		return mfe.compareEqual(fieldValue, filter.Value)
	case types.OpNotEqual:
		equal, err := mfe.compareEqual(fieldValue, filter.Value)
		return !equal, err
	case types.OpLess:
		return mfe.compareLess(fieldValue, filter.Value)
	case types.OpLessEq:
		return mfe.compareLessEqual(fieldValue, filter.Value)
	case types.OpGreater:
		return mfe.compareGreater(fieldValue, filter.Value)
	case types.OpGreaterEq:
		return mfe.compareGreaterEqual(fieldValue, filter.Value)
	case types.OpIn:
		return mfe.compareIn(fieldValue, filter.Value)
	case types.OpNotIn:
		in, err := mfe.compareIn(fieldValue, filter.Value)
		return !in, err
	case types.OpLike:
		return mfe.compareLike(fieldValue, filter.Value)
	case types.OpNotLike:
		like, err := mfe.compareLike(fieldValue, filter.Value)
		return !like, err
	case types.OpContains:
		return mfe.compareContains(fieldValue, filter.Value)
	case types.OpStartsWith:
		return mfe.compareStartsWith(fieldValue, filter.Value)
	case types.OpEndsWith:
		return mfe.compareEndsWith(fieldValue, filter.Value)
	case types.OpPresent:
		return mfe.comparePresent(fieldValue)
	default:
		return false, errors.Newf("unsupported operator: %s", filter.Operator)
	}
}

// getFieldValue extracts a field value from an object, supporting nested field access.
// Converts snake_case field names to PascalCase for Go struct field lookup.
func (mfe *FilterEvaluator) getFieldValue(
	objValue reflect.Value, fieldPath string,
) (interface{}, error) {
	// Handle nested field access (e.g., "Task.State" or "task.state")
	fieldParts := strings.Split(fieldPath, ".")
	currentValue := objValue

	for _, fieldName := range fieldParts {
		// Convert snake_case to PascalCase for Go struct field names
		// e.g., "creation_datetime" -> "CreationDatetime"
		pascalFieldName := strcase.ToCamel(fieldName)
		methodName := "Get" + pascalFieldName
		fieldFound := false

		// Try getter method on current value (works for structs, pointers, interfaces)
		method := currentValue.MethodByName(methodName)
		if method.IsValid() {
			results := method.Call(nil)
			if len(results) > 0 {
				currentValue = results[0]
				fieldFound = true
			}
		}

		if fieldFound {
			continue
		}

		// Handle pointer types - dereference and try again
		if currentValue.Kind() == reflect.Ptr && !currentValue.IsNil() {
			elemValue := currentValue.Elem()

			// Try method on the dereferenced value
			method := elemValue.MethodByName(methodName)
			if method.IsValid() {
				results := method.Call(nil)
				if len(results) > 0 {
					currentValue = results[0]
					fieldFound = true
				}
			}

			if !fieldFound {
				// Try struct field on dereferenced value
				if elemValue.Kind() == reflect.Struct {
					field := elemValue.FieldByName(pascalFieldName)
					if field.IsValid() {
						currentValue = field
						fieldFound = true
					}
				}
			}
		}

		if fieldFound {
			continue
		}

		// Handle struct fields directly
		if currentValue.Kind() == reflect.Struct {
			field := currentValue.FieldByName(pascalFieldName)
			if field.IsValid() {
				currentValue = field
				fieldFound = true
			}
		}

		if fieldFound {
			continue
		}

		// Handle interface types - get concrete value and try again
		if currentValue.Kind() == reflect.Interface && !currentValue.IsNil() {
			concreteValue := currentValue.Elem()

			// Try method on concrete type
			method := concreteValue.MethodByName(methodName)
			if method.IsValid() {
				results := method.Call(nil)
				if len(results) > 0 {
					currentValue = results[0]
					fieldFound = true
				}
			}

			if !fieldFound && concreteValue.Kind() == reflect.Ptr && !concreteValue.IsNil() {
				// Try method on pointer to concrete type
				method := concreteValue.MethodByName(methodName)
				if method.IsValid() {
					results := method.Call(nil)
					if len(results) > 0 {
						currentValue = results[0]
						fieldFound = true
					}
				}
			}
		}

		if !fieldFound {
			return nil, errors.Newf("field %s not found", fieldName)
		}
	}

	return currentValue.Interface(), nil
}

// Comparison methods for different operators
func (mfe *FilterEvaluator) compareEqual(fieldValue, filterValue interface{}) (bool, error) {
	return reflect.DeepEqual(fieldValue, filterValue), nil
}

func (mfe *FilterEvaluator) compareLess(fieldValue, filterValue interface{}) (bool, error) {
	return mfe.compareValues(fieldValue, filterValue, func(cmp int) bool { return cmp < 0 })
}

func (mfe *FilterEvaluator) compareLessEqual(fieldValue, filterValue interface{}) (bool, error) {
	return mfe.compareValues(fieldValue, filterValue, func(cmp int) bool { return cmp <= 0 })
}

func (mfe *FilterEvaluator) compareGreater(fieldValue, filterValue interface{}) (bool, error) {
	return mfe.compareValues(fieldValue, filterValue, func(cmp int) bool { return cmp > 0 })
}

func (mfe *FilterEvaluator) compareGreaterEqual(fieldValue, filterValue interface{}) (bool, error) {
	return mfe.compareValues(fieldValue, filterValue, func(cmp int) bool { return cmp >= 0 })
}

func (mfe *FilterEvaluator) compareIn(fieldValue, filterValue interface{}) (bool, error) {
	values := reflect.ValueOf(filterValue)
	if values.Kind() != reflect.Slice && values.Kind() != reflect.Array {
		return false, errors.Newf("IN operator requires slice/array, got %T", filterValue)
	}

	for i := range values.Len() {
		if reflect.DeepEqual(fieldValue, values.Index(i).Interface()) {
			return true, nil
		}
	}
	return false, nil
}

func (mfe *FilterEvaluator) compareLike(fieldValue, filterValue interface{}) (bool, error) {
	fieldStr, ok := fieldValue.(string)
	if !ok {
		return false, errors.Newf("LIKE operator requires string field, got %T", fieldValue)
	}

	filterStr, ok := filterValue.(string)
	if !ok {
		return false, errors.Newf("LIKE operator requires string value, got %T", filterValue)
	}

	// Simple contains check for memory implementation
	return strings.Contains(strings.ToLower(fieldStr), strings.ToLower(filterStr)), nil
}

// compareValues compares two values using a comparison function.
func (mfe *FilterEvaluator) compareValues(
	fieldValue, filterValue interface{}, cmpFunc func(int) bool,
) (bool, error) {
	// Handle time.Time comparison
	if fieldTime, ok := fieldValue.(time.Time); ok {
		if filterTime, ok := filterValue.(time.Time); ok {
			cmp := fieldTime.Compare(filterTime)
			return cmpFunc(cmp), nil
		}
		return false, errors.New("cannot compare time.Time with non-time.Time value")
	}

	// Handle numeric comparisons
	fieldVal := reflect.ValueOf(fieldValue)
	filterVal := reflect.ValueOf(filterValue)

	// Convert to float64 for comparison if both are numeric
	if fieldVal.Kind() >= reflect.Int && fieldVal.Kind() <= reflect.Float64 &&
		filterVal.Kind() >= reflect.Int && filterVal.Kind() <= reflect.Float64 {

		fieldFloat := mfe.toFloat64(fieldVal)
		filterFloat := mfe.toFloat64(filterVal)

		if fieldFloat < filterFloat {
			return cmpFunc(-1), nil
		} else if fieldFloat > filterFloat {
			return cmpFunc(1), nil
		} else {
			return cmpFunc(0), nil
		}
	}

	// Handle string comparison
	if fieldStr, ok := fieldValue.(string); ok {
		if filterStr, ok := filterValue.(string); ok {
			cmp := strings.Compare(fieldStr, filterStr)
			return cmpFunc(cmp), nil
		}
	}

	return false, errors.Newf("cannot compare values of types %T and %T", fieldValue, filterValue)
}

// toFloat64 converts numeric values to float64 for comparison.
func (mfe *FilterEvaluator) toFloat64(val reflect.Value) float64 {
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

// compareContains checks if the field value contains the filter value (case-insensitive).
func (mfe *FilterEvaluator) compareContains(fieldValue, filterValue interface{}) (bool, error) {
	fieldStr, ok := fieldValue.(string)
	if !ok {
		return false, errors.Newf("contains operator requires string field, got %T", fieldValue)
	}
	filterStr, ok := filterValue.(string)
	if !ok {
		return false, errors.Newf("contains operator requires string value, got %T", filterValue)
	}
	return strings.Contains(strings.ToLower(fieldStr), strings.ToLower(filterStr)), nil
}

// compareStartsWith checks if the field value starts with the filter value (case-insensitive).
func (mfe *FilterEvaluator) compareStartsWith(fieldValue, filterValue interface{}) (bool, error) {
	fieldStr, ok := fieldValue.(string)
	if !ok {
		return false, errors.Newf("startsWith operator requires string field, got %T", fieldValue)
	}
	filterStr, ok := filterValue.(string)
	if !ok {
		return false, errors.Newf("startsWith operator requires string value, got %T", filterValue)
	}
	return strings.HasPrefix(strings.ToLower(fieldStr), strings.ToLower(filterStr)), nil
}

// compareEndsWith checks if the field value ends with the filter value (case-insensitive).
func (mfe *FilterEvaluator) compareEndsWith(fieldValue, filterValue interface{}) (bool, error) {
	fieldStr, ok := fieldValue.(string)
	if !ok {
		return false, errors.Newf("endsWith operator requires string field, got %T", fieldValue)
	}
	filterStr, ok := filterValue.(string)
	if !ok {
		return false, errors.Newf("endsWith operator requires string value, got %T", filterValue)
	}
	return strings.HasSuffix(strings.ToLower(fieldStr), strings.ToLower(filterStr)), nil
}

// comparePresent checks if the field is present and not nil/zero.
func (mfe *FilterEvaluator) comparePresent(fieldValue interface{}) (bool, error) {
	if fieldValue == nil {
		return false, nil
	}
	val := reflect.ValueOf(fieldValue)
	return !val.IsZero(), nil
}
