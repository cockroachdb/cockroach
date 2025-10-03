// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package filters

import (
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/errors"
)

// GetSupportedOperatorsForType returns the list of operators supported for a given Go type.
func GetSupportedOperatorsForType(t reflect.Type) []types.FilterOperator {
	switch t.Kind() {
	case reflect.String:
		return []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpIn, types.OpNotIn, types.OpLike, types.OpNotLike}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpLess, types.OpLessEq, types.OpGreater, types.OpGreaterEq, types.OpIn, types.OpNotIn}
	case reflect.Bool:
		return []types.FilterOperator{types.OpEqual, types.OpNotEqual}
	default:
		// Check if it's a time.Time or similar comparable type
		if t == reflect.TypeOf(time.Time{}) {
			return []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpLess, types.OpLessEq, types.OpGreater, types.OpGreaterEq, types.OpIn, types.OpNotIn}
		}
		// For other types, only allow equality checks
		return []types.FilterOperator{types.OpEqual, types.OpNotEqual, types.OpIn, types.OpNotIn}
	}
}

// ValidateOperatorForType checks if an operator is valid for a given Go type.
func ValidateOperatorForType(operator types.FilterOperator, t reflect.Type) error {
	supportedOps := GetSupportedOperatorsForType(t)
	for _, op := range supportedOps {
		if op == operator {
			return nil
		}
	}
	return errors.Newf("operator %s not supported for type %s", operator, t.String())
}
