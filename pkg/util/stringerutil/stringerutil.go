// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stringerutil

import (
	"fmt"
	"sort"
)

type enum interface {
	fmt.Stringer
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type index interface {
	uint8 | uint16 | uint32 | uint64
}

// StringToEnumValueMap generates a map of String -> enum value.
// enumIndex is the stringer generated _Enum_index slice.
// enumString is the stringer generated _Enum_name string.
// minValue is the minimum enum value.
func StringToEnumValueMap[T enum, I index](
	enumIndex []I, enumString string, minValue T, maxValue T,
) map[string]T {
	numValues := maxValue - minValue
	result := make(map[string]T, numValues)
	for i := T(0); i <= numValues; i++ {
		startIndex := enumIndex[i]
		endIndex := enumIndex[i+T(1)]
		s := enumString[startIndex:endIndex]
		result[s] = i + minValue
	}
	return result
}

// EnumValues generates a slice of all enum values sorted lexicographically.
// minValue is the minimum enum value.
// maxValue is the maximum enum value.
func EnumValues[T enum](minValue, maxValue T) []T {
	result := make([]T, 0, maxValue-minValue)
	for i := minValue; i <= maxValue; i++ {
		result = append(result, i)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].String() < result[j].String()
	})
	return result
}
