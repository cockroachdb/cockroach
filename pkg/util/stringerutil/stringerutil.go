package stringerutil

import (
	"fmt"
	"sort"
)

type enum interface {
	fmt.Stringer
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type index interface {
	uint8 | uint16 | uint32 | uint64
}

// StringToEnumValueMap generates a map of String -> enum value.
// enumIndex is the stringer generated _Enum_index slice.
// enumString is the stringer generated _Enum_name string.
// offset is the first enum value.
func StringToEnumValueMap[T enum, I index](
	enumIndex []I, enumString string, offset int,
) map[string]T {
	numValues := len(enumIndex) - 1
	result := make(map[string]T, numValues)
	for i := 0; i < numValues; i++ {
		startIndex := enumIndex[i]
		endIndex := enumIndex[i+1]
		s := enumString[startIndex:endIndex]
		result[s] = T(i + offset)
	}
	return result
}

// EnumValues generates a slice of all enum values sorted lexicographically.
// enumIndex is the stringer generated _Enum_index slice.
// offset is the first enum value.
func EnumValues[T enum, I index](enumIndex []I, offset int) []T {
	numValues := len(enumIndex) - 1
	result := make([]T, 0, numValues)
	for i := 0; i < numValues; i++ {
		result = append(result, T(i+offset))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].String() < result[j].String()
	})
	return result
}
