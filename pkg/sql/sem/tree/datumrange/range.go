// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package datumrange

import (
	"encoding/binary"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/errors"
)

// GetRangesBeforeAndAfter returns the size of the before and after ranges based
// on the lower and upper bounds provided. If swap is true, the upper and lower
// bounds of both ranges are swapped. Returns ok=true if these range sizes are
// calculated successfully, and false otherwise. The calculations for
// rangeBefore and rangeAfter are datatype dependent.
//
// For numeric types, we can simply find the difference between the lower and
// upper bounds for rangeBefore/rangeAfter.
//
// For non-numeric types, we can convert each bound into sorted key bytes (CRDB
// key representation) to find their range. As we do need a lot of precision in
// our range estimate, we can remove the common prefix between the lower and
// upper bounds, and limit the byte array to 8 bytes. This also simplifies our
// implementation since we won't need to handle an arbitrary length of bounds.
// Following the conversion, we must zero extend the byte arrays to ensure the
// length is uniform between lower and upper bounds. This process is highlighted
// below, where [\bear - \bobcat] represents the before range and
// [\bluejay - \boar] represents the after range.
//
//	bear    := [18  98  101 97  114 0   1          ]
//	        => [101 97  114 0   1   0   0   0      ]
//
//	bluejay := [18  98  108 117 101 106 97  121 0 1]
//	        => [108 117 101 106 97  121 0   1      ]
//
//	boar    := [18  98  111 97  114 0   1          ]
//	        => [111 97  114 0   1   0   0   0      ]
//
//	bobcat  := [18  98  111 98  99  97  116 0   1  ]
//	        => [111 98  99  97  116 0   1   0      ]
//
// We can now find the range before/after by finding the difference between
// the lower and upper bounds:
//
//		 rangeBefore := [111 98  99  97  116 0   1   0] -
//	                 [101 97  114 0   1   0   0   0]
//
//	  rangeAfter  := [111 97  114 0   1   0   0   0] -
//	                 [108 117 101 106 97  121 0   1]
//
// Subtracting the uint64 representations of the byte arrays, the resulting
// rangeBefore and rangeAfter are:
//
//		 rangeBefore := 8,026,086,756,136,779,776 - 7,305,245,414,897,221,632
//	              := 720,841,341,239,558,100
//
//		 rangeAfter := 8,025,821,355,276,500,992 - 7,815,264,235,947,622,400
//	             := 210,557,119,328,878,600
func GetRangesBeforeAndAfter(
	beforeLowerBound, beforeUpperBound, afterLowerBound, afterUpperBound tree.Datum, swap bool,
) (rangeBefore, rangeAfter float64, ok bool) {
	// If the data types don't match, don't bother trying to calculate the range
	// sizes. This should almost never happen, but we want to avoid type
	// assertion errors below.
	typesMatch :=
		beforeLowerBound.ResolvedType().Equivalent(beforeUpperBound.ResolvedType()) &&
			beforeUpperBound.ResolvedType().Equivalent(afterLowerBound.ResolvedType()) &&
			afterLowerBound.ResolvedType().Equivalent(afterUpperBound.ResolvedType())
	if !typesMatch {
		return 0, 0, false
	}

	if swap {
		beforeLowerBound, beforeUpperBound = beforeUpperBound, beforeLowerBound
		afterLowerBound, afterUpperBound = afterUpperBound, afterLowerBound
	}

	// The calculations below assume that all bounds are inclusive.
	// TODO(rytaft): handle more types here.
	switch beforeLowerBound.ResolvedType().Family() {
	case types.IntFamily:
		rangeBefore = float64(*beforeUpperBound.(*tree.DInt)) - float64(*beforeLowerBound.(*tree.DInt))
		rangeAfter = float64(*afterUpperBound.(*tree.DInt)) - float64(*afterLowerBound.(*tree.DInt))
		return rangeBefore, rangeAfter, true

	case types.DateFamily:
		lowerBefore := beforeLowerBound.(*tree.DDate)
		upperBefore := beforeUpperBound.(*tree.DDate)
		lowerAfter := afterLowerBound.(*tree.DDate)
		upperAfter := afterUpperBound.(*tree.DDate)
		if lowerBefore.IsFinite() && upperBefore.IsFinite() && lowerAfter.IsFinite() && upperAfter.IsFinite() {
			rangeBefore = float64(upperBefore.PGEpochDays()) - float64(lowerBefore.PGEpochDays())
			rangeAfter = float64(upperAfter.PGEpochDays()) - float64(lowerAfter.PGEpochDays())
			return rangeBefore, rangeAfter, true
		}
		return 0, 0, false

	case types.DecimalFamily:
		lowerBefore, err := beforeLowerBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		upperBefore, err := beforeUpperBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		lowerAfter, err := afterLowerBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		upperAfter, err := afterUpperBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		rangeBefore = upperBefore - lowerBefore
		rangeAfter = upperAfter - lowerAfter
		return rangeBefore, rangeAfter, true

	case types.FloatFamily:
		rangeBefore = float64(*beforeUpperBound.(*tree.DFloat)) - float64(*beforeLowerBound.(*tree.DFloat))
		rangeAfter = float64(*afterUpperBound.(*tree.DFloat)) - float64(*afterLowerBound.(*tree.DFloat))
		return rangeBefore, rangeAfter, true

	case types.TimestampFamily:
		lowerBefore := beforeLowerBound.(*tree.DTimestamp).Time
		upperBefore := beforeUpperBound.(*tree.DTimestamp).Time
		lowerAfter := afterLowerBound.(*tree.DTimestamp).Time
		upperAfter := afterUpperBound.(*tree.DTimestamp).Time
		rangeBefore = float64(upperBefore.Sub(lowerBefore))
		rangeAfter = float64(upperAfter.Sub(lowerAfter))
		return rangeBefore, rangeAfter, true

	case types.TimestampTZFamily:
		lowerBefore := beforeLowerBound.(*tree.DTimestampTZ).Time
		upperBefore := beforeUpperBound.(*tree.DTimestampTZ).Time
		lowerAfter := afterLowerBound.(*tree.DTimestampTZ).Time
		upperAfter := afterUpperBound.(*tree.DTimestampTZ).Time
		rangeBefore = float64(upperBefore.Sub(lowerBefore))
		rangeAfter = float64(upperAfter.Sub(lowerAfter))
		return rangeBefore, rangeAfter, true

	case types.TimeFamily:
		lowerBefore := beforeLowerBound.(*tree.DTime)
		upperBefore := beforeUpperBound.(*tree.DTime)
		lowerAfter := afterLowerBound.(*tree.DTime)
		upperAfter := afterUpperBound.(*tree.DTime)
		rangeBefore = float64(*upperBefore) - float64(*lowerBefore)
		rangeAfter = float64(*upperAfter) - float64(*lowerAfter)
		return rangeBefore, rangeAfter, true

	case types.TimeTZFamily:
		// timeTZOffsetSecsRange is the total number of possible values for offset.
		timeTZOffsetSecsRange := timetz.MaxTimeTZOffsetSecs - timetz.MinTimeTZOffsetSecs + 1

		// Find the ranges in microseconds based on the absolute times of the range
		// boundaries.
		lowerBefore := beforeLowerBound.(*tree.DTimeTZ)
		upperBefore := beforeUpperBound.(*tree.DTimeTZ)
		lowerAfter := afterLowerBound.(*tree.DTimeTZ)
		upperAfter := afterUpperBound.(*tree.DTimeTZ)
		rangeBefore = float64(upperBefore.ToTime().Sub(lowerBefore.ToTime()) / time.Microsecond)
		rangeAfter = float64(upperAfter.ToTime().Sub(lowerAfter.ToTime()) / time.Microsecond)

		// Account for the offset.
		rangeBefore *= float64(timeTZOffsetSecsRange)
		rangeAfter *= float64(timeTZOffsetSecsRange)
		rangeBefore += float64(upperBefore.OffsetSecs - lowerBefore.OffsetSecs)
		rangeAfter += float64(upperAfter.OffsetSecs - lowerAfter.OffsetSecs)
		return rangeBefore, rangeAfter, true

	case types.StringFamily, types.BytesFamily, types.UuidFamily, types.INetFamily:
		// For non-numeric types, convert the datums to encoded keys to
		// approximate the range. We utilize an array to reduce repetitive code.
		boundArr := [4]tree.Datum{
			beforeLowerBound, beforeUpperBound, afterLowerBound, afterUpperBound,
		}
		var boundArrByte [4][]byte

		for i := range boundArr {
			var err error
			// Encode each bound value into a sortable byte format.
			boundArrByte[i], err = keyside.Encode(nil, boundArr[i], encoding.Ascending)
			if err != nil {
				return 0, 0, false
			}
		}

		// Remove common prefix.
		ind := getCommonPrefix(boundArrByte)
		for i := range boundArrByte {
			// Fix length of byte arrays to 8 bytes.
			boundArrByte[i] = getFixedLenArr(boundArrByte[i], ind, 8 /* fixLen */)
		}

		rangeBefore = float64(binary.BigEndian.Uint64(boundArrByte[1]) -
			binary.BigEndian.Uint64(boundArrByte[0]))
		rangeAfter = float64(binary.BigEndian.Uint64(boundArrByte[3]) -
			binary.BigEndian.Uint64(boundArrByte[2]))

		return rangeBefore, rangeAfter, true

	default:
		// Range calculations are not supported for the given type family.
		return 0, 0, false
	}
}

// getCommonPrefix returns the first index where the value at said index differs
// across all byte arrays in byteArr. byteArr must contain at least one element
// to compute a common prefix.
func getCommonPrefix(byteArr [4][]byte) int {
	// Checks if the current value at index is the same between all byte arrays.
	currIndMatching := func(ind int) bool {
		for i := 0; i < len(byteArr); i++ {
			if ind >= len(byteArr[i]) || byteArr[0][ind] != byteArr[i][ind] {
				return false
			}
		}
		return true
	}

	ind := 0
	for currIndMatching(ind) {
		ind++
	}

	return ind
}

// getFixedLenArr returns a byte array of size fixLen starting from specified
// index within the original byte array.
func getFixedLenArr(byteArr []byte, ind, fixLen int) []byte {

	if len(byteArr) <= 0 {
		panic(errors.AssertionFailedf("byteArr must have at least one element"))
	}

	if fixLen <= 0 {
		panic(errors.AssertionFailedf("desired fixLen must be greater than 0"))
	}

	if ind < 0 || ind > len(byteArr) {
		panic(errors.AssertionFailedf("ind must be contained within byteArr"))
	}

	// If byteArr is insufficient to hold desired size of byte array (fixLen),
	// allocate new array, else return subarray of size fixLen starting at ind
	if len(byteArr) < ind+fixLen {
		byteArrFix := make([]byte, fixLen)
		copy(byteArrFix, byteArr[ind:])
		return byteArrFix
	}

	return byteArr[ind : ind+fixLen]
}
