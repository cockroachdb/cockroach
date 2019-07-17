// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestIntegerAddition(t *testing.T) {
	// The addition overload is the same for all integer widths, so we only test
	// one of them.
	assertPanic(t, tree.ErrIntOutOfRange, func() { performPlusInt16(1, math.MaxInt16) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performPlusInt16(-1, math.MinInt16) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performPlusInt16(math.MaxInt16, 1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performPlusInt16(math.MinInt16, -1) })

	assertIntegerEquals(t, math.MaxInt16, int(performPlusInt16(1, math.MaxInt16-1)))
	assertIntegerEquals(t, math.MinInt16, int(performPlusInt16(-1, math.MinInt16+1)))
	assertIntegerEquals(t, math.MaxInt16-1, int(performPlusInt16(-1, math.MaxInt16)))
	assertIntegerEquals(t, math.MinInt16+1, int(performPlusInt16(1, math.MinInt16)))

	assertIntegerEquals(t, 22, int(performPlusInt16(10, 12)))
	assertIntegerEquals(t, -22, int(performPlusInt16(-10, -12)))
	assertIntegerEquals(t, 2, int(performPlusInt16(-10, 12)))
	assertIntegerEquals(t, -2, int(performPlusInt16(10, -12)))
}

func TestIntegerSubtraction(t *testing.T) {
	// The subtraction overload is the same for all integer widths, so we only
	// test one of them.
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMinusInt16(1, -math.MaxInt16) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMinusInt16(-2, math.MaxInt16) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMinusInt16(math.MaxInt16, -1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMinusInt16(math.MinInt16, 1) })

	assertIntegerEquals(t, math.MaxInt16, int(performMinusInt16(1, -math.MaxInt16+1)))
	assertIntegerEquals(t, math.MinInt16, int(performMinusInt16(-1, math.MaxInt16)))
	assertIntegerEquals(t, math.MaxInt16-1, int(performMinusInt16(-1, -math.MaxInt16)))
	assertIntegerEquals(t, math.MinInt16+1, int(performMinusInt16(0, math.MaxInt16)))

	assertIntegerEquals(t, -2, int(performMinusInt16(10, 12)))
	assertIntegerEquals(t, 2, int(performMinusInt16(-10, -12)))
	assertIntegerEquals(t, -22, int(performMinusInt16(-10, 12)))
	assertIntegerEquals(t, 22, int(performMinusInt16(10, -12)))
}

func TestIntegerDivision(t *testing.T) {
	assertPanic(t, tree.ErrIntOutOfRange, func() { performDivInt8(math.MinInt8, -1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performDivInt16(math.MinInt16, -1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performDivInt32(math.MinInt32, -1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performDivInt64(math.MinInt64, -1) })

	assertPanic(t, tree.ErrDivByZero, func() { performDivInt8(10, 0) })
	assertPanic(t, tree.ErrDivByZero, func() { performDivInt16(10, 0) })
	assertPanic(t, tree.ErrDivByZero, func() { performDivInt32(10, 0) })
	assertPanic(t, tree.ErrDivByZero, func() { performDivInt64(10, 0) })

	assertIntegerEquals(t, -math.MaxInt8, int(performDivInt8(math.MaxInt8, -1)))
	assertIntegerEquals(t, -math.MaxInt16, int(performDivInt16(math.MaxInt16, -1)))
	assertIntegerEquals(t, -math.MaxInt32, int(performDivInt32(math.MaxInt32, -1)))
	assertIntegerEquals(t, -math.MaxInt64, int(performDivInt64(math.MaxInt64, -1)))

	assertIntegerEquals(t, 0, int(performDivInt16(10, 12)))
	assertIntegerEquals(t, 0, int(performDivInt16(-10, -12)))
	assertIntegerEquals(t, -1, int(performDivInt16(-12, 10)))
	assertIntegerEquals(t, -1, int(performDivInt16(12, -10)))
}

func TestIntegerMultiplication(t *testing.T) {
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt8(math.MaxInt8-1, 100) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt8(math.MaxInt8-1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt8(math.MinInt8+1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt8(math.MinInt8+1, 100) })

	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt16(math.MaxInt16-1, 100) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt16(math.MaxInt16-1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt16(math.MinInt16+1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt16(math.MinInt16+1, 100) })

	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt32(math.MaxInt32-1, 100) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt32(math.MaxInt32-1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt32(math.MinInt32+1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt32(math.MinInt32+1, 100) })

	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt64(math.MaxInt64-1, 100) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt64(math.MaxInt64-1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt64(math.MinInt64+1, 3) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt64(math.MinInt64+1, 100) })

	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt8(math.MinInt8, -1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt16(math.MinInt16, -1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt32(math.MinInt32, -1) })
	assertPanic(t, tree.ErrIntOutOfRange, func() { performMultInt64(math.MinInt64, -1) })

	assertIntegerEquals(t, -math.MaxInt8, int(performMultInt8(math.MaxInt8, -1)))
	assertIntegerEquals(t, -math.MaxInt16, int(performMultInt16(math.MaxInt16, -1)))
	assertIntegerEquals(t, -math.MaxInt32, int(performMultInt32(math.MaxInt32, -1)))
	assertIntegerEquals(t, -math.MaxInt64, int(performMultInt64(math.MaxInt64, -1)))

	assertIntegerEquals(t, 0, int(performMultInt8(math.MinInt8, 0)))
	assertIntegerEquals(t, 0, int(performMultInt16(math.MinInt16, 0)))
	assertIntegerEquals(t, 0, int(performMultInt32(math.MinInt32, 0)))
	assertIntegerEquals(t, 0, int(performMultInt64(math.MinInt64, 0)))

	assertIntegerEquals(t, 120, int(performMultInt8(10, 12)))
	assertIntegerEquals(t, 120, int(performMultInt16(-10, -12)))
	assertIntegerEquals(t, -120, int(performMultInt32(-12, 10)))
	assertIntegerEquals(t, -120, int(performMultInt64(12, -10)))
}

func assertIntegerEquals(t *testing.T, expected, actual int) {
	if expected != actual {
		t.Errorf("Expected <%d> but found <%d>", expected, actual)
	}
}

func assertPanic(t *testing.T, expectedError error, f func()) {
	defer func() {
		if r := recover(); r != nil {
			if r == expectedError {
				return
			}
			t.Errorf("The code had an unexpected panic: %v", r)
		}
		t.Errorf("The code did not panic")
	}()
	f()
}
