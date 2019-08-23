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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestIntegerAddition(t *testing.T) {
	// The addition overload is the same for all integer widths, so we only test
	// one of them.
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16Int16(1, math.MaxInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16Int16(-1, math.MinInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16Int16(math.MaxInt16, 1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16Int16(math.MinInt16, -1) }))

	require.Equal(t, int16(math.MaxInt16), performPlusInt16Int16(1, math.MaxInt16-1))
	require.Equal(t, int16(math.MinInt16), performPlusInt16Int16(-1, math.MinInt16+1))
	require.Equal(t, int16(math.MaxInt16-1), performPlusInt16Int16(-1, math.MaxInt16))
	require.Equal(t, int16(math.MinInt16+1), performPlusInt16Int16(1, math.MinInt16))

	require.Equal(t, int16(22), performPlusInt16Int16(10, 12))
	require.Equal(t, int16(-22), performPlusInt16Int16(-10, -12))
	require.Equal(t, int16(2), performPlusInt16Int16(-10, 12))
	require.Equal(t, int16(-2), performPlusInt16Int16(10, -12))
}

func TestIntegerSubtraction(t *testing.T) {
	// The subtraction overload is the same for all integer widths, so we only
	// test one of them.
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16Int16(1, -math.MaxInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16Int16(-2, math.MaxInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16Int16(math.MaxInt16, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16Int16(math.MinInt16, 1) }))

	require.Equal(t, int16(math.MaxInt16), performMinusInt16Int16(1, -math.MaxInt16+1))
	require.Equal(t, int16(math.MinInt16), performMinusInt16Int16(-1, math.MaxInt16))
	require.Equal(t, int16(math.MaxInt16-1), performMinusInt16Int16(-1, -math.MaxInt16))
	require.Equal(t, int16(math.MinInt16+1), performMinusInt16Int16(0, math.MaxInt16))

	require.Equal(t, int16(-2), performMinusInt16Int16(10, 12))
	require.Equal(t, int16(2), performMinusInt16Int16(-10, -12))
	require.Equal(t, int16(-22), performMinusInt16Int16(-10, 12))
	require.Equal(t, int16(22), performMinusInt16Int16(10, -12))
}

func TestIntegerDivision(t *testing.T) {
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt8Int8(math.MinInt8, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt16Int16(math.MinInt16, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt32Int32(math.MinInt32, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt64Int64(math.MinInt64, -1) }))

	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt8Int8(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt16Int16(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt32Int32(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt64Int64(10, 0) }))

	require.Equal(t, int8(-math.MaxInt8), performDivInt8Int8(math.MaxInt8, -1))
	require.Equal(t, int16(-math.MaxInt16), performDivInt16Int16(math.MaxInt16, -1))
	require.Equal(t, int32(-math.MaxInt32), performDivInt32Int32(math.MaxInt32, -1))
	require.Equal(t, int64(-math.MaxInt64), performDivInt64Int64(math.MaxInt64, -1))

	require.Equal(t, int16(0), performDivInt16Int16(10, 12))
	require.Equal(t, int16(0), performDivInt16Int16(-10, -12))
	require.Equal(t, int16(-1), performDivInt16Int16(-12, 10))
	require.Equal(t, int16(-1), performDivInt16Int16(12, -10))
}

func TestIntegerMultiplication(t *testing.T) {
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8Int8(math.MaxInt8-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8Int8(math.MaxInt8-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8Int8(math.MinInt8+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8Int8(math.MinInt8+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16Int16(math.MaxInt16-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16Int16(math.MaxInt16-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16Int16(math.MinInt16+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16Int16(math.MinInt16+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32Int32(math.MaxInt32-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32Int32(math.MaxInt32-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32Int32(math.MinInt32+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32Int32(math.MinInt32+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MaxInt64-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MaxInt64-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MinInt64+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MinInt64+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8Int8(math.MinInt8, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16Int16(math.MinInt16, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32Int32(math.MinInt32, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MinInt64, -1) }))

	require.Equal(t, int8(-math.MaxInt8), performMultInt8Int8(math.MaxInt8, -1))
	require.Equal(t, int16(-math.MaxInt16), performMultInt16Int16(math.MaxInt16, -1))
	require.Equal(t, int32(-math.MaxInt32), performMultInt32Int32(math.MaxInt32, -1))
	require.Equal(t, int64(-math.MaxInt64), performMultInt64Int64(math.MaxInt64, -1))

	require.Equal(t, int8(0), performMultInt8Int8(math.MinInt8, 0))
	require.Equal(t, int16(0), performMultInt16Int16(math.MinInt16, 0))
	require.Equal(t, int32(0), performMultInt32Int32(math.MinInt32, 0))
	require.Equal(t, int64(0), performMultInt64Int64(math.MinInt64, 0))

	require.Equal(t, int8(120), performMultInt8Int8(10, 12))
	require.Equal(t, int16(120), performMultInt16Int16(-10, -12))
	require.Equal(t, int32(-120), performMultInt32Int32(-12, 10))
	require.Equal(t, int64(-120), performMultInt64Int64(12, -10))
}

func TestDecimalComparison(t *testing.T) {
	d := apd.Decimal{}
	if _, err := d.SetFloat64(1.234); err != nil {
		t.Error(err)
	}
	require.Equal(t, true, performEQDecimalDecimal(d, d))
	require.Equal(t, false, performEQDecimalFloat32(d, 0.43))
	require.Equal(t, true, performEQDecimalFloat64(d, 1.234))
	require.Equal(t, false, performEQDecimalInt8(d, 0))
	require.Equal(t, false, performEQDecimalInt16(d, 1))
	require.Equal(t, false, performEQDecimalInt32(d, 2))
	require.Equal(t, false, performEQDecimalInt64(d, 3))

	require.Equal(t, false, performLTDecimalDecimal(d, d))
	require.Equal(t, false, performLTDecimalFloat32(d, 0.43))
	require.Equal(t, true, performLTDecimalFloat64(d, 4.234))
	require.Equal(t, false, performLTDecimalInt8(d, 0))
	require.Equal(t, false, performLTDecimalInt16(d, 1))
	require.Equal(t, true, performLTDecimalInt32(d, 2))
	require.Equal(t, true, performLTDecimalInt64(d, 3))

	require.Equal(t, true, performGEDecimalDecimal(d, d))
	require.Equal(t, true, performGEDecimalFloat32(d, 0.43))
	require.Equal(t, false, performGEDecimalFloat64(d, 4.234))
	require.Equal(t, true, performGEDecimalInt8(d, 0))
	require.Equal(t, true, performGEDecimalInt16(d, 1))
	require.Equal(t, false, performGEDecimalInt32(d, 2))
	require.Equal(t, false, performGEDecimalInt64(d, 3))
}

func TestFloatComparison(t *testing.T) {
	f := 1.234
	require.Equal(t, false, performEQFloat64Float32(f, 0.43))
	require.Equal(t, true, performEQFloat64Float64(f, 1.234))
	require.Equal(t, false, performEQFloat64Int8(f, 0))
	require.Equal(t, false, performEQFloat64Int16(f, 1))
	require.Equal(t, false, performEQFloat64Int32(f, 2))
	require.Equal(t, false, performEQFloat64Int64(f, 3))

	require.Equal(t, false, performLTFloat64Float32(f, 0.43))
	require.Equal(t, true, performLTFloat64Float64(f, 4.234))
	require.Equal(t, false, performLTFloat64Int8(f, 0))
	require.Equal(t, false, performLTFloat64Int16(f, 1))
	require.Equal(t, true, performLTFloat64Int32(f, 2))
	require.Equal(t, true, performLTFloat64Int64(f, 3))

	require.Equal(t, true, performGEFloat64Float32(f, 0.43))
	require.Equal(t, false, performGEFloat64Float64(f, 4.234))
	require.Equal(t, true, performGEFloat64Int8(f, 0))
	require.Equal(t, true, performGEFloat64Int16(f, 1))
	require.Equal(t, false, performGEFloat64Int32(f, 2))
	require.Equal(t, false, performGEFloat64Int64(f, 3))
}
