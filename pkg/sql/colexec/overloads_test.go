// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"math"
	"testing"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestIntegerAddition(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	defer leaktest.AfterTest(t)()
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
	defer leaktest.AfterTest(t)()
	d := &apd.Decimal{}
	var res apd.Decimal

	res = performDivInt16Int16(math.MinInt16, -1)
	require.Equal(t, 0, res.Cmp(d.SetFinite(-math.MinInt16, 0)))
	res = performDivInt32Int32(math.MinInt32, -1)
	require.Equal(t, 0, res.Cmp(d.SetFinite(-math.MinInt32, 0)))
	res = performDivInt64Int64(math.MinInt64, -1)
	d.SetFinite(math.MinInt64, 0)
	if _, err := tree.DecimalCtx.Neg(d, d); err != nil {
		t.Error(err)
	}
	require.Equal(t, 0, res.Cmp(d))

	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt16Int16(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt32Int32(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt64Int64(10, 0) }))

	res = performDivInt16Int16(math.MaxInt16, -1)
	require.Equal(t, 0, res.Cmp(d.SetFinite(-math.MaxInt16, 0)))
	res = performDivInt32Int32(math.MaxInt32, -1)
	require.Equal(t, 0, res.Cmp(d.SetFinite(-math.MaxInt32, 0)))
	res = performDivInt64Int64(math.MaxInt64, -1)
	require.Equal(t, 0, res.Cmp(d.SetFinite(-math.MaxInt64, 0)))
}

func TestIntegerMultiplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16Int16(math.MinInt16, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32Int32(math.MinInt32, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MinInt64, -1) }))

	require.Equal(t, int16(-math.MaxInt16), performMultInt16Int16(math.MaxInt16, -1))
	require.Equal(t, int32(-math.MaxInt32), performMultInt32Int32(math.MaxInt32, -1))
	require.Equal(t, int64(-math.MaxInt64), performMultInt64Int64(math.MaxInt64, -1))

	require.Equal(t, int16(0), performMultInt16Int16(math.MinInt16, 0))
	require.Equal(t, int32(0), performMultInt32Int32(math.MinInt32, 0))
	require.Equal(t, int64(0), performMultInt64Int64(math.MinInt64, 0))

	require.Equal(t, int16(120), performMultInt16Int16(-10, -12))
	require.Equal(t, int32(-120), performMultInt32Int32(-12, 10))
	require.Equal(t, int64(-120), performMultInt64Int64(12, -10))
}

func TestMixedTypeInteger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	require.Equal(t, int64(22), performPlusInt16Int32(10, 12))
	require.Equal(t, int64(-22), performPlusInt16Int64(-10, -12))
	require.Equal(t, int64(2), performPlusInt64Int32(-10, 12))
	require.Equal(t, int64(-2), performPlusInt64Int16(10, -12))

	require.Equal(t, int64(-2), performMinusInt16Int32(10, 12))
	require.Equal(t, int64(2), performMinusInt16Int64(-10, -12))
	require.Equal(t, int64(-22), performMinusInt64Int32(-10, 12))
	require.Equal(t, int64(22), performMinusInt64Int16(10, -12))

	require.Equal(t, int64(120), performMultInt16Int32(10, 12))
	require.Equal(t, int64(120), performMultInt16Int64(-10, -12))
	require.Equal(t, int64(-120), performMultInt64Int32(-12, 10))
	require.Equal(t, int64(-120), performMultInt64Int16(12, -10))

	d := &apd.Decimal{}
	var res apd.Decimal

	res = performDivInt16Int32(4, 2)
	require.Equal(t, 0, res.Cmp(d.SetFinite(2, 0)))
	res = performDivInt16Int64(6, 2)
	require.Equal(t, 0, res.Cmp(d.SetFinite(3, 0)))
	res = performDivInt64Int32(12, 3)
	require.Equal(t, 0, res.Cmp(d.SetFinite(4, 0)))
	res = performDivInt64Int16(20, 4)
	require.Equal(t, 0, res.Cmp(d.SetFinite(5, 0)))
}

func TestDecimalDivByZero(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nonZeroDec, zeroDec := apd.Decimal{}, apd.Decimal{}
	nonZeroDec.SetFinite(4, 0)
	zeroDec.SetFinite(0, 0)

	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivDecimalInt16(nonZeroDec, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivDecimalInt32(nonZeroDec, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivDecimalInt64(nonZeroDec, 0) }))

	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt64Decimal(2, zeroDec) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt32Decimal(2, zeroDec) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt16Decimal(2, zeroDec) }))

	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivDecimalDecimal(nonZeroDec, zeroDec) }))
}

func TestDecimalComparison(t *testing.T) {
	defer leaktest.AfterTest(t)()
	d := apd.Decimal{}
	if _, err := d.SetFloat64(1.234); err != nil {
		t.Error(err)
	}
	require.Equal(t, true, performEQDecimalDecimal(d, d))
	require.Equal(t, true, performEQDecimalFloat64(d, 1.234))
	require.Equal(t, false, performEQDecimalInt16(d, 1))
	require.Equal(t, false, performEQDecimalInt32(d, 2))
	require.Equal(t, false, performEQDecimalInt64(d, 3))

	require.Equal(t, false, performLTDecimalDecimal(d, d))
	require.Equal(t, true, performLTDecimalFloat64(d, 4.234))
	require.Equal(t, false, performLTDecimalInt16(d, 1))
	require.Equal(t, true, performLTDecimalInt32(d, 2))
	require.Equal(t, true, performLTDecimalInt64(d, 3))

	require.Equal(t, true, performGEDecimalDecimal(d, d))
	require.Equal(t, false, performGEDecimalFloat64(d, 4.234))
	require.Equal(t, true, performGEDecimalInt16(d, 1))
	require.Equal(t, false, performGEDecimalInt32(d, 2))
	require.Equal(t, false, performGEDecimalInt64(d, 3))
}

func TestFloatComparison(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := 1.234
	d := apd.Decimal{}
	if _, err := d.SetFloat64(f); err != nil {
		t.Error(err)
	}
	require.Equal(t, true, performEQFloat64Decimal(f, d))
	require.Equal(t, true, performEQFloat64Float64(f, 1.234))
	require.Equal(t, false, performEQFloat64Int16(f, 1))
	require.Equal(t, false, performEQFloat64Int32(f, 2))
	require.Equal(t, false, performEQFloat64Int64(f, 3))

	require.Equal(t, false, performLTFloat64Decimal(f, d))
	require.Equal(t, true, performLTFloat64Float64(f, 4.234))
	require.Equal(t, false, performLTFloat64Int16(f, 1))
	require.Equal(t, true, performLTFloat64Int32(f, 2))
	require.Equal(t, true, performLTFloat64Int64(f, 3))

	require.Equal(t, true, performGEFloat64Decimal(f, d))
	require.Equal(t, false, performGEFloat64Float64(f, 4.234))
	require.Equal(t, true, performGEFloat64Int16(f, 1))
	require.Equal(t, false, performGEFloat64Int32(f, 2))
	require.Equal(t, false, performGEFloat64Int64(f, 3))
}

func TestIntComparison(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i := int64(2)
	d := apd.Decimal{}
	if _, err := d.SetFloat64(1.234); err != nil {
		t.Error(err)
	}
	require.Equal(t, false, performEQInt64Decimal(i, d))
	require.Equal(t, false, performEQInt64Float64(i, 1.234))
	require.Equal(t, false, performEQInt64Int16(i, 1))
	require.Equal(t, true, performEQInt64Int32(i, 2))
	require.Equal(t, false, performEQInt64Int64(i, 3))

	require.Equal(t, false, performLTInt64Decimal(i, d))
	require.Equal(t, true, performLTInt64Float64(i, 4.234))
	require.Equal(t, false, performLTInt64Int16(i, 1))
	require.Equal(t, false, performLTInt64Int32(i, 2))
	require.Equal(t, true, performLTInt64Int64(i, 3))

	require.Equal(t, true, performGEInt64Decimal(i, d))
	require.Equal(t, false, performGEInt64Float64(i, 4.234))
	require.Equal(t, true, performGEInt64Int16(i, 1))
	require.Equal(t, true, performGEInt64Int32(i, 2))
	require.Equal(t, false, performGEInt64Int64(i, 3))
}
