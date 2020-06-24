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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestIntegerAddition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The addition overload is the same for all integer widths, so we only test
	// one of them.
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performPlusInt64Int64(1, math.MaxInt64) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performPlusInt64Int64(-1, math.MinInt64) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performPlusInt64Int64(math.MaxInt64, 1) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performPlusInt64Int64(math.MinInt64, -1) }), tree.ErrIntOutOfRange))

	require.Equal(t, int64(math.MaxInt64), performPlusInt64Int64(1, math.MaxInt64-1))
	require.Equal(t, int64(math.MinInt64), performPlusInt64Int64(-1, math.MinInt64+1))
	require.Equal(t, int64(math.MaxInt64-1), performPlusInt64Int64(-1, math.MaxInt64))
	require.Equal(t, int64(math.MinInt64+1), performPlusInt64Int64(1, math.MinInt64))

	require.Equal(t, int64(22), performPlusInt64Int64(10, 12))
	require.Equal(t, int64(-22), performPlusInt64Int64(-10, -12))
	require.Equal(t, int64(2), performPlusInt64Int64(-10, 12))
	require.Equal(t, int64(-2), performPlusInt64Int64(10, -12))
}

func TestIntegerSubtraction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The subtraction overload is the same for all integer widths, so we only
	// test one of them.
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMinusInt64Int64(1, -math.MaxInt64) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMinusInt64Int64(-2, math.MaxInt64) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMinusInt64Int64(math.MaxInt64, -1) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMinusInt64Int64(math.MinInt64, 1) }), tree.ErrIntOutOfRange))

	require.Equal(t, int64(math.MaxInt64), performMinusInt64Int64(1, -math.MaxInt64+1))
	require.Equal(t, int64(math.MinInt64), performMinusInt64Int64(-1, math.MaxInt64))
	require.Equal(t, int64(math.MaxInt64-1), performMinusInt64Int64(-1, -math.MaxInt64))
	require.Equal(t, int64(math.MinInt64+1), performMinusInt64Int64(0, math.MaxInt64))

	require.Equal(t, int64(-2), performMinusInt64Int64(10, 12))
	require.Equal(t, int64(2), performMinusInt64Int64(-10, -12))
	require.Equal(t, int64(-22), performMinusInt64Int64(-10, 12))
	require.Equal(t, int64(22), performMinusInt64Int64(10, -12))
}

func TestIntegerDivision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	d := &apd.Decimal{}
	var res apd.Decimal

	res = performDivInt16Int16(math.MinInt16, -1)
	require.Equal(t, 0, res.Cmp(d.SetInt64(-math.MinInt16)))
	res = performDivInt32Int32(math.MinInt32, -1)
	require.Equal(t, 0, res.Cmp(d.SetInt64(-math.MinInt32)))
	res = performDivInt64Int64(math.MinInt64, -1)
	d.SetInt64(math.MinInt64)
	if _, err := tree.DecimalCtx.Neg(d, d); err != nil {
		t.Error(err)
	}
	require.Equal(t, 0, res.Cmp(d))

	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivInt16Int16(10, 0) }), tree.ErrDivByZero))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivInt32Int32(10, 0) }), tree.ErrDivByZero))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivInt64Int64(10, 0) }), tree.ErrDivByZero))

	res = performDivInt16Int16(math.MaxInt16, -1)
	require.Equal(t, 0, res.Cmp(d.SetInt64(-math.MaxInt16)))
	res = performDivInt32Int32(math.MaxInt32, -1)
	require.Equal(t, 0, res.Cmp(d.SetInt64(-math.MaxInt32)))
	res = performDivInt64Int64(math.MaxInt64, -1)
	require.Equal(t, 0, res.Cmp(d.SetInt64(-math.MaxInt64)))
}

func TestIntegerMultiplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	require.Equal(t, 100*(int64(math.MaxInt16)-1), performMultInt16Int16(math.MaxInt16-1, 100))
	require.Equal(t, 3*(int64(math.MaxInt16)-1), performMultInt16Int16(math.MaxInt16-1, 3))
	require.Equal(t, 3*(int64(math.MinInt16)+1), performMultInt16Int16(math.MinInt16+1, 3))
	require.Equal(t, 100*(int64(math.MinInt16)+1), performMultInt16Int16(math.MinInt16+1, 100))

	require.Equal(t, 100*(int64(math.MaxInt32)-1), performMultInt32Int32(math.MaxInt32-1, 100))
	require.Equal(t, 3*(int64(math.MaxInt32)-1), performMultInt32Int32(math.MaxInt32-1, 3))
	require.Equal(t, 3*(int64(math.MinInt32)+1), performMultInt32Int32(math.MinInt32+1, 3))
	require.Equal(t, 100*(int64(math.MinInt32)+1), performMultInt32Int32(math.MinInt32+1, 100))

	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MaxInt64-1, 100) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MaxInt64-1, 3) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MinInt64+1, 3) }), tree.ErrIntOutOfRange))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MinInt64+1, 100) }), tree.ErrIntOutOfRange))

	require.Equal(t, -int64(math.MinInt16), performMultInt16Int16(math.MinInt16, -1))
	require.Equal(t, -int64(math.MinInt32), performMultInt32Int32(math.MinInt32, -1))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performMultInt64Int64(math.MinInt64, -1) }), tree.ErrIntOutOfRange))

	require.Equal(t, int64(-math.MaxInt16), performMultInt16Int16(math.MaxInt16, -1))
	require.Equal(t, int64(-math.MaxInt32), performMultInt32Int32(math.MaxInt32, -1))
	require.Equal(t, int64(-math.MaxInt64), performMultInt64Int64(math.MaxInt64, -1))

	require.Equal(t, int64(0), performMultInt16Int16(math.MinInt16, 0))
	require.Equal(t, int64(0), performMultInt32Int32(math.MinInt32, 0))
	require.Equal(t, int64(0), performMultInt64Int64(math.MinInt64, 0))

	require.Equal(t, int64(120), performMultInt16Int16(-10, -12))
	require.Equal(t, int64(-120), performMultInt32Int32(-12, 10))
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
	require.Equal(t, 0, res.Cmp(d.SetInt64(2)))
	res = performDivInt16Int64(6, 2)
	require.Equal(t, 0, res.Cmp(d.SetInt64(3)))
	res = performDivInt64Int32(12, 3)
	require.Equal(t, 0, res.Cmp(d.SetInt64(4)))
	res = performDivInt64Int16(20, 4)
	require.Equal(t, 0, res.Cmp(d.SetInt64(5)))
}

func TestDecimalDivByZero(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nonZeroDec, zeroDec := apd.Decimal{}, apd.Decimal{}
	nonZeroDec.SetInt64(4)
	zeroDec.SetInt64(0)

	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivDecimalInt16(nonZeroDec, 0) }), tree.ErrDivByZero))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivDecimalInt32(nonZeroDec, 0) }), tree.ErrDivByZero))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivDecimalInt64(nonZeroDec, 0) }), tree.ErrDivByZero))

	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivInt64Decimal(2, zeroDec) }), tree.ErrDivByZero))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivInt32Decimal(2, zeroDec) }), tree.ErrDivByZero))
	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivInt16Decimal(2, zeroDec) }), tree.ErrDivByZero))

	require.True(t, errors.Is(colexecerror.CatchVectorizedRuntimeError(func() { performDivDecimalDecimal(nonZeroDec, zeroDec) }), tree.ErrDivByZero))
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
