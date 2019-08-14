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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegerAddition(t *testing.T) {
	// The addition overload is the same for all integer widths, so we only test
	// one of them.
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16(1, math.MaxInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16(-1, math.MinInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16(math.MaxInt16, 1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performPlusInt16(math.MinInt16, -1) }))

	assert.Equal(t, int16(math.MaxInt16), performPlusInt16(1, math.MaxInt16-1))
	assert.Equal(t, int16(math.MinInt16), performPlusInt16(-1, math.MinInt16+1))
	assert.Equal(t, int16(math.MaxInt16-1), performPlusInt16(-1, math.MaxInt16))
	assert.Equal(t, int16(math.MinInt16+1), performPlusInt16(1, math.MinInt16))

	assert.Equal(t, int16(22), performPlusInt16(10, 12))
	assert.Equal(t, int16(-22), performPlusInt16(-10, -12))
	assert.Equal(t, int16(2), performPlusInt16(-10, 12))
	assert.Equal(t, int16(-2), performPlusInt16(10, -12))
}

func TestIntegerSubtraction(t *testing.T) {
	// The subtraction overload is the same for all integer widths, so we only
	// test one of them.
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16(1, -math.MaxInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16(-2, math.MaxInt16) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16(math.MaxInt16, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMinusInt16(math.MinInt16, 1) }))

	assert.Equal(t, int16(math.MaxInt16), performMinusInt16(1, -math.MaxInt16+1))
	assert.Equal(t, int16(math.MinInt16), performMinusInt16(-1, math.MaxInt16))
	assert.Equal(t, int16(math.MaxInt16-1), performMinusInt16(-1, -math.MaxInt16))
	assert.Equal(t, int16(math.MinInt16+1), performMinusInt16(0, math.MaxInt16))

	assert.Equal(t, int16(-2), performMinusInt16(10, 12))
	assert.Equal(t, int16(2), performMinusInt16(-10, -12))
	assert.Equal(t, int16(-22), performMinusInt16(-10, 12))
	assert.Equal(t, int16(22), performMinusInt16(10, -12))
}

func TestIntegerDivision(t *testing.T) {
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt8(math.MinInt8, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt16(math.MinInt16, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt32(math.MinInt32, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performDivInt64(math.MinInt64, -1) }))

	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt8(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt16(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt32(10, 0) }))
	require.Equal(t, tree.ErrDivByZero, execerror.CatchVectorizedRuntimeError(func() { performDivInt64(10, 0) }))

	assert.Equal(t, int8(-math.MaxInt8), performDivInt8(math.MaxInt8, -1))
	assert.Equal(t, int16(-math.MaxInt16), performDivInt16(math.MaxInt16, -1))
	assert.Equal(t, int32(-math.MaxInt32), performDivInt32(math.MaxInt32, -1))
	assert.Equal(t, int64(-math.MaxInt64), performDivInt64(math.MaxInt64, -1))

	assert.Equal(t, int16(0), performDivInt16(10, 12))
	assert.Equal(t, int16(0), performDivInt16(-10, -12))
	assert.Equal(t, int16(-1), performDivInt16(-12, 10))
	assert.Equal(t, int16(-1), performDivInt16(12, -10))
}

func TestIntegerMultiplication(t *testing.T) {
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8(math.MaxInt8-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8(math.MaxInt8-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8(math.MinInt8+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8(math.MinInt8+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16(math.MaxInt16-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16(math.MaxInt16-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16(math.MinInt16+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16(math.MinInt16+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32(math.MaxInt32-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32(math.MaxInt32-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32(math.MinInt32+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32(math.MinInt32+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64(math.MaxInt64-1, 100) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64(math.MaxInt64-1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64(math.MinInt64+1, 3) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64(math.MinInt64+1, 100) }))

	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt8(math.MinInt8, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt16(math.MinInt16, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt32(math.MinInt32, -1) }))
	require.Equal(t, tree.ErrIntOutOfRange, execerror.CatchVectorizedRuntimeError(func() { performMultInt64(math.MinInt64, -1) }))

	assert.Equal(t, int8(-math.MaxInt8), performMultInt8(math.MaxInt8, -1))
	assert.Equal(t, int16(-math.MaxInt16), performMultInt16(math.MaxInt16, -1))
	assert.Equal(t, int32(-math.MaxInt32), performMultInt32(math.MaxInt32, -1))
	assert.Equal(t, int64(-math.MaxInt64), performMultInt64(math.MaxInt64, -1))

	assert.Equal(t, int8(0), performMultInt8(math.MinInt8, 0))
	assert.Equal(t, int16(0), performMultInt16(math.MinInt16, 0))
	assert.Equal(t, int32(0), performMultInt32(math.MinInt32, 0))
	assert.Equal(t, int64(0), performMultInt64(math.MinInt64, 0))

	assert.Equal(t, int8(120), performMultInt8(10, 12))
	assert.Equal(t, int16(120), performMultInt16(-10, -12))
	assert.Equal(t, int32(-120), performMultInt32(-12, 10))
	assert.Equal(t, int64(-120), performMultInt64(12, -10))
}
