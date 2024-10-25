// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package num32

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScalar(t *testing.T) {
	negZeroBits := math.Float32frombits(0x80000000)

	require.Equal(t, float32(0), Abs(negZeroBits))
	require.Equal(t, float32(10.5), Abs(10.5))
	require.Equal(t, float32(10.5), Abs(-10.5))
	require.Equal(t, Inf32, Abs(Inf32))
	require.Equal(t, Inf32, Abs(-Inf32))
	require.True(t, IsNaN(Abs(NaN32)))

	require.Equal(t, float32(1000), Sqrt(1_000_000))
	require.InDelta(t, float32(1.4142135), Sqrt(2), 0.0000001)
	require.Equal(t, float32(0), Sqrt(0))
	require.Equal(t, negZeroBits, Sqrt(negZeroBits))
	require.Equal(t, Inf32, Sqrt(Inf32))
	require.True(t, IsNaN(Sqrt(-1)))
	require.True(t, IsNaN(Sqrt(NaN32)))

	require.False(t, IsNaN(-1_000_000))
	require.False(t, IsNaN(0))
	require.False(t, IsNaN(Inf32))
	require.True(t, IsNaN(NaN32))
}
