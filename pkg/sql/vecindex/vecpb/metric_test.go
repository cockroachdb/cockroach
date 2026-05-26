// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

func TestMetric(t *testing.T) {
	l2Sq := MeasureDistance(L2SquaredDistance, vector.T{1, 2}, vector.T{4, 3})
	require.Equal(t, float32(10), l2Sq)

	ip := MeasureDistance(InnerProductDistance, vector.T{1, 2}, vector.T{4, 3})
	require.Equal(t, float32(-10), ip)

	cos := MeasureDistance(CosineDistance, vector.T{1, 2}, vector.T{4, 3})
	require.Equal(t, float32(-9), cos)

	cos = MeasureDistance(CosineDistance, vector.T{1, 0}, vector.T{0.7071, 0.7071})
	require.Equal(t, float64(0.2929), scalar.Round(float64(cos), 4))

	// Test zero product of norms.
	cos = MeasureDistance(CosineDistance, vector.T{1, 0}, vector.T{0, 1})
	require.Equal(t, float32(1), cos)
}
