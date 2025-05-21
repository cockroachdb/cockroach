// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecdist

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

func TestMetric(t *testing.T) {
	l2Sq := Measure(L2Squared, vector.T{1, 2}, vector.T{4, 3})
	require.Equal(t, float32(10), l2Sq)

	ip := Measure(InnerProduct, vector.T{1, 2}, vector.T{4, 3})
	require.Equal(t, float32(-10), ip)

	cos := Measure(Cosine, vector.T{1, 2}, vector.T{4, 3})
	require.Equal(t, float32(-9), cos)

	cos = Measure(Cosine, vector.T{1, 0}, vector.T{0.7071, 0.7071})
	require.Equal(t, float64(0.2929), scalar.Round(float64(cos), 4))

	// Test zero product of norms.
	cos = Measure(Cosine, vector.T{1, 0}, vector.T{0, 1})
	require.Equal(t, float32(1), cos)
}

func TestParseMetric(t *testing.T) {
	testCases := []struct {
		input    string
		expect   Metric
		hasError bool
	}{
		{L2Squared.String(), L2Squared, false},
		{"l2squared", L2Squared, false},
		{"l2-squared", L2Squared, false},
		{InnerProduct.String(), InnerProduct, false},
		{"innerproduct", InnerProduct, false},
		{"inner-product", InnerProduct, false},
		{Cosine.String(), Cosine, false},
		{"cosine", Cosine, false},
		{"not-a-metric", 0, true},
	}

	for _, tc := range testCases {
		metric, err := ParseMetric(tc.input)
		if tc.hasError {
			require.Errorf(t, err, "input: %q", tc.input)
		} else {
			require.NoErrorf(t, err, "input: %q", tc.input)
			require.Equal(t, tc.expect, metric, "input: %q", tc.input)
		}
	}
}
