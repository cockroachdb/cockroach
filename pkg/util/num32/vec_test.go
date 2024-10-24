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

var NaN32 = float32(math.NaN())
var Inf32 = float32(math.Inf(1))

func TestDistances(t *testing.T) {
	// Test L1, L2, Cosine distance.
	testCases := []struct {
		v1     []float32
		v2     []float32
		l1     float32
		l2s    float32
		panics bool
	}{
		{v1: []float32{}, v2: []float32{}, l1: 0, l2s: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{4, 5, 6}, l1: 9, l2s: 27},
		{v1: []float32{-1, -2, -3}, v2: []float32{-4, -5, -6}, l1: 9, l2s: 27},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 3}, l1: 0, l2s: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 4}, l1: 1, l2s: 1},
		{v1: []float32{NaN32}, v2: []float32{1}, l1: NaN32, l2s: NaN32},
		{v1: []float32{Inf32}, v2: []float32{1}, l1: Inf32, l2s: Inf32},
		{v1: []float32{1, 2}, v2: []float32{3, 4, 5}, panics: true},
	}

	for _, tc := range testCases {
		if !tc.panics {
			l1 := L1Distance(tc.v1, tc.v2)
			l2s := L2SquaredDistance(tc.v1, tc.v2)
			require.InDelta(t, tc.l1, l1, 0.000001)
			require.InDelta(t, tc.l2s, l2s, 0.000001)
		} else {
			require.Panics(t, func() { L1Distance(tc.v1, tc.v2) })
			require.Panics(t, func() { L2SquaredDistance(tc.v1, tc.v2) })
		}
	}
}

func TestInnerProduct(t *testing.T) {
	// Test inner product and negative inner product
	testCases := []struct {
		v1     []float32
		v2     []float32
		ip     float32
		panics bool
	}{
		{v1: []float32{}, v2: []float32{}, ip: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{4, 5, 6}, ip: 32},
		{v1: []float32{-1, -2, -3}, v2: []float32{-4, -5, -6}, ip: 32},
		{v1: []float32{0, 0, 0}, v2: []float32{0, 0, 0}, ip: 0},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 3}, ip: 14},
		{v1: []float32{1, 2, 3}, v2: []float32{1, 2, 4}, ip: 17},
		{v1: []float32{NaN32}, v2: []float32{1}, ip: NaN32},
		{v1: []float32{Inf32}, v2: []float32{1}, ip: Inf32},
		{v1: []float32{1, 2}, v2: []float32{3, 4, 5}, panics: true},
	}

	for _, tc := range testCases {
		if !tc.panics {
			ip := InnerProduct(tc.v1, tc.v2)
			require.InDelta(t, tc.ip, ip, 0.000001)
		} else {
			require.Panics(t, func() { InnerProduct(tc.v1, tc.v2) })
		}
	}
}
