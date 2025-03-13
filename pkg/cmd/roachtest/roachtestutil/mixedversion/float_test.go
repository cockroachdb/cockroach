// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFloatRands1(t *testing.T) {
	r := newRand()
	count := 0
	for i := 0; i < 1000000; i++ {
		f := r.Float64()
		if f > 0.5 {
			count++
		}
	}
	require.Equal(t, count, 500939)
}

func TestFloatRands2(t *testing.T) {
	r := newRand()
	count := 0
	for i := 0; i < 1000000; i++ {
		f := r.Float64()
		if f < 0.5 {
			count++
		}
	}
	require.Equal(t, count, 499061)
}
