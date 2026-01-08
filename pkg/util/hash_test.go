// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestFibHash(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	counts := make(map[uint64]int)
	for range 1000 {
		h := FibHash(rng.Uint64(), 3)
		counts[h]++
	}

	// Let's test using a ptr value. We use this slice to keep the pointers alive
	// so that we don't just get the same pointer each time.
	vs := make([]*int, 0, 1000)
	for range 1000 {
		v := new(int)
		vs = append(vs, v)
		h := FibHash(uint64(uintptr(unsafe.Pointer(v))), 3)
		counts[h]++
	}
	require.Equal(t, 1000, len(vs))
	// We can only make some weak assertions about the distribution here.
	for k, v := range counts {
		require.True(t, k < 8)
		perfectDistributionCount := 2000 / 8
		require.Less(t, v, perfectDistributionCount*2)
	}
}
