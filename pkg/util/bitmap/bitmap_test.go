// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bitmap

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestBitmap(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	n := rng.Intn(1000) + 1

	bm := NewBitmap(n)
	naive := make([]bool, n)
	for op := 0; op < 1000; op++ {
		i := rng.Intn(n)
		bm.Set(i)
		naive[i] = true
		for j := 0; j < n; j++ {
			require.Equal(t, naive[j], bm.IsSet(j))
		}
	}
}
