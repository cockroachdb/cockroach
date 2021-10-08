// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestAllocMergeRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	run := func(t *testing.T, N, P int) {
		require.True(t, N >= P) // test assumes this invariant
		pools := make([]*testAllocPool, P)
		allocs := make([]Alloc, N)

		// Make P pools.
		for i := range pools {
			pools[i] = &testAllocPool{}
		}

		// Allocate N allocs from the P pools.
		poolPerm := rand.Perm(P)
		for i := range allocs {
			allocs[i] = pools[poolPerm[i%P]].alloc()
		}

		// Randomly merge the allocs together.
		perm := rand.Perm(N)
		for i := 0; i < N-1; i++ {
			p := perm[i]
			toMergeInto := perm[i+1+rand.Intn(N-i-1)]
			allocs[toMergeInto].Merge(&allocs[p])
		}

		// Ensure that the remaining alloc, which has received all of the
		// others, has P-1 other allocs.
		require.Len(t, allocs[perm[N-1]].otherPoolAllocs, P-1)
		for i := 0; i < N-1; i++ {
			require.True(t, allocs[perm[i]].isZero())
		}

		// Ensure that all N allocations worth of data are still outstanding
		sum := func() (ret int) {
			for _, p := range pools {
				ret += p.getN()
			}
			return ret
		}
		require.Equal(t, N, sum())

		// Release the remaining alloc.
		allocs[perm[N-1]].Release(context.Background())
		// Ensure it now is zero-valued.
		require.True(t, allocs[perm[N-1]].isZero())
		// Ensure that all of the resources have been released.
		require.Equal(t, 0, sum())
	}
	for _, np := range []struct{ N, P int }{
		{1, 1},
		{2, 2},
		{1000, 2},
		{10000, 1000},
	} {
		t.Run(fmt.Sprintf("N=%d,P=%d", np.N, np.P), func(t *testing.T) {
			run(t, np.N, np.P)
		})
	}
}

type testAllocPool struct {
	syncutil.Mutex
	n int64
}

// Release implements kvevent.pool interface.
func (ap *testAllocPool) Release(ctx context.Context, bytes, entries int64) {
	ap.Lock()
	defer ap.Unlock()
	if ap.n == 0 {
		panic("can't release zero resources")
	}
	ap.n -= bytes
}

func (ap *testAllocPool) alloc() Alloc {
	ap.Lock()
	defer ap.Unlock()
	ap.n++
	return TestingMakeAlloc(1, ap)
}

func (ap *testAllocPool) getN() int {
	ap.Lock()
	defer ap.Unlock()
	return int(ap.n)
}
