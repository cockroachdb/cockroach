// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package shuffle

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testSlice []int

// testSlice implements shuffle.Interface.
func (ts testSlice) Len() int      { return len(ts) }
func (ts testSlice) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }

func TestShuffle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	old := randSyncPool.New
	defer func() { randSyncPool.New = old }()
	r := rand.New(rand.NewSource(0))
	randSyncPool.New = func() interface{} {
		return r
	}

	verify := func(original, expected testSlice) {
		t.Helper()
		Shuffle(original)
		if !reflect.DeepEqual(original, expected) {
			t.Errorf("expected %v, got %v", expected, original)
		}
	}

	ts := testSlice{}
	verify(ts, testSlice{})
	verify(ts, testSlice{})

	ts = testSlice{1}
	verify(ts, testSlice{1})
	verify(ts, testSlice{1})

	ts = testSlice{1, 2}
	verify(ts, testSlice{1, 2})
	verify(ts, testSlice{2, 1})

	ts = testSlice{1, 2, 3}
	verify(ts, testSlice{3, 1, 2})
	verify(ts, testSlice{2, 3, 1})
	verify(ts, testSlice{1, 3, 2})
	verify(ts, testSlice{3, 1, 2})

	ts = testSlice{1, 2, 3, 4, 5}
	verify(ts, testSlice{5, 1, 3, 4, 2})
	verify(ts, testSlice{2, 5, 3, 1, 4})
	verify(ts, testSlice{3, 2, 5, 4, 1})
	verify(ts, testSlice{1, 2, 4, 3, 5})
	verify(ts, testSlice{3, 1, 5, 2, 4})

	verify(ts[2:2], testSlice{})
	verify(ts[0:0], testSlice{})
	verify(ts[5:5], testSlice{})
	verify(ts[3:5], testSlice{4, 2})
	verify(ts[3:5], testSlice{2, 4})
	verify(ts[0:2], testSlice{1, 3})
	verify(ts[0:2], testSlice{1, 3})
	verify(ts[1:4], testSlice{5, 2, 3})
	verify(ts[1:4], testSlice{3, 5, 2})
	verify(ts[0:4], testSlice{2, 3, 1, 5})
	verify(ts[0:4], testSlice{5, 3, 1, 2})

	verify(ts, testSlice{2, 3, 1, 5, 4})
}

type ints []int

func (i ints) Len() int      { return len(i) }
func (i ints) Swap(a, b int) { i[a], i[b] = i[b], i[a] }

// BenchmarkConcurrentShuffle is used to demonstrate that the Shuffle
// function scales with cores. Once upon a time, it did not.
func BenchmarkConcurrentShuffle(b *testing.B) {
	for _, concurrency := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			for _, size := range []int{1 << 7, 1 << 10, 1 << 13} {
				b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
					b.SetBytes(int64(size * int(unsafe.Sizeof(0))))
					bufs := make([]ints, 0, concurrency)
					for i := 0; i < concurrency; i++ {
						bufs = append(bufs, rand.Perm(size))
					}
					ns := distribute(b.N, concurrency)
					var wg sync.WaitGroup
					wg.Add(concurrency)
					b.ResetTimer()
					for i := 0; i < concurrency; i++ {
						go func(buf *ints, n int) {
							defer wg.Done()
							for j := 0; j < n; j++ {
								Shuffle(buf)
							}
						}(&bufs[i], ns[i])
					}
					wg.Wait()
				})
			}
		})
	}
}

// distribute returns a slice of <num> integers that add up to <total> and are
// within +/-1 of each other.
func distribute(total, num int) []int {
	res := make([]int, num)
	for i := range res {
		// Use the average number of remaining connections.
		div := len(res) - i
		res[i] = (total + div/2) / div
		total -= res[i]
	}
	return res
}
