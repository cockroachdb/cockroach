// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package queue

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func countChunks[T any](q Queue[T]) (n int) {
	for h := q.head; h != nil; h = h.next {
		n++
	}
	return n
}

func testQueue[T any](t *testing.T, n int, g func(i int) T) {
	var q Queue[T]
	require.Equal(t, 0, q.Len())
	require.True(t, q.Empty())
	require.Equal(t, 0, countChunks(q))

	var corpus []T
	if n > 0 || rand.Int()%2 == 0 { // when n is 0, keep corpus nil (50%), or initialized.
		corpus = make([]T, n)
		for i := 0; i < n; i++ {
			corpus[i] = g(i)
		}
	}

	expectChunks := func(n int) int {
		return int(math.Ceil(float64(n) / float64(chunkSize)))
	}

	insertCorpus := func() {
		for _, v := range corpus {
			q.Push(v)
		}
	}

	insertCorpus()
	require.Equal(t, expectChunks(len(corpus)), countChunks(q))

	n = 0
	for !q.Empty() {
		v, ok := q.PopFront()
		require.True(t, ok)
		require.Equal(t, corpus[n], v)
		n++
	}

	require.Equal(t, 0, q.Len())
	require.True(t, q.Empty())

	// Insert corpus again, but this time just release all chunks.
	insertCorpus()
	require.Equal(t, expectChunks(len(corpus)), countChunks(q))
	q.Release()
	require.Equal(t, 0, countChunks(q))
	require.Equal(t, 0, q.Len())
}

func queueTestCases[T any](t *testing.T, g func(i int) T) {
	for _, n := range []int{0, 1, chunkSize - 1, chunkSize, chunkSize + 1, rand.Intn(4 * chunkSize)} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			testQueue[T](t, n, g)
		})
	}
}

func TestQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("int", func(t *testing.T) {
		queueTestCases[int](t, func(i int) int {
			return rand.Int()
		})
	})

	t.Run("string", func(t *testing.T) {
		queueTestCases[string](t, func(i int) string {
			return strconv.Itoa(rand.Int())
		})
	})

	t.Run("[]int", func(t *testing.T) {
		queueTestCases[[]int](t, func(i int) (res []int) {
			for i := 0; i < rand.Intn(100); i++ {
				res = append(res, rand.Int())
			}
			return res
		})
	})

	t.Run("struct", func(t *testing.T) {
		type rec struct {
			i int
		}
		queueTestCases[rec](t, func(i int) rec {
			return rec{i: i}
		})
	})
}

func queueBench[T any](b *testing.B, makeT func(i int, rnd *rand.Rand) T) {
	for _, popChance := range []float64{0.5, 0.25, 0.1} {
		b.Run(fmt.Sprintf("pop=%.2f%%", popChance*100), func(b *testing.B) {
			b.ReportAllocs()
			rnd, _ := randutil.NewTestRand()
			var q Queue[T]
			for i := 0; i < b.N; i++ {
				if rnd.Float64() < popChance {
					_, _ = q.PopFront()
				} else {
					q.Push(makeT(i, rnd))
				}
			}
			b.Logf("queue len %d; chunks %d", q.Len(), countChunks(q))
		})
	}
}

func BenchmarkQueue(b *testing.B) {
	defer leaktest.AfterTest(b)()

	b.Run("int", func(b *testing.B) {
		queueBench[int](b, func(i int, rnd *rand.Rand) int {
			return i
		})
	})
	b.Run("*int", func(b *testing.B) {
		queueBench[*int](b, func(i int, rnd *rand.Rand) *int {
			p := new(int)
			*p = i
			return p
		})
	})

	type record struct {
		a     int
		slice []byte
	}

	b.Run("struct", func(b *testing.B) {
		queueBench[record](b, func(i int, rnd *rand.Rand) record {
			return record{a: i}
		})
	})
}
