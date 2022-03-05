// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// kvPair is a bytes -> bytes kv pair.
type kvPair struct {
	key   roachpb.Key
	value []byte
}

func makeTestData(num int) (kvs []kvPair, totalSize sz) {
	kvs = make([]kvPair, num)
	r, _ := randutil.NewTestRand()
	alloc := make([]byte, num*500)
	randutil.ReadTestdataBytes(r, alloc)
	for i := range kvs {
		if len(alloc) < 1500 {
			const refill = 15000
			alloc = make([]byte, refill)
			randutil.ReadTestdataBytes(r, alloc)
		}
		kvs[i].key = alloc[:randutil.RandIntInRange(r, 2, 100)]
		alloc = alloc[len(kvs[i].key):]
		kvs[i].value = alloc[:randutil.RandIntInRange(r, 0, 1000)]
		alloc = alloc[len(kvs[i].value):]
		totalSize += sz(len(kvs[i].key) + len(kvs[i].value))
	}
	return kvs, totalSize
}

func checkIterable(t *testing.T, src []kvPair, b iterable) {
	for i := 0; i < len(src); i++ {
		require.True(t, b.Next())
		require.Equal(t, src[i].key, b.Key())

		// Collect all values for runs of same key to compare insensitive to order.
		if i+1 < len(src) && bytes.Equal(src[i].key, src[i+1].key) {
			var expectVals [][]byte
			var gotVals [][]byte
			for j := i; j < len(src) && bytes.Equal(src[i].key, src[j].key); j++ {
				expectVals = append(expectVals, src[j].value)
				gotVals = append(gotVals, append([]byte{}, b.Value()...))
				require.True(t, b.Next())
			}
			require.ElementsMatch(t, expectVals, gotVals)
			i += len(expectVals)
		} else {
			if len(src[i].value) == 0 {
				require.Equal(t, len(src[i].value), len(b.Value()))
			} else {
				require.Equal(t, src[i].value, b.Value(), "mismatch for key %x at pos %d", src[i].key, i)
			}
		}
	}
}

func TestKvBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src, totalSize := makeTestData(50000)

	// Write everything to our buf.
	b := kvBuf{}
	b.Reset()

	for i := range src {
		require.NoError(t, b.append(src[i].key, src[i].value))
	}

	// Sanity check our buf has right size.
	require.Equal(t, len(src), b.Len())
	require.Equal(t, totalSize+sz(len(src)*16), b.MemSize())

	// Read back what we wrote.
	for i := range src {
		require.Equal(t, src[i].key, b.KeyAt(i))
		if len(src[i].value) > 0 {
			require.Equal(t, src[i].value, b.ValueAt(i))
		} else {
			require.Equal(t, len(src[i].value), len(b.ValueAt(i)))
		}
	}
	checkIterable(t, src, &b)
	b.pos = -1
	// Sort both and then ensure they match.
	sort.Slice(src, func(i, j int) bool { return bytes.Compare(src[i].key, src[j].key) < 0 })
	sort.Sort(&b)
	for i := range src {
		require.Equal(t, src[i].key, b.KeyAt(i))
		if len(src[i].value) > 0 {
			require.Equal(t, src[i].value, b.ValueAt(i))
		} else {
			require.Equal(t, len(src[i].value), len(b.ValueAt(i)))
		}
	}
	checkIterable(t, src, &b)
}

func TestSortedBuffersSingleUncompressed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src, totalSize := makeTestData(100)

	// Write everything to our buf.
	cur := kvBuf{}
	cur.Reset()

	b := &sortedBuffers{}
	b.reset()

	for i := range src {
		require.NoError(t, cur.append(src[i].key, src[i].value))
	}
	require.Equal(t, totalSize+sz(len(src)*16), cur.MemSize())

	require.NoError(t, b.add(&cur))
	require.Equal(t, totalSize+sz(len(src)*16), b.memSize)

	checkIterable(t, src, b)

}

func TestSortedBufferSingleCompressed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src, totalSize := makeTestData(1000)

	cur := &kvBuf{}
	cur.Reset()

	b := &sortedBuffers{}
	b.reset()

	for i := range src {
		require.NoError(t, cur.append(src[i].key, src[i].value))
	}
	require.Equal(t, totalSize+sz(len(src)*16), cur.MemSize())

	compressed, err := compressBuffer(cur)
	require.NoError(t, err)
	require.NoError(t, b.add(compressed))
	sort.Sort(cur)
	cur.Reset()

	checkIterable(t, src, b)
}

func TestSortedBufferMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src, totalSize := makeTestData(200)
	b := &sortedBuffers{}

	for _, numBufs := range []int{1, 2, 5, 10} {
		t.Run(fmt.Sprintf("bufs=%d", numBufs), func(t *testing.T) {
			bufs := make([]kvBuf, numBufs)
			for i := range bufs {
				bufs[i].Reset()
			}

			for i := range src {
				buf := &bufs[i%numBufs]
				require.NoError(t, buf.append(src[i].key, src[i].value))
			}

			sort.Slice(src, func(i, j int) bool {
				return bytes.Compare(src[i].key, src[j].key) < 0
			})

			b.reset()
			for i := range bufs {
				sort.Sort(&bufs[i])
				require.NoError(t, b.add(&bufs[i]))
			}
			require.Equal(t, totalSize+sz(len(src)*16), b.memSize)

			checkIterable(t, src, b)
		})
	}
}

func TestSortedBufferMixedBuffers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	src, _ := makeTestData(2000)
	b := &sortedBuffers{}

	for _, numBufs := range []int{1, 2, 3, 5, 10, 50} {
		t.Run(fmt.Sprintf("bufs=%d", numBufs), func(t *testing.T) {
			bufs := make([]kvBuf, numBufs)
			for i := range bufs {
				bufs[i].Reset()
			}
			for i := range src {
				bufNum := i % numBufs
				buf := &bufs[bufNum]
				require.NoError(t, buf.append(src[i].key, src[i].value))
			}
			sort.Slice(src, func(i, j int) bool { return bytes.Compare(src[i].key, src[j].key) < 0 })
			b.reset()
			for i := range bufs {
				sort.Sort(&bufs[i])
				if i%3 == 1 {
					require.NoError(t, b.add(&bufs[i]))
				} else {
					comp, err := compressBuffer(&bufs[i])
					require.NoError(t, err)
					require.NoError(t, b.add(comp))
				}
			}
			checkIterable(t, src, b)
		})
	}
}
