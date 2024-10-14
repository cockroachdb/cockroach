// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ring

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRingBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const operationCount = 100
	var buffer Buffer[int]
	naiveBuffer := make([]interface{}, 0, operationCount)
	for i := 0; i < operationCount; i++ {
		switch rand.Intn(5) {
		case 0:
			buffer.AddFirst(i)
			naiveBuffer = append([]interface{}{i}, naiveBuffer...)
		case 1:
			buffer.AddLast(i)
			naiveBuffer = append(naiveBuffer, i)
		case 2:
			if len(naiveBuffer) > 0 {
				buffer.RemoveFirst()
				// NB: shift to preserve length.
				copy(naiveBuffer, naiveBuffer[1:])
				naiveBuffer = naiveBuffer[:len(naiveBuffer)-1]
			}
		case 3:
			if len(naiveBuffer) > 0 {
				buffer.RemoveLast()
				naiveBuffer = naiveBuffer[:len(naiveBuffer)-1]
			}
		case 4:
			// If there's extra capacity, resize to trim it.
			require.LessOrEqual(t, len(naiveBuffer), buffer.Cap())
			spareCap := buffer.Cap() - len(naiveBuffer)
			if spareCap > 0 {
				buffer.Resize(len(naiveBuffer) + rand.Intn(spareCap))
			}
		default:
			t.Fatal("unexpected")
		}
		contents := make([]interface{}, 0, buffer.Len())
		for _, v := range buffer.all() {
			contents = append(contents, v)
		}
		require.Equal(t, naiveBuffer, contents)
	}
}

func TestRingBufferCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var b Buffer[string]

	require.Panics(t, func() { b.Reserve(-1) })
	require.Equal(t, 0, b.Len())
	require.Equal(t, 0, b.Cap())

	b.Reserve(0)
	require.Equal(t, 0, b.Len())
	require.Equal(t, 0, b.Cap())

	b.AddFirst("a")
	require.Equal(t, 1, b.Len())
	require.Equal(t, 1, b.Cap())
	require.Panics(t, func() { b.Reserve(0) })
	require.Equal(t, 1, b.Len())
	require.Equal(t, 1, b.Cap())
	b.Reserve(1)
	require.Equal(t, 1, b.Len())
	require.Equal(t, 1, b.Cap())
	b.Reserve(2)
	require.Equal(t, 1, b.Len())
	require.Equal(t, 2, b.Cap())

	b.AddLast("z")
	require.Equal(t, 2, b.Len())
	require.Equal(t, 2, b.Cap())
	require.Panics(t, func() { b.Reserve(1) })
	require.Equal(t, 2, b.Len())
	require.Equal(t, 2, b.Cap())
	b.Reserve(2)
	require.Equal(t, 2, b.Len())
	require.Equal(t, 2, b.Cap())
	b.Reserve(9)
	require.Equal(t, 2, b.Len())
	require.Equal(t, 9, b.Cap())

	b.RemoveFirst()
	require.Equal(t, 1, b.Len())
	require.Equal(t, 9, b.Cap())
	b.Reserve(1)
	require.Equal(t, 1, b.Len())
	require.Equal(t, 9, b.Cap())
	b.RemoveLast()
	require.Equal(t, 0, b.Len())
	require.Equal(t, 9, b.Cap())
	b.Reserve(0)
	require.Equal(t, 0, b.Len())
	require.Equal(t, 9, b.Cap())

	b.Resize(3)
	require.Equal(t, 0, b.Len())
	require.Equal(t, 3, b.Cap())
}
