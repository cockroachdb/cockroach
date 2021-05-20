// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ring

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const maxCount = 100

func testRingBuffer(t *testing.T, count int) {
	var buffer Buffer
	naiveBuffer := make([]interface{}, 0, count)
	for elementIdx := 0; elementIdx < count; elementIdx++ {
		switch rand.Intn(4) {
		case 0:
			buffer.AddFirst(elementIdx)
			naiveBuffer = append([]interface{}{elementIdx}, naiveBuffer...)
		case 1:
			buffer.AddLast(elementIdx)
			naiveBuffer = append(naiveBuffer, elementIdx)
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
		default:
			t.Fatal("unexpected")
		}

		require.Equal(t, len(naiveBuffer), buffer.Len())
		for pos, el := range naiveBuffer {
			res := buffer.Get(pos)
			require.Equal(t, el, res)
		}
		if len(naiveBuffer) > 0 {
			require.Equal(t, naiveBuffer[0], buffer.GetFirst())
			require.Equal(t, naiveBuffer[len(naiveBuffer)-1], buffer.GetLast())
		}
	}
}

func TestRingBuffer(t *testing.T) {
	for count := 1; count <= maxCount; count++ {
		t.Run("Parallel", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			testRingBuffer(t, count)
		})
	}
}

func TestRingBufferCapacity(t *testing.T) {
	var b Buffer

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
}
