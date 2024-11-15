// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracker

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/container/ring"
	"github.com/stretchr/testify/require"
)

func checkEquality(t *testing.T, expected Inflights, actual Inflights) {
	expBuf := expected.buffer
	actualBuf := actual.buffer
	expected.buffer = ring.Buffer[inflight]{}
	actual.buffer = ring.Buffer[inflight]{}
	require.Equal(t, expected, actual)
	require.Equal(t, expBuf.Length(), actualBuf.Length())
	for i := 0; i < expBuf.Length(); i++ {
		require.Equal(t, expBuf.At(i), actualBuf.At(i))
	}
}

func TestInflightsAdd(t *testing.T) {
	in := &Inflights{
		size: 10,
	}

	for i := 0; i < 5; i++ {
		in.Add(uint64(i), uint64(100+i))
	}

	wantIn := &Inflights{
		bytes: 510,
		size:  10,
		buffer: inflightsBuffer(
			//       ↓------------
			[]uint64{0, 1, 2, 3, 4},
			[]uint64{100, 101, 102, 103, 104}),
	}
	checkEquality(t, *wantIn, *in)

	for i := 5; i < 10; i++ {
		in.Add(uint64(i), uint64(100+i))
	}

	wantIn2 := &Inflights{
		bytes: 1045,
		size:  10,
		buffer: inflightsBuffer(
			//       ↓---------------------------
			[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			[]uint64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}),
	}
	checkEquality(t, *wantIn2, *in)

	// Can grow beyond size.
	for i := 10; i < 15; i++ {
		in.Add(uint64(i), uint64(100+i))
	}

	wantIn3 := &Inflights{
		bytes: 1605,
		size:  10,
		buffer: inflightsBuffer(
			//       ↓---------------------------
			[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
			[]uint64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114}),
	}
	checkEquality(t, *wantIn3, *in)
}

func TestInflightFreeTo(t *testing.T) {
	in := NewInflights(10, 0)
	for i := 0; i < 10; i++ {
		in.Add(uint64(i), uint64(100+i))
	}

	in.FreeLE(0)

	wantIn0 := &Inflights{
		bytes: 945,
		size:  10,
		buffer: inflightsBuffer(
			//       ↓------------------------
			[]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			[]uint64{101, 102, 103, 104, 105, 106, 107, 108, 109}),
	}
	checkEquality(t, *wantIn0, *in)

	in.FreeLE(4)

	wantIn := &Inflights{
		bytes: 535,
		size:  10,
		buffer: inflightsBuffer(
			//       ↓------------
			[]uint64{5, 6, 7, 8, 9},
			[]uint64{105, 106, 107, 108, 109}),
	}
	checkEquality(t, *wantIn, *in)

	in.FreeLE(8)

	wantIn2 := &Inflights{
		bytes: 109,
		size:  10,
		buffer: inflightsBuffer(
			//                                  ↓
			[]uint64{9},
			[]uint64{109}),
	}
	checkEquality(t, *wantIn2, *in)

	for i := 10; i < 15; i++ {
		in.Add(uint64(i), uint64(100+i))
	}

	in.FreeLE(12)

	wantIn3 := &Inflights{
		bytes: 227,
		size:  10,
		buffer: inflightsBuffer(
			//       ↓-----
			[]uint64{13, 14},
			[]uint64{113, 114}),
	}
	checkEquality(t, *wantIn3, *in)

	in.FreeLE(14)

	wantIn4 := &Inflights{
		size: 10,
	}
	checkEquality(t, *wantIn4, *in)
}

func TestInflightsFull(t *testing.T) {
	for _, tc := range []struct {
		name     string
		size     int
		maxBytes uint64
		fullAt   int
		freeLE   uint64
		againAt  int
	}{
		{name: "always-full", size: 0, fullAt: 0},
		{name: "single-entry", size: 1, fullAt: 1, freeLE: 1, againAt: 2},
		{name: "single-entry-overflow", size: 1, maxBytes: 10, fullAt: 1, freeLE: 1, againAt: 2},
		{name: "multi-entry", size: 15, fullAt: 15, freeLE: 6, againAt: 22},
		{name: "slight-overflow", size: 8, maxBytes: 400, fullAt: 4, freeLE: 2, againAt: 7},
		{name: "exact-max-bytes", size: 8, maxBytes: 406, fullAt: 4, freeLE: 3, againAt: 8},
		{name: "larger-overflow", size: 15, maxBytes: 408, fullAt: 5, freeLE: 1, againAt: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := NewInflights(tc.size, tc.maxBytes)

			addUntilFull := func(begin, end int) {
				for i := begin; i < end; i++ {
					require.False(t, in.Full(), "full at %d, want %d", i, end)
					in.Add(uint64(i), uint64(100+i))
				}
				require.True(t, in.Full())
			}

			addUntilFull(0, tc.fullAt)
			in.FreeLE(tc.freeLE)
			addUntilFull(tc.fullAt, tc.againAt)

			in.Add(100, 1024)
			require.True(t, in.Full()) // the full tracker remains full
		})
	}
}

func TestInflightsReset(t *testing.T) {
	in := NewInflights(10, 1000)
	// Imitate a semi-realistic flow during which the inflight tracker is
	// periodically reset to empty. Byte usage must not "leak" across resets.
	index := uint64(0)
	for epoch := 0; epoch < 100; epoch++ {
		in.reset()
		// Add 5 messages. They should not max out the limit yet.
		for i := 0; i < 5; i++ {
			require.False(t, in.Full())
			index++
			in.Add(index, 16)
		}
		// Ack all but last 2 indices.
		in.FreeLE(index - 2)
		require.False(t, in.Full())
		require.Equal(t, 2, in.Count())
	}
	in.FreeLE(index)
	require.Equal(t, 0, in.Count())
}

func inflightsBuffer(indices []uint64, sizes []uint64) ring.Buffer[inflight] {
	if len(indices) != len(sizes) {
		panic("len(indices) != len(sizes)")
	}
	var buffer ring.Buffer[inflight]
	for i, idx := range indices {
		buffer.Push(inflight{index: idx, bytes: sizes[i]})
	}
	return buffer
}
