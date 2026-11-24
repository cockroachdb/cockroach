// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func makeSample(id int) ASHSample {
	return ASHSample{
		SampleTime:    time.Unix(int64(id), 0),
		GoroutineID:   int64(id),
		WorkloadID:    fmt.Sprintf("w%d", id),
		WorkEventType: WorkCPU,
		WorkEvent:     fmt.Sprintf("event-%d", id),
	}
}

// sampleIDs extracts goroutine IDs from a sample slice for easy assertion.
func sampleIDs(samples []ASHSample) []int64 {
	ids := make([]int64, len(samples))
	for i, s := range samples {
		ids[i] = s.GoroutineID
	}
	return ids
}

func TestRingBufferAddAndGet(t *testing.T) {
	testCases := []struct {
		name     string
		capacity int
		// addIDs are the sample IDs to add in order.
		addIDs []int
		// wantIDs are the expected goroutine IDs from GetAll, oldest to newest.
		wantIDs []int64
	}{
		{
			name:     "capacity 1, add nothing",
			capacity: 1,
			addIDs:   nil,
			wantIDs:  nil,
		},
		{
			name:     "capacity 1, add 1",
			capacity: 1,
			addIDs:   []int{1},
			wantIDs:  []int64{1},
		},
		{
			name:     "capacity 1, add 3 (overwrite)",
			capacity: 1,
			addIDs:   []int{1, 2, 3},
			wantIDs:  []int64{3},
		},
		{
			name:     "capacity 2, add 1 (under capacity)",
			capacity: 2,
			addIDs:   []int{1},
			wantIDs:  []int64{1},
		},
		{
			name:     "capacity 2, add 2 (at capacity)",
			capacity: 2,
			addIDs:   []int{1, 2},
			wantIDs:  []int64{1, 2},
		},
		{
			name:     "capacity 2, add 5 (wraparound)",
			capacity: 2,
			addIDs:   []int{1, 2, 3, 4, 5},
			wantIDs:  []int64{4, 5},
		},
		{
			name:     "capacity 5, add 3 (under capacity)",
			capacity: 5,
			addIDs:   []int{1, 2, 3},
			wantIDs:  []int64{1, 2, 3},
		},
		{
			name:     "capacity 5, add 5 (at capacity)",
			capacity: 5,
			addIDs:   []int{1, 2, 3, 4, 5},
			wantIDs:  []int64{1, 2, 3, 4, 5},
		},
		{
			name:     "capacity 5, add 12 (multiple wraps)",
			capacity: 5,
			addIDs:   []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			wantIDs:  []int64{8, 9, 10, 11, 12},
		},
		{
			name:     "capacity 100, add 50 (under capacity)",
			capacity: 100,
			addIDs:   makeRange(1, 50),
			wantIDs:  makeInt64Range(1, 50),
		},
		{
			name:     "capacity 100, add 150 (wraparound)",
			capacity: 100,
			addIDs:   makeRange(1, 150),
			wantIDs:  makeInt64Range(51, 150),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb := NewRingBuffer(tc.capacity)
			for _, id := range tc.addIDs {
				rb.Add(makeSample(id))
			}

			wantLen := len(tc.wantIDs)
			require.Equal(t, wantLen, rb.Len())

			samples := rb.GetAll(nil)
			if wantLen == 0 {
				require.Empty(t, samples)
			} else {
				require.Equal(t, tc.wantIDs, sampleIDs(samples))
			}
		})
	}
}

func TestRingBufferGetAllReusableSlice(t *testing.T) {
	rb := NewRingBuffer(5)
	for i := 1; i <= 3; i++ {
		rb.Add(makeSample(i))
	}

	// Pass a pre-allocated slice with sufficient capacity.
	buf := make([]ASHSample, 0, 10)
	result := rb.GetAll(buf)
	require.Len(t, result, 3)
	require.Equal(t, int64(1), result[0].GoroutineID)

	// Pass a slice with insufficient capacity; a new one is allocated.
	small := make([]ASHSample, 0, 1)
	result2 := rb.GetAll(small)
	require.Len(t, result2, 3)
	require.Equal(t, int64(1), result2[0].GoroutineID)

	// Empty buffer returns zero-length slice.
	rb.Clear()
	result3 := rb.GetAll(buf)
	require.Empty(t, result3)
}

func TestRingBufferClear(t *testing.T) {
	testCases := []struct {
		name     string
		capacity int
		addCount int
	}{
		{name: "capacity 1", capacity: 1, addCount: 1},
		{name: "capacity 2, partially filled", capacity: 2, addCount: 1},
		{name: "capacity 5, fully filled", capacity: 5, addCount: 5},
		{name: "capacity 5, wrapped", capacity: 5, addCount: 8},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb := NewRingBuffer(tc.capacity)
			for i := 1; i <= tc.addCount; i++ {
				rb.Add(makeSample(i))
			}

			rb.Clear()
			require.Equal(t, 0, rb.Len())
			require.Empty(t, rb.GetAll(nil))

			// Buffer should be reusable after clear.
			rb.Add(makeSample(100))
			require.Equal(t, 1, rb.Len())
			require.Equal(t, int64(100), rb.GetAll(nil)[0].GoroutineID)
		})
	}
}

func TestRingBufferResize(t *testing.T) {
	testCases := []struct {
		name        string
		capacity    int
		addIDs      []int
		newCapacity int
		wantIDs     []int64
	}{
		{
			name:        "grow from 1 to 5",
			capacity:    1,
			addIDs:      []int{1},
			newCapacity: 5,
			wantIDs:     []int64{1},
		},
		{
			name:        "shrink from 5 to 2, keeps newest",
			capacity:    5,
			addIDs:      []int{1, 2, 3, 4, 5},
			newCapacity: 2,
			wantIDs:     []int64{4, 5},
		},
		{
			name:        "shrink to 1",
			capacity:    5,
			addIDs:      []int{1, 2, 3, 4, 5},
			newCapacity: 1,
			wantIDs:     []int64{5},
		},
		{
			name:        "same capacity is no-op",
			capacity:    3,
			addIDs:      []int{1, 2, 3},
			newCapacity: 3,
			wantIDs:     []int64{1, 2, 3},
		},
		{
			name:        "grow wrapped buffer",
			capacity:    3,
			addIDs:      []int{1, 2, 3, 4, 5},
			newCapacity: 6,
			wantIDs:     []int64{3, 4, 5},
		},
		{
			name:        "shrink wrapped buffer",
			capacity:    4,
			addIDs:      []int{1, 2, 3, 4, 5, 6},
			newCapacity: 2,
			wantIDs:     []int64{5, 6},
		},
		{
			name:        "resize empty buffer",
			capacity:    5,
			addIDs:      nil,
			newCapacity: 3,
			wantIDs:     nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb := NewRingBuffer(tc.capacity)
			for _, id := range tc.addIDs {
				rb.Add(makeSample(id))
			}

			rb.Resize(tc.newCapacity)

			wantLen := len(tc.wantIDs)
			require.Equal(t, wantLen, rb.Len())

			samples := rb.GetAll(nil)
			if wantLen == 0 {
				require.Empty(t, samples)
			} else {
				require.Equal(t, tc.wantIDs, sampleIDs(samples))
			}

			// Verify the buffer is usable after resize by adding more.
			rb.Add(makeSample(999))
			require.LessOrEqual(t, rb.Len(), tc.newCapacity)
			all := rb.GetAll(nil)
			require.Equal(t, int64(999), all[len(all)-1].GoroutineID)
		})
	}
}

func TestRingBufferResizeInvalid(t *testing.T) {
	rb := NewRingBuffer(5)
	rb.Add(makeSample(1))

	// Resize to 0 or negative should be no-op.
	rb.Resize(0)
	require.Equal(t, 1, rb.Len())

	rb.Resize(-1)
	require.Equal(t, 1, rb.Len())
}

func TestRingBufferRangeReverse(t *testing.T) {
	t.Run("empty buffer", func(t *testing.T) {
		rb := NewRingBuffer(5)
		var visited []int64
		rb.RangeReverse(func(s ASHSample) bool {
			visited = append(visited, s.GoroutineID)
			return true
		})
		require.Empty(t, visited)
	})

	t.Run("partial fill", func(t *testing.T) {
		rb := NewRingBuffer(5)
		for i := 1; i <= 3; i++ {
			rb.Add(makeSample(i))
		}
		var visited []int64
		rb.RangeReverse(func(s ASHSample) bool {
			visited = append(visited, s.GoroutineID)
			return true
		})
		// Newest to oldest.
		require.Equal(t, []int64{3, 2, 1}, visited)
	})

	t.Run("full buffer with wraparound", func(t *testing.T) {
		rb := NewRingBuffer(3)
		for i := 1; i <= 5; i++ {
			rb.Add(makeSample(i))
		}
		var visited []int64
		rb.RangeReverse(func(s ASHSample) bool {
			visited = append(visited, s.GoroutineID)
			return true
		})
		// Newest to oldest: 5, 4, 3.
		require.Equal(t, []int64{5, 4, 3}, visited)
	})

	t.Run("early termination", func(t *testing.T) {
		rb := NewRingBuffer(5)
		for i := 1; i <= 5; i++ {
			rb.Add(makeSample(i))
		}
		var visited []int64
		rb.RangeReverse(func(s ASHSample) bool {
			visited = append(visited, s.GoroutineID)
			return len(visited) < 2
		})
		// Stops after 2 newest samples.
		require.Equal(t, []int64{5, 4}, visited)
	})

	t.Run("reverse of GetAll order", func(t *testing.T) {
		rb := NewRingBuffer(4)
		for i := 1; i <= 7; i++ {
			rb.Add(makeSample(i))
		}
		var reverseIDs []int64
		rb.RangeReverse(func(s ASHSample) bool {
			reverseIDs = append(reverseIDs, s.GoroutineID)
			return true
		})
		getAllIDs := sampleIDs(rb.GetAll(nil))
		// RangeReverse should produce the reverse of GetAll.
		for i, j := 0, len(getAllIDs)-1; i < j; i, j = i+1, j-1 {
			getAllIDs[i], getAllIDs[j] = getAllIDs[j], getAllIDs[i]
		}
		require.Equal(t, getAllIDs, reverseIDs)
	})
}

// makeRange returns a slice of ints from start to end inclusive.
func makeRange(start, end int) []int {
	r := make([]int, 0, end-start+1)
	for i := start; i <= end; i++ {
		r = append(r, i)
	}
	return r
}

// makeInt64Range returns a slice of int64s from start to end inclusive.
func makeInt64Range(start, end int) []int64 {
	r := make([]int64, 0, end-start+1)
	for i := start; i <= end; i++ {
		r = append(r, int64(i))
	}
	return r
}
