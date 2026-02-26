// Copyright 2025 The Cockroach Authors.
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

func TestRingBufferAddAndGet(t *testing.T) {
	rb := NewRingBuffer(5)
	require.Equal(t, 0, rb.Len())
	require.Nil(t, rb.GetAll())

	for i := 1; i <= 3; i++ {
		rb.Add(makeSample(i))
	}
	require.Equal(t, 3, rb.Len())

	samples := rb.GetAll()
	require.Len(t, samples, 3)
	for i, s := range samples {
		require.Equal(t, int64(i+1), s.GoroutineID)
	}
}

func TestRingBufferWraparound(t *testing.T) {
	rb := NewRingBuffer(4)

	// Write 10 samples into a buffer of capacity 4.
	for i := 1; i <= 10; i++ {
		rb.Add(makeSample(i))
	}
	require.Equal(t, 4, rb.Len())

	// Should contain the 4 newest in oldest-to-newest order.
	samples := rb.GetAll()
	require.Len(t, samples, 4)
	for i, s := range samples {
		require.Equal(t, int64(i+7), s.GoroutineID)
	}
}

func TestRingBufferClear(t *testing.T) {
	rb := NewRingBuffer(5)
	for i := 1; i <= 5; i++ {
		rb.Add(makeSample(i))
	}

	rb.Clear()
	require.Equal(t, 0, rb.Len())
	require.Nil(t, rb.GetAll())

	// Buffer should be reusable after clear.
	rb.Add(makeSample(100))
	require.Equal(t, 1, rb.Len())
	require.Equal(t, int64(100), rb.GetAll()[0].GoroutineID)
}

func TestRingBufferResize(t *testing.T) {
	rb := NewRingBuffer(3)
	for i := 1; i <= 5; i++ {
		rb.Add(makeSample(i))
	}
	// Buffer has wrapped; contains 3, 4, 5.

	// Expand: all samples preserved in order.
	rb.Resize(6)
	require.Equal(t, 3, rb.Len())
	samples := rb.GetAll()
	require.Equal(t, int64(3), samples[0].GoroutineID)
	require.Equal(t, int64(5), samples[2].GoroutineID)

	// Add more to confirm the expanded capacity works.
	for i := 6; i <= 8; i++ {
		rb.Add(makeSample(i))
	}
	require.Equal(t, 6, rb.Len())

	// Shrink: only the newest samples kept.
	rb.Resize(2)
	require.Equal(t, 2, rb.Len())
	samples = rb.GetAll()
	require.Equal(t, int64(7), samples[0].GoroutineID)
	require.Equal(t, int64(8), samples[1].GoroutineID)
}
