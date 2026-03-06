// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAggregateSamples(t *testing.T) {
	now := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)
	lookback := 60 * time.Second

	t.Run("empty input", func(t *testing.T) {
		result := AggregateSamples(nil, now, lookback)
		require.Nil(t, result)
	})

	t.Run("all outside window", func(t *testing.T) {
		samples := []ASHSample{
			{SampleTime: now.Add(-2 * time.Minute), WorkEventType: WorkCPU, WorkEvent: "A"},
			{SampleTime: now.Add(-90 * time.Second), WorkEventType: WorkIO, WorkEvent: "B"},
		}
		result := AggregateSamples(samples, now, lookback)
		require.Nil(t, result)
	})

	t.Run("boundary inclusion", func(t *testing.T) {
		// Sample exactly at the cutoff (now - lookback) should be included.
		samples := []ASHSample{
			{SampleTime: now.Add(-lookback), WorkEventType: WorkCPU, WorkEvent: "exact"},
		}
		result := AggregateSamples(samples, now, lookback)
		require.Len(t, result, 1)
		require.Equal(t, int64(1), result[0].Count)
		require.Equal(t, "exact", result[0].WorkEvent)
	})

	t.Run("samples after now are excluded", func(t *testing.T) {
		samples := []ASHSample{
			{SampleTime: now.Add(-10 * time.Second), WorkEventType: WorkCPU, WorkEvent: "in"},
			{SampleTime: now.Add(1 * time.Second), WorkEventType: WorkCPU, WorkEvent: "future"},
		}
		result := AggregateSamples(samples, now, lookback)
		require.Len(t, result, 1)
		require.Equal(t, "in", result[0].WorkEvent)
	})

	t.Run("multiple groups sorted by count", func(t *testing.T) {
		samples := []ASHSample{
			// 3 CPU samples.
			{SampleTime: now.Add(-30 * time.Second), WorkEventType: WorkCPU, WorkEvent: "ReplicaSend", WorkloadID: "aaa"},
			{SampleTime: now.Add(-20 * time.Second), WorkEventType: WorkCPU, WorkEvent: "ReplicaSend", WorkloadID: "aaa"},
			{SampleTime: now.Add(-10 * time.Second), WorkEventType: WorkCPU, WorkEvent: "ReplicaSend", WorkloadID: "aaa"},
			// 1 IO sample.
			{SampleTime: now.Add(-25 * time.Second), WorkEventType: WorkIO, WorkEvent: "StorageEval", WorkloadID: "bbb"},
			// 2 Lock samples.
			{SampleTime: now.Add(-15 * time.Second), WorkEventType: WorkLock, WorkEvent: "LockWait", WorkloadID: "ccc"},
			{SampleTime: now.Add(-5 * time.Second), WorkEventType: WorkLock, WorkEvent: "LockWait", WorkloadID: "ccc"},
		}
		result := AggregateSamples(samples, now, lookback)
		require.Len(t, result, 3)

		// Sorted by count descending.
		require.Equal(t, int64(3), result[0].Count)
		require.Equal(t, WorkCPU, result[0].WorkEventType)
		require.Equal(t, "ReplicaSend", result[0].WorkEvent)
		require.Equal(t, "aaa", result[0].WorkloadID)

		require.Equal(t, int64(2), result[1].Count)
		require.Equal(t, WorkLock, result[1].WorkEventType)
		require.Equal(t, "LockWait", result[1].WorkEvent)

		require.Equal(t, int64(1), result[2].Count)
		require.Equal(t, WorkIO, result[2].WorkEventType)
		require.Equal(t, "StorageEval", result[2].WorkEvent)
	})

	t.Run("same event type different workload IDs", func(t *testing.T) {
		samples := []ASHSample{
			{SampleTime: now.Add(-10 * time.Second), WorkEventType: WorkCPU, WorkEvent: "X", WorkloadID: "w1"},
			{SampleTime: now.Add(-9 * time.Second), WorkEventType: WorkCPU, WorkEvent: "X", WorkloadID: "w2"},
		}
		result := AggregateSamples(samples, now, lookback)
		require.Len(t, result, 2)
		// Both should have count 1.
		require.Equal(t, int64(1), result[0].Count)
		require.Equal(t, int64(1), result[1].Count)
	})

	t.Run("mixed in-window and out-of-window", func(t *testing.T) {
		samples := []ASHSample{
			// Out of window.
			{SampleTime: now.Add(-2 * time.Minute), WorkEventType: WorkCPU, WorkEvent: "old", WorkloadID: "x"},
			{SampleTime: now.Add(-90 * time.Second), WorkEventType: WorkCPU, WorkEvent: "old", WorkloadID: "x"},
			// In window.
			{SampleTime: now.Add(-50 * time.Second), WorkEventType: WorkCPU, WorkEvent: "new", WorkloadID: "y"},
			{SampleTime: now.Add(-40 * time.Second), WorkEventType: WorkCPU, WorkEvent: "new", WorkloadID: "y"},
			{SampleTime: now.Add(-30 * time.Second), WorkEventType: WorkIO, WorkEvent: "io", WorkloadID: "z"},
		}
		result := AggregateSamples(samples, now, lookback)
		require.Len(t, result, 2)
		require.Equal(t, int64(2), result[0].Count)
		require.Equal(t, "new", result[0].WorkEvent)
		require.Equal(t, int64(1), result[1].Count)
		require.Equal(t, "io", result[1].WorkEvent)
	})
}
