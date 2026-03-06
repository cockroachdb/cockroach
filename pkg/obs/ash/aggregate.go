// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"sort"
	"time"
)

// AggregatedASH represents a group of ASH samples that share the same
// (WorkEventType, WorkEvent, WorkloadID) key. Count is the number of
// samples observed in the aggregation window.
type AggregatedASH struct {
	WorkEventType WorkEventType
	WorkEvent     string
	WorkloadID    string
	Count         int64
}

// aggKey is the grouping key used by AggregateSamples.
type aggKey struct {
	workEventType WorkEventType
	workEvent     string
	workloadID    string
}

// AggregateSamples filters samples to [now-lookback, now] and groups
// them by (WorkEventType, WorkEvent, WorkloadID). The result is sorted
// by Count descending. samples must be ordered oldest-to-newest (as
// returned by RingBuffer.GetAll).
func AggregateSamples(samples []ASHSample, now time.Time, lookback time.Duration) []AggregatedASH {
	cutoff := now.Add(-lookback)

	// Find the start index: since samples are oldest-to-newest, iterate
	// backward from the end to find the cutoff. Everything from startIdx
	// onward is within the window.
	startIdx := len(samples)
	for i := len(samples) - 1; i >= 0; i-- {
		if samples[i].SampleTime.Before(cutoff) {
			break
		}
		startIdx = i
	}

	if startIdx >= len(samples) {
		return nil
	}

	// Aggregate using a map.
	counts := make(map[aggKey]int64)
	for i := startIdx; i < len(samples); i++ {
		s := &samples[i]
		if s.SampleTime.After(now) {
			continue
		}
		k := aggKey{
			workEventType: s.WorkEventType,
			workEvent:     s.WorkEvent,
			workloadID:    s.WorkloadID,
		}
		counts[k]++
	}

	if len(counts) == 0 {
		return nil
	}

	result := make([]AggregatedASH, 0, len(counts))
	for k, c := range counts {
		result = append(result, AggregatedASH{
			WorkEventType: k.workEventType,
			WorkEvent:     k.workEvent,
			WorkloadID:    k.workloadID,
			Count:         c,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Count > result[j].Count
	})

	return result
}
