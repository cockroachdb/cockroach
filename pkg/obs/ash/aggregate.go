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

// AggregateSamples filters samples to [now-lookback, now] and groups
// them by (WorkEventType, WorkEvent, WorkloadID). The result is sorted
// by Count descending. samples must be ordered oldest-to-newest (as
// returned by RingBuffer.GetAll).
//
// The grouping key is workloadKey, the same type used by the periodic
// log summary in the sampler.
func AggregateSamples(samples []ASHSample, now time.Time, lookback time.Duration) []AggregatedASH {
	cutoff := now.Add(-lookback)

	// Binary search for the first sample at or after the cutoff. Since
	// samples are sorted oldest-to-newest, everything from startIdx
	// onward is within the window.
	startIdx := sort.Search(len(samples), func(i int) bool {
		return !samples[i].SampleTime.Before(cutoff)
	})

	if startIdx >= len(samples) {
		return nil
	}

	// Aggregate using a map keyed by workloadKey.
	counts := make(map[workloadKey]int64)
	for i := startIdx; i < len(samples); i++ {
		s := &samples[i]
		if s.SampleTime.After(now) {
			continue
		}
		k := workloadKey{
			WorkEventType: s.WorkEventType,
			WorkEvent:     s.WorkEvent,
			WorkloadID:    s.WorkloadID,
		}
		counts[k]++
	}

	if len(counts) == 0 {
		return nil
	}

	result := make([]AggregatedASH, 0, len(counts))
	for k, c := range counts {
		result = append(result, AggregatedASH{
			WorkEventType: k.WorkEventType,
			WorkEvent:     k.WorkEvent,
			WorkloadID:    k.WorkloadID,
			Count:         c,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Count > result[j].Count
	})

	return result
}
