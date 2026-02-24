// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"hash/fnv"
	"math"
	"sort"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var hashDelimiter = []byte{'|'}

// floatPrecisionMultiplier is used to round floating-point calculations to 9 decimal places
// to eliminate binary representation errors in IEEE-754 floating-point arithmetic.
const floatPrecisionMultiplier = 1e9 // 9 decimal places

type metricState struct {
	previousValue float64
	firstSeen     bool
}

// CumulativeToDeltaProcessor maintains state for counter metrics to calculate deltas.
// It is concurrency-safe and maintains accurate delta calculations across
// different data chunks by tracking previous values per metric+tags combination.
type CumulativeToDeltaProcessor struct {
	mu     syncutil.RWMutex
	states map[uint64]*metricState // keyed by hash of metric name and tags
}

func NewCumulativeToDeltaProcessor() *CumulativeToDeltaProcessor {
	return &CumulativeToDeltaProcessor{
		states: make(map[uint64]*metricState),
	}
}

// getMetricKey generates a hash-based key for the metric+tags combination.
// This assumes tags are provided in a consistent order (which they are in tsdump_upload.go
// where per-series tags are added first, followed by global tags in deterministic order).
func (dc *CumulativeToDeltaProcessor) getMetricKey(metricName string, tags []string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(metricName))
	_, _ = hash.Write(hashDelimiter)
	for _, tag := range tags {
		_, _ = hash.Write([]byte(tag))
		_, _ = hash.Write(hashDelimiter)
	}
	return hash.Sum64()
}

// baselineOffset is the duration by which the first observed counter value
// is shifted into the past by 60 days. This moves the raw cumulative baseline out of
// the visible dashboard window so it doesn't appear as a spike, while
// still preserving it in Datadog for cumsum() reconstruction.
const baselineOffset = 60 * 24 * time.Hour

// processCounterMetric converts cumulative counter values to deltas between
// consecutive points by modifying the series in-place. The first data point
// for a newly-seen metric retains its original cumulative value but its
// timestamp is shifted to T - 60 days so the baseline lives outside the
// visible dashboard window (avoiding the initial spike). Subsequent points
// become deltas from the previous point. Counter resets (current < previous)
// are handled by using current value as delta.
//
// The isSorted parameter indicates whether the points are already sorted by
// timestamp, avoiding an unnecessary sort operation when the data is already
// in order.
func (dc *CumulativeToDeltaProcessor) processCounterMetric(
	series *datadogV2.MetricSeries, isSorted bool,
) error {
	if series.Type == nil || *series.Type != datadogV2.METRICINTAKETYPE_COUNT {
		return nil
	}

	// sort the points by timestamp if not already sorted
	if !isSorted {
		sort.Slice(series.Points, func(i, j int) bool {
			return *series.Points[i].Timestamp < *series.Points[j].Timestamp
		})
	}

	metricKey := dc.getMetricKey(series.Metric, series.Tags)

	dc.mu.Lock()
	defer dc.mu.Unlock()

	state, exists := dc.states[metricKey]
	if !exists {
		state = &metricState{firstSeen: true}
		dc.states[metricKey] = state
	}

	for i := range series.Points {
		point := &series.Points[i]
		currentValue := *point.Value
		if state.firstSeen {
			state.previousValue = currentValue
			state.firstSeen = false
			// Shift the baseline data point to T - 60 days so the raw
			// cumulative value doesn't appear as a spike in the actual
			// data window. The value is preserved for cumsum() queries.
			*point.Timestamp = time.Unix(*point.Timestamp, 0).Add(-1 * baselineOffset).Unix()
			continue
		}

		// Calculate delta.
		// Round the difference using floatPrecisionMultiplier. Floats in Go (IEEE-754) can't represent most
		// decimals exactly (e.g. 1.8 becomes 1.799999...). Without rounding, small binary errors
		// can leak into downstream representations. We normalize here to the precision defined
		// by floatPrecisionMultiplier.
		*point.Value = math.Round((currentValue-state.previousValue)*floatPrecisionMultiplier) / floatPrecisionMultiplier
		if currentValue < state.previousValue {
			// if counter reset detected (e.g., process restart)
			// use the current value as the delta since last reset
			*point.Value = currentValue
		}

		state.previousValue = currentValue
	}

	return nil
}
