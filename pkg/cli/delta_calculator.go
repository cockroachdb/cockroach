// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"strings"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type metricState struct {
	previousValue float64
	firstSeen     bool
}

// DeltaCalculator maintains state for counter metrics to calculate deltas.
// It is concurrency-safe and maintains accurate delta calculations across
// different data chunks by tracking previous values per metric+tags combination.
type DeltaCalculator struct {
	mu     syncutil.RWMutex
	states map[string]*metricState // keyed by metric name and tags
}

func NewDeltaCalculator() *DeltaCalculator {
	return &DeltaCalculator{
		states: make(map[string]*metricState),
	}
}

func (dc *DeltaCalculator) getMetricKey(metricName string, tags []string) string {
	var keyBuilder strings.Builder
	keyBuilder.WriteString(metricName)
	for _, tag := range tags {
		keyBuilder.WriteString("|")
		keyBuilder.WriteString(tag)
	}
	return keyBuilder.String()
}

// processCounterMetric converts cumulative counter values to deltas between consecutive points
// by modifying the series in-place. First point keeps original value. Subsequent points
// become delta from previous point. Counter resets (current < previous) are handled by
// using current value as delta.
func (dc *DeltaCalculator) processCounterMetric(series *datadogV2.MetricSeries) error {
	if series.Type == nil || *series.Type != datadogV2.METRICINTAKETYPE_COUNT {
		return nil
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
			*point.Value = currentValue
			return nil
		}

		// calculate delta
		*point.Value = currentValue - state.previousValue
		if currentValue < state.previousValue {
			// if counter reset detected (e.g., process restart)
			// use the current value as the delta since last reset
			*point.Value = currentValue
		}

		state.previousValue = currentValue
	}

	return nil
}
