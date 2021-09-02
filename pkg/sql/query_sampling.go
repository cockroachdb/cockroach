// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import "github.com/cockroachdb/cockroach/pkg/settings"

// Default value used to designate a rate at which logs to the telemetry channel
// will be sampled.
const defaultTelemetrySampleRate = 0.00001

var telemetrySampleRate = settings.RegisterFloatSetting(
	"sql.telemetry.query_sampling.sample_rate",
	"the rate/probability at which we sample queries for telemetry",
	defaultTelemetrySampleRate,
	settings.FloatBetweenZeroAndOneInclusive,
)

// Default value for the QPS threshold used to determine whether telemetry logs
// will be sampled.
const defaultQPSThreshold = 2000

var telemetryQPSThreshold = settings.RegisterIntSetting(
	"sql.telemetry.query_sampling.qps_threshold",
	"the QPS threshold at which we begin sampling DML statements for telemetry logs",
	defaultQPSThreshold,
	settings.NonNegativeInt,
)

// Default value for the alpha used for the exponential smoothing formula used
// to approximate cluster QPS. Value should be between 0 and 1. Values closer to
// 1 place greater weighting with recent QPS values, values closer to 0 place
// greater weighting with historical QPS values.
const defaultSmoothingAlpha float64 = 0.8

var telemetrySmoothingAlpha = settings.RegisterFloatSetting(
	"sql.telemetry.query_sampling.smoothing_alpha",
	"the smoothing coefficient for exponential smoothing, used to approximate cluster QPS",
	defaultSmoothingAlpha,
	settings.FloatBetweenZeroAndOneInclusive,
)

// Default length of the rolling interval used to hold query counts.
const defaultRollingInterval int64 = 10

var telemetryRollingInterval = settings.RegisterIntSetting(
	"sql.telemetry.query_sampling.rolling_interval",
	"the size of the rolling interval used in telemetry metrics for logging",
	defaultRollingInterval,
	settings.PositiveInt,
)

// expSmoothQPS calculates a smoothed QPS value from TelemetryLoggingMetrics query counts.
func (t *TelemetryLoggingMetrics) expSmoothQPS() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// If the interval length is of size 1 return the latest query count.
	if t.getInterval() == 1 {
		lastCount := t.mu.rollingQueryCounts.lastQueryCount().count
		return lastCount
	}

	currIdx := t.mu.rollingQueryCounts.endPointer()
	startIdx := t.mu.rollingQueryCounts.nextIndex(currIdx)

	if currIdx != startIdx {
		t.mu.RUnlock()
		t.gatherQPS(currIdx, startIdx)
		t.mu.RLock()
	}

	var smoothQPS float64

	for i := len(t.mu.MovingQPS) - 1; i >= 0; i-- {
		qpsVal := float64(t.mu.MovingQPS[i])
		// On first entry, there are no previous values to approximate a smooth QPS
		// value. Consequently, we use the average QPS value as the initial smoothed
		// QPS value. Using just the initial value can cause skewing in the
		// exponential smoothing calculation.
		if i == len(t.mu.MovingQPS)-1 {
			var totalQPSVal int
			for _, qps := range t.mu.MovingQPS {
				totalQPSVal += int(qps)
			}
			avgQPSVal := totalQPSVal / len(t.mu.MovingQPS)
			smoothQPS += float64(avgQPSVal)
		} else {
			smoothQPS = t.smoothingAlpha*qpsVal + (1-t.smoothingAlpha)*smoothQPS
		}
	}
	return int64(smoothQPS)
}

// gatherQPS gathers the QPS values between each query count pairing. Computed
// QPS values are stored in the MovingQPS field of the TelemetryLoggingMetrics
// object.
func (t *TelemetryLoggingMetrics) gatherQPS(currIdx int, startIdx int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.MovingQPS = t.mu.MovingQPS[:0]
	for currIdx != startIdx {
		prevIdx := t.mu.rollingQueryCounts.prevIndex(currIdx)

		curr := t.mu.rollingQueryCounts.getQueryCount(currIdx)
		prev := t.mu.rollingQueryCounts.getQueryCount(prevIdx)

		qpsVal := calcAvgQPS(curr, prev)
		t.mu.MovingQPS = append(t.mu.MovingQPS, qpsVal)

		currIdx = t.mu.rollingQueryCounts.prevIndex(currIdx)
	}
}

// calcAvgQPS gets the average cluster QPS between two timestamps. The
// difference in the number of queries executed between the timestamps is
// divided by the number of seconds between the timestamps.
func calcAvgQPS(currQueryCount *queryCountAndTime, prevQueryCount *queryCountAndTime) int64 {
	// If the current query count is empty, return 0.
	if *currQueryCount == (queryCountAndTime{}) {
		return 0
	}
	// Determine the time since the previous query count in number of seconds.
	timeSincePrev := currQueryCount.timestamp.Sub(prevQueryCount.timestamp).Seconds()
	// Calculate the QPS since the previous query count:
	// (current number of queries) / (difference in seconds since last timestamp)
	// Timestamps between query counts are at least 1 second long, no need to
	// check for divide by 0.
	clusterQPS := currQueryCount.count / int64(timeSincePrev)
	return clusterQPS
}
