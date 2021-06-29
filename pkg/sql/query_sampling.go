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

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

const defaultTelemetrySampleRate = 0.00001

var telemetrySampleRate = settings.RegisterFloatSetting(
	"sql.telemetry.query_sampling.sample_rate",
	"the rate/probability at which we sample queries for telemetry",
	defaultTelemetrySampleRate,
	settings.NonNegativeFloat,
)

// Default value for the QPS threshold used to determine whether telemetry logs
// will be sampled.
const defaultQPSThreshold = 100

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
	settings.NonNegativeFloat,
)

// Default length of the rolling interval used to hold query counts.
const defaultRollingInterval int64 = 10

var telemetryRollingInterval = settings.RegisterIntSetting(
	"sql.telemetry.query_sampling.rolling_interval",
	"the size of the rolling interval used in telemetry metrics for logging",
	defaultRollingInterval,
	settings.PositiveInt,
)

// ExpSmoothQPS calculates a smoothed QPS value from TelemetryLoggingMetrics query counts.
func (t *TelemetryLoggingMetrics) ExpSmoothQPS() int64 {
	t.mu.RLock()
	// If the interval length is of size 1 return the latest query count.
	if t.getInterval() == 1 {
		lastCount := t.mu.RollingQueryCounts.LastQueryCount().Count()
		t.mu.RUnlock()
		return lastCount
	}

	var totalQPSVal int
	var smoothQPS float64

	currIdx := t.mu.RollingQueryCounts.EndPointer()
	startIdx := t.mu.RollingQueryCounts.NextIndex(currIdx)

	t.mu.RUnlock()
	t.ResetMovingQPS()
	t.mu.RLock()

	for currIdx != startIdx {
		prevIdx := t.mu.RollingQueryCounts.PrevIndex(currIdx)
		curr := t.mu.RollingQueryCounts.GetQueryCount(currIdx)
		prev := t.mu.RollingQueryCounts.GetQueryCount(prevIdx)
		qpsVal := calcAvgQPS(curr, prev)

		t.mu.RUnlock()
		t.InsertMovingQPS(qpsVal)
		t.mu.RLock()

		totalQPSVal += int(qpsVal)

		currIdx = t.mu.RollingQueryCounts.PrevIndex(currIdx)
	}

	for i := len(t.mu.MovingQPS) - 1; i >= 0; i-- {
		qpsVal := float64(t.mu.MovingQPS[i])
		// On first entry, there are no previous values to approximate a smooth QPS value.
		// Consequently, we use the average QPS value as the initial smoothed QPS value.
		// Using just the initial value can cause skewing in the exponential smoothing calculation.
		if i == len(t.mu.MovingQPS)-1 {
			avgQPSVal := totalQPSVal / len(t.mu.MovingQPS)
			smoothQPS += float64(avgQPSVal)
		} else {
			smoothQPS = t.smoothingAlpha*qpsVal + (1-t.smoothingAlpha)*smoothQPS
		}
	}
	t.mu.RUnlock()
	return int64(smoothQPS)
}

// calcAvgQPS gets the average cluster QPS between two timestamps. The
// difference in the number of queries executed between the timestamps is
// divided by the number of seconds between the timestamps.
func calcAvgQPS(currQueryCount *QueryCountAndTime, prevQueryCount *QueryCountAndTime) int64 {
	// If the current query count is empty, return 0.
	if *currQueryCount == (QueryCountAndTime{}) {
		return 0
	}
	// Determine the time since the previous query count in number of seconds.
	timeSincePrev := currQueryCount.Timestamp().Sub(prevQueryCount.Timestamp()).Seconds()
	// Calculate the QPS since the previous query count:
	// (current number of queries) / (difference in seconds since last timestamp)
	// Timestamps between query counts are at least 1 second long, no need to
	// check for divide by 0.
	clusterQPS := currQueryCount.Count() / int64(timeSincePrev)
	return clusterQPS
}

// sampleRatePass is a sampling function. It generates a random float between
// 0 and 1 and compares it to the given samplingRate. If the random number is
// less than the given samplingRate, the item should be sampled.
func sampleRatePass(rndSource *rand.Rand, samplingRate float64) bool {
	randNum := rndSource.Float64()
	return randNum < samplingRate
}
