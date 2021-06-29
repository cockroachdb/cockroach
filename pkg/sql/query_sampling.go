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

const defaultSampleRate = 0.00001

var telemetrySampleRate = settings.RegisterFloatSetting(
	"sql.telemetry.query_sampling.sample_rate",
	"the rate/probability at which we sample queries for telemetry",
	defaultSampleRate,
	settings.NonNegativeFloat,
)

const defaultQPSThreshold = 100

var telemetryQPSThreshold = settings.RegisterIntSetting(
	"sql.telemetry.query_sampling.qps_threshold",
	"the QPS threshold at which we begin sampling DML statements for telemetry logs",
	defaultQPSThreshold,
	settings.NonNegativeInt,
)

const defaultSmoothingAlpha float64 = 0.8

var telemetrySmoothingAlpha = settings.RegisterFloatSetting(
	"sql.telemetry.query_sampling.smoothing_alpha",
	"the smoothing coefficient for exponential smoothing, used to approximate cluster QPS",
	defaultSmoothingAlpha,
	settings.NonNegativeFloat,
)

const defaultRollingInterval int64 = 10

var telemetryRollingInterval = settings.RegisterIntSetting(
	"sql.telemetry.query_sampling.rolling_interval",
	"the size of the rolling interval used in telemetry metrics for logging",
	defaultRollingInterval,
	settings.PositiveInt,
)

// ExpSmoothQPS calculates a smoothed QPS value from TelemetryLoggingMetrics query counts.
func (t *TelemetryLoggingMetrics) ExpSmoothQPS() int64 {
	t.Lock()
	defer t.Unlock()

	// If the interval length is of size 1 return the latest query count.
	if t.getInterval() == 1 {
		return t.LastQueryCount().Count()
	}

	var totalQPSVal int
	var smoothQPS float64

	currIdx := t.RollingQueryCounts.End
	startIdx := t.RollingQueryCounts.NextIndex(t.RollingQueryCounts.End)
	t.MovingQPS = t.MovingQPS[:0]
	for currIdx != startIdx {
		prevIdx := t.RollingQueryCounts.PrevIndex(currIdx)
		curr := t.GetQueryCount(currIdx)
		prev := t.GetQueryCount(prevIdx)
		qpsVal := calcAvgQPS(curr, prev)

		t.MovingQPS = append(t.MovingQPS, qpsVal)
		totalQPSVal += int(qpsVal)

		currIdx = t.RollingQueryCounts.PrevIndex(currIdx)
	}

	for i := len(t.MovingQPS) - 1; i >= 0; i-- {
		qpsVal := float64(t.MovingQPS[i])
		// On first entry, there are no previous values to approximate a smooth QPS value.
		// Consequently, we use the average QPS value as the initial smoothed QPS value.
		// Using just the initial value can cause skewing in the exponential smoothing calculation.
		if i == len(t.MovingQPS)-1 {
			avgQPSVal := totalQPSVal / len(t.MovingQPS)
			smoothQPS += float64(avgQPSVal)
		} else {
			smoothQPS = t.smoothingAlpha*qpsVal + (1-t.smoothingAlpha)*smoothQPS
		}
	}
	return int64(smoothQPS)
}

// calcAvgQPS gets the average cluster QPS between two timestamps. The difference in the number of queries executed
// between the timestamps is divided by the number of seconds between the timestamps.
func calcAvgQPS(currQueryCount *QueryCountAndTime, prevQueryCount *QueryCountAndTime) int64 {
	// If the current query count is empty, return 0.
	if *currQueryCount == (QueryCountAndTime{}) {
		return 0
	}
	// Determine the time since the previous query count in number of seconds.
	timeSincePrev := currQueryCount.Timestamp().Sub(prevQueryCount.Timestamp()).Seconds()
	// Calculate the QPS since the previous query count:
	//	(current number of queries) / (difference in seconds since last timestamp)
	// Timestamps between query counts are at least 1 second long, no need to check for
	// divide by 0.
	clusterQPS := currQueryCount.Count() / int64(timeSincePrev)
	return clusterQPS
}

// sampleRatePass is a sampling function. It generates a random float between 0 and 1 and
// compares it to the given samplingRate. If the random number less than the given
// samplingRate, the item should be sampled.
func sampleRatePass(samplingRate float64) bool {
	randNum := rand.Float64()
	return randNum < samplingRate
}
