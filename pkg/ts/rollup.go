// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"context"
	"math"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type rollupDatapoint struct {
	timestampNanos int64
	first          float64
	last           float64
	min            float64
	max            float64
	sum            float64
	count          uint32
	variance       float64
}

type rollupData struct {
	name       string
	source     string
	datapoints []rollupDatapoint
}

func (rd *rollupData) toInternal(
	keyDuration, sampleDuration int64,
) ([]roachpb.InternalTimeSeriesData, error) {
	if err := tspb.VerifySlabAndSampleDuration(keyDuration, sampleDuration); err != nil {
		return nil, err
	}

	// This slice must be preallocated to avoid reallocation on `append` because
	// we maintain pointers to its elements in the map below.
	result := make([]roachpb.InternalTimeSeriesData, 0, len(rd.datapoints))
	// Pointers because they need to mutate the stuff in the slice above.
	resultByKeyTime := make(map[int64]*roachpb.InternalTimeSeriesData)

	for _, dp := range rd.datapoints {
		// Determine which InternalTimeSeriesData this datapoint belongs to,
		// creating if it has not already been created for a previous sample.
		keyTime := normalizeToPeriod(dp.timestampNanos, keyDuration)
		itsd, ok := resultByKeyTime[keyTime]
		if !ok {
			result = append(result, roachpb.InternalTimeSeriesData{
				StartTimestampNanos: keyTime,
				SampleDurationNanos: sampleDuration,
			})
			itsd = &result[len(result)-1]
			resultByKeyTime[keyTime] = itsd
		}

		itsd.Offset = append(itsd.Offset, itsd.OffsetForTimestamp(dp.timestampNanos))
		itsd.Last = append(itsd.Last, dp.last)
		itsd.First = append(itsd.First, dp.first)
		itsd.Min = append(itsd.Min, dp.min)
		itsd.Max = append(itsd.Max, dp.max)
		itsd.Count = append(itsd.Count, dp.count)
		itsd.Sum = append(itsd.Sum, dp.sum)
		itsd.Variance = append(itsd.Variance, dp.variance)
	}

	return result, nil
}

func computeRollupsFromData(data tspb.TimeSeriesData, rollupPeriodNanos int64) rollupData {
	rollup := rollupData{
		name:   data.Name,
		source: data.Source,
	}

	createRollupPoint := func(timestamp int64, dataSlice []tspb.TimeSeriesDatapoint) {
		result := rollupDatapoint{
			timestampNanos: timestamp,
			max:            -math.MaxFloat64,
			min:            math.MaxFloat64,
		}
		for i, dp := range dataSlice {
			if i == 0 {
				result.first = dp.Value
			}
			result.last = dp.Value
			result.max = math.Max(result.max, dp.Value)
			result.min = math.Min(result.min, dp.Value)

			if result.count > 0 {
				result.variance = computeParallelVariance(
					parallelVarianceArgs{
						count:    result.count,
						average:  result.sum / float64(result.count),
						variance: result.variance,
					},
					parallelVarianceArgs{
						count:    1,
						average:  dp.Value,
						variance: 0,
					},
				)
			}

			result.count++
			result.sum += dp.Value
		}

		rollup.datapoints = append(rollup.datapoints, result)
	}

	dps := data.Datapoints
	for len(dps) > 0 {
		rollupTimestamp := normalizeToPeriod(dps[0].TimestampNanos, rollupPeriodNanos)
		endIdx := sort.Search(len(dps), func(i int) bool {
			return normalizeToPeriod(dps[i].TimestampNanos, rollupPeriodNanos) > rollupTimestamp
		})
		createRollupPoint(rollupTimestamp, dps[:endIdx])
		dps = dps[endIdx:]
	}

	return rollup
}

func (db *DB) rollupTimeSeries(
	ctx context.Context,
	timeSeriesList []timeSeriesResolutionInfo,
	now hlc.Timestamp,
	qmc QueryMemoryContext,
) error {
	thresholds := db.computeThresholds(now.WallTime)
	for _, timeSeries := range timeSeriesList {
		// Only process rollup if this resolution has a target rollup resolution.
		targetResolution, hasRollup := timeSeries.Resolution.TargetRollupResolution()
		if !hasRollup {
			continue
		}

		// Query from beginning of time up to the threshold for this resolution.
		threshold := thresholds[timeSeries.Resolution]

		// Create an initial targetSpan to find data for this series, starting at
		// the beginning of time and ending with the threshold time. Queries use
		// MaxSpanRequestKeys to limit the number of rows in memory at one time,
		// and will use ResumeSpan to issue additional queries if necessary.
		targetSpan := roachpb.Span{
			Key: MakeDataKey(timeSeries.Name, "" /* source */, timeSeries.Resolution, 0),
			EndKey: MakeDataKey(
				timeSeries.Name, "" /* source */, timeSeries.Resolution, threshold,
			),
		}

		// For each row, generate a rollup datapoint and add it to the correct
		// rollupData object.
		rollupDataMap := make(map[string]rollupData)

		account := qmc.workerMonitor.MakeBoundAccount()
		defer account.Close(ctx)

		childQmc := QueryMemoryContext{
			workerMonitor:      qmc.workerMonitor,
			resultAccount:      &account,
			QueryMemoryOptions: qmc.QueryMemoryOptions,
		}
		for querySpan := targetSpan; querySpan.Valid(); {
			var err error
			querySpan, err = db.queryAndComputeRollupsForSpan(
				ctx, timeSeries, querySpan, targetResolution, rollupDataMap, childQmc,
			)
			if err != nil {
				return err
			}
		}

		// Write computed rollupDataMap to disk
		var rollupDataSlice []rollupData
		for _, data := range rollupDataMap {
			rollupDataSlice = append(rollupDataSlice, data)
		}
		if err := db.storeRollup(ctx, targetResolution, rollupDataSlice); err != nil {
			return err
		}
	}
	return nil
}

// queryAndComputeRollupsForSpan queries time series data from the provided
// span, up to a maximum limit of rows based on memory limits.
func (db *DB) queryAndComputeRollupsForSpan(
	ctx context.Context,
	series timeSeriesResolutionInfo,
	span roachpb.Span,
	targetResolution Resolution,
	rollupDataMap map[string]rollupData,
	qmc QueryMemoryContext,
) (roachpb.Span, error) {
	b := &kv.Batch{}
	b.Header.MaxSpanRequestKeys = qmc.GetMaxRollupSlabs(series.Resolution)
	b.Scan(span.Key, span.EndKey)
	if err := db.db.Run(ctx, b); err != nil {
		return roachpb.Span{}, err
	}

	// Convert result data into a map of source strings to ordered spans of
	// time series data.
	diskAccount := qmc.workerMonitor.MakeBoundAccount()
	defer diskAccount.Close(ctx)
	sourceSpans, err := convertKeysToSpans(ctx, b.Results[0].Rows, &diskAccount)
	if err != nil {
		return roachpb.Span{}, err
	}

	// For each source, iterate over the data span and compute
	// rollupDatapoints.
	for source, span := range sourceSpans {
		rollup, ok := rollupDataMap[source]
		if !ok {
			rollup = rollupData{
				name:   series.Name,
				source: source,
			}
			if err := qmc.resultAccount.Grow(ctx, int64(unsafe.Sizeof(rollup))); err != nil {
				return roachpb.Span{}, err
			}
		}

		var end timeSeriesSpanIterator
		for start := makeTimeSeriesSpanIterator(span); start.isValid(); start = end {
			rollupPeriod := targetResolution.SampleDuration()
			sampleTimestamp := normalizeToPeriod(start.timestamp, rollupPeriod)
			datapoint := rollupDatapoint{
				timestampNanos: sampleTimestamp,
				max:            -math.MaxFloat64,
				min:            math.MaxFloat64,
				first:          start.first(),
			}
			if err := qmc.resultAccount.Grow(ctx, int64(unsafe.Sizeof(datapoint))); err != nil {
				return roachpb.Span{}, err
			}
			for end = start; end.isValid() && normalizeToPeriod(end.timestamp, rollupPeriod) == sampleTimestamp; end.forward() {
				datapoint.last = end.last()
				datapoint.max = math.Max(datapoint.max, end.max())
				datapoint.min = math.Min(datapoint.min, end.min())

				// Chan et al. algorithm for computing parallel variance. This allows
				// the combination of two previously computed sample variances into a
				// variance for the combined sample; this is needed when further
				// downsampling previously downsampled variance values.
				if datapoint.count > 0 {
					datapoint.variance = computeParallelVariance(
						parallelVarianceArgs{
							count:    end.count(),
							average:  end.average(),
							variance: end.variance(),
						},
						parallelVarianceArgs{
							count:    datapoint.count,
							average:  datapoint.sum / float64(datapoint.count),
							variance: datapoint.variance,
						},
					)
				}

				datapoint.count += end.count()
				datapoint.sum += end.sum()
			}
			rollup.datapoints = append(rollup.datapoints, datapoint)
		}
		rollupDataMap[source] = rollup
	}
	return b.Results[0].ResumeSpanAsValue(), nil
}

type parallelVarianceArgs struct {
	count    uint32
	average  float64
	variance float64
}

// computeParallelVariance computes the combined variance of two previously
// computed sample variances. This is an implementation of the Chan et al.
// algorithm for computing parallel variance. This allows the combination of two
// previously computed sample variances into a variance for the combined sample;
// this is needed when further downsampling previously downsampled variance
// values. Note that it is exactly equivalent to the more widely used Welford's
// algorithm when either variance set has a count of one.
func computeParallelVariance(left, right parallelVarianceArgs) float64 {
	leftCount := float64(left.count)
	rightCount := float64(right.count)
	totalCount := leftCount + rightCount
	averageDelta := left.average - right.average
	leftSumOfSquareDeviations := left.variance * leftCount
	rightSumOfSquareDeviations := right.variance * rightCount
	totalSumOfSquareDeviations := leftSumOfSquareDeviations + rightSumOfSquareDeviations + (averageDelta*averageDelta)*rightCount*leftCount/totalCount
	return totalSumOfSquareDeviations / totalCount
}
