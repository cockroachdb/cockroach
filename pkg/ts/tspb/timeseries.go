// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tspb

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ToInternal places the datapoints in a TimeSeriesData message into one or
// more InternalTimeSeriesData messages. The structure and number of messages
// returned depends on two variables: a key duration, and a sample duration.
//
// The key duration is an interval length in nanoseconds used to determine
// how many intervals are grouped into a single InternalTimeSeriesData
// message.
//
// The sample duration is also an interval length in nanoseconds; it must be
// less than or equal to the key duration, and must also evenly divide the key
// duration. Datapoints which fall into the same sample interval will be
// aggregated together into a single Sample.
//
// Example: Assume the desired result is to aggregate individual datapoints into
// the same sample if they occurred within the same second; additionally, all
// samples which occur within the same hour should be stored at the same key
// location within the same InternalTimeSeriesValue. The sample duration should
// be 10^9 nanoseconds (value of time.Second), and the key duration should be
// (3600*10^9) nanoseconds (value of time.Hour).
//
// Note that this method does not accumulate data into individual samples in the
// case where multiple datapoints fall into the same sample period. Internal
// data should be merged into the cockroach data store before being read; the
// engine is responsible for accumulating samples.
//
// The returned slice of InternalTimeSeriesData objects will not be sorted.
//
// For more information on how time series data is stored, see
// InternalTimeSeriesData and its related structures.
func (ts TimeSeriesData) ToInternal(
	keyDuration, sampleDuration int64, columnar bool,
) ([]roachpb.InternalTimeSeriesData, error) {
	if err := VerifySlabAndSampleDuration(keyDuration, sampleDuration); err != nil {
		return nil, err
	}

	// This slice must be preallocated to avoid reallocation on `append` because
	// we maintain pointers to its elements in the map below.
	result := make([]roachpb.InternalTimeSeriesData, 0, len(ts.Datapoints))
	// Pointers because they need to mutate the stuff in the slice above.
	resultByKeyTime := make(map[int64]*roachpb.InternalTimeSeriesData)

	for _, dp := range ts.Datapoints {
		// Determine which InternalTimeSeriesData this datapoint belongs to,
		// creating if it has not already been created for a previous sample.
		keyTime := (dp.TimestampNanos / keyDuration) * keyDuration
		itsd, ok := resultByKeyTime[keyTime]
		if !ok {
			result = append(result, roachpb.InternalTimeSeriesData{
				StartTimestampNanos: keyTime,
				SampleDurationNanos: sampleDuration,
			})
			itsd = &result[len(result)-1]
			resultByKeyTime[keyTime] = itsd
		}

		// Create a new sample for this datapoint and place it into the
		// InternalTimeSeriesData.
		if columnar {
			itsd.Offset = append(itsd.Offset, itsd.OffsetForTimestamp(dp.TimestampNanos))
			itsd.Last = append(itsd.Last, dp.Value)
		} else {
			itsd.Samples = append(itsd.Samples, roachpb.InternalTimeSeriesSample{
				Offset: itsd.OffsetForTimestamp(dp.TimestampNanos),
				Count:  1,
				Sum:    dp.Value,
			})
		}
	}

	return result, nil
}

// VerifySlabAndSampleDuration verifies that he supplied slab resolution is
// compatible with the supplied sample resolution, returning an error if they
// are not compatible.
func VerifySlabAndSampleDuration(slabDuration, sampleDuration int64) error {
	if slabDuration%sampleDuration != 0 {
		return fmt.Errorf(
			"sample duration %d does not evenly divide key duration %d",
			sampleDuration, slabDuration)
	}
	if slabDuration < sampleDuration {
		return fmt.Errorf(
			"sample duration %d is not less than or equal to key duration %d",
			sampleDuration, slabDuration)
	}

	return nil
}
