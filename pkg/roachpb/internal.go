// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package roachpb

// IsColumnar returns true if this InternalTimeSeriesData stores its samples
// in columnar format.
func (data *InternalTimeSeriesData) IsColumnar() bool {
	return len(data.Offset) > 0
}

// IsRollup returns true if this InternalTimeSeriesData is both in columnar
// format and contains "rollup" data.
func (data *InternalTimeSeriesData) IsRollup() bool {
	return len(data.Count) > 0
}

// SampleCount returns the number of samples contained in this
// InternalTimeSeriesData.
func (data *InternalTimeSeriesData) SampleCount() int {
	if data.IsColumnar() {
		return len(data.Offset)
	}
	return len(data.Samples)
}

// OffsetForTimestamp returns the offset within this collection that would
// represent the provided timestamp.
func (data *InternalTimeSeriesData) OffsetForTimestamp(timestampNanos int64) int32 {
	return int32((timestampNanos - data.StartTimestampNanos) / data.SampleDurationNanos)
}

// TimestampForOffset returns the timestamp that would represent the provided
// offset in this collection.
func (data *InternalTimeSeriesData) TimestampForOffset(offset int32) int64 {
	return data.StartTimestampNanos + int64(offset)*data.SampleDurationNanos
}
