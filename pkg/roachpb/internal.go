// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// ResetRetainingSlices clears all fields in the InternalTimeSeriesData, but
// retains any backing slices.
func (data *InternalTimeSeriesData) ResetRetainingSlices() {
	samples := data.Samples[:0]
	offset := data.Offset[:0]
	last := data.Last[:0]
	count := data.Count[:0]
	sum := data.Sum[:0]
	maxSlice := data.Max[:0]
	minSlice := data.Min[:0]
	first := data.First[:0]
	variance := data.Variance[:0]

	// We make sure we don't cause correctness issues if new fields are added.
	*data = InternalTimeSeriesData{
		Samples:  samples,
		Offset:   offset,
		Last:     last,
		Count:    count,
		Sum:      sum,
		Max:      maxSlice,
		Min:      minSlice,
		First:    first,
		Variance: variance,
	}
}
