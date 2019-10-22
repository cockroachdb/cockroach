// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// sortAndDeduplicateRows sorts all the samples field of the time series data
// structure according to the samples' `Offset`s. At the same time, samples with
// duplicate offset values are removed - only the last sample with a given offset
// in the collection is retained.
func sortAndDeduplicateRows(ts *roachpb.InternalTimeSeriesData) {
	// In the common case, appending the newer entries to the older entries
	// will result in an already ordered result, and there will be one sample
	// per offset. Optimize for that case.
	isSortedUniq := true
	for i := 1; i < len(ts.Samples); i++ {
		if ts.Samples[i-1].Offset >= ts.Samples[i].Offset {
			isSortedUniq = false
			break
		}
	}
	if isSortedUniq {
		return
	}

	// Create an auxiliary array of array indexes, and sort that array according
	// to the corresponding offset value in the ts.Samples collection. This
	// yields the permutation of the current array indexes that will place the
	// samples into sorted order. In order to guarantee only the last sample with
	// a duplicated offset is retained, we must do a stable sort.
	sortedSrcIdxs := make([]int, len(ts.Samples))
	for i := range sortedSrcIdxs {
		sortedSrcIdxs[i] = i
	}
	sort.SliceStable(sortedSrcIdxs, func(i, j int) bool {
		return ts.Samples[sortedSrcIdxs[i]].Offset < ts.Samples[sortedSrcIdxs[j]].Offset
	})

	// Remove any duplicates from the permutation, keeping the *last* element
	// merged for any given offset.
	uniqSortedSrcIdxs := make([]int, 0, len(ts.Samples))
	for destIdx := range sortedSrcIdxs {
		if destIdx == len(sortedSrcIdxs)-1 || ts.Samples[sortedSrcIdxs[destIdx]].Offset != ts.Samples[sortedSrcIdxs[destIdx+1]].Offset {
			uniqSortedSrcIdxs = append(uniqSortedSrcIdxs, sortedSrcIdxs[destIdx])
		}
	}

	origSamples := ts.Samples
	ts.Samples = make([]roachpb.InternalTimeSeriesSample, len(uniqSortedSrcIdxs))

	// Apply the permutation in the auxiliary array to all of the relevant column
	// arrays in the data set.
	for destIdx, srcIdx := range uniqSortedSrcIdxs {
		ts.Samples[destIdx] = origSamples[srcIdx]
	}
}

// sortAndDeduplicateColumns sorts all column fields of the time series data
// structure according to the timeseries's `Offset` column. At the same time,
// duplicate offset values are removed - only the last instance of an offset in
// the collection is retained.
func sortAndDeduplicateColumns(ts *roachpb.InternalTimeSeriesData) {
	// In the common case, appending the newer entries to the older entries
	// will result in an already ordered result with no duplicated offsets.
	// Optimize for that case.
	isSortedUniq := true
	for i := 1; i < len(ts.Offset); i++ {
		if ts.Offset[i-1] >= ts.Offset[i] {
			isSortedUniq = false
			break
		}
	}
	if isSortedUniq {
		return
	}

	// Create an auxiliary array of array indexes, and sort that array according
	// to the corresponding offset value in the `ts.Offset` collection. This yields
	// the permutation of the current array indexes that will place the offsets into
	// sorted order. In order to guarantee only the last column values corresponding
	// to a duplicated offset are retained, we must do a stable sort.
	sortedSrcIdxs := make([]int, len(ts.Offset))
	for i := range sortedSrcIdxs {
		sortedSrcIdxs[i] = i
	}
	sort.SliceStable(sortedSrcIdxs, func(i, j int) bool {
		return ts.Offset[sortedSrcIdxs[i]] < ts.Offset[sortedSrcIdxs[j]]
	})

	// Remove any duplicates from the permutation, keeping the *last* element
	// merged for any given offset.
	uniqSortedSrcIdxs := make([]int, 0, len(ts.Offset))
	for destIdx := range sortedSrcIdxs {
		if destIdx == len(sortedSrcIdxs)-1 || ts.Offset[sortedSrcIdxs[destIdx]] != ts.Offset[sortedSrcIdxs[destIdx+1]] {
			uniqSortedSrcIdxs = append(uniqSortedSrcIdxs, sortedSrcIdxs[destIdx])
		}
	}

	origOffset, origLast, origCount, origSum, origMin, origMax, origFirst, origVariance :=
		ts.Offset, ts.Last, ts.Count, ts.Sum, ts.Min, ts.Max, ts.First, ts.Variance
	ts.Offset = make([]int32, len(uniqSortedSrcIdxs))
	ts.Last = make([]float64, len(uniqSortedSrcIdxs))
	// These columns are only present at resolutions generated as rollups. We
	// detect this by checking if there are any count columns present (the
	// choice of "count" is arbitrary, all of these columns will be present or
	// not).
	if len(origCount) > 0 {
		ts.Count = make([]uint32, len(uniqSortedSrcIdxs))
		ts.Sum = make([]float64, len(uniqSortedSrcIdxs))
		ts.Min = make([]float64, len(uniqSortedSrcIdxs))
		ts.Max = make([]float64, len(uniqSortedSrcIdxs))
		ts.First = make([]float64, len(uniqSortedSrcIdxs))
		ts.Variance = make([]float64, len(uniqSortedSrcIdxs))
	}

	// Apply the permutation in the auxiliary array to all of the relevant column
	// arrays in the data set.
	for destIdx, srcIdx := range uniqSortedSrcIdxs {
		ts.Offset[destIdx] = origOffset[srcIdx]
		ts.Last[destIdx] = origLast[srcIdx]

		if len(origCount) > 0 {
			ts.Count[destIdx] = origCount[srcIdx]
			ts.Sum[destIdx] = origSum[srcIdx]
			ts.Min[destIdx] = origMin[srcIdx]
			ts.Max[destIdx] = origMax[srcIdx]
			ts.First[destIdx] = origFirst[srcIdx]
			ts.Variance[destIdx] = origVariance[srcIdx]
		}
	}
}

// ensureColumnar detects time series data which is in the old row format,
// converting the row data into the new columnar format.
func ensureColumnar(ts *roachpb.InternalTimeSeriesData) {
	for _, sample := range ts.Samples {
		ts.Offset = append(ts.Offset, sample.Offset)
		ts.Last = append(ts.Last, sample.Sum)
	}
	ts.Samples = ts.Samples[:0]
}

// mergeTimeSeries combines two `InternalTimeSeriesData`s and returns the result as an
// `InternalTimeSeriesData`.  The inputs cannot be merged if they have different start
// timestamps or sample durations.
func mergeTimeSeries(
	oldTs, newTs roachpb.InternalTimeSeriesData,
) (roachpb.InternalTimeSeriesData, error) {
	if oldTs.StartTimestampNanos != newTs.StartTimestampNanos {
		return roachpb.InternalTimeSeriesData{}, errors.Errorf("start timestamp mismatch")
	}
	if oldTs.SampleDurationNanos != newTs.SampleDurationNanos {
		return roachpb.InternalTimeSeriesData{}, errors.Errorf("sample duration mismatch")
	}

	// TODO(ajkr): confirm it is the case that (1) today's CRDB always merges timeseries
	// values in columnar format, and (2) today's CRDB does not need to be downgrade-
	// compatible with any version that supports row format only. Then we can drop support
	// for row format entirely. It requires significant cleanup effort as many tests target
	// the row format.
	if len(oldTs.Offset) > 0 || len(newTs.Offset) > 0 {
		ensureColumnar(&oldTs)
		ensureColumnar(&newTs)
		proto.Merge(&oldTs, &newTs)
		sortAndDeduplicateColumns(&oldTs)
	} else {
		proto.Merge(&oldTs, &newTs)
		sortAndDeduplicateRows(&oldTs)
	}
	return oldTs, nil
}

// mergeTimeSeriesValues attempts to merge two values which contain
// InternalTimeSeriesData messages.
func mergeTimeSeriesValues(oldTsBytes, newTsBytes []byte) ([]byte, error) {
	var oldTs, newTs, mergedTs roachpb.InternalTimeSeriesData
	if err := protoutil.Unmarshal(oldTsBytes, &oldTs); err != nil {
		return nil, errors.Errorf("corrupted old timeseries: %v", err)
	}
	if err := protoutil.Unmarshal(newTsBytes, &newTs); err != nil {
		return nil, errors.Errorf("corrupted new timeseries: %v", err)
	}

	var err error
	if mergedTs, err = mergeTimeSeries(oldTs, newTs); err != nil {
		return nil, errors.Errorf("mergeTimeSeries: %v", err)
	}

	res, err := protoutil.Marshal(&mergedTs)
	if err != nil {
		return nil, errors.Errorf("corrupted merged timeseries: %v", err)
	}
	return res, nil
}

// merge combines two serialized `MVCCMetadata`s and returns the result as a serialized
// `MVCCMetadata`.
//
// Replay Advisory: Because merge commands pass through raft, it is possible
// for merging values to be "replayed". Currently, the only actual use of
// the merge system is for time series data, which is safe against replay;
// however, this property is not general for all potential mergeable types.
// If a future need arises to merge another type of data, replay protection
// will likely need to be a consideration.
func merge(key, oldValue, newValue, buf []byte) ([]byte, error) {
	const (
		checksumSize = 4
		tagPos       = checksumSize
		headerSize   = checksumSize + 1
	)

	var oldMeta, newMeta, mergedMeta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(oldValue, &oldMeta); err != nil {
		return nil, errors.Errorf("corrupted old operand value: %v", err)
	}
	if len(oldMeta.RawBytes) < headerSize {
		return nil, errors.Errorf("old operand value too short")
	}
	if err := protoutil.Unmarshal(newValue, &newMeta); err != nil {
		return nil, errors.Errorf("corrupted new operand value: %v", err)
	}
	if len(newMeta.RawBytes) < headerSize {
		return nil, errors.Errorf("new operand value too short")
	}

	tsTag := byte(roachpb.ValueType_TIMESERIES)
	if oldMeta.RawBytes[tagPos] == tsTag || newMeta.RawBytes[tagPos] == tsTag {
		if oldMeta.RawBytes[tagPos] != tsTag || newMeta.RawBytes[tagPos] != tsTag {
			return nil, errors.Errorf("inconsistent value types for timeseries merge")
		}
		tsBytes, err := mergeTimeSeriesValues(
			oldMeta.RawBytes[headerSize:], newMeta.RawBytes[headerSize:])
		if err != nil {
			return nil, errors.Errorf("mergeTimeSeriesValues: %v", err)
		}
		header := make([]byte, headerSize)
		header[tagPos] = tsTag
		mergedMeta.RawBytes = append(header, tsBytes...)
	} else {
		// For non-timeseries values, merge is a simple append.
		mergedMeta.RawBytes = append(oldMeta.RawBytes, newMeta.RawBytes[headerSize:]...)
	}

	res, err := protoutil.Marshal(&mergedMeta)
	if err != nil {
		return nil, errors.Errorf("corrupted merged value: %v", err)
	}
	return res, nil
}
