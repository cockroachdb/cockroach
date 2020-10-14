// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"io"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// MVCCValueMerger implements the `ValueMerger` interface. It buffers
// deserialized values in a slice in order specified by `oldToNew`.
// It determines the order of incoming operands by whether they were added
// with `MergeNewer()` or `MergeOlder()`, reversing the slice as necessary
// to ensure operands are always appended. It merges these deserialized
// operands when `Finish()` is called.
//
// It supports merging either all `roachpb.InternalTimeSeriesData` values
// or all non-timeseries values. Attempting to merge a mixture of timeseries
// and non-timeseries values will result in an error.
type MVCCValueMerger struct {
	timeSeriesOps []roachpb.InternalTimeSeriesData
	rawByteOps    [][]byte
	oldestMergeTS hlc.LegacyTimestamp
	oldToNew      bool

	// Used to avoid heap allocations when passing pointer to `Unmarshal()`.
	meta enginepb.MVCCMetadata
}

const (
	mvccChecksumSize = 4
	mvccTagPos       = mvccChecksumSize
	mvccHeaderSize   = mvccChecksumSize + 1
)

func (t *MVCCValueMerger) ensureOrder(oldToNew bool) {
	if oldToNew == t.oldToNew {
		return
	}
	// Only one of the two loop bodies should actually execute under error-free
	// conditions, i.e., all operands are either timeseries or all are non-
	// timeseries.
	for i := 0; i < len(t.timeSeriesOps)/2; i++ {
		t.timeSeriesOps[i], t.timeSeriesOps[len(t.timeSeriesOps)-1-i] = t.timeSeriesOps[len(t.timeSeriesOps)-1-i], t.timeSeriesOps[i]
	}
	for i := 0; i < len(t.rawByteOps)/2; i++ {
		t.rawByteOps[i], t.rawByteOps[len(t.rawByteOps)-1-i] = t.rawByteOps[len(t.rawByteOps)-1-i], t.rawByteOps[i]
	}
	t.oldToNew = oldToNew
}

func (t *MVCCValueMerger) deserializeMVCCValueAndAppend(value []byte) error {
	if err := protoutil.Unmarshal(value, &t.meta); err != nil {
		return errors.Errorf("corrupted operand value: %v", err)
	}
	if len(t.meta.RawBytes) < mvccHeaderSize {
		return errors.Errorf("operand value too short")
	}
	if t.meta.RawBytes[mvccTagPos] == byte(roachpb.ValueType_TIMESERIES) {
		if t.rawByteOps != nil {
			return errors.Errorf("inconsistent value types for timeseries merge")
		}
		t.timeSeriesOps = append(t.timeSeriesOps, roachpb.InternalTimeSeriesData{})
		ts := &t.timeSeriesOps[len(t.timeSeriesOps)-1]
		if err := protoutil.Unmarshal(t.meta.RawBytes[mvccHeaderSize:], ts); err != nil {
			return errors.Errorf("corrupted timeseries: %v", err)
		}
	} else {
		if t.timeSeriesOps != nil {
			return errors.Errorf("inconsistent value types for non-timeseries merge")
		}
		t.rawByteOps = append(t.rawByteOps, t.meta.RawBytes[mvccHeaderSize:])
	}
	// Save the timestamp of the oldest value since that is consistent with the
	// behavior of the C++ DBMergeOperator.
	if t.meta.MergeTimestamp != nil && (t.oldestMergeTS == hlc.LegacyTimestamp{} || !t.oldToNew) {
		t.oldestMergeTS = *t.meta.MergeTimestamp
	}
	return nil
}

// MergeNewer deserializes the value and appends it to the slice corresponding to its type
// (timeseries or non-timeseries). The slice will be reversed if needed such that it is in
// old-to-new order.
func (t *MVCCValueMerger) MergeNewer(value []byte) error {
	t.ensureOrder(true /* oldToNew */)
	if err := t.deserializeMVCCValueAndAppend(value); err != nil {
		return err
	}
	return nil
}

// MergeOlder deserializes the value and appends it to the slice corresponding to its type
// (timeseries or non-timeseries). The slice will be reversed if needed such that it is in
// new-to-old order.
func (t *MVCCValueMerger) MergeOlder(value []byte) error {
	t.ensureOrder(false /* oldToNew */)
	if err := t.deserializeMVCCValueAndAppend(value); err != nil {
		return err
	}
	return nil
}

// Finish combines the buffered values from all `Merge*()` calls and marshals the result.
// In case of non-timeseries the values are simply concatenated from old to new. In case
// of timeseries the values are sorted, deduplicated, and potentially migrated to columnar
// format. When deduplicating, only the latest sample for a given offset is retained.
func (t *MVCCValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	isColumnar := false
	if t.timeSeriesOps == nil && t.rawByteOps == nil {
		return nil, nil, errors.Errorf("empty merge unsupported")
	}
	t.ensureOrder(true /* oldToNew */)
	if t.timeSeriesOps == nil {
		// Concatenate non-timeseries operands from old to new
		totalLen := 0
		for _, rawByteOp := range t.rawByteOps {
			totalLen += len(rawByteOp)
		}
		// See the motivating comment in mvcc.proto.
		var meta enginepb.MVCCMetadataSubsetForMergeSerialization
		meta.RawBytes = make([]byte, mvccHeaderSize, mvccHeaderSize+totalLen)
		meta.RawBytes[mvccTagPos] = byte(roachpb.ValueType_BYTES)
		for _, rawByteOp := range t.rawByteOps {
			meta.RawBytes = append(meta.RawBytes, rawByteOp...)
		}
		res, err := protoutil.Marshal(&meta)
		if err != nil {
			return nil, nil, err
		}
		return res, nil, nil
	}

	// TODO(ajkr): confirm it is the case that (1) today's CRDB always merges timeseries
	// values in columnar format, and (2) today's CRDB does not need to be downgrade-
	// compatible with any version that supports row format only. Then we can drop support
	// for row format entirely. It requires significant cleanup effort as many tests target
	// the row format.
	var merged roachpb.InternalTimeSeriesData
	merged.StartTimestampNanos = t.timeSeriesOps[0].StartTimestampNanos
	merged.SampleDurationNanos = t.timeSeriesOps[0].SampleDurationNanos
	for _, timeSeriesOp := range t.timeSeriesOps {
		if timeSeriesOp.StartTimestampNanos != merged.StartTimestampNanos {
			return nil, nil, errors.Errorf("start timestamp mismatch")
		}
		if timeSeriesOp.SampleDurationNanos != merged.SampleDurationNanos {
			return nil, nil, errors.Errorf("sample duration mismatch")
		}
		if !isColumnar && len(timeSeriesOp.Offset) > 0 {
			ensureColumnar(&merged)
			ensureColumnar(&timeSeriesOp)
			isColumnar = true
		} else if isColumnar {
			ensureColumnar(&timeSeriesOp)
		}
		proto.Merge(&merged, &timeSeriesOp)
	}
	if isColumnar {
		sortAndDeduplicateColumns(&merged)
	} else {
		sortAndDeduplicateRows(&merged)
	}
	tsBytes, err := protoutil.Marshal(&merged)
	if err != nil {
		return nil, nil, err
	}
	// See the motivating comment in mvcc.proto.
	var meta enginepb.MVCCMetadataSubsetForMergeSerialization
	if !(t.oldestMergeTS == hlc.LegacyTimestamp{}) {
		meta.MergeTimestamp = &t.oldestMergeTS
	}
	tsTag := byte(roachpb.ValueType_TIMESERIES)
	header := make([]byte, mvccHeaderSize)
	header[mvccTagPos] = tsTag
	meta.RawBytes = append(header, tsBytes...)
	res, err := protoutil.Marshal(&meta)
	if err != nil {
		return nil, nil, err
	}
	return res, nil, nil
}

func serializeMergeInputs(sources ...roachpb.InternalTimeSeriesData) ([][]byte, error) {
	// Wrap each proto in an inlined MVCC value, and marshal each wrapped value
	// to bytes. This is the format required by the engine.
	srcBytes := make([][]byte, 0, len(sources))
	var val roachpb.Value
	for _, src := range sources {
		if err := val.SetProto(&src); err != nil {
			return nil, err
		}
		bytes, err := protoutil.Marshal(&enginepb.MVCCMetadata{
			RawBytes: val.RawBytes,
		})
		if err != nil {
			return nil, err
		}
		srcBytes = append(srcBytes, bytes)
	}
	return srcBytes, nil
}

func deserializeMergeOutput(mergedBytes []byte) (roachpb.InternalTimeSeriesData, error) {
	// Unmarshal merged bytes and extract the time series value within.
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(mergedBytes, &meta); err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	mergedTS, err := MakeValue(meta).GetTimeseries()
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	return mergedTS, nil
}

// MergeInternalTimeSeriesData exports the engine's MVCC merge logic for
// InternalTimeSeriesData to higher level packages. This is intended primarily
// for consumption by high level testing of time series functionality.
// If usePartialMerge is true, the operands are merged together using a partial
// merge operation first, and are then merged in to the initial state.
func MergeInternalTimeSeriesData(
	usePartialMerge bool, sources ...roachpb.InternalTimeSeriesData,
) (roachpb.InternalTimeSeriesData, error) {
	// Merge every element into a nil byte slice, one at a time.
	var mvccMerger MVCCValueMerger
	srcBytes, err := serializeMergeInputs(sources...)
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	for _, bytes := range srcBytes {
		if err := mvccMerger.MergeNewer(bytes); err != nil {
			return roachpb.InternalTimeSeriesData{}, err
		}
	}
	resBytes, closer, err := mvccMerger.Finish(!usePartialMerge)
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	res, err := deserializeMergeOutput(resBytes)
	if closer != nil {
		_ = closer.Close()
	}
	return res, err
}
