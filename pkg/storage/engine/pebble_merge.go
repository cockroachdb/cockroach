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
	"container/list"
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

// MVCCValueMerger implements the `ValueMerger` interface. It buffers
// deserialized `MVCCMetadata` in a linked list in order of decreasing age
// in the DB, which it determines by whether the operand was added via
// `MergeNewer()` or `MergeOlder()`. It merges these deserialized
// operands when `Finish()` is called.
//
// It supports merging either all `roachpb.InternalTimeSeriesData` values
// or all non-timeseries values. Attempting to merge a mixture of timeseries
// and non-timeseries values will result in an error. In case of non-
// timeseries the values are simply concatenated from old to new.
type MVCCValueMerger struct {
	valsOrderedOldToNew *list.List
}

const (
	mvccChecksumSize = 4
	mvccTagPos       = mvccChecksumSize
	mvccHeaderSize   = mvccChecksumSize + 1
)

func deserializeMVCCValue(value []byte) (enginepb.MVCCMetadata, error) {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(value, &meta); err != nil {
		return enginepb.MVCCMetadata{}, errors.Errorf("corrupted operand value: %v", err)
	}
	return meta, nil
}

func deserializeTimeSeries(value []byte) (roachpb.InternalTimeSeriesData, error) {
	var ts roachpb.InternalTimeSeriesData
	if err := protoutil.Unmarshal(value, &ts); err != nil {
		return roachpb.InternalTimeSeriesData{}, errors.Errorf("corrupted timeseries: %v", err)
	}
	return ts, nil
}

func (t *MVCCValueMerger) MergeNewer(value []byte) error {
	if t.valsOrderedOldToNew == nil {
		t.valsOrderedOldToNew = list.New()
	}
	res, err := deserializeMVCCValue(value)
	if err != nil {
		return err
	}
	t.valsOrderedOldToNew.PushBack(res)
	return nil
}

func (t *MVCCValueMerger) MergeOlder(value []byte) error {
	if t.valsOrderedOldToNew == nil {
		t.valsOrderedOldToNew = list.New()
	}
	res, err := deserializeMVCCValue(value)
	if err != nil {
		return err
	}
	t.valsOrderedOldToNew.PushFront(res)
	return nil
}

func (t *MVCCValueMerger) Finish() ([]byte, error) {
	isColumnar := false
	front := t.valsOrderedOldToNew.Front()
	if front == nil {
		return nil, errors.Errorf("empty merge unsupported")
	}

	isTimeSeries := func(value enginepb.MVCCMetadata) (bool, error) {
		if len(value.RawBytes) < mvccHeaderSize {
			return false, errors.Errorf("operand value too short")
		}
		return value.RawBytes[mvccTagPos] == byte(roachpb.ValueType_TIMESERIES), nil
	}
	isTs, err := isTimeSeries(front.Value.(enginepb.MVCCMetadata))
	if err != nil {
		return nil, err
	}

	if !isTs {
		// Concatenate non-timeseries operands from old to new
		opValues := make([][]byte, 0, t.valsOrderedOldToNew.Len())
		totalOpLen := 0
		for e := t.valsOrderedOldToNew.Front(); e != nil; e = e.Next() {
			val := e.Value.(enginepb.MVCCMetadata)
			isTs, err := isTimeSeries(val)
			if err != nil {
				return nil, err
			}
			if isTs {
				return nil, errors.Errorf("inconsistent value types for non-timeseries merge")
			}
			opValues = append(opValues, val.RawBytes[mvccHeaderSize:])
			totalOpLen += len(val.RawBytes[mvccHeaderSize:])
		}
		var meta enginepb.MVCCMetadata
		meta.RawBytes = make([]byte, mvccHeaderSize, mvccHeaderSize+totalOpLen)
		meta.RawBytes[mvccTagPos] = byte(roachpb.ValueType_BYTES)
		for _, opValue := range opValues {
			meta.RawBytes = append(meta.RawBytes, opValue...)
		}
		res, err := protoutil.Marshal(&meta)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	// TODO(ajkr): confirm it is the case that (1) today's CRDB always merges timeseries
	// values in columnar format, and (2) today's CRDB does not need to be downgrade-
	// compatible with any version that supports row format only. Then we can drop support
	// for row format entirely. It requires significant cleanup effort as many tests target
	// the row format.
	var merged roachpb.InternalTimeSeriesData
	for e := t.valsOrderedOldToNew.Front(); e != nil; e = e.Next() {
		isTs, err := isTimeSeries(e.Value.(enginepb.MVCCMetadata))
		if err != nil {
			return nil, err
		}
		if !isTs {
			return nil, errors.Errorf("inconsistent value types for timeseries merge")
		}
		ts, err := deserializeTimeSeries(e.Value.(enginepb.MVCCMetadata).RawBytes[mvccHeaderSize:])
		if err != nil {
			return nil, err
		}
		if e == t.valsOrderedOldToNew.Front() {
			merged.StartTimestampNanos = ts.StartTimestampNanos
			merged.SampleDurationNanos = ts.SampleDurationNanos
		} else {
			if ts.StartTimestampNanos != merged.StartTimestampNanos {
				return nil, errors.Errorf("start timestamp mismatch")
			}
			if ts.SampleDurationNanos != merged.SampleDurationNanos {
				return nil, errors.Errorf("sample duration mismatch")
			}
		}

		if !isColumnar && len(ts.Offset) > 0 {
			ensureColumnar(&merged)
			ensureColumnar(&ts)
			isColumnar = true
		} else if isColumnar {
			ensureColumnar(&ts)
		}
		proto.Merge(&merged, &ts)
	}
	if isColumnar {
		sortAndDeduplicateColumns(&merged)
	} else {
		sortAndDeduplicateRows(&merged)
	}
	tsBytes, err := protoutil.Marshal(&merged)
	if err != nil {
		return nil, err
	}
	var meta enginepb.MVCCMetadata
	tsTag := byte(roachpb.ValueType_TIMESERIES)
	header := make([]byte, mvccHeaderSize)
	header[mvccTagPos] = tsTag
	meta.RawBytes = append(header, tsBytes...)
	res, err := protoutil.Marshal(&meta)
	if err != nil {
		return nil, err
	}
	return res, nil
}
