// Copyright 2014 The Cockroach Authors.
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
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
)

var testtime = int64(-446061360000000000)

type tsSample struct {
	offset int32
	count  uint32
	sum    float64
	max    float64
	min    float64
}

type tsColumnSample struct {
	offset   int32
	last     float64
	count    uint32
	first    float64
	sum      float64
	max      float64
	min      float64
	variance float64
}

func gibberishString(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(math.MaxUint8 + 1))
	}
	return string(b)
}

func mustMarshal(m protoutil.Message) []byte {
	b, err := protoutil.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

func appender(s string) []byte {
	val := roachpb.MakeValueFromString(s)
	v := &enginepb.MVCCMetadataSubsetForMergeSerialization{RawBytes: val.RawBytes}
	return mustMarshal(v)
}

// timeSeriesRow generates a simple InternalTimeSeriesData object which starts
// at the given timestamp and has samples of the given duration. The time series
// is written using the older sample-row data format. The object is stored in an
// MVCCMetadata object and marshaled to bytes.
func timeSeriesRow(start int64, duration int64, samples ...tsSample) []byte {
	tsv := timeSeriesRowAsValue(start, duration, samples...)
	return mustMarshal(&enginepb.MVCCMetadataSubsetForMergeSerialization{RawBytes: tsv.RawBytes})
}

func timeSeriesRowAsValue(start int64, duration int64, samples ...tsSample) roachpb.Value {
	ts := &roachpb.InternalTimeSeriesData{
		StartTimestampNanos: start,
		SampleDurationNanos: duration,
	}
	for _, sample := range samples {
		newSample := roachpb.InternalTimeSeriesSample{
			Offset: sample.offset,
			Count:  sample.count,
			Sum:    sample.sum,
		}
		if sample.count > 1 {
			newSample.Max = proto.Float64(sample.max)
			newSample.Min = proto.Float64(sample.min)
		}
		ts.Samples = append(ts.Samples, newSample)
	}
	var v roachpb.Value
	if err := v.SetProto(ts); err != nil {
		panic(err)
	}
	return v
}

func timeSeriesColumn(start int64, duration int64, rollup bool, samples ...tsColumnSample) []byte {
	tsv := timeSeriesColumnAsValue(start, duration, rollup, samples...)
	return mustMarshal(&enginepb.MVCCMetadata{RawBytes: tsv.RawBytes})
}

func timeSeriesColumnAsValue(
	start int64, duration int64, rollup bool, samples ...tsColumnSample,
) roachpb.Value {
	ts := &roachpb.InternalTimeSeriesData{
		StartTimestampNanos: start,
		SampleDurationNanos: duration,
	}
	for _, sample := range samples {
		ts.Offset = append(ts.Offset, sample.offset)
		ts.Last = append(ts.Last, sample.last)
		if rollup {
			ts.Sum = append(ts.Sum, sample.sum)
			ts.Count = append(ts.Count, sample.count)
			ts.Min = append(ts.Min, sample.min)
			ts.Max = append(ts.Max, sample.max)
			ts.First = append(ts.First, sample.first)
			ts.Variance = append(ts.Variance, sample.variance)
		}
	}
	var v roachpb.Value
	if err := v.SetProto(ts); err != nil {
		panic(err)
	}
	return v
}

// TestGoMergeCorruption tests the function goMerge with error inputs but does not test the
// integration with the storage engines. For that, see the engine tests.
func TestGoMergeCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	badCombinations := []struct {
		existing, update []byte
	}{
		{appender(""), nil},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			nil,
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			appender("a"),
		},
		{
			appender("a"),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime+1, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 100, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
		},
	}
	for i, c := range badCombinations {
		_, err := goMerge(c.existing, c.update)
		if err == nil {
			t.Errorf("goMerge: %d: expected error", i)
		}
		_, err = mergeValuesPebble(false /* reverse */, [][]byte{c.existing, c.update})
		if err == nil {
			t.Fatalf("pebble merge forward: %d: expected error", i)
		}
		_, err = mergeValuesPebble(true /* reverse */, [][]byte{c.existing, c.update})
		if err == nil {
			t.Fatalf("pebble merge reverse: %d: expected error", i)
		}
	}
}

// TestGoMergeAppend tests the function goMerge with the default append operator
// but does not test the integration with the storage engines. For that, see the
// engine tests.
func TestGoMergeAppend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	gibber1, gibber2 := gibberishString(100), gibberishString(200)

	testCasesAppender := []struct {
		existing, update, expected []byte
	}{
		{appender(""), appender(""), appender("")},
		{nil, appender(""), appender("")},
		{nil, nil, mustMarshal(&enginepb.MVCCMetadata{RawBytes: []byte{}})},
		{appender("\n "), appender(" \t "), appender("\n  \t ")},
		{appender("ქართული"), appender("\nKhartuli"), appender("ქართული\nKhartuli")},
		{appender(gibber1), appender(gibber2), appender(gibber1 + gibber2)},
	}

	for i, c := range testCasesAppender {
		result, err := goMerge(c.existing, c.update)
		if err != nil {
			t.Errorf("goMerge error: %d: %+v", i, err)
			continue
		}
		var resultV, expectedV enginepb.MVCCMetadata
		if err := protoutil.Unmarshal(result, &resultV); err != nil {
			t.Fatal(err)
		}
		if err := protoutil.Unmarshal(c.expected, &expectedV); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(resultV, expectedV) {
			t.Errorf("goMerge error: %d: want %+v, got %+v", i, expectedV, resultV)
		}
	}
}

func mergeValuesPebble(reverse bool, srcBytes [][]byte) ([]byte, error) {
	if reverse {
		valueMerger, err := MVCCMerger.Merge(nil /* key */, srcBytes[len(srcBytes)-1])
		if err != nil {
			return nil, err
		}
		for i := len(srcBytes) - 2; i >= 0; i-- {
			err := valueMerger.MergeOlder(srcBytes[i])
			if err != nil {
				return nil, err
			}
		}
		val, _, err := valueMerger.Finish()
		return val, err
	}
	valueMerger, err := MVCCMerger.Merge(nil /* key */, srcBytes[0])
	if err != nil {
		return nil, err
	}
	for _, bytes := range srcBytes[1:] {
		err := valueMerger.MergeNewer(bytes)
		if err != nil {
			return nil, err
		}
	}
	val, _, err := valueMerger.Finish()
	return val, err
}

func mergeInternalTimeSeriesDataPebble(
	reverse bool, sources ...roachpb.InternalTimeSeriesData,
) (roachpb.InternalTimeSeriesData, error) {
	srcBytes, err := serializeMergeInputs(sources...)
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	mergedBytes, err := mergeValuesPebble(reverse, srcBytes)
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	return deserializeMergeOutput(mergedBytes)
}

// TestGoMergeTimeSeries tests the function goMerge with the timeseries operator
// but does not test the integration with the storage engines. For that, see
// the engine tests.
func TestGoMergeTimeSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Each time series test case is a list of byte slice. The last byte slice
	// is the expected result; all preceding slices will be merged together
	// to generate the actual result.
	testCasesTimeSeries := [][][]byte{
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 10, 10, 10},
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 100, 100, 100},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 100, 100, 100},
				{2, 1, 5, 5, 5},
				{3, 1, 5, 5, 5},
			}...),
		},
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{2, 1, 5, 5, 5},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{1, 1, 5, 5, 5},
				{2, 1, 5, 5, 5},
			}...),
		},
		// Column Tests.
		// Basic initial merge.
		{
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 1},
				{offset: 6, last: 1},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 1},
				{offset: 6, last: 1},
			}...),
		},
		// Ensure initial merge sorts and deduplicates.
		{
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 6, last: 1},
				{offset: 2, last: 1},
				{offset: 2, last: 4},
				{offset: 3, last: 8},
				{offset: 2, last: 3},
				{offset: 8, last: 8},
				{offset: 6, last: 1},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 3, last: 8},
				{offset: 6, last: 1},
				{offset: 8, last: 8},
			}...),
		},
		// Specially constructed sort case: ensure permutation sorting system is
		// working.
		{
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 3, last: 3},
				{offset: 1, last: 1},
				{offset: 4, last: 4},
				{offset: 2, last: 2},
				{offset: 5, last: 5},
				{offset: 6, last: 6},
				{offset: 7, last: 7},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 1, last: 1},
				{offset: 2, last: 2},
				{offset: 3, last: 3},
				{offset: 4, last: 4},
				{offset: 5, last: 5},
				{offset: 6, last: 6},
				{offset: 7, last: 7},
			}...),
		},
		// Simple merge.
		{
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 4, last: 3},
				{offset: 5, last: 3},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 6, last: 1},
				{offset: 8, last: 1},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 4, last: 3},
				{offset: 5, last: 3},
				{offset: 6, last: 1},
				{offset: 8, last: 1},
			}...),
		},
		// Merge with sorting and deduplication.
		{
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 4, last: 3},
				{offset: 5, last: 3},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 8, last: 1},
				{offset: 4, last: 2},
				{offset: 6, last: 1},
				{offset: 5, last: 4},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 4, last: 2},
				{offset: 5, last: 4},
				{offset: 6, last: 1},
				{offset: 8, last: 1},
			}...),
		},
		// Rollup Merge: all columns present in output.
		{
			timeSeriesColumn(testtime, 1000, true, []tsColumnSample{
				{2, 3, 2, 4, 9, 6, 3, 2},
				{4, 3, 2, 4, 9, 6, 3, 2},
				{5, 3, 2, 4, 9, 6, 3, 2},
			}...),
			timeSeriesColumn(testtime, 1000, true, []tsColumnSample{
				{6, 1, 2, 4, 9, 6, 3, 2},
				{8, 1, 2, 4, 9, 6, 3, 2},
			}...),
			timeSeriesColumn(testtime, 1000, true, []tsColumnSample{
				{2, 3, 2, 4, 9, 6, 3, 2},
				{4, 3, 2, 4, 9, 6, 3, 2},
				{5, 3, 2, 4, 9, 6, 3, 2},
				{6, 1, 2, 4, 9, 6, 3, 2},
				{8, 1, 2, 4, 9, 6, 3, 2},
			}...),
		},
		// Rollup Merge sort + deduplicate: all columns present in output.
		{
			timeSeriesColumn(testtime, 1000, true, []tsColumnSample{
				{2, 3, 2, 4, 9, 6, 3, 2},
				{4, 3, 2, 4, 9, 6, 3, 2},
				{5, 3, 2, 4, 9, 6, 3, 2},
			}...),
			timeSeriesColumn(testtime, 1000, true, []tsColumnSample{
				{8, 1, 2, 4, 9, 6, 3, 2},
				{4, 5, 4, 6, 10, 7, 4, 3},
				{6, 1, 2, 4, 9, 6, 3, 2},
				{3, 1, 1, 1, 1, 1, 1, 1},
			}...),
			timeSeriesColumn(testtime, 1000, true, []tsColumnSample{
				{2, 3, 2, 4, 9, 6, 3, 2},
				{3, 1, 1, 1, 1, 1, 1, 1},
				{4, 5, 4, 6, 10, 7, 4, 3},
				{5, 3, 2, 4, 9, 6, 3, 2},
				{6, 1, 2, 4, 9, 6, 3, 2},
				{8, 1, 2, 4, 9, 6, 3, 2},
			}...),
		},
		// Conversion from row format to columnar format - this occurs when one
		// of the sides is row-formatted, while the other is column formatted. This
		// process ignores the existing min/max/count columns, as rollups were never
		// fully implemented over the row-based layout.
		{
			timeSeriesRow(testtime, 1000, []tsSample{
				{2, 1, 3, 5, 5},
				{4, 1, 3, 5, 5},
				{5, 1, 3, 5, 5},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 8, last: 1},
				{offset: 4, last: 2},
				{offset: 6, last: 1},
				{offset: 5, last: 4},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 4, last: 2},
				{offset: 5, last: 4},
				{offset: 6, last: 1},
				{offset: 8, last: 1},
			}...),
		},
		{
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 4, last: 3},
				{offset: 5, last: 3},
			}...),
			timeSeriesRow(testtime, 1000, []tsSample{
				{8, 1, 1, 1, 1},
				{4, 1, 2, 1, 1},
				{6, 1, 1, 1, 1},
				{5, 1, 4, 1, 1},
			}...),
			timeSeriesColumn(testtime, 1000, false, []tsColumnSample{
				{offset: 2, last: 3},
				{offset: 4, last: 2},
				{offset: 5, last: 4},
				{offset: 6, last: 1},
				{offset: 8, last: 1},
			}...),
		},
	}

	for _, c := range testCasesTimeSeries {
		expectedTS := unmarshalTimeSeries(t, c[len(c)-1])
		var operands []roachpb.InternalTimeSeriesData
		for _, bytes := range c[:len(c)-1] {
			operands = append(operands, unmarshalTimeSeries(t, bytes))
		}

		t.Run("", func(t *testing.T) {
			// Test merging the operands under several conditions:
			// + With and without using the partial merge operator (which combines
			// operands quickly with the expectation that they will be properly merged
			// eventually).
			// + With and without merging into an initial nil value. Note that some
			// tests have only one operand and are only run when merging into nil.
			for _, partialMerge := range []bool{true, false} {
				for _, mergeIntoNil := range []bool{true, false} {
					if !mergeIntoNil && len(operands) == 1 {
						continue
					}

					resultTS, err := MergeInternalTimeSeriesData(mergeIntoNil, partialMerge, operands...)
					if err != nil {
						t.Errorf(
							"MergeInternalTimeSeriesData mergeIntoNil=%t partial=%t error: %s",
							mergeIntoNil,
							partialMerge,
							err.Error(),
						)
					}
					if a, e := resultTS, expectedTS; !reflect.DeepEqual(a, e) {
						t.Errorf(
							"MergeInternalTimeSeriesData  mergeIntoNil=%t partial=%t returned wrong result got %v, wanted %v",
							mergeIntoNil,
							partialMerge,
							a,
							e,
						)
					}
				}
			}
			resultTS, err := mergeInternalTimeSeriesDataPebble(false /* reverse */, operands...)
			if err != nil {
				t.Errorf("pebble merge forward error: %s", err.Error())
			}
			if a, e := resultTS, expectedTS; !reflect.DeepEqual(a, e) {
				t.Errorf("pebble merge forward returned wrong result got %v, wanted %v", a, e)
			}
			resultTS, err = mergeInternalTimeSeriesDataPebble(true /* reverse */, operands...)
			if err != nil {
				t.Errorf("pebble merge reverse error: %s", err.Error())
			}
			if a, e := resultTS, expectedTS; !reflect.DeepEqual(a, e) {
				t.Errorf("pebble merge reverse returned wrong result got %v, wanted %v", a, e)
			}
		})
	}
}

// unmarshalTimeSeries unmarshals the time series value stored in the given byte
// array. It is assumed that the time series value was originally marshaled as
// a MVCCMetadata with an inline value.
func unmarshalTimeSeries(t testing.TB, b []byte) roachpb.InternalTimeSeriesData {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(b, &meta); err != nil {
		t.Fatalf("error unmarshalling time series in text: %s", err.Error())
	}
	valueTS, err := MakeValue(meta).GetTimeseries()
	if err != nil {
		t.Fatalf("error unmarshalling time series in text: %s", err.Error())
	}
	return valueTS
}
