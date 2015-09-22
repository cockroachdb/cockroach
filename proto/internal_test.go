// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package proto

import (
	"bytes"
	"testing"

	gogoproto "github.com/gogo/protobuf/proto"
)

func TestTimeSeriesToValue(t *testing.T) {
	tsOriginal := &InternalTimeSeriesData{
		StartTimestampNanos: 1415398729000000000,
		SampleDurationNanos: 1000000000,
		Samples: []*InternalTimeSeriesSample{
			{
				Offset: 1,
				Count:  1,
				Sum:    64,
			},
			{
				Offset: 2,
				Count:  1,
				Sum:    2,
			},
			{
				Offset: 3,
				Count:  1,
				Sum:    3,
			},
		},
	}

	// Wrap the TSD into a Value
	valueOriginal, err := tsOriginal.ToValue()
	if err != nil {
		t.Fatalf("error marshaling InternalTimeSeriesData: %s", err.Error())
	}
	if a, e := valueOriginal.GetTag(), ValueType_TIMESERIES; a != e {
		t.Errorf("Value did not have expected tag value of %s, had %s", e, a)
	}

	// Ensure the Value's 'bytes' field contains the marshalled TSD
	tsEncoded, err := gogoproto.Marshal(tsOriginal)
	if err != nil {
		t.Fatalf("error marshaling TimeSeriesData: %s", err.Error())
	}
	if a, e := valueOriginal.Bytes, tsEncoded; !bytes.Equal(a, e) {
		t.Errorf("bytes field was not properly encoded: expected %v, got %v", e, a)
	}

	// Extract the TSD from the Value
	tsNew, err := InternalTimeSeriesDataFromValue(valueOriginal)
	if err != nil {
		t.Errorf("error extracting Time Series: %s", err.Error())
	}
	if !gogoproto.Equal(tsOriginal, tsNew) {
		t.Errorf("extracted time series not equivalent to original; %v != %v", tsNew, tsOriginal)
	}

	// Make sure ExtractTimeSeries doesn't work on non-TimeSeries values
	valueNotTs := &Value{
		Bytes: []byte("testvalue"),
	}
	if _, err := InternalTimeSeriesDataFromValue(valueNotTs); err == nil {
		t.Errorf("did not receive expected error when extracting TimeSeries from regular Byte value.")
	}
}
