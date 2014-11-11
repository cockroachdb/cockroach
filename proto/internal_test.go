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

	gogoproto "code.google.com/p/gogoprotobuf/proto"
)

func TestTimeSeriesToValue(t *testing.T) {
	tsOriginal := &TimeSeriesData{
		StartTimestamp:    1415398729,
		DurationInSeconds: 3600,
		SamplePrecision:   SECONDS,
		Data: []*TimeSeriesDataPoint{
			{
				Offset:   100,
				ValueInt: gogoproto.Int64(1),
			},
			{
				Offset:   200,
				ValueInt: gogoproto.Int64(2),
			},
			{
				Offset:   300,
				ValueInt: gogoproto.Int64(3),
			},
		},
	}

	// Wrap the TSD into a Value
	valueOriginal, err := tsOriginal.ToValue()
	if err != nil {
		t.Fatalf("error marshaling TimeSeriesData: %s", err.Error())
	}
	if a, e := valueOriginal.GetTag(), _CR_TS.String(); a != e {
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
	tsNew, err := TimeSeriesFromValue(valueOriginal)
	if err != nil {
		t.Errorf("error extracting Time Series: %s")
	}
	if !gogoproto.Equal(tsOriginal, tsNew) {
		t.Errorf("extracted time series not equivalent to original; %v != %v", tsNew, tsOriginal)
	}

	// Make sure ExtractTimeSeries doesn't work on non-TimeSeries values
	valueNotTs := &Value{
		Bytes: []byte("testvalue"),
	}
	if _, err := TimeSeriesFromValue(valueNotTs); err == nil {
		t.Errorf("did not receive expected error when extracting TimeSeries from regular Byte value.")
	}
}
