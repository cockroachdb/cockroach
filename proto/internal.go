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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

package proto

import (
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// ToValue generates a Value message which contains an encoded copy of this
// TimeSeriesData in its "bytes" field. The returned Value will also have its
// "tag" string set to the TIME_SERIES constant.
func (ts *InternalTimeSeriesData) ToValue() (*Value, error) {
	b, err := gogoproto.Marshal(ts)
	if err != nil {
		return nil, err
	}
	return &Value{
		Bytes: b,
		Tag:   gogoproto.String(_CR_TS.String()),
	}, nil
}

// InternalTimeSeriesDataFromValue attempts to extract an InternalTimeSeriesData
// message from the "bytes" field of the given value.
func InternalTimeSeriesDataFromValue(value *Value) (*InternalTimeSeriesData, error) {
	if value.GetTag() != _CR_TS.String() {
		return nil, util.Errorf("value is not tagged as containing TimeSeriesData: %v", value)
	}
	var ts InternalTimeSeriesData
	err := gogoproto.Unmarshal(value.Bytes, &ts)
	if err != nil {
		return nil, util.Errorf("TimeSeriesData could not be unmarshalled from value: %v %s", value, err)
	}
	return &ts, nil
}

// Add adds a request to the internal batch request.
func (br *InternalBatchRequest) Add(args Request) {
	union := InternalRequestUnion{}
	if !union.SetValue(args) {
		// TODO(tschottdorf) evaluate whether this should return an error.
		log.Fatalf("unable to add %T to internal batch request", args)
	}
	if br.Key == nil {
		br.Key = args.Header().Key
		br.EndKey = args.Header().EndKey
	}
	br.Requests = append(br.Requests, union)
}

// Add adds a response to the internal batch response.
func (br *InternalBatchResponse) Add(reply Response) {
	union := InternalResponseUnion{}
	if !union.SetValue(reply) {
		// TODO(tschottdorf) evaluate whether this should return an error.
		log.Fatalf("unable to add %T to internal batch response", reply)
	}
	br.Responses = append(br.Responses, union)
}

// Average returns the average value for this sample.
func (samp *InternalTimeSeriesSample) Average() float64 {
	if samp.Count == 0 {
		return 0
	}
	return samp.Sum / float64(samp.Count)
}

// Maximum returns the maximum value encountered by this sample.
func (samp *InternalTimeSeriesSample) Maximum() float64 {
	if samp.Count < 2 {
		return samp.Sum
	}
	return samp.GetMax()
}

// Minimum returns the minimum value encountered by this sample.
func (samp *InternalTimeSeriesSample) Minimum() float64 {
	if samp.Count < 2 {
		return samp.Sum
	}
	return samp.GetMin()
}
