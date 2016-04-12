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
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package engine

import (
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

// MergeInternalTimeSeriesData exports the engine's C++ merge logic for
// InternalTimeSeriesData to higher level packages. This is intended primarily
// for consumption by high level testing of time series functionality.
func MergeInternalTimeSeriesData(
	sources ...roachpb.InternalTimeSeriesData,
) (roachpb.InternalTimeSeriesData, error) {
	// Wrap each proto in an inlined MVCC value, and marshal each wrapped value
	// to bytes. This is the format required by the engine.
	srcBytes := make([][]byte, 0, len(sources))
	for _, src := range sources {
		var val roachpb.Value
		if err := val.SetProto(&src); err != nil {
			return roachpb.InternalTimeSeriesData{}, err
		}
		bytes, err := protoutil.Marshal(&MVCCMetadata{
			RawBytes: val.RawBytes,
		})
		if err != nil {
			return roachpb.InternalTimeSeriesData{}, err
		}
		srcBytes = append(srcBytes, bytes)
	}

	// Merge every element into a nil byte slice, one at a time.
	var (
		mergedBytes []byte
		err         error
	)
	for _, bytes := range srcBytes {
		mergedBytes, err = goMerge(mergedBytes, bytes)
		if err != nil {
			return roachpb.InternalTimeSeriesData{}, err
		}
	}

	// Unmarshal merged bytes and extract the time series value within.
	var meta MVCCMetadata
	if err := proto.Unmarshal(mergedBytes, &meta); err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	mergedTS, err := meta.Value().GetTimeseries()
	if err != nil {
		return roachpb.InternalTimeSeriesData{}, err
	}
	return mergedTS, nil
}
