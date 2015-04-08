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
// Author: Matt Tracy (matt@cockroachlabs.com)

package engine

import (
	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

// MergeInternalTimeSeriesData exports the engine's C++ merge logic for
// InternalTimeSeriesData to higher level packages. This is intended primarily
// for consumption by high level testing of time series functionality.
func MergeInternalTimeSeriesData(sources ...*proto.InternalTimeSeriesData) (
	*proto.InternalTimeSeriesData, error) {
	// Wrap each proto in an inlined MVCC value, and marshal each wrapped value
	// to bytes. This is the format required by the engine.
	srcBytes := make([][]byte, 0, len(sources))
	for _, src := range sources {
		val, err := src.ToValue()
		if err != nil {
			return nil, err
		}
		bytes, err := gogoproto.Marshal(&proto.MVCCMetadata{
			Value: val,
		})
		if err != nil {
			return nil, err
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
			return nil, err
		}
	}

	// Unmarshal merged bytes and extract the time series value within.
	var mvccValue proto.MVCCMetadata
	if err := gogoproto.Unmarshal(mergedBytes, &mvccValue); err != nil {
		return nil, err
	}
	mergedTS, err := proto.InternalTimeSeriesDataFromValue(mvccValue.Value)
	if err != nil {
		return nil, err
	}
	return mergedTS, nil
}
