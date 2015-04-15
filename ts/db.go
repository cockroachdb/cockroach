// Copyright 2015 The Cockroach Authors.
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

package ts

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
)

// DB provides Cockroach's Time Series API.
type DB struct {
	kv *client.KV
}

// NewDB creates a new DB instance.
func NewDB(kv *client.KV) *DB {
	return &DB{
		kv: kv,
	}
}

// storeData attempts to store the supplied time series data on the server.
// Data will be sampled at the supplied resolution.
func (db *DB) storeData(r Resolution, data proto.TimeSeriesData) error {
	internalData, err := data.ToInternal(r.KeyDuration(), r.SampleDuration())
	if err != nil {
		return err
	}

	for _, idata := range internalData {
		key := MakeDataKey(data.Name, data.Source, r, idata.StartTimestampNanos)
		value, err := idata.ToValue()
		if err != nil {
			return err
		}

		// TODO(mrtracy): If there are multiple values to merge, they could be
		// batched together instead of being called individually.
		if err := db.kv.Call(
			&proto.InternalMergeRequest{
				RequestHeader: proto.RequestHeader{
					Key: key,
				},
				Value: *value,
			},
			&proto.InternalMergeResponse{},
		); err != nil {
			return err
		}
	}

	return nil
}
