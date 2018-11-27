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

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

// CollectIntentRows collects the key-value pairs for each intent provided. It
// also verifies that the ReturnIntents option is allowed.
//
// TODO(nvanbenschoten): mvccGetInternal should return the intent values directly
// when ReturnIntents is true. Since this will initially only be used for
// RangeLookups and since this is how they currently collect intent values, this
// is ok for now.
func CollectIntentRows(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, intents []roachpb.Intent,
) ([]roachpb.KeyValue, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	res := make([]roachpb.KeyValue, 0, len(intents))
	for _, intent := range intents {
		val, _, err := engine.MVCCGetAsTxn(
			ctx, batch, intent.Key, intent.Txn.Timestamp, intent.Txn,
		)
		if err != nil {
			return nil, err
		}
		if val == nil {
			// Intent is a deletion.
			continue
		}
		res = append(res, roachpb.KeyValue{
			Key:   intent.Key,
			Value: *val,
		})
	}
	return res, nil
}
