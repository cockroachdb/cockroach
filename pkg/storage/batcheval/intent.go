// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	ctx context.Context, batch engine.Reader, cArgs CommandArgs, intents []roachpb.Intent,
) ([]roachpb.KeyValue, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	res := make([]roachpb.KeyValue, 0, len(intents))
	for _, intent := range intents {
		val, _, err := engine.MVCCGetAsTxn(
			ctx, batch, intent.Key, intent.Txn.WriteTimestamp, intent.Txn,
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
