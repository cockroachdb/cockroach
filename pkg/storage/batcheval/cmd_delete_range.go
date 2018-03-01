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
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterCommand(roachpb.DeleteRange, DefaultDeclareKeys, DeleteRange)
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func DeleteRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.DeleteRangeRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.DeleteRangeResponse)

	var timestamp hlc.Timestamp
	if !args.Inline {
		timestamp = h.Timestamp
	}
	deleted, resumeSpan, num, err := engine.MVCCDeleteRange(
		ctx, batch, cArgs.Stats, args.Key, args.EndKey, cArgs.MaxKeys, timestamp, h.Txn, args.ReturnKeys,
	)
	if err == nil {
		reply.Keys = deleted
		// DeleteRange requires that we retry on push (for snapshot) to
		// avoid the lost delete range anomaly.
		if h.Txn != nil && h.Txn.Isolation == enginepb.SNAPSHOT {
			clonedTxn := h.Txn.Clone()
			clonedTxn.RetryOnPush = true
			reply.Txn = &clonedTxn
		}
	}
	reply.NumKeys = num
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}
	return result.Result{}, err
}
