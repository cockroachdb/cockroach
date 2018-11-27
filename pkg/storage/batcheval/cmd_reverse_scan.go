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

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

func init() {
	RegisterCommand(roachpb.ReverseScan, DefaultDeclareKeys, ReverseScan)
}

// ReverseScan scans the key range specified by start key through
// end key in descending order up to some maximum number of results.
// maxKeys stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func ReverseScan(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ReverseScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ReverseScanResponse)

	var err error
	var intents []roachpb.Intent
	var resumeSpan *roachpb.Span

	switch args.ScanFormat {
	case roachpb.BATCH_RESPONSE:
		var kvData []byte
		var numKvs int64
		kvData, numKvs, resumeSpan, intents, err = engine.MVCCScanToBytes(
			ctx, batch, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp,
			engine.MVCCScanOptions{
				Inconsistent: h.ReadConsistency != roachpb.CONSISTENT,
				Txn:          h.Txn,
				Reverse:      true,
			})
		if err != nil {
			return result.Result{}, err
		}
		reply.NumKeys = numKvs
		reply.BatchResponse = kvData
	case roachpb.KEY_VALUES:
		var rows []roachpb.KeyValue
		rows, resumeSpan, intents, err = engine.MVCCScan(
			ctx, batch, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, engine.MVCCScanOptions{
				Inconsistent: h.ReadConsistency != roachpb.CONSISTENT,
				Txn:          h.Txn,
				Reverse:      true,
			})
		if err != nil {
			return result.Result{}, err
		}
		reply.NumKeys = int64(len(rows))
		reply.Rows = rows
	default:
		panic(fmt.Sprintf("Unknown scanFormat %d", args.ScanFormat))
	}

	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		reply.IntentRows, err = CollectIntentRows(ctx, batch, cArgs, intents)
	}
	return result.FromIntents(intents, args), err
}
