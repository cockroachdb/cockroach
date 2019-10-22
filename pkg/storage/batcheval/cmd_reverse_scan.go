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
		var kvData [][]byte
		var numKvs int64
		kvData, numKvs, resumeSpan, intents, err = engine.MVCCScanToBytes(
			ctx, batch, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp,
			engine.MVCCScanOptions{
				Inconsistent:   h.ReadConsistency != roachpb.CONSISTENT,
				IgnoreSequence: shouldIgnoreSequenceNums(),
				Txn:            h.Txn,
				Reverse:        true,
			})
		if err != nil {
			return result.Result{}, err
		}
		reply.NumKeys = numKvs
		reply.BatchResponses = kvData
	case roachpb.KEY_VALUES:
		var rows []roachpb.KeyValue
		rows, resumeSpan, intents, err = engine.MVCCScan(
			ctx, batch, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, engine.MVCCScanOptions{
				Inconsistent:   h.ReadConsistency != roachpb.CONSISTENT,
				IgnoreSequence: shouldIgnoreSequenceNums(),
				Txn:            h.Txn,
				Reverse:        true,
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
