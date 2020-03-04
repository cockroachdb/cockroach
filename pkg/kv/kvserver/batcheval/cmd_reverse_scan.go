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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadOnlyCommand(roachpb.ReverseScan, DefaultDeclareIsolatedKeys, ReverseScan)
}

// ReverseScan scans the key range specified by start key through
// end key in descending order up to some maximum number of results.
// maxKeys stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func ReverseScan(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ReverseScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ReverseScanResponse)

	var res storage.MVCCScanResult
	var err error

	opts := storage.MVCCScanOptions{
		Inconsistent: h.ReadConsistency != roachpb.CONSISTENT,
		Txn:          h.Txn,
		MaxKeys:      h.MaxSpanRequestKeys,
		TargetBytes:  h.TargetBytes,
		Reverse:      true,
	}

	switch args.ScanFormat {
	case roachpb.BATCH_RESPONSE:
		res, err = storage.MVCCScanToBytes(
			ctx, reader, args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.BatchResponses = res.KVData
	case roachpb.KEY_VALUES:
		res, err = storage.MVCCScan(
			ctx, reader, args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.Rows = res.KVs
	default:
		panic(fmt.Sprintf("Unknown scanFormat %d", args.ScanFormat))
	}

	reply.NumKeys = res.NumKeys
	reply.NumBytes = res.NumBytes

	if res.ResumeSpan != nil {
		reply.ResumeSpan = res.ResumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		reply.IntentRows, err = CollectIntentRows(ctx, reader, cArgs, res.Intents)
	}
	return result.FromEncounteredIntents(res.Intents), err
}
