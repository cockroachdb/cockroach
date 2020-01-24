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
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Scan, DefaultDeclareKeys, Scan)
}

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, reader engine.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)

	var err error
	var intents []roachpb.Intent
	var resumeSpan *roachpb.Span

	switch args.ScanFormat {
	case roachpb.BATCH_RESPONSE:
		var kvData [][]byte
		var numKvs int64
		opts := engine.MVCCScanOptions{
			Inconsistent: h.ReadConsistency != roachpb.CONSISTENT,
			Txn:          h.Txn,
			MaxBytes:     h.MaxSpanResponseBytes,
		}
		kvData, numKvs, resumeSpan, intents, err = engine.MVCCScanToBytes(
			ctx, reader, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp,
			opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.NumKeys = numKvs
		reply.BatchResponses = kvData
		var bytes int
		for _, repr := range kvData {
			bytes += len(repr)
		}
		if h.MaxSpanResponseBytes > 0 {
			log.Infof(ctx, "size-limited: returning %d KVs totaling %s (limit %s)", reply.NumKeys, humanizeutil.IBytes(int64(bytes)), humanizeutil.IBytes(h.MaxSpanResponseBytes))
		}
	case roachpb.KEY_VALUES:
		var rows []roachpb.KeyValue
		rows, resumeSpan, intents, err = engine.MVCCScan(
			ctx, reader, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, engine.MVCCScanOptions{
				Inconsistent: h.ReadConsistency != roachpb.CONSISTENT,
				Txn:          h.Txn,
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
		reply.IntentRows, err = CollectIntentRows(ctx, reader, cArgs, intents)
	}
	return result.FromEncounteredIntents(intents), err
}
