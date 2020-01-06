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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Scan, scanDeclareKeys, Scan)
}

func scanDeclareKeys(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	scan := req.(*roachpb.ScanRequest)
	var access spanset.SpanAccess
	if scan.SelectForUpdate {
		access = spanset.SpanReadWrite
	} else {
		access = spanset.SpanReadOnly
	}

	if keys.IsLocal(scan.Key) {
		spans.AddNonMVCC(access, scan.Span())
	} else {
		spans.AddMVCC(access, scan.Span(), header.Timestamp)
	}
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

	var writeTooOldOnWriteInFuture bool
	if args.SelectForUpdate {
		writeTooOldOnWriteInFuture = true
	} else {
		writeTooOldOnWriteInFuture = false
	}

	var err error
	var intents []roachpb.Intent
	var resumeSpan *roachpb.Span
	var res result.Result

	switch args.ScanFormat {
	case roachpb.BATCH_RESPONSE:
		var kvData [][]byte
		var numKvs int64
		kvData, numKvs, resumeSpan, intents, err = engine.MVCCScanToBytes(
			ctx, reader, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp,
			engine.MVCCScanOptions{
				Inconsistent:               h.ReadConsistency != roachpb.CONSISTENT,
				Txn:                        h.Txn,
				WriteTooOldOnWriteInFuture: writeTooOldOnWriteInFuture,
			})
		if err != nil {
			return result.Result{}, err
		}
		reply.NumKeys = numKvs
		reply.BatchResponses = kvData
		if args.SelectForUpdate && h.Txn != nil {
			res.Local.UpdatedIntents = make([]roachpb.Intent, numKvs)
			var i int
			if err := engine.MVCCScanDecodeKeyValues(kvData, func(key engine.MVCCKey, _ []byte) error {
				res.Local.UpdatedIntents[i] = roachpb.Intent{
					Span: roachpb.Span{Key: key.Key}, Txn: h.Txn.TxnMeta, Status: roachpb.PENDING,
				}
				return nil
			}); err != nil {
				return result.Result{}, err
			}
		}
	case roachpb.KEY_VALUES:
		var rows []roachpb.KeyValue
		rows, resumeSpan, intents, err = engine.MVCCScan(
			ctx, reader, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, engine.MVCCScanOptions{
				Inconsistent:               h.ReadConsistency != roachpb.CONSISTENT,
				Txn:                        h.Txn,
				WriteTooOldOnWriteInFuture: writeTooOldOnWriteInFuture,
			})
		if err != nil {
			return result.Result{}, err
		}
		reply.NumKeys = int64(len(rows))
		reply.Rows = rows
		if args.SelectForUpdate && h.Txn != nil {
			res.Local.UpdatedIntents = make([]roachpb.Intent, len(rows))
			for i, row := range rows {
				res.Local.UpdatedIntents[i] = roachpb.Intent{
					Span: roachpb.Span{Key: row.Key}, Txn: h.Txn.TxnMeta, Status: roachpb.PENDING,
				}
			}
		}
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
	res.Local.EncounteredIntents = intents
	return res, err
}
