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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(roachpb.DeleteRange, declareKeysDeleteRange, DeleteRange)
}

func declareKeysDeleteRange(
	_ *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	args := req.(*roachpb.DeleteRangeRequest)
	access := spanset.SpanReadWrite

	if args.Inline || keys.IsLocal(req.Header().Span().Key) {
		spans.AddNonMVCC(access, req.Header().Span())
	} else {
		spans.AddMVCC(access, req.Header().Span(), header.Timestamp)
	}
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func DeleteRange(
	ctx context.Context, readWriter engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.DeleteRangeRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.DeleteRangeResponse)

	var timestamp hlc.Timestamp
	if !args.Inline {
		timestamp = h.Timestamp
	}
	deleted, resumeSpan, num, err := engine.MVCCDeleteRange(
		ctx, readWriter, cArgs.Stats, args.Key, args.EndKey, cArgs.MaxKeys, timestamp, h.Txn, args.ReturnKeys,
	)
	if err == nil {
		reply.Keys = deleted
	}
	reply.NumKeys = num
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}
	return result.Result{}, err
}
