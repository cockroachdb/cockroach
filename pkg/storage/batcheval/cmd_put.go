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
	RegisterReadWriteCommand(roachpb.Put, declareKeysPut, Put)
}

func declareKeysPut(
	_ *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	args := req.(*roachpb.PutRequest)
	access := spanset.SpanReadWrite

	if args.Inline || keys.IsLocal(req.Header().Span().Key) {
		spans.AddNonMVCC(access, req.Header().Span())
	} else {
		spans.AddMVCC(access, req.Header().Span(), header.Timestamp)
	}
}

// Put sets the value for a specified key.
func Put(
	ctx context.Context, readWriter engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.PutRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}
	if h.DistinctSpans {
		if b, ok := readWriter.(engine.Batch); ok {
			// Use the distinct batch for both blind and normal ops so that we don't
			// accidentally flush mutations to make them visible to the distinct
			// batch.
			readWriter = b.Distinct()
			defer readWriter.Close()
		}
	}
	if args.Blind {
		return result.Result{}, engine.MVCCBlindPut(ctx, readWriter, ms, args.Key, ts, args.Value, h.Txn)
	}
	return result.Result{}, engine.MVCCPut(ctx, readWriter, ms, args.Key, ts, args.Value, h.Txn)
}
