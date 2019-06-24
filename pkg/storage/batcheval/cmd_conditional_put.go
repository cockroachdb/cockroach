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
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

func init() {
	RegisterCommand(roachpb.ConditionalPut, DefaultDeclareKeys, ConditionalPut)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func ConditionalPut(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ConditionalPutRequest)
	h := cArgs.Header

	if h.DistinctSpans {
		if b, ok := batch.(engine.Batch); ok {
			// Use the distinct batch for both blind and normal ops so that we don't
			// accidentally flush mutations to make them visible to the distinct
			// batch.
			batch = b.Distinct()
			defer batch.Close()
		}
	}
	handleMissing := engine.CPutMissingBehavior(args.AllowIfDoesNotExist)
	if args.Blind {
		return result.Result{}, engine.MVCCBlindConditionalPut(ctx, batch, cArgs.Stats, args.Key, h.Timestamp, args.Value, args.ExpValue, handleMissing, h.Txn)
	}
	return result.Result{}, engine.MVCCConditionalPut(ctx, batch, cArgs.Stats, args.Key, h.Timestamp, args.Value, args.ExpValue, handleMissing, h.Txn)
}
