// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

func init() {
	RegisterCommand(roachpb.InitPut, DefaultDeclareKeys, InitPut)
}

// InitPut sets the value for a specified key only if it doesn't exist. It
// returns a ConditionFailedError if the key exists with an existing value that
// is different from the value provided. If FailOnTombstone is set to true,
// tombstones count as mismatched values and will cause a ConditionFailedError.
func InitPut(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.InitPutRequest)
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
	if args.Blind {
		return result.Result{}, engine.MVCCBlindInitPut(ctx, batch, cArgs.Stats, args.Key, h.Timestamp, args.Value, args.FailOnTombstones, h.Txn)
	}
	return result.Result{}, engine.MVCCInitPut(ctx, batch, cArgs.Stats, args.Key, h.Timestamp, args.Value, args.FailOnTombstones, h.Txn)
}
