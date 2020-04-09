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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Get, DefaultDeclareIsolatedKeys, Get)
}

// Get returns the value for a specified key.
func Get(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.GetRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.GetResponse)

	val, intent, err := storage.MVCCGet(ctx, reader, args.Key, h.Timestamp, storage.MVCCGetOptions{
		Inconsistent: h.ReadConsistency != roachpb.CONSISTENT,
		Txn:          h.Txn,
	})
	if err != nil {
		return result.Result{}, err
	}
	var intents []roachpb.Intent
	if intent != nil {
		intents = append(intents, *intent)
	}

	reply.Value = val
	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		var intentVals []roachpb.KeyValue
		// NOTE: MVCCGet uses a Prefix iterator, so we want to use one in
		// CollectIntentRows as well so that we're guaranteed to use the same
		// cached iterator and observe a consistent snapshot of the engine.
		const usePrefixIter = true
		intentVals, err = CollectIntentRows(ctx, reader, usePrefixIter, intents)
		if err == nil {
			switch len(intentVals) {
			case 0:
			case 1:
				reply.IntentValue = &intentVals[0].Value
			default:
				log.Fatalf(ctx, "more than 1 intent on single key: %v", intentVals)
			}
		}
	}
	return result.FromEncounteredIntents(intents), err
}
