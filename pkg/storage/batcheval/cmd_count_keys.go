// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

func init() {
	RegisterCommand(roachpb.CountKeys, DefaultDeclareKeys, CountKeys)
}

func CountKeys(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.CountKeysRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.CountKeysResponse)

	desc := cArgs.EvalCtx.Desc()
	if args.Key.Equal(desc.StartKey.AsRawKey()) && args.Key.Equal(desc.EndKey.AsRawKey()) {
		stats := cArgs.EvalCtx.GetMVCCStats()
		if !stats.ContainsEstimates && stats.IntentCount == 0 {
			reply.Count = stats.LiveCount
			return result.Result{}, nil
		}
	}

	var rows []roachpb.KeyValue
	rows, _, intents, err := engine.MVCCScan(ctx, batch, args.Key, args.EndKey,
		cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	if err != nil {
		return result.Result{}, err
	}
	reply.Count = int64(len(rows))
	return result.FromIntents(intents, args), err
}
