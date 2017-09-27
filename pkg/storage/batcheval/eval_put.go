// Copyright 2014 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// EvalPut sets the value for a specified key.
func EvalPut(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (Result, error) {
	args := cArgs.Args.(*roachpb.PutRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	var ts hlc.Timestamp
	if !args.Inline {
		ts = h.Timestamp
	}
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
		return Result{}, engine.MVCCBlindPut(ctx, batch, ms, args.Key, ts, args.Value, h.Txn)
	}
	return Result{}, engine.MVCCPut(ctx, batch, ms, args.Key, ts, args.Value, h.Txn)
}
