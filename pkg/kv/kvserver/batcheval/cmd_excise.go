// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	// Taking out latches/locks across the entire SST span is very coarse, and we
	// could instead iterate over the SST and take out point latches/locks, but
	// the cost is likely not worth it since Excise is often used with
	// unpopulated spans.
	RegisterReadWriteCommand(kvpb.Excise, DefaultDeclareKeys, EvalExcise)
}

// EvalExcise evaluates a Excise command.
func EvalExcise(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ExciseRequest)
	start, end := storage.MVCCKey{Key: args.Key}, storage.MVCCKey{Key: args.EndKey}

	// Since we can't know the exact range stats, mark it as an estimate.
	s := *cArgs.Stats
	s.ContainsEstimates++
	cArgs.Stats.Add(s)

	ltStart, _ := keys.LockTableSingleKey(args.Span().Key, nil)
	ltEnd, _ := keys.LockTableSingleKey(args.Span().EndKey, nil)

	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			Excise: &kvserverpb.ReplicatedEvalResult_Excise{
				Span:          roachpb.Span{Key: start.Key, EndKey: end.Key},
				LockTableSpan: roachpb.Span{Key: ltStart, EndKey: ltEnd},
			},
		},
	}, nil
}
