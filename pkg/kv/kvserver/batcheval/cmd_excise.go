// Copyright 2025 The Cockroach Authors.
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
	RegisterReadWriteCommand(kvpb.Excise, DefaultDeclareKeys, EvalExcise)
}

// EvalExcise evaluates a Excise command.
func EvalExcise(
	_ context.Context, _ storage.ReadWriter, cArgs CommandArgs, _ kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ExciseRequest)
	start, end := storage.MVCCKey{Key: args.Key}, storage.MVCCKey{Key: args.EndKey}

	// Since we can't know the exact range stats, mark it as an estimate.
	cArgs.Stats.ContainsEstimates++

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
