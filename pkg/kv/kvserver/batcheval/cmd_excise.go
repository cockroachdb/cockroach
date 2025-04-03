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
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.Excise, DefaultDeclareKeys, EvalExcise)
}

// EvalExcise evaluates a Excise command.
func EvalExcise(
	_ context.Context, _ storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ExciseRequest)
	start, end := args.Key, args.EndKey

	// Verify that the start and end keys are for global key space. Excising
	// non-global keys is not allowed as it would leave the replica in a bad
	// state. For example, we don't want to allow excising a range descriptor.
	rStart, err := keys.Addr(start)
	if err != nil {
		return result.Result{}, err
	}

	rEnd, err := keys.Addr(end)
	if err != nil {
		return result.Result{}, err
	}

	if !start.Equal(rStart.AsRawKey()) {
		return result.Result{},
			errors.Errorf("excise can only be run against global keys, but found start key: %s", start)
	}

	if !end.Equal(rEnd.AsRawKey()) {
		return result.Result{},
			errors.Errorf("excise can only be run against global keys, but found end key: %s", end)
	}

	// Since we can't know the exact range stats, mark it as an estimate.
	cArgs.Stats.ContainsEstimates++

	ltStart, _ := keys.LockTableSingleKey(args.Span().Key, nil)
	ltEnd, _ := keys.LockTableSingleKey(args.Span().EndKey, nil)

	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			Excise: &kvserverpb.ReplicatedEvalResult_Excise{
				Span:          roachpb.Span{Key: start, EndKey: end},
				LockTableSpan: roachpb.Span{Key: ltStart, EndKey: ltEnd},
			},
		},
	}, nil
}
