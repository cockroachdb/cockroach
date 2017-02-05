// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package storageccl

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func init() {
	storage.SetWriteBatchCmd(storage.Command{
		DeclareKeys: storage.DefaultDeclareKeys,
		Eval:        evalWriteBatch,
	})
}

// evalWriteBatch applies the operations encoded in a BatchRepr.
func evalWriteBatch(
	ctx context.Context, batch engine.ReadWriter, cArgs storage.CommandArgs, resp roachpb.Response,
) (storage.EvalResult, error) {
	if !storage.ProposerEvaluatedKVEnabled() {
		// To reduce the footprint of things that have ever been downstream of
		// raft, forbid this command from running without proposer evaluated kv.
		panic("command WriteBatch is not allowed without proposer evaluated KV")
	}

	args := cArgs.Args.(*roachpb.WriteBatchRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("WriteBatch %s-%s", args.Key, args.EndKey))
	defer tracing.FinishSpan(span)

	// We can't use the normal RangeKeyMismatchError mechanism for dealing with
	// splits because args.Data should stay an opaque blob to DistSender.
	if args.DataSpan.Key.Compare(args.Key) < 0 || args.DataSpan.EndKey.Compare(args.EndKey) > 0 {
		// TODO(dan): Add a new field in roachpb.Error, so the client can catch
		// this and retry.
		return storage.EvalResult{}, errors.New("data spans multiple ranges")
	}

	mvccStartKey := engine.MVCCKey{Key: args.Key}
	mvccEndKey := engine.MVCCKey{Key: args.EndKey}

	// Verify that the keys in the batch are within the range specified by the
	// request header.
	msBatch, err := engineccl.VerifyBatchRepr(args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return storage.EvalResult{}, err
	}
	ms.Add(msBatch)

	// Verify that the key range specified in the request span is empty.
	iter := batch.NewIterator(false)
	defer iter.Close()
	iter.Seek(mvccStartKey)
	if iter.Valid() && iter.Key().Less(mvccEndKey) {
		return storage.EvalResult{}, errors.New("WriteBatch can only be called on empty ranges")
	}

	if err := batch.ApplyBatchRepr(args.Data); err != nil {
		return storage.EvalResult{}, err
	}
	return storage.EvalResult{}, nil
}
