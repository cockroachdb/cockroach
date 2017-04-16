// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func init() {
	storage.SetWriteBatchCmd(storage.Command{
		DeclareKeys: storage.DefaultDeclareKeys,
		Eval:        evalWriteBatch,
	})
}

// evalWriteBatch applies the operations encoded in a BatchRepr. Any existing
// data in the affected keyrange is first cleared (not tombstoned), which makes
// this command idempotent.
func evalWriteBatch(
	ctx context.Context, batch engine.ReadWriter, cArgs storage.CommandArgs, _ roachpb.Response,
) (storage.EvalResult, error) {

	args := cArgs.Args.(*roachpb.WriteBatchRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("WriteBatch [%s,%s)", args.Key, args.EndKey))
	defer tracing.FinishSpan(span)
	if log.V(1) {
		log.Infof(ctx, "writebatch [%s,%s)", args.Key, args.EndKey)
	}

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

	// Check if there was data in the affected keyrange. If so, delete it (and
	// adjust the MVCCStats) before applying the WriteBatch data.
	iter := batch.NewIterator(false)
	defer iter.Close()
	iter.Seek(mvccStartKey)
	if ok, err := iter.Valid(); err != nil {
		return storage.EvalResult{}, err
	} else if ok && iter.Key().Less(mvccEndKey) {
		existingStats, err := iter.ComputeStats(mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
		if err != nil {
			return storage.EvalResult{}, err
		}
		// If this is a SpanSetIterator, we have to unwrap it because
		// ClearIterRange needs a plain rocksdb iterator (and can't unwrap
		// it itself because of import cycles).
		// TODO(dan): Ideally, this would use `batch.ClearRange` but it doesn't
		// yet work with read-write batches.
		if ssi, ok := iter.(*storage.SpanSetIterator); ok {
			iter = ssi.Iterator()
		}
		if err := batch.ClearIterRange(iter, mvccStartKey, mvccEndKey); err != nil {
			return storage.EvalResult{}, err
		}
		ms.Subtract(existingStats)
	}

	if err := batch.ApplyBatchRepr(args.Data, false /* !sync */); err != nil {
		return storage.EvalResult{}, err
	}
	return storage.EvalResult{}, nil
}
