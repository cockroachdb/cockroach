// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func init() {
	batcheval.RegisterReadWriteCommand(roachpb.WriteBatch, batcheval.DefaultDeclareKeys, evalWriteBatch)
}

// evalWriteBatch applies the operations encoded in a BatchRepr. Any existing
// data in the affected keyrange is first cleared (not tombstoned), which makes
// this command idempotent.
func evalWriteBatch(
	ctx context.Context, batch storage.ReadWriter, cArgs batcheval.CommandArgs, _ roachpb.Response,
) (result.Result, error) {

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
		return result.Result{}, errors.New("data spans multiple ranges")
	}

	mvccStartKey := storage.MVCCKey{Key: args.Key}
	mvccEndKey := storage.MVCCKey{Key: args.EndKey}

	// Verify that the keys in the batch are within the range specified by the
	// request header.
	msBatch, err := engineccl.VerifyBatchRepr(args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, err
	}
	ms.Add(msBatch)

	// Check if there was data in the affected keyrange. If so, delete it (and
	// adjust the MVCCStats) before applying the WriteBatch data.
	existingStats, err := clearExistingData(ctx, batch, args.Key, args.EndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "clearing existing data")
	}
	ms.Subtract(existingStats)

	if err := batch.ApplyBatchRepr(args.Data, false /* sync */); err != nil {
		return result.Result{}, err
	}
	return result.Result{}, nil
}

func clearExistingData(
	ctx context.Context, batch storage.ReadWriter, start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	{
		isEmpty := true
		if err := batch.Iterate(start, end, func(_ storage.MVCCKeyValue) (bool, error) {
			isEmpty = false
			return true, nil // stop right away
		}); err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "while checking for empty key space")
		}

		if isEmpty {
			return enginepb.MVCCStats{}, nil
		}
	}

	iter := batch.NewIterator(storage.IterOptions{UpperBound: end})
	defer iter.Close()

	iter.SeekGE(storage.MakeMVCCMetadataKey(start))
	if ok, err := iter.Valid(); err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok && !iter.UnsafeKey().Less(storage.MakeMVCCMetadataKey(end)) {
		return enginepb.MVCCStats{}, nil
	}

	existingStats, err := iter.ComputeStats(start, end, nowNanos)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	log.Eventf(ctx, "target key range not empty, will clear existing data: %+v", existingStats)
	// If this is a Iterator, we have to unwrap it because
	// ClearIterRange needs a plain rocksdb iterator (and can't unwrap
	// it itself because of import cycles).
	if ssi, ok := iter.(*spanset.Iterator); ok {
		iter = ssi.Iterator()
	}
	// TODO(dan): Ideally, this would use `batch.ClearRange` but it doesn't
	// yet work with read-write batches (or IngestExternalData).
	if err := batch.ClearIterRange(iter, start, end); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return existingStats, nil
}
