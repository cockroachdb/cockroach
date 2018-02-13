// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	batcheval.RegisterCommand(roachpb.AddSSTable, batcheval.DefaultDeclareKeys, evalAddSSTable)
}

func evalAddSSTable(
	ctx context.Context, batch engine.ReadWriter, cArgs batcheval.CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.AddSSTableRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	mvccStartKey, mvccEndKey := engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey}

	// TODO(tschottdorf): restore the below in some form (gets in the way of testing).
	// _, span := tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", args.Key, args.EndKey))
	// defer tracing.FinishSpan(span)
	log.Eventf(ctx, "evaluating AddSSTable [%s,%s)", mvccStartKey.Key, mvccEndKey.Key)

	// Compute the stats for any existing data in the affected span. The sstable
	// being ingested can overwrite all, some, or none of the existing kvs.
	// (Note: the expected case is that it's none or, in the case of a retry of
	// the request, all.) So subtract out the existing mvcc stats, and add back
	// what they'll be after the sstable is ingested.
	existingIter := batch.NewIterator(false)
	defer existingIter.Close()
	existingIter.Seek(mvccStartKey)
	if ok, err := existingIter.Valid(); err != nil {
		return result.Result{}, errors.Wrap(err, "computing existing stats")
	} else if ok && existingIter.UnsafeKey().Less(mvccEndKey) {
		log.Eventf(ctx, "target key range not empty, will merge existing data with sstable")
	}
	// This ComputeStats is cheap if the span is empty.
	existingStats, err := existingIter.ComputeStats(mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "computing existing stats")
	}
	ms.Subtract(existingStats)

	// Verify that the keys in the sstable are within the range specified by the
	// request header, verify the key-value checksums, and compute the new
	// MVCCStats.
	stats, err := verifySSTable(
		existingIter, args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "verifying sstable data")
	}
	ms.Add(stats)

	return result.Result{
		Replicated: storagebase.ReplicatedEvalResult{
			AddSSTable: &storagebase.ReplicatedEvalResult_AddSSTable{
				Data:  args.Data,
				CRC32: util.CRC32(args.Data),
			},
		},
	}, nil
}

func verifySSTable(
	existingIter engine.SimpleIterator, data []byte, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	// To verify every KV is a valid roachpb.KeyValue in the range [start, end)
	// we a) pass a verify flag on the iterator so that as ComputeStatsGo calls
	// Next, we're also verifying each KV pair. We explicitly check the first key
	// is >= start and then that we do not find a key after end.
	dataIter, err := engineccl.NewMemSSTIterator(data, true)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer dataIter.Close()

	// Check that the first key is in the expected range.
	dataIter.Seek(engine.MVCCKey{Key: keys.MinKey})
	ok, err := dataIter.Valid()
	if err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok {
		if unsafeKey := dataIter.UnsafeKey(); unsafeKey.Less(start) {
			return enginepb.MVCCStats{}, errors.Errorf("first key %s not in request range [%s,%s)",
				unsafeKey.Key, start.Key, end.Key)
		}
	}

	// In the case that two iterators have an entry with the same key and
	// timestamp, MultiIterator breaks ties by preferring later ones in the
	// ordering. So it's important that the sstable iterator comes after the one
	// for the existing data (because the sstable will overwrite it when
	// ingested).
	mergedIter := engineccl.MakeMultiIterator([]engine.SimpleIterator{existingIter, dataIter})
	defer mergedIter.Close()

	stats, err := engine.ComputeStatsGo(mergedIter, start, end, nowNanos)
	if err != nil {
		return stats, err
	}

	dataIter.Seek(end)
	ok, err = dataIter.Valid()
	if err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok {
		if unsafeKey := dataIter.UnsafeKey(); !unsafeKey.Less(end) {
			return enginepb.MVCCStats{}, errors.Errorf("last key %s not in request range [%s,%s)",
				unsafeKey.Key, start.Key, end.Key)
		}
	}
	return stats, nil
}
