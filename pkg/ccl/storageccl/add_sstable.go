// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	storage.SetAddSSTableCmd(storage.Command{
		DeclareKeys: storage.DefaultDeclareKeys,
		Eval:        evalAddSSTable,
	})
}

func evalAddSSTable(
	ctx context.Context, batch engine.ReadWriter, cArgs storage.CommandArgs, _ roachpb.Response,
) (storage.EvalResult, error) {
	args := cArgs.Args.(*roachpb.AddSSTableRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	mvccStartKey, mvccEndKey := engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey}

	// TODO(tschottdorf): restore the below in some form (gets in the way of testing).
	// _, span := tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", args.Key, args.EndKey))
	// defer tracing.FinishSpan(span)
	log.Eventf(ctx, "evaluating AddSSTable")

	// Compute the stats for any existing data in the affected span. The sstable
	// being ingested can overwrite all, some, or none of the existing kvs.
	// (Note: the expected case is that it's none or, in the case of a retry of
	// the request, all.) So subtract out the existing mvcc stats, and add back
	// what they'll be after the sstable is ingested.
	existingIter := batch.NewIterator(false)
	defer existingIter.Close()
	existingIter.Seek(mvccStartKey)
	if ok, err := existingIter.Valid(); err != nil {
		return storage.EvalResult{}, errors.Wrap(err, "computing existing stats")
	} else if ok && existingIter.UnsafeKey().Less(mvccEndKey) {
		log.Eventf(ctx, "target key range not empty, will merge existing data with sstable")
	}
	// This ComputeStats is cheap if the span is empty.
	existingStats, err := existingIter.ComputeStats(mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return storage.EvalResult{}, errors.Wrap(err, "computing existing stats")
	}
	ms.Subtract(existingStats)

	// Verify that the keys in the sstable are within the range specified by the
	// request header, verify the key-value checksums, and compute the new
	// MVCCStats.
	stats, err := verifySSTable(
		existingIter, args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return storage.EvalResult{}, errors.Wrap(err, "verifying sstable data")
	}
	ms.Add(stats)

	return storage.EvalResult{
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
	dataIter, err := engineccl.NewMemSSTIterator(data)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer dataIter.Close()

	dataIter.Seek(engine.MVCCKey{Key: keys.MinKey})
	ok, err := dataIter.Valid()
	for ; ok; ok, err = dataIter.Valid() {
		unsafeKey := dataIter.UnsafeKey()
		if unsafeKey.Less(start) || !unsafeKey.Less(end) {
			// TODO(dan): Add a new field in roachpb.Error, so the client can
			// catch this and retry. It can happen if the range splits between
			// when the client constructs the file and sends the request.
			return enginepb.MVCCStats{}, errors.Errorf("key %s not in request range [%s,%s)",
				unsafeKey.Key, start.Key, end.Key)
		}

		v := roachpb.Value{RawBytes: dataIter.UnsafeValue()}
		if err := v.Verify(unsafeKey.Key); err != nil {
			return enginepb.MVCCStats{}, err
		}
		dataIter.Next()
	}
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	// In the case that two iterators have an entry with the same key and
	// timestamp, MultiIterator breaks ties by preferring later ones in the
	// ordering. So it's important that the sstable iterator comes after the one
	// for the existing data (because the sstable will overwrite it when
	// ingested).
	mergedIter := engineccl.MakeMultiIterator([]engine.SimpleIterator{existingIter, dataIter})
	defer mergedIter.Close()

	// TODO(dan): This unnecessarily iterates the sstable a second time, see if
	// combining this computation with the above checksum verification speeds
	// anything up.
	return engine.ComputeStatsGo(mergedIter, start, end, nowNanos)
}
