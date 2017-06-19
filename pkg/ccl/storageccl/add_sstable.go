// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

	_, span := tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", args.Key, args.EndKey))
	defer tracing.FinishSpan(span)
	if log.V(1) {
		log.Infof(ctx, "addsstable [%s,%s)", args.Key, args.EndKey)
	}

	// Verify that the keys in the sstable are within the range specified by the
	// request header, verify the key-value checksums, and compute the new
	// MVCCStats.
	mvccStartKey, mvccEndKey := engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey}
	stats, err := verifySSTable(args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return storage.EvalResult{}, errors.Wrap(err, "verifying sstable data")
	}
	ms.Add(stats)

	// Check if there was data in the affected keyrange. If so, delete it (and
	// adjust the MVCCStats) before applying the SSTable.
	//
	// TODO(tschottdorf): this could be a large proposal (perhaps too large to
	// be accepted). Better to compute the stats and send them along, but make
	// the actual clear a below-Raft side effect.
	existingStats, err := clearExistingData(batch, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return storage.EvalResult{}, errors.Wrap(err, "clearing existing data")
	}
	ms.Subtract(existingStats)

	pd := storage.EvalResult{
		Replicated: storagebase.ReplicatedEvalResult{
			AddSSTable: &storagebase.ReplicatedEvalResult_AddSSTable{
				Data: args.Data,
			},
		},
	}
	return pd, nil
}

func verifySSTable(
	data []byte, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	sstReader := engine.MakeRocksDBSstFileReader()
	if err := sstReader.IngestExternalFile(data); err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer sstReader.Close()
	iter := sstReader.NewIterator(false)
	defer iter.Close()

	iter.Seek(engine.MVCCKey{Key: keys.MinKey})
	ok, err := iter.Valid()
	for ; ok; ok, err = iter.Valid() {
		unsafeKey := iter.UnsafeKey()
		if unsafeKey.Less(start) || !unsafeKey.Less(end) {
			// TODO(dan): Add a new field in roachpb.Error, so the client can
			// catch this and retry. It can happen if the range splits between
			// when the client constructs the file and sends the request.
			return enginepb.MVCCStats{}, errors.Errorf("key %s not in request range [%s,%s)",
				unsafeKey.Key, start.Key, end.Key)
		}

		v := roachpb.Value{RawBytes: iter.UnsafeValue()}
		if err := v.Verify(unsafeKey.Key); err != nil {
			return enginepb.MVCCStats{}, err
		}
		iter.Next()
	}
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	return iter.ComputeStats(start, end, nowNanos)
}

func clearExistingData(
	batch engine.ReadWriter, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	iter := batch.NewIterator(false)
	defer iter.Close()

	iter.Seek(start)
	if ok, err := iter.Valid(); err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok && !iter.UnsafeKey().Less(end) {
		return enginepb.MVCCStats{}, nil
	}

	existingStats, err := iter.ComputeStats(start, end, nowNanos)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	// If this is a SpanSetIterator, we have to unwrap it because
	// ClearIterRange needs a plain rocksdb iterator (and can't unwrap
	// it itself because of import cycles).
	if ssi, ok := iter.(*storage.SpanSetIterator); ok {
		iter = ssi.Iterator()
	}
	// TODO(dan): Ideally, this would use `batch.ClearRange` but it doesn't
	// yet work with read-write batches (or IngestExternalData).
	if err := batch.ClearIterRange(iter, start, end); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return existingStats, nil
}
