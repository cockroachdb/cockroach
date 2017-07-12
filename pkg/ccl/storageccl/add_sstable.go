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

	// TODO(tschottdorf): restore the below in some form (gets in the way of testing).
	// _, span := tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", args.Key, args.EndKey))
	// defer tracing.FinishSpan(span)
	log.Eventf(ctx, "evaluating AddSSTable")

	// Verify that the keys in the sstable are within the range specified by the
	// request header, verify the key-value checksums, and compute the new
	// MVCCStats.
	mvccStartKey, mvccEndKey := engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey}
	stats, err := verifySSTable(args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return storage.EvalResult{}, errors.Wrap(err, "verifying sstable data")
	}

	// Check if there was data in the affected keyrange. If so, delete it.
	existingStats, err := clearExistingData(ctx, batch, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return storage.EvalResult{}, errors.Wrap(err, "clearing existing data")
	}

	if existingStats != (enginepb.MVCCStats{}) {
		log.Eventf(ctx, "key range contains existing data %+v, falling back to regular WriteBatch", existingStats)
		ms.Subtract(existingStats) // account for clearExistingData

		// If we're overwriting data, linking the SSTable is tricky since we have
		// to link before we delete, and so the deletions could clobber the data
		// in the SSTable. Instead, we put everything into a WriteBatch. Not
		// pretty, but this case should be the exception.
		reader := engine.MakeRocksDBSstFileReader()
		defer reader.Close()
		if err := reader.IngestExternalFile(args.Data); err != nil {
			return storage.EvalResult{}, errors.Wrap(err, "while preparing SSTable for copy into WriteBatch")
		}
		var v roachpb.Value
		if err := reader.Iterate(mvccStartKey, mvccEndKey, func(kv engine.MVCCKeyValue) (bool, error) {
			v.RawBytes = kv.Value
			return false, engine.MVCCPut(ctx, batch, ms, kv.Key.Key, kv.Key.Timestamp, v, nil /* txn */)
		}); err != nil {
			return storage.EvalResult{}, errors.Wrap(err, "copying SSTable into batch")
		}
		// Return with only a WriteBatch and no sideloading.
		return storage.EvalResult{}, nil
	}

	// More frequent case: the underlying key range is empty and we can ingest an SSTable.
	log.Event(ctx, "key range is empty; commencing with SSTable proposal")
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
	data []byte, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	iter, err := engineccl.NewMemSSTIterator(data)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
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

	// TODO(dan): This unnecessarily iterates the sstable a second time, see if
	// combining this computation with the above checksum verification speeds
	// anything up.
	return engine.ComputeStatsGo(iter, start, end, nowNanos)
}

func clearExistingData(
	ctx context.Context, batch engine.ReadWriter, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	{
		isEmpty := true
		if err := batch.Iterate(start, end, func(_ engine.MVCCKeyValue) (bool, error) {
			isEmpty = false
			return true, nil // stop right away
		}); err != nil {
			return enginepb.MVCCStats{}, errors.Wrap(err, "while checking for empty key space")
		}

		if isEmpty {
			return enginepb.MVCCStats{}, nil
		}
	}

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

	log.Eventf(ctx, "target key range not empty, will clear existing data: %+v", existingStats)
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
