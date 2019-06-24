// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.AddSSTable, DefaultDeclareKeys, EvalAddSSTable)
}

// EvalAddSSTable evaluates an AddSSTable command.
func EvalAddSSTable(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.AddSSTableRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	mvccStartKey, mvccEndKey := engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey}

	// TODO(tschottdorf): restore the below in some form (gets in the way of testing).
	// _, span := tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", args.Key, args.EndKey))
	// defer tracing.FinishSpan(span)
	log.Eventf(ctx, "evaluating AddSSTable [%s,%s)", mvccStartKey.Key, mvccEndKey.Key)

	// IMPORT INTO should not proceed if any KVs from the SST shadow existing data
	// entries - #38044.
	if args.DisallowShadowing {
		if err := checkForKeyCollisions(ctx, batch, mvccStartKey, mvccEndKey, args.Data); err != nil {
			return result.Result{}, errors.Wrap(err, "checking for key collisions")
		}
	}

	// Verify that the keys in the sstable are within the range specified by the
	// request header, verify the key-value checksums, and compute the new
	// MVCCStats.
	stats, err := verifySSTable(
		args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "verifying sstable data")
	}

	// The above MVCCStats represents what is in this new SST.
	//
	// *If* the keys in the SST do not conflict with keys currently in this range,
	// then adding the stats for this SST to the range stats should yield the
	// correct overall stats.
	//
	// *However*, if the keys in this range *do* overlap with keys already in this
	// range, then adding the SST semantically *replaces*, rather than adds, those
	// keys, and the net effect on the stats is not so simple.
	//
	// To perfectly compute the correct net stats, you could a) determine the
	// stats for the span of the existing range that this SST covers and subtract
	// it from the range's stats, then b) use a merging iterator that reads from
	// the SST and then underlying range and compute the stats of that merged
	// span, and then add those stats back in. That would result in correct stats
	// that reflect the merging semantics when the SST "shadows" an existing key.
	//
	// If the underlying range is mostly empty, this isn't terribly expensive --
	// computing the existing stats to subtract is cheap, as there is little or no
	// existing data to traverse and b) is also pretty cheap -- the merging
	// iterator can quickly iterate the in-memory SST.
	//
	// However, if the underlying range is _not_ empty, then this is not cheap:
	// recomputing its stats involves traversing lots of data, and iterating the
	// merged iterator has to constantly go back and forth to the RocksDB-backed
	// (cgo) iterator.
	//
	// If we assume that most SSTs don't shadow too many keys, then the error of
	// simply adding the SST stats directly to the range stats is minimal. In the
	// worst-case, when we retry a whole SST, then it could be overcounting the
	// entire file, but we can hope that that is rare. In the worst case, it may
	// cause splitting an under-filled range that would later merge when the
	// over-count is fixed.
	//
	// We can indicate that these stats contain this estimation using the flag in
	// the MVCC stats so that later re-computations will not be surprised to find
	// any discrepancies.
	//
	// Callers can trigger such a re-computation to fixup any discrepancies (and
	// remove the ContainsEstimates flag) after they are done ingesting files by
	// sending an explicit recompute.
	stats.ContainsEstimates = true
	ms.Add(stats)

	return result.Result{
		Replicated: storagepb.ReplicatedEvalResult{
			AddSSTable: &storagepb.ReplicatedEvalResult_AddSSTable{
				Data:  args.Data,
				CRC32: util.CRC32(args.Data),
			},
		},
	}, nil
}

func checkForKeyCollisions(
	ctx context.Context,
	batch engine.ReadWriter,
	mvccStartKey engine.MVCCKey,
	mvccEndKey engine.MVCCKey,
	data []byte,
) error {

	// Create iterator over the existing data.
	existingDataIter := batch.NewIterator(engine.IterOptions{UpperBound: mvccEndKey.Key})
	defer existingDataIter.Close()
	existingDataIter.Seek(mvccStartKey)
	if ok, err := existingDataIter.Valid(); err != nil {
		return errors.Wrap(err, "checking for key collisions")
	} else if ok && !existingDataIter.UnsafeKey().Less(mvccEndKey) {
		// Target key range is empty, so it is safe to ingest.
		return nil
	}

	// Create a C++ iterator over the SST being added. This iterator is used to
	// perform a check for key collisions between the SST being ingested, and the
	// exisiting data. As the collision check is in C++ we are unable to use a
	// pure go iterator as in verifySSTable.
	//
	// TODO(adityamaru): reuse this iterator in verifySSTable.
	sst := engine.MakeRocksDBSstFileReader()
	defer sst.Close()

	if err := sst.IngestExternalFile(data); err != nil {
		return err
	}
	sstIterator := sst.NewIterator(engine.IterOptions{UpperBound: mvccEndKey.Key})
	defer sstIterator.Close()
	sstIterator.Seek(mvccStartKey)
	if ok, err := sstIterator.Valid(); err != nil || !ok {
		return errors.Wrap(err, "checking for key collisions")
	}

	checkErr := engine.CheckForKeyCollisions(existingDataIter, sstIterator)
	return checkErr
}

func verifySSTable(
	data []byte, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	// To verify every KV is a valid roachpb.KeyValue in the range [start, end)
	// we a) pass a verify flag on the iterator so that as ComputeStatsGo calls
	// Next, we're also verifying each KV pair. We explicitly check the first key
	// is >= start and then that we do not find a key after end.
	dataIter, err := engine.NewMemSSTIterator(data, true)
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

	stats, err := engine.ComputeStatsGo(dataIter, start, end, nowNanos)
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
