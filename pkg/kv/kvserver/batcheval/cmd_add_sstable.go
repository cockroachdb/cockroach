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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

func init() {
	RegisterReadWriteCommand(roachpb.AddSSTable, DefaultDeclareKeys, EvalAddSSTable)
}

// EvalAddSSTable evaluates an AddSSTable command.
// NB: These sstables do not contain intents/locks, so the code below only
// needs to deal with MVCCKeys.
func EvalAddSSTable(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.AddSSTableRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	mvccStartKey, mvccEndKey := storage.MVCCKey{Key: args.Key}, storage.MVCCKey{Key: args.EndKey}

	// TODO(tschottdorf): restore the below in some form (gets in the way of testing).
	// _, span := tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", args.Key, args.EndKey))
	// defer span.Finish()
	log.Eventf(ctx, "evaluating AddSSTable [%s,%s)", mvccStartKey.Key, mvccEndKey.Key)

	// IMPORT INTO should not proceed if any KVs from the SST shadow existing data
	// entries - #38044.
	var skippedKVStats enginepb.MVCCStats
	var err error
	if args.DisallowShadowing {
		if skippedKVStats, err = checkForKeyCollisions(ctx, readWriter, mvccStartKey, mvccEndKey, args.Data); err != nil {
			return result.Result{}, errors.Wrap(err, "checking for key collisions")
		}
	}

	// Verify that the keys in the sstable are within the range specified by the
	// request header, and if the request did not include pre-computed stats,
	// compute the expected MVCC stats delta of ingesting the SST.
	dataIter, err := storage.NewMemSSTIterator(args.Data, true)
	if err != nil {
		return result.Result{}, err
	}
	defer dataIter.Close()

	// Check that the first key is in the expected range.
	dataIter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
	ok, err := dataIter.Valid()
	if err != nil {
		return result.Result{}, err
	} else if ok {
		if unsafeKey := dataIter.UnsafeKey(); unsafeKey.Less(mvccStartKey) {
			return result.Result{}, errors.Errorf("first key %s not in request range [%s,%s)",
				unsafeKey.Key, mvccStartKey.Key, mvccEndKey.Key)
		}
	}

	// Get the MVCCStats for the SST being ingested.
	var stats enginepb.MVCCStats
	if args.MVCCStats != nil {
		stats = *args.MVCCStats
	}

	// Stats are computed on-the-fly when shadowing of keys is disallowed. If we
	// took the fast path and race is enabled, assert the stats were correctly
	// computed.
	verifyFastPath := args.DisallowShadowing && util.RaceEnabled
	if args.MVCCStats == nil || verifyFastPath {
		log.VEventf(ctx, 2, "computing MVCCStats for SSTable [%s,%s)", mvccStartKey.Key, mvccEndKey.Key)

		computed, err := storage.ComputeStatsForRange(
			dataIter, mvccStartKey.Key, mvccEndKey.Key, h.Timestamp.WallTime)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "computing SSTable MVCC stats")
		}

		if verifyFastPath {
			// Update the timestamp to that of the recently computed stats to get the
			// diff passing.
			stats.LastUpdateNanos = computed.LastUpdateNanos
			if !stats.Equal(computed) {
				log.Fatalf(ctx, "fast-path MVCCStats computation gave wrong result: diff(fast, computed) = %s",
					pretty.Diff(stats, computed))
			}
		}
		stats = computed
	}

	dataIter.SeekGE(mvccEndKey)
	ok, err = dataIter.Valid()
	if err != nil {
		return result.Result{}, err
	} else if ok {
		return result.Result{}, errors.Errorf("last key %s not in request range [%s,%s)",
			dataIter.UnsafeKey(), mvccStartKey.Key, mvccEndKey.Key)
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
	// merged iterator has to constantly go back and forth to the iterator.
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
	//
	// There is a significant performance win to be achieved by ensuring that the
	// stats computed are not estimates as it prevents recomputation on splits.
	// Running AddSSTable with disallowShadowing=true gets us close to this as we
	// do not allow colliding keys to be ingested. However, in the situation that
	// two SSTs have KV(s) which "perfectly" shadow an existing key (equal ts and
	// value), we do not consider this a collision. While the KV would just
	// overwrite the existing data, the stats would be added to the cumulative
	// stats of the AddSSTable command, causing a double count for such KVs.
	// Therefore, we compute the stats for these "skipped" KVs on-the-fly while
	// checking for the collision condition in C++ and subtract them from the
	// stats of the SST being ingested before adding them to the running
	// cumulative for this command. These stats can then be marked as accurate.
	if args.DisallowShadowing {
		stats.Subtract(skippedKVStats)
		stats.ContainsEstimates = 0
	} else {
		stats.ContainsEstimates++
	}

	ms.Add(stats)

	if args.IngestAsWrites {
		log.VEventf(ctx, 2, "ingesting SST (%d keys/%d bytes) via regular write batch", stats.KeyCount, len(args.Data))
		dataIter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
		for {
			ok, err := dataIter.Valid()
			if err != nil {
				return result.Result{}, err
			} else if !ok {
				break
			}
			// NB: This is *not* a general transformation of any arbitrary SST to a
			// WriteBatch: it assumes every key in the SST is a simple Set. This is
			// already assumed elsewhere in this RPC though, so that's OK here.
			k := dataIter.UnsafeKey()
			if k.Timestamp.IsEmpty() {
				if err := readWriter.PutUnversioned(k.Key, dataIter.UnsafeValue()); err != nil {
					return result.Result{}, err
				}
			} else {
				if err := readWriter.PutMVCC(dataIter.UnsafeKey(), dataIter.UnsafeValue()); err != nil {
					return result.Result{}, err
				}
			}
			dataIter.Next()
		}
		return result.Result{}, nil
	}

	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			AddSSTable: &kvserverpb.ReplicatedEvalResult_AddSSTable{
				Data:  args.Data,
				CRC32: util.CRC32(args.Data),
			},
		},
	}, nil
}

func checkForKeyCollisions(
	_ context.Context,
	reader storage.Reader,
	mvccStartKey storage.MVCCKey,
	mvccEndKey storage.MVCCKey,
	data []byte,
) (enginepb.MVCCStats, error) {
	// Create iterator over the existing data.
	existingDataIter := reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: mvccEndKey.Key})
	defer existingDataIter.Close()
	existingDataIter.SeekGE(mvccStartKey)
	if ok, err := existingDataIter.Valid(); err != nil {
		return enginepb.MVCCStats{}, errors.Wrap(err, "checking for key collisions")
	} else if !ok {
		// Target key range is empty, so it is safe to ingest.
		return enginepb.MVCCStats{}, nil
	}

	return existingDataIter.CheckForKeyCollisions(data, mvccStartKey.Key, mvccEndKey.Key)
}
