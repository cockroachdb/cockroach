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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/kr/pretty"
)

func init() {
	// Taking out latches/locks across the entire SST span is very coarse, and we
	// could instead iterate over the SST and take out point latches/locks, but
	// the cost is likely not worth it since AddSSTable is often used with
	// unpopulated spans.
	RegisterReadWriteCommand(roachpb.AddSSTable, DefaultDeclareIsolatedKeys, EvalAddSSTable)
}

// AddSSTableRewriteConcurrency sets the concurrency of a single SST rewrite.
var AddSSTableRewriteConcurrency = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.bulk_io_write.sst_rewrite_concurrency.per_call",
	"concurrency to use when rewriting sstable timestamps by block, or 0 to use a loop",
	int64(util.ConstantWithMetamorphicTestRange("addsst-rewrite-concurrency", 0, 0, 16)),
	settings.NonNegativeInt,
)

// EvalAddSSTable evaluates an AddSSTable command. For details, see doc comment
// on AddSSTableRequest.
func EvalAddSSTable(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.AddSSTableRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	start, end := storage.MVCCKey{Key: args.Key}, storage.MVCCKey{Key: args.EndKey}
	sst := args.Data
	sstToReqTS := args.SSTTimestampToRequestTimestamp

	var span *tracing.Span
	var err error
	ctx, span = tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", start.Key, end.Key))
	defer span.Finish()
	log.Eventf(ctx, "evaluating AddSSTable [%s,%s)", start.Key, end.Key)

	// Under the race detector, check that the SST contents satisfy AddSSTable
	// requirements. We don't always do this otherwise, due to the cost.
	if util.RaceEnabled {
		if err := assertSSTContents(sst, sstToReqTS, args.MVCCStats); err != nil {
			return result.Result{}, err
		}
	}

	// If requested and necessary, rewrite the SST's MVCC timestamps to the
	// request timestamp. This ensures the writes comply with the timestamp cache
	// and closed timestamp, i.e. by not writing to timestamps that have already
	// been observed or closed.
	if sstToReqTS.IsSet() && (h.Timestamp != sstToReqTS ||
		util.ConstantWithMetamorphicTestBool("addsst-rewrite-forced", false)) {
		st := cArgs.EvalCtx.ClusterSettings()
		// TODO(dt): use a quotapool.
		conc := int(AddSSTableRewriteConcurrency.Get(&cArgs.EvalCtx.ClusterSettings().SV))
		sst, err = storage.UpdateSSTTimestamps(ctx, st, sst, sstToReqTS, h.Timestamp, conc)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "updating SST timestamps")
		}
	}

	var statsDelta enginepb.MVCCStats
	maxIntents := storage.MaxIntentsPerWriteIntentError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	checkConflicts := args.DisallowConflicts || args.DisallowShadowing ||
		!args.DisallowShadowingBelow.IsEmpty()
	if checkConflicts {
		// If requested, check for MVCC conflicts with existing keys. This enforces
		// all MVCC invariants by returning WriteTooOldError for any existing
		// values at or above the SST timestamp, returning WriteIntentError to
		// resolve any encountered intents, and accurately updating MVCC stats.
		//
		// Additionally, if DisallowShadowing or DisallowShadowingBelow is set, it
		// will not write above existing/visible values (but will write above
		// tombstones).
		statsDelta, err = storage.CheckSSTConflicts(ctx, sst, readWriter, start, end,
			args.DisallowShadowing, args.DisallowShadowingBelow, maxIntents)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "checking for key collisions")
		}

	} else {
		// If not checking for MVCC conflicts, at least check for separated intents.
		// The caller is expected to make sure there are no writers across the span,
		// and thus no or few intents, so this is cheap in the common case.
		intents, err := storage.ScanIntents(ctx, readWriter, start.Key, end.Key, maxIntents, 0)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "scanning intents")
		} else if len(intents) > 0 {
			return result.Result{}, &roachpb.WriteIntentError{Intents: intents}
		}
	}

	// Verify that the keys in the sstable are within the range specified by the
	// request header, and if the request did not include pre-computed stats,
	// compute the expected MVCC stats delta of ingesting the SST.
	sstIter, err := storage.NewMemSSTIterator(sst, true)
	if err != nil {
		return result.Result{}, err
	}
	defer sstIter.Close()

	// Check that the first key is in the expected range.
	sstIter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
	ok, err := sstIter.Valid()
	if err != nil {
		return result.Result{}, err
	} else if ok {
		if unsafeKey := sstIter.UnsafeKey(); unsafeKey.Less(start) {
			return result.Result{}, errors.Errorf("first key %s not in request range [%s,%s)",
				unsafeKey.Key, start.Key, end.Key)
		}
	}

	// Get the MVCCStats for the SST being ingested.
	var stats enginepb.MVCCStats
	if args.MVCCStats != nil {
		stats = *args.MVCCStats
	} else {
		log.VEventf(ctx, 2, "computing MVCCStats for SSTable [%s,%s)", start.Key, end.Key)
		stats, err = storage.ComputeStatsForRange(sstIter, start.Key, end.Key, h.Timestamp.WallTime)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "computing SSTable MVCC stats")
		}
	}

	sstIter.SeekGE(end)
	ok, err = sstIter.Valid()
	if err != nil {
		return result.Result{}, err
	} else if ok {
		return result.Result{}, errors.Errorf("last key %s not in request range [%s,%s)",
			sstIter.UnsafeKey(), start.Key, end.Key)
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
	if checkConflicts {
		stats.Add(statsDelta)
		stats.ContainsEstimates = 0
	} else {
		stats.ContainsEstimates++
	}

	ms.Add(stats)

	var mvccHistoryMutation *kvserverpb.ReplicatedEvalResult_MVCCHistoryMutation
	if sstToReqTS.IsEmpty() {
		mvccHistoryMutation = &kvserverpb.ReplicatedEvalResult_MVCCHistoryMutation{
			Spans: []roachpb.Span{{Key: start.Key, EndKey: end.Key}},
		}
	}

	if args.IngestAsWrites {
		span.RecordStructured(&types.StringValue{Value: fmt.Sprintf("ingesting SST (%d keys/%d bytes) via regular write batch", stats.KeyCount, len(sst))})
		log.VEventf(ctx, 2, "ingesting SST (%d keys/%d bytes) via regular write batch", stats.KeyCount, len(sst))
		sstIter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
		for {
			ok, err := sstIter.Valid()
			if err != nil {
				return result.Result{}, err
			} else if !ok {
				break
			}
			// NB: This is *not* a general transformation of any arbitrary SST to a
			// WriteBatch: it assumes every key in the SST is a simple Set. This is
			// already assumed elsewhere in this RPC though, so that's OK here.
			k := sstIter.UnsafeKey()
			if k.Timestamp.IsEmpty() {
				if err := readWriter.PutUnversioned(k.Key, sstIter.UnsafeValue()); err != nil {
					return result.Result{}, err
				}
			} else {
				if err := readWriter.PutMVCC(k, sstIter.UnsafeValue()); err != nil {
					return result.Result{}, err
				}
			}
			// The above MVCC functions do not record logical operations, but we must
			// use them because e.g. storage.MVCCPut() changes the semantics of the
			// write by not allowing writing below existing keys, and we want to
			// retain parity with regular SST ingestion which does allow this. We
			// therefore record these operations ourselves.
			if sstToReqTS.IsSet() {
				readWriter.LogLogicalOp(storage.MVCCWriteValueOpType, storage.MVCCLogicalOpDetails{
					Key:       k.Key,
					Timestamp: k.Timestamp,
				})
			}
			sstIter.Next()
		}
		return result.Result{
			Replicated: kvserverpb.ReplicatedEvalResult{
				MVCCHistoryMutation: mvccHistoryMutation,
			},
			Local: result.LocalResult{
				Metrics: &result.Metrics{
					AddSSTableAsWrites: 1,
				},
			},
		}, nil
	}

	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			AddSSTable: &kvserverpb.ReplicatedEvalResult_AddSSTable{
				Data:             sst,
				CRC32:            util.CRC32(sst),
				Span:             roachpb.Span{Key: start.Key, EndKey: end.Key},
				AtWriteTimestamp: sstToReqTS.IsSet(),
			},
			MVCCHistoryMutation: mvccHistoryMutation,
		},
	}, nil
}

// assertSSTContents checks that the SST contains expected inputs:
//
// * Only SST set operations (not explicitly verified).
// * No intents, tombstones, or unversioned values.
// * If sstTimestamp is set, all MVCC timestamps equal it.
// * Given MVCC stats match the SST contents.
func assertSSTContents(sst []byte, sstTimestamp hlc.Timestamp, stats *enginepb.MVCCStats) error {
	iter, err := storage.NewMemSSTIterator(sst, true)
	if err != nil {
		return err
	}
	defer iter.Close()

	// Check SST KV pairs.
	iter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
	for {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		key, value := iter.UnsafeKey(), iter.UnsafeValue()
		if key.Timestamp.IsEmpty() {
			return errors.AssertionFailedf("SST contains inline value or intent for key %s", key)
		}
		if len(value) == 0 {
			return errors.AssertionFailedf("SST contains tombstone for key %s", key)
		}
		if sstTimestamp.IsSet() && key.Timestamp != sstTimestamp {
			return errors.AssertionFailedf("SST has unexpected timestamp %s (expected %s) for key %s",
				key.Timestamp, sstTimestamp, key.Key)
		}
		iter.Next()
	}

	// Compare statistics with those passed by client. We calculate them at the
	// same timestamp as the given statistics, since they may contain
	// timing-dependent values (typically MVCC garbage, i.e. multiple versions).
	if stats != nil {
		given := *stats
		actual, err := storage.ComputeStatsForRange(
			iter, keys.MinKey, keys.MaxKey, given.LastUpdateNanos)
		if err != nil {
			return errors.Wrap(err, "failed to compare stats: %w")
		}
		if !given.Equal(actual) {
			return errors.AssertionFailedf("SST stats are incorrect: diff(given, actual) = %s",
				pretty.Diff(given, actual))
		}
	}

	return nil
}
