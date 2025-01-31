// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
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
	RegisterReadWriteCommand(kvpb.AddSSTable, declareKeysAddSSTable, EvalAddSSTable)
}

func declareKeysAddSSTable(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	reqHeader := req.Header()
	if err := DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset); err != nil {
		return err
	}
	// We look up the range descriptor key to return its span.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	var mvccStats *enginepb.MVCCStats
	switch v := req.(type) {
	case *kvpb.AddSSTableRequest:
		mvccStats = v.MVCCStats
	case *kvpb.LinkExternalSSTableRequest:
		mvccStats = v.ExternalFile.MVCCStats
	default:
		return errors.AssertionFailedf("unexpected rpc request called on declareKeysAddSStable")
	}
	// TODO(bilal): Audit all AddSSTable callers to ensure they send MVCCStats.
	if mvccStats == nil || mvccStats.RangeKeyCount > 0 {
		// NB: The range end key is not available, so this will pessimistically
		// latch up to args.EndKey.Next(). If EndKey falls on the range end key, the
		// span will be tightened during evaluation.
		// Even if we obtain latches beyond the end range here, it won't cause
		// contention with the subsequent range because latches are enforced per
		// range.
		l, r := rangeTombstonePeekBounds(reqHeader.Key, reqHeader.EndKey, rs.GetStartKey().AsRawKey(), nil)
		latchSpans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{Key: l, EndKey: r}, header.Timestamp)

		// Obtain a read only lock on range key GC key to serialize with
		// range tombstone GC requests.
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.MVCCRangeKeyGCKey(rs.GetRangeID()),
		})
	}
	return nil
}

// AddSSTableRewriteConcurrency sets the concurrency of a single SST rewrite.
var AddSSTableRewriteConcurrency = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.bulk_io_write.sst_rewrite_concurrency.per_call",
	"concurrency to use when rewriting sstable timestamps by block, or 0 to use a loop",
	int64(metamorphic.ConstantWithTestRange("addsst-rewrite-concurrency", 4, 0, 16)),
	settings.NonNegativeInt,
)

// AddSSTableRequireAtRequestTimestamp will reject any AddSSTable requests that
// aren't sent with SSTTimestampToRequestTimestamp. This can be used to verify
// that all callers have been migrated to use SSTTimestampToRequestTimestamp.
var AddSSTableRequireAtRequestTimestamp = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.bulk_io_write.sst_require_at_request_timestamp.enabled",
	"rejects addsstable requests that don't write at the request timestamp",
	false,
)

// prefixSeekCollisionCheckRatio specifies the minimum engine:sst byte ratio at
// which we do prefix seeks in CheckSSTCollisions instead of regular seeks.
// Prefix seeks make more sense if the inbound SST is wide relative to the engine
// i.e. the engine:sst byte/key ratio is high and skewed in favour of the engine.
// In those cases, since we are less likely to skip SST keys with regular seeks
// on the engine, it is more efficient to utilize bloom filters on the engine's
// SSTs and do a prefix seek in the engine for every key in the SST being added.
//
// A value of 10 was experimentally determined to be sufficient in separating
// most cases of wide SSTs from other cases where a regular seek would be
// beneficial (i.e. incoming SSTs that are very narrow in the engine's keyspace).
// For cases where the ratio is 1 or less (i.e. more SST key bytes than engine
// key bytes in the span being ingested), regular seeks are always beneficial,
// while for cases where the ratio is 10 or higher, prefix seeks were always
// found to be beneficial. This value can likely be set anywhere within the 1-10
// range, but it was conservatively set to 10 to not induce regressions as the
// old status quo was regular seeks.
const prefixSeekCollisionCheckRatio = 10

var forceRewrite = metamorphic.ConstantWithTestBool("addsst-rewrite-forced", false)

// EvalAddSSTable evaluates an AddSSTable command. For details, see doc comment
// on AddSSTableRequest.
func EvalAddSSTable(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.AddSSTableRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	start, end := storage.MVCCKey{Key: args.Key}, storage.MVCCKey{Key: args.EndKey}
	sst := args.Data
	sstToReqTS := args.SSTTimestampToRequestTimestamp

	var span *tracing.Span
	var err error
	ctx, span = tracing.ChildSpan(ctx, "EvalAddSSTable")
	defer span.Finish()
	log.Eventf(ctx, "evaluating AddSSTable [%s,%s)", start.Key, end.Key)

	if min := storage.MinCapacityForBulkIngest.Get(&cArgs.EvalCtx.ClusterSettings().SV); min > 0 {
		cap, err := cArgs.EvalCtx.GetEngineCapacity()
		if err != nil {
			return result.Result{}, err
		}
		if remaining := float64(cap.Available) / float64(cap.Capacity); remaining < min {
			return result.Result{}, &kvpb.InsufficientSpaceError{
				StoreID:   cArgs.EvalCtx.StoreID(),
				Op:        "ingest data",
				Available: cap.Available,
				Capacity:  cap.Capacity,
				Required:  min,
			}
		}
	}

	// Reject AddSSTable requests not writing at the request timestamp if requested.
	if AddSSTableRequireAtRequestTimestamp.Get(&cArgs.EvalCtx.ClusterSettings().SV) &&
		sstToReqTS.IsEmpty() {
		return result.Result{}, errors.AssertionFailedf(
			"AddSSTable requests must set SSTTimestampToRequestTimestamp")
	}

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
	var sstReqStatsDelta enginepb.MVCCStats
	var sstTimestamp hlc.Timestamp
	if sstToReqTS.IsSet() {
		sstTimestamp = h.Timestamp
		if h.Timestamp != sstToReqTS || forceRewrite {
			st := cArgs.EvalCtx.ClusterSettings()
			// TODO(dt): use a quotapool.
			conc := int(AddSSTableRewriteConcurrency.Get(&cArgs.EvalCtx.ClusterSettings().SV))
			log.VEventf(ctx, 2, "rewriting timestamps for SSTable [%s,%s) from %s to %s",
				start.Key, end.Key, sstToReqTS, h.Timestamp)
			sst, sstReqStatsDelta, err = storage.UpdateSSTTimestamps(
				ctx, st, sst, sstToReqTS, h.Timestamp, conc, args.MVCCStats)
			if err != nil {
				return result.Result{}, errors.Wrap(err, "updating SST timestamps")
			}
		}
	}

	var statsDelta enginepb.MVCCStats
	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	targetLockConflictBytes := storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	checkConflicts := args.DisallowConflicts || !args.DisallowShadowingBelow.IsEmpty()
	if checkConflicts {
		// If requested, check for MVCC conflicts with existing keys. This enforces
		// all MVCC invariants by returning WriteTooOldError for any existing
		// values at or above the SST timestamp, returning LockConflictError to
		// resolve any encountered intents, and accurately updating MVCC stats.
		//
		// Additionally, if DisallowShadowingBelow is set, it will not write
		// above existing/visible values (but will write above tombstones).
		//
		// If the overlap between the ingested SST and the engine is large (i.e.
		// the SST is wide in keyspace), or if the ingested SST is very small,
		// use prefix seeks in CheckSSTConflicts. This ends up being more performant
		// as it avoids expensive seeks with index/data block loading in the common
		// case of no conflicts.
		usePrefixSeek := false
		bytes, err := cArgs.EvalCtx.GetApproximateDiskBytes(start.Key, end.Key)
		if err == nil {
			usePrefixSeek = bytes > prefixSeekCollisionCheckRatio*uint64(len(sst))
		}
		if args.MVCCStats != nil {
			// If the incoming SST is small, use a prefix seek. Very little time is
			// usually spent iterating over keys in these cases anyway, so it makes
			// sense to use prefix seeks when the max number of seeks we'll do is
			// bounded with a small number on the SST side.
			usePrefixSeek = usePrefixSeek || args.MVCCStats.KeyCount < 100
		}
		desc := cArgs.EvalCtx.Desc()
		leftPeekBound, rightPeekBound := rangeTombstonePeekBounds(
			args.Key, args.EndKey, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())

		log.VEventf(ctx, 2, "checking conflicts for SSTable [%s,%s)", start.Key, end.Key)
		statsDelta, err = storage.CheckSSTConflicts(ctx, sst, readWriter, start, end, leftPeekBound, rightPeekBound,
			args.DisallowShadowingBelow, sstTimestamp, maxLockConflicts, targetLockConflictBytes, usePrefixSeek)
		statsDelta.Add(sstReqStatsDelta)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "checking for key collisions")
		}

	} else {
		// If not checking for MVCC conflicts, at least check for locks. The
		// caller is expected to make sure there are no writers across the span,
		// and thus no or few locks, so this is cheap in the common case.
		log.VEventf(ctx, 2, "checking conflicting locks for SSTable [%s,%s)", start.Key, end.Key)
		locks, err := storage.ScanLocks(
			ctx, readWriter, start.Key, end.Key, maxLockConflicts, 0)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "scanning locks")
		} else if len(locks) > 0 {
			return result.Result{}, &kvpb.LockConflictError{Locks: locks}
		}
	}

	// Verify that the keys in the sstable are within the range specified by the
	// request header, and if the request did not include pre-computed stats,
	// compute the expected MVCC stats delta of ingesting the SST.
	sstIter, err := storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return result.Result{}, err
	}
	defer sstIter.Close()

	// Check that the first key is in the expected range.
	sstIter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
	if ok, err := sstIter.Valid(); err != nil {
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
		stats, err = storage.ComputeStatsForIter(sstIter, h.Timestamp.WallTime)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "computing SSTable MVCC stats")
		}
	}

	sstIter.SeekGE(end)
	if ok, err := sstIter.Valid(); err != nil {
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
		if statsDelta.ContainsEstimates == 0 {
			stats.ContainsEstimates = 0
		}
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

	reply := resp.(*kvpb.AddSSTableResponse)
	reply.RangeSpan = cArgs.EvalCtx.Desc().KeySpan().AsRawSpanWithNoLocals()
	reply.AvailableBytes = cArgs.EvalCtx.GetMaxBytes(ctx) - cArgs.EvalCtx.GetMVCCStats().Total() - stats.Total()

	// If requested, locate and return the start of the span following the file
	// span which may be non-empty, that is, the first key after the file's end
	// at which there may be existing data in the range. "May" is operative here:
	// it allows us to avoid consistency/isolation promises and thus avoid needing
	// to latch the entire remainder of the range and/or look through intents in
	// addition, and instead just use this key-only iterator. If a caller actually
	// needs to know what data is there, it must issue its own real Scan.
	if args.ReturnFollowingLikelyNonEmptySpanStart {
		existingIter, err := spanset.DisableReaderAssertions(readWriter).NewMVCCIterator(
			ctx,
			storage.MVCCKeyIterKind, // don't care if it is committed or not, just that it isn't empty.
			storage.IterOptions{
				KeyTypes:     storage.IterKeyTypePointsAndRanges,
				UpperBound:   reply.RangeSpan.EndKey,
				ReadCategory: fs.BatchEvalReadCategory,
			})
		if err != nil {
			return result.Result{}, errors.Wrap(err, "error when creating iterator for non-empty span")
		}
		defer existingIter.Close()
		existingIter.SeekGE(end)
		if ok, err := existingIter.Valid(); err != nil {
			return result.Result{}, errors.Wrap(err, "error while searching for non-empty span start")
		} else if ok {
			reply.FollowingLikelyNonEmptySpanStart = existingIter.UnsafeKey().Key.Clone()
		}
	}

	// Ingest the SST as regular writes if requested. This is *not* a general
	// transformation of any arbitrary SST to a WriteBatch: it assumes every key
	// in the SST is a simple Set or RangeKeySet. This is already assumed
	// elsewhere in this RPC though, so that's OK here.
	//
	// The MVCC engine writer methods do not record logical operations, but we
	// must use them because e.g. storage.MVCCPut() changes the semantics of the
	// write by not allowing writing below existing keys, and we want to retain
	// parity with regular SST ingestion which does allow this. We therefore
	// have to record logical ops ourselves.
	if args.IngestAsWrites {
		span.RecordStructured(&types.StringValue{Value: fmt.Sprintf("ingesting SST (%d keys/%d bytes) via regular write batch", stats.KeyCount, len(sst))})
		log.VEventf(ctx, 2, "ingesting SST (%d keys/%d bytes) via regular write batch", stats.KeyCount, len(sst))

		// Ingest point keys.
		pointIter, err := storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
			KeyTypes:   storage.IterKeyTypePointsOnly,
			UpperBound: keys.MaxKey,
		})
		if err != nil {
			return result.Result{}, err
		}
		defer pointIter.Close()

		for pointIter.SeekGE(storage.NilKey); ; pointIter.Next() {
			if ok, err := pointIter.Valid(); err != nil {
				return result.Result{}, err
			} else if !ok {
				break
			}
			key := pointIter.UnsafeKey()
			v, err := pointIter.UnsafeValue()
			if err != nil {
				return result.Result{}, err
			}
			if key.Timestamp.IsEmpty() {
				if err := readWriter.PutUnversioned(key.Key, v); err != nil {
					return result.Result{}, err
				}
			} else {
				if err := readWriter.PutRawMVCC(key, v); err != nil {
					return result.Result{}, err
				}
			}
			if sstToReqTS.IsSet() {
				readWriter.LogLogicalOp(storage.MVCCWriteValueOpType, storage.MVCCLogicalOpDetails{
					Key:       key.Key,
					Timestamp: key.Timestamp,
				})
			}
		}

		// Ingest range keys.
		rangeIter, err := storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
			KeyTypes:   storage.IterKeyTypeRangesOnly,
			UpperBound: keys.MaxKey,
		})
		if err != nil {
			return result.Result{}, err
		}
		defer rangeIter.Close()

		for rangeIter.SeekGE(storage.NilKey); ; rangeIter.Next() {
			if ok, err := rangeIter.Valid(); err != nil {
				return result.Result{}, err
			} else if !ok {
				break
			}
			rangeKeys := rangeIter.RangeKeys()
			for _, v := range rangeKeys.Versions {
				if err = readWriter.PutRawMVCCRangeKey(rangeKeys.AsRangeKey(v), v.Value); err != nil {
					return result.Result{}, err
				}
				if sstToReqTS.IsSet() {
					readWriter.LogLogicalOp(storage.MVCCDeleteRangeOpType, storage.MVCCLogicalOpDetails{
						Key:       rangeKeys.Bounds.Key,
						EndKey:    rangeKeys.Bounds.EndKey,
						Timestamp: v.Timestamp,
					})
				}
			}
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
// * No intents or unversioned values.
// * If sstTimestamp is set, all MVCC timestamps equal it.
// * The LocalTimestamp in the MVCCValueHeader is empty.
// * Given MVCC stats match the SST contents.
func assertSSTContents(sst []byte, sstTimestamp hlc.Timestamp, stats *enginepb.MVCCStats) error {

	// Check SST point keys.
	iter, err := storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.SeekGE(storage.NilKey); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		key := iter.UnsafeKey()
		if key.Timestamp.IsEmpty() {
			return errors.AssertionFailedf("SST contains inline value or intent for key %s", key)
		}
		if sstTimestamp.IsSet() && key.Timestamp != sstTimestamp {
			return errors.AssertionFailedf("SST has unexpected timestamp %s (expected %s) for key %s",
				key.Timestamp, sstTimestamp, key.Key)
		}
		value, err := storage.DecodeMVCCValueAndErr(iter.UnsafeValue())
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"SST contains invalid value for key %s", key)
		}
		if !value.MVCCValueHeader.LocalTimestamp.IsEmpty() {
			return errors.AssertionFailedf("SST contains non-empty Local Timestamp in the MVCC value"+
				" header for key %s", key)
		}
	}

	// Check SST range keys.
	iter, err = storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.SeekGE(storage.NilKey); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		rangeKeys := iter.RangeKeys()
		for _, v := range rangeKeys.Versions {
			rangeKey := rangeKeys.AsRangeKey(v)
			if err := rangeKey.Validate(); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err, "SST contains invalid range key")
			}
			if sstTimestamp.IsSet() && v.Timestamp != sstTimestamp {
				return errors.AssertionFailedf(
					"SST has unexpected timestamp %s (expected %s) for range key %s",
					v.Timestamp, sstTimestamp, rangeKeys.Bounds)
			}
			value, err := storage.DecodeMVCCValue(v.Value)
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"SST contains invalid range key value for range key %s", rangeKey)
			}
			if !value.IsTombstone() {
				return errors.AssertionFailedf("SST contains non-tombstone range key %s", rangeKey)
			}
			if value.MVCCValueHeader != (enginepb.MVCCValueHeader{}) {
				return errors.AssertionFailedf("SST contains non-empty MVCC value header for range key %s",
					rangeKey)
			}
		}
	}

	// Compare statistics with those passed by client. We calculate them at the
	// same timestamp as the given statistics, since they may contain
	// timing-dependent values (typically MVCC garbage, i.e. multiple versions).
	if stats != nil {
		iter, err = storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
			KeyTypes:   storage.IterKeyTypePointsAndRanges,
			LowerBound: keys.MinKey,
			UpperBound: keys.MaxKey,
		})
		if err != nil {
			return err
		}
		defer iter.Close()
		iter.SeekGE(storage.MVCCKey{Key: keys.MinKey})

		given := *stats
		actual, err := storage.ComputeStatsForIter(iter, given.LastUpdateNanos)
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
