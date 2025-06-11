// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk/bulkpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type sstAdder struct {
	settings *cluster.Settings
	db       *kv.DB

	// disallowShadowingBelow is described on kvpb.AddSSTableRequest.
	disallowShadowingBelow hlc.Timestamp

	// priority is the admission priority used for AddSSTable
	// requests.
	priority admissionpb.WorkPriority

	// writeAtBatchTS is true if the SST should be written at the
	// batch timestamp. If this is set to true, then all the keys in
	//the sst must have the same timestamp and they must be equal to
	// the batch timestamp.
	// TODO(jeffswenson): remove this from `sstAdder` in a stand alone PR.
	writeAtBatchTS bool
}

func newSSTAdder(
	db *kv.DB,
	settings *cluster.Settings,
	writeAtBatchTS bool,
	disallowShadowingBelow hlc.Timestamp,
	priority admissionpb.WorkPriority,
) *sstAdder {
	return &sstAdder{
		db:                     db,
		disallowShadowingBelow: disallowShadowingBelow,
		priority:               priority,
		settings:               settings,
		writeAtBatchTS:         writeAtBatchTS,
	}
}

type sstSpan struct {
	start, end roachpb.Key // [inclusive, exclusive)
	sstBytes   []byte
	stats      enginepb.MVCCStats
}

type addSSTResult struct {
	timestamp                        hlc.Timestamp
	availableBytes                   int64
	followingLikelyNonEmptySpanStart roachpb.Key
	rangeSpan                        roachpb.Span
}

// addSSTable retries db.AddSSTable if retryable errors occur, including if the
// SST spans a split, in which case it is iterated and split into two SSTs, one
// for each side of the split in the error, and each are retried.
func (a *sstAdder) AddSSTable(
	ctx context.Context,
	batchTS hlc.Timestamp,
	start, end roachpb.Key,
	sstBytes []byte,
	stats enginepb.MVCCStats,
	ingestionPerformanceStats *bulkpb.IngestionPerformanceStats,
) ([]addSSTResult, error) {
	ctx, sp := tracing.ChildSpan(ctx, "*SSTBatcher.addSSTable")
	defer sp.Finish()

	sendStart := timeutil.Now()
	if ingestionPerformanceStats == nil {
		return nil, errors.AssertionFailedf("ingestionPerformanceStats should not be nil")
	}

	// Currently, the SSTBatcher cannot ingest range keys, so it is safe to
	// ComputeStats with an iterator that only surfaces point keys.
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: start,
		UpperBound: end,
	}
	iter, err := storage.NewMemSSTIterator(sstBytes, true, iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if stats == (enginepb.MVCCStats{}) {
		// TODO(jeffswenson): Audit AddSST callers to see if they generate
		// server side stats now. Accurately computing stats in the face of replays
		// requires the server to do it.
		iter.SeekGE(storage.MVCCKey{Key: start})
		// NB: even though this ComputeStatsForIter call exhausts the iterator, we
		// can reuse/re-seek on the iterator, as part of the MVCCIterator contract.
		stats, err = storage.ComputeStatsForIter(iter, sendStart.UnixNano())
		if err != nil {
			return nil, errors.Wrapf(err, "computing stats for SST [%s, %s)", start, end)
		}
	}

	work := []*sstSpan{{start: start, end: end, sstBytes: sstBytes, stats: stats}}
	var results []addSSTResult
	var files int
	for len(work) > 0 {
		item := work[0]
		work = work[1:]
		if err := func() error {
			var err error
			opts := retry.Options{
				InitialBackoff: 30 * time.Millisecond,
				Multiplier:     2,
				MaxRetries:     10,
			}
			for r := retry.StartWithCtx(ctx, opts); r.Next(); {
				log.VEventf(ctx, 4, "sending %s AddSSTable [%s,%s)", sz(len(item.sstBytes)), item.start, item.end)
				// If this SST is "too small", the fixed costs associated with adding an
				// SST – in terms of triggering flushes, extra compactions, etc – would
				// exceed the savings we get from skipping regular, key-by-key writes,
				// and we're better off just putting its contents in a regular batch.
				// This isn't perfect: We're still incurring extra overhead constructing
				// SSTables just for use as a wire-format, but the rest of the
				// implementation of bulk-ingestion assumes certainly semantics of the
				// AddSSTable API - like ingest at arbitrary timestamps or collision
				// detection - making it is simpler to just always use the same API
				// and just switch how it writes its result.
				ingestAsWriteBatch := false
				if a.settings != nil && int64(len(item.sstBytes)) < tooSmallSSTSize.Get(&a.settings.SV) {
					log.VEventf(ctx, 3, "ingest data is too small (%d keys/%d bytes) for SSTable, adding via regular batch", item.stats.KeyCount, len(item.sstBytes))
					ingestAsWriteBatch = true
					ingestionPerformanceStats.AsWrites++
				}

				req := &kvpb.AddSSTableRequest{
					RequestHeader:                          kvpb.RequestHeader{Key: item.start, EndKey: item.end},
					Data:                                   item.sstBytes,
					DisallowShadowingBelow:                 a.disallowShadowingBelow,
					MVCCStats:                              &item.stats,
					IngestAsWrites:                         ingestAsWriteBatch,
					ReturnFollowingLikelyNonEmptySpanStart: true,
				}
				if a.writeAtBatchTS {
					req.SSTTimestampToRequestTimestamp = batchTS
				}

				ba := &kvpb.BatchRequest{
					Header: kvpb.Header{Timestamp: batchTS, ClientRangeInfo: roachpb.ClientRangeInfo{ExplicitlyRequested: true}},
					AdmissionHeader: kvpb.AdmissionHeader{
						Priority:                 int32(a.priority),
						CreateTime:               timeutil.Now().UnixNano(),
						Source:                   kvpb.AdmissionHeader_FROM_SQL,
						NoMemoryReservedAtSource: true,
					},
				}

				ba.Add(req)
				beforeSend := timeutil.Now()

				sendCtx, sendSp := tracing.ChildSpan(ctx, "*SSTBatcher.addSSTable/Send")
				br, pErr := a.db.NonTransactionalSender().Send(sendCtx, ba)
				sendSp.Finish()

				sendTime := timeutil.Since(beforeSend)

				ingestionPerformanceStats.SendWait += sendTime
				if br != nil && len(br.BatchResponse_Header.RangeInfos) > 0 {
					// Should only ever really be one iteration but if somehow it isn't,
					// e.g. if a request was redirected, go ahead and count it against all
					// involved stores; if it is small this edge case is immaterial, and
					// if it is large, it's probably one big one but we don't know which
					// so just blame them all (averaging it out could hide one big delay).
					for i := range br.BatchResponse_Header.RangeInfos {
						ingestionPerformanceStats.SendWaitByStore[br.BatchResponse_Header.RangeInfos[i].Lease.Replica.StoreID] += sendTime
					}
				}

				if pErr == nil {
					resp := br.Responses[0].GetInner().(*kvpb.AddSSTableResponse)
					results = append(results, addSSTResult{
						timestamp:                        br.Timestamp,
						availableBytes:                   resp.AvailableBytes,
						followingLikelyNonEmptySpanStart: resp.FollowingLikelyNonEmptySpanStart,
						rangeSpan:                        resp.RangeSpan,
					})
					files++
					log.VEventf(ctx, 3, "adding %s AddSSTable [%s,%s) took %v", sz(len(item.sstBytes)), item.start, item.end, sendTime)
					return nil
				}

				err = pErr.GoError()
				// Retry on AmbiguousResult.
				if errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) {
					log.Warningf(ctx, "addsstable [%s,%s) attempt %d failed: %+v", start, end, r.CurrentAttempt(), err)
					continue
				}
				// This range has split -- we need to split the SST to try again.
				if m := (*kvpb.RangeKeyMismatchError)(nil); errors.As(err, &m) {
					// TODO(andrei): We just use the first of m.Ranges; presumably we
					// should be using all of them to avoid further retries.
					mr, err := m.MismatchedRange()
					if err != nil {
						return err
					}
					split := mr.Desc.EndKey.AsRawKey()
					log.Infof(ctx, "SSTable cannot be added spanning range bounds %v, retrying...", split)
					left, right, err := createSplitSSTable(ctx, item.start, split, iter, a.settings)
					if err != nil {
						return err
					}
					if err := addStatsToSplitTables(left, right, item, sendStart); err != nil {
						return err
					}
					// Add more work.
					work = append([]*sstSpan{left, right}, work...)
					return nil
				}
			}
			return err
		}(); err != nil {
			return nil, errors.Wrapf(err, "addsstable [%s,%s)", item.start, item.end)
		}
		// explicitly deallocate SST. This will not deallocate the
		// top level SST which is kept around to iterate over.
		item.sstBytes = nil
	}
	ingestionPerformanceStats.SplitRetries += int64(files - 1)

	log.VEventf(ctx, 3, "AddSSTable [%v, %v) added %d files and took %v", start, end, files, timeutil.Since(sendStart))
	return results, nil
}

// createSplitSSTable is a helper for splitting up SSTs. The iterator
// passed in is over the top level SST passed into AddSSTTable().
func createSplitSSTable(
	ctx context.Context,
	start, splitKey roachpb.Key,
	iter storage.SimpleMVCCIterator,
	settings *cluster.Settings,
) (*sstSpan, *sstSpan, error) {
	sstFile := &storage.MemObject{}
	if start.Compare(splitKey) >= 0 {
		return nil, nil, errors.AssertionFailedf("start key %s of original sst must be greater than than split key %s", start, splitKey)
	}
	w := storage.MakeIngestionSSTWriter(ctx, settings, sstFile)
	defer w.Close()

	split := false
	var first, last roachpb.Key
	var left, right *sstSpan

	iter.SeekGE(storage.MVCCKey{Key: start})
	for {
		if ok, err := iter.Valid(); err != nil {
			return nil, nil, err
		} else if !ok {
			break
		}

		key := iter.UnsafeKey()

		if !split && key.Key.Compare(splitKey) >= 0 {
			err := w.Finish()
			if err != nil {
				return nil, nil, err
			}

			left = &sstSpan{start: first, end: last.Next(), sstBytes: sstFile.Data()}
			*sstFile = storage.MemObject{}
			w = storage.MakeIngestionSSTWriter(ctx, settings, sstFile)
			split = true
			first = nil
			last = nil
		}

		if len(first) == 0 {
			first = append(first[:0], key.Key...)
		}
		last = append(last[:0], key.Key...)

		v, err := iter.UnsafeValue()
		if err != nil {
			return nil, nil, err
		}
		if err := w.Put(key, v); err != nil {
			return nil, nil, err
		}

		iter.Next()
	}

	err := w.Finish()
	if err != nil {
		return nil, nil, err
	}
	if !split {
		return nil, nil, errors.AssertionFailedf("split key %s after last key %s", splitKey, last.Next())
	}
	right = &sstSpan{start: first, end: last.Next(), sstBytes: sstFile.Data()}
	return left, right, nil
}

// addStatsToSplitTables computes the stats of the new lhs and rhs SSTs by
// computing the rhs sst stats, then computing the lhs stats as
// originalStats-rhsStats.
func addStatsToSplitTables(left, right, original *sstSpan, sendStartTimestamp time.Time) error {
	// Needs a new iterator with new bounds.
	statsIter, err := storage.NewMemSSTIterator(original.sstBytes, true, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: right.start,
		UpperBound: right.end,
	})
	if err != nil {
		return err
	}
	statsIter.SeekGE(storage.MVCCKey{Key: right.start})
	right.stats, err = storage.ComputeStatsForIter(statsIter, sendStartTimestamp.Unix())
	statsIter.Close()
	if err != nil {
		return err
	}
	left.stats = original.stats
	left.stats.Subtract(right.stats)
	return nil
}
