// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// maxScatterSize is the size limit included in scatters sent for as-we-write
// splits which expect to just move empty spans to balance ingestion, to avoid
// them becoming expensive moves of existing data if sent to a non-empty range.
const maxScatterSize = 4 << 20

var (
	tooSmallSSTSize = settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"kv.bulk_io_write.small_write_size",
		"size below which a 'bulk' write will be performed as a normal write instead",
		400*1<<10, // 400 Kib
	)

	ingestDelay = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"bulkio.ingest.flush_delay",
		"amount of time to wait before sending a file to the KV/Storage layer to ingest",
		0,
		settings.NonNegativeDuration,
	)
)

// SSTBatcher is a helper for bulk-adding many KVs in chunks via AddSSTable. An
// SSTBatcher can be handed KVs repeatedly and will make them into SSTs that are
// added when they reach the configured size, tracking the total added rows,
// bytes, etc. If configured with a non-nil, populated range cache, it will use
// it to attempt to flush SSTs before they cross range boundaries to minimize
// expensive on-split retries.
type SSTBatcher struct {
	name     string
	db       *kv.DB
	rc       *rangecache.RangeCache
	settings *cluster.Settings

	// disallowShadowingBelow is described on roachpb.AddSSTableRequest.
	disallowShadowingBelow hlc.Timestamp

	// skips duplicate keys (iff they are buffered together). This is true when
	// used to backfill an inverted index. An array in JSONB with multiple values
	// which are the same, will all correspond to the same kv in the inverted
	// index. The method which generates these kvs does not dedup, thus we rely on
	// the SSTBatcher to dedup them (by skipping), rather than throwing a
	// DuplicateKeyError.
	// This is also true when used with IMPORT. Import
	// generally prohibits the ingestion of KVs which will shadow existing data,
	// with the exception of duplicates having the same value and timestamp. To
	// maintain uniform behavior, duplicates in the same batch with equal values
	// will not raise a DuplicateKeyError.
	skipDuplicates bool
	// ingestAll can only be set when disallowShadowingBelow is empty and
	// skipDuplicates is false. It will never return a duplicateKey error and
	// continue ingesting all data provided to it.
	ingestAll bool

	// batchTS is the timestamp that will be set on batch requests used to send
	// produced SSTs.
	batchTS hlc.Timestamp

	// writeAtBatchTS is passed to the writeAtBatchTs argument to db.AddSStable.
	writeAtBatchTS bool

	initialSplitDone bool

	// The rest of the fields accumulated state as opposed to configuration. Some,
	// like totalRows, are accumulated _across_ batches and are not reset between
	// batches when Reset() is called.
	totalRows     roachpb.BulkOpSummary
	stats         ingestionPerformanceStats
	disableSplits bool

	maxWriteTS hlc.Timestamp

	// The rest of the fields are per-batch and are reset via Reset() before each
	// batch is started.
	sstWriter       storage.SSTWriter
	sstFile         *storage.MemFile
	batchStartKey   []byte
	batchEndKey     []byte
	batchEndValue   []byte
	flushKeyChecked bool
	flushKey        roachpb.Key
	// lastRange is the span and remaining capacity of the last range added to,
	// for checking if the next addition would overfill it.
	lastRange struct {
		span            roachpb.Span
		remaining       sz
		nextExistingKey roachpb.Key
	}
	// stores on-the-fly stats for the SST if disallowShadowingBelow is set.
	ms enginepb.MVCCStats
	// rows written in the current batch.
	rowCounter storage.RowCounter
}

// MakeSSTBatcher makes a ready-to-use SSTBatcher.
func MakeSSTBatcher(
	ctx context.Context,
	name string,
	db *kv.DB,
	settings *cluster.Settings,
	disallowShadowingBelow hlc.Timestamp,
	writeAtBatchTs bool,
	splitFilledRanges bool,
) (*SSTBatcher, error) {
	b := &SSTBatcher{
		name:                   name,
		db:                     db,
		settings:               settings,
		disallowShadowingBelow: disallowShadowingBelow,
		writeAtBatchTS:         writeAtBatchTs,
		disableSplits:          !splitFilledRanges,
		stats:                  ingestionPerformanceStats{sendWaitByStore: make(map[roachpb.StoreID]time.Duration)},
	}
	err := b.Reset(ctx)
	return b, err
}

// MakeStreamSSTBatcher creates a batcher configured to ingest duplicate keys
// that might be received from a cluster to cluster stream.
func MakeStreamSSTBatcher(
	ctx context.Context, db *kv.DB, settings *cluster.Settings,
) (*SSTBatcher, error) {
	b := &SSTBatcher{db: db, settings: settings, ingestAll: true, stats: ingestionPerformanceStats{sendWaitByStore: make(map[roachpb.StoreID]time.Duration)}}
	err := b.Reset(ctx)
	return b, err
}

func (b *SSTBatcher) updateMVCCStats(key storage.MVCCKey, value []byte) {
	metaKeySize := int64(len(key.Key)) + 1
	metaValSize := int64(0)
	b.ms.LiveBytes += metaKeySize
	b.ms.LiveCount++
	b.ms.KeyBytes += metaKeySize
	b.ms.ValBytes += metaValSize
	b.ms.KeyCount++

	totalBytes := int64(len(value)) + storage.MVCCVersionTimestampSize
	b.ms.LiveBytes += totalBytes
	b.ms.KeyBytes += storage.MVCCVersionTimestampSize
	b.ms.ValBytes += int64(len(value))
	b.ms.ValCount++
}

// AddMVCCKey adds a key+timestamp/value pair to the batch (flushing if needed).
// This is only for callers that want to control the timestamp on individual
// keys -- like RESTORE where we want the restored data to look like the backup.
// Keys must be added in order.
func (b *SSTBatcher) AddMVCCKey(ctx context.Context, key storage.MVCCKey, value []byte) error {
	if len(b.batchEndKey) > 0 && bytes.Equal(b.batchEndKey, key.Key) && !b.ingestAll {
		if b.skipDuplicates && bytes.Equal(b.batchEndValue, value) {
			return nil
		}

		err := &kvserverbase.DuplicateKeyError{}
		err.Key = append(err.Key, key.Key...)
		err.Value = append(err.Value, value...)
		return err
	}
	// Check if we need to flush current batch *before* adding the next k/v --
	// the batcher may want to flush the keys it already has, either because it
	// is full or because it wants this key in a separate batch due to splits.
	if err := b.flushIfNeeded(ctx, key.Key); err != nil {
		return err
	}

	if b.writeAtBatchTS {
		if b.batchTS.IsEmpty() {
			b.batchTS = b.db.Clock().Now()
		}
		key.Timestamp = b.batchTS
	}

	// Update the range currently represented in this batch, as necessary.
	if len(b.batchStartKey) == 0 {
		b.batchStartKey = append(b.batchStartKey[:0], key.Key...)
	}
	b.batchEndKey = append(b.batchEndKey[:0], key.Key...)
	b.batchEndValue = append(b.batchEndValue[:0], value...)

	if err := b.rowCounter.Count(key.Key); err != nil {
		return err
	}

	// If we do not allowing shadowing of keys when ingesting an SST via
	// AddSSTable, then we can update the MVCCStats on the fly because we are
	// guaranteed to ingest unique keys. This saves us an extra iteration in
	// AddSSTable which has been identified as a significant performance
	// regression for IMPORT.
	if !b.disallowShadowingBelow.IsEmpty() {
		b.updateMVCCStats(key, value)
	}

	return b.sstWriter.Put(key, value)
}

// Reset clears all state in the batcher and prepares it for reuse.
func (b *SSTBatcher) Reset(ctx context.Context) error {
	b.sstWriter.Close()
	b.sstFile = &storage.MemFile{}
	// Create "Ingestion" SSTs in the newer RocksDBv2 format only if  all nodes
	// in the cluster can support it. Until then, for backward compatibility,
	// create SSTs in the leveldb format ("backup" ones).
	b.sstWriter = storage.MakeIngestionSSTWriter(ctx, b.settings, b.sstFile)
	b.batchStartKey = b.batchStartKey[:0]
	b.batchEndKey = b.batchEndKey[:0]
	b.batchEndValue = b.batchEndValue[:0]
	b.flushKey = nil
	b.flushKeyChecked = false
	b.ms.Reset()

	if b.writeAtBatchTS {
		b.batchTS = hlc.Timestamp{}
	}

	b.rowCounter.BulkOpSummary.Reset()
	return nil
}

const (
	manualFlush = iota
	sizeFlush
	rangeFlush
)

func (b *SSTBatcher) flushIfNeeded(ctx context.Context, nextKey roachpb.Key) error {
	// If this is the first key we have seen (since being reset), attempt to find
	// the end of the range it is in so we can flush the SST before crossing it,
	// because AddSSTable cannot span ranges and will need to be split and retried
	// from scratch if we generate an SST that ends up doing so.
	if !b.flushKeyChecked && b.rc != nil {
		b.flushKeyChecked = true
		if k, err := keys.Addr(nextKey); err != nil {
			log.Warningf(ctx, "failed to get RKey for flush key lookup")
		} else {
			r := b.rc.GetCached(ctx, k, false /* inverted */)
			if r != nil {
				b.flushKey = r.Desc().EndKey.AsRawKey()
				log.VEventf(ctx, 3, "%s building sstable that will flush before %v", b.name, b.flushKey)
			} else {
				log.VEventf(ctx, 2, "%s no cached range desc available to determine sst flush key", b.name)
			}
		}
	}

	if b.flushKey != nil && b.flushKey.Compare(nextKey) <= 0 {
		if err := b.doFlush(ctx, rangeFlush); err != nil {
			return err
		}
		return b.Reset(ctx)
	}

	if b.sstWriter.DataSize >= ingestFileSize(b.settings) {
		// We're at/over size target, so we want to flush, but first check if we are
		// at a new row boundary. Having row-aligned boundaries is not actually
		// required by anything, but has the nice property of meaning a split will
		// fall between files. This is particularly useful mid-IMPORT when we split
		// at the beginning of the next file, as that split is at the row boundary
		// at *or before* that file's start. If the file boundary is not aligned,
		// the prior file overhangs beginning of the row in which the next file
		// starts, so when we split at that row, that overhang into the RHS that we
		// just wrote will be rewritten by the subsequent scatter. By waiting for a
		// row boundary, we ensure any split is actually between files.
		prevRow, prevErr := keys.EnsureSafeSplitKey(b.batchEndKey)
		nextRow, nextErr := keys.EnsureSafeSplitKey(nextKey)
		if prevErr == nil && nextErr == nil && bytes.Equal(prevRow, nextRow) {
			// An error decoding either key implies it is not a valid row key and thus
			// not the same row for our purposes; we don't care what the error is.
			return nil // keep going to row boundary.
		}
		if err := b.doFlush(ctx, sizeFlush); err != nil {
			return err
		}
		return b.Reset(ctx)
	}
	return nil
}

// Flush sends the current batch, if any.
func (b *SSTBatcher) Flush(ctx context.Context) error {
	if err := b.doFlush(ctx, manualFlush); err != nil {
		return err
	}
	if !b.maxWriteTS.IsEmpty() {
		if now := b.db.Clock().Now(); now.Less(b.maxWriteTS) {
			guess := timing(b.maxWriteTS.WallTime - now.WallTime)
			log.VEventf(ctx, 1, "%s batcher waiting %s until max write time %s", b.name, guess, b.maxWriteTS)
			if err := b.db.Clock().SleepUntil(ctx, b.maxWriteTS); err != nil {
				return err
			}
			b.stats.commitWait += timeutil.Since(now.GoTime())
		}
		b.maxWriteTS.Reset()
	}
	return nil
}

func (b *SSTBatcher) doFlush(ctx context.Context, reason int) error {
	if b.sstWriter.DataSize == 0 {
		return nil
	}
	beforeFlush := timeutil.Now()

	b.stats.batches++

	if delay := ingestDelay.Get(&b.settings.SV); delay != 0 {
		if delay > time.Second || log.V(1) {
			log.Infof(ctx, "%s delaying %s before flushing ingestion buffer...", b.name, delay)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	if err := b.sstWriter.Finish(); err != nil {
		return errors.Wrapf(err, "finishing constructed sstable")
	}

	start := roachpb.Key(append([]byte(nil), b.batchStartKey...))
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(append([]byte(nil), b.batchEndKey...)).Next()

	size := sz(b.sstWriter.DataSize)

	if reason == sizeFlush {
		log.VEventf(ctx, 3, "%s flushing %s SST due to size > %s", b.name, size, sz(ingestFileSize(b.settings)))
		b.stats.batchesDueToSize++
	} else if reason == rangeFlush {
		log.VEventf(ctx, 3, "%s flushing %s SST due to range boundary %s", b.name, size, b.flushKey)
		b.stats.batchesDueToRange++
	}

	shouldSplit := false
	if !b.disableSplits {
		if b.lastRange.span.ContainsKey(start) && size >= b.lastRange.remaining {
			// If this file is starting in the same span we last added to and is bigger
			// than the size that range had when we last added to it, then we should
			// split off the suffix of that range where this file starts and add it to
			// that new range after scattering it.
			log.VEventf(ctx, 2, "%s batcher splitting full range %s before adding file starting at %s",
				b.name, b.lastRange.span, start)
			shouldSplit = true
		} else if reason == sizeFlush && !b.initialSplitDone && b.stats.batches == 1 {
			// If we didn't make initial splits, and this is our first flush and is due
			// to filling the buffer, then we may have our own span and should drop a
			// split at the first key to separate it out.
			log.VEventf(ctx, 1, "%s splitting on first flush to separate ingestion span using key %s",
				b.name, start)
			shouldSplit = true
		}
	}

	if shouldSplit {
		expire := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Minute * 10).UnixNano()}

		// If there was existing data as of last add that is above the file we are
		// about to add, split there first, both since that could be why the range
		// is full as well as because if we do not, that existing data will need to
		// be moved when we scatter the span we're splitting for ingestion.
		if len(b.lastRange.nextExistingKey) > 0 && end.Compare(b.lastRange.nextExistingKey) < 0 {
			log.VEventf(ctx, 2, "%s splitting above file span %s at existing key %s",
				b.name, roachpb.Span{Key: start, EndKey: end}, b.lastRange.nextExistingKey)
			splitAbove, err := keys.EnsureSafeSplitKey(b.lastRange.nextExistingKey)
			if err != nil {
				log.Warningf(ctx, "%s failed to generate split-above key: %v", b.name, err)
			} else {
				beforeSplit := timeutil.Now()
				err := b.db.AdminSplit(ctx, splitAbove, expire)
				b.stats.splitWait += timeutil.Since(beforeSplit)
				if err != nil {
					log.Warningf(ctx, "%s failed to split-above: %v", b.name, err)
				} else {
					b.stats.splits++
				}
			}
		}

		splitAt, err := keys.EnsureSafeSplitKey(start)
		if err != nil {
			log.Warningf(ctx, "%s failed to generate split key: %v", b.name, err)
		} else {
			beforeSplit := timeutil.Now()
			err := b.db.AdminSplit(ctx, splitAt, expire)
			b.stats.splitWait += timeutil.Since(beforeSplit)
			if err != nil {
				log.Warningf(ctx, "%s failed to split: %v", b.name, err)
			} else {
				b.stats.splits++

				// Now scatter the RHS before we proceed to ingest into it. We know it
				// should be empty since we split above if there was a nextExistingKey.
				beforeScatter := timeutil.Now()
				resp, err := b.db.AdminScatter(ctx, splitAt, maxScatterSize)
				b.stats.scatterWait += timeutil.Since(beforeScatter)
				if err != nil {
					// err could be a max size violation, but this is unexpected since we
					// split before, so a warning is probably ok.
					log.Warningf(ctx, "%s failed to scatter	: %v", b.name, err)
				} else {
					b.stats.scatters++
					if resp.MVCCStats != nil {
						moved := sz(resp.MVCCStats.Total())
						b.stats.scatterMoved += moved
						if moved > 0 {
							log.VEventf(ctx, 1, "%s split scattered %s in non-empty range %s", b.name, moved, resp.RangeInfos[0].Desc.KeySpan().AsRawSpanWithNoLocals())
						}
					}
				}
			}
		}
	}

	// If the stats have been computed on-the-fly, set the last updated time
	// before ingesting the SST.
	if (b.ms != enginepb.MVCCStats{}) {
		b.ms.LastUpdateNanos = timeutil.Now().UnixNano()
	}

	if err := b.addSSTable(ctx, start, end, b.sstFile.Data()); err != nil {
		return err
	}

	b.rowCounter.DataSize += b.sstWriter.DataSize
	b.totalRows.Add(b.rowCounter.BulkOpSummary)
	b.stats.batchWait += timeutil.Since(beforeFlush)
	b.stats.dataSize += sz(b.sstWriter.DataSize)
	return nil
}

// Close closes the underlying SST builder.
func (b *SSTBatcher) Close() {
	b.sstWriter.Close()
}

// GetBatchSummary returns this batcher's total added rows/bytes/etc.
func (b *SSTBatcher) GetBatchSummary() roachpb.BulkOpSummary {
	return b.rowCounter.BulkOpSummary
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *SSTBatcher) GetSummary() roachpb.BulkOpSummary {
	return b.totalRows
}

type sstSpan struct {
	start, end roachpb.Key
	sstBytes   []byte
	stats      enginepb.MVCCStats
}

// addSSTable retries db.AddSSTable if retryable errors occur, including if the
// SST spans a split, in which case it is iterated and split into two SSTs, one
// for each side of the split in the error, and each are retried.
func (b *SSTBatcher) addSSTable(
	ctx context.Context, start, end roachpb.Key, sstBytes []byte,
) error {
	sendStart := timeutil.Now()
	iter, err := storage.NewMemSSTIterator(sstBytes, true)
	if err != nil {
		return err
	}
	defer iter.Close()

	var stats enginepb.MVCCStats
	if (b.ms == enginepb.MVCCStats{}) {
		stats, err = storage.ComputeStatsForRange(iter, start, end, sendStart.UnixNano())
		if err != nil {
			return errors.Wrapf(err, "computing stats for SST [%s, %s)", start, end)
		}
	} else {
		stats = b.ms
	}

	work := []*sstSpan{{start: start, end: end, sstBytes: sstBytes, stats: stats}}
	var files int
	const maxAddSSTableRetries = 10
	for len(work) > 0 {
		item := work[0]
		work = work[1:]
		if err := func() error {
			var err error
			for i := 0; i < maxAddSSTableRetries; i++ {
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
				if b.settings != nil && int64(len(item.sstBytes)) < tooSmallSSTSize.Get(&b.settings.SV) {
					log.VEventf(ctx, 3, "ingest data is too small (%d keys/%d bytes) for SSTable, adding via regular batch", item.stats.KeyCount, len(item.sstBytes))
					ingestAsWriteBatch = true
				}

				req := &roachpb.AddSSTableRequest{
					RequestHeader:                          roachpb.RequestHeader{Key: item.start, EndKey: item.end},
					Data:                                   item.sstBytes,
					DisallowShadowing:                      !b.disallowShadowingBelow.IsEmpty(),
					DisallowShadowingBelow:                 b.disallowShadowingBelow,
					MVCCStats:                              &item.stats,
					IngestAsWrites:                         ingestAsWriteBatch,
					ReturnFollowingLikelyNonEmptySpanStart: true,
				}
				if b.writeAtBatchTS {
					req.SSTTimestampToRequestTimestamp = b.batchTS
				}

				ba := roachpb.BatchRequest{
					Header: roachpb.Header{Timestamp: b.batchTS, ClientRangeInfo: roachpb.ClientRangeInfo{ExplicitlyRequested: true}},
					AdmissionHeader: roachpb.AdmissionHeader{
						Priority:                 int32(admission.BulkNormalPri),
						CreateTime:               timeutil.Now().UnixNano(),
						Source:                   roachpb.AdmissionHeader_FROM_SQL,
						NoMemoryReservedAtSource: true,
					},
				}
				ba.Add(req)
				beforeSend := timeutil.Now()
				br, pErr := b.db.NonTransactionalSender().Send(ctx, ba)
				sendTime := timeutil.Since(beforeSend)
				b.stats.sendWait += sendTime

				if br != nil && len(br.BatchResponse_Header.RangeInfos) > 0 {
					// Should only ever really be one iteration but if somehow it isn't,
					// e.g. if a request was redirected, go ahead and count it against all
					// involved stores; if it is small this edge case is immaterial, and
					// if it is large, it's probably one big one but we don't know which
					// so just blame them all (averaging it out could hide one big delay).
					for i := range br.BatchResponse_Header.RangeInfos {
						b.stats.sendWaitByStore[br.BatchResponse_Header.RangeInfos[i].Lease.Replica.StoreID] += sendTime
					}
				}

				if pErr == nil {
					resp := br.Responses[0].GetInner().(*roachpb.AddSSTableResponse)
					if b.writeAtBatchTS {
						b.maxWriteTS.Forward(br.Timestamp)
					}
					b.lastRange.span = resp.RangeSpan
					if resp.RangeSpan.Valid() {
						b.flushKey = resp.RangeSpan.EndKey
						b.lastRange.remaining = sz(resp.AvailableBytes)
						b.lastRange.nextExistingKey = resp.FollowingLikelyNonEmptySpanStart
					}
					files++
					log.VEventf(ctx, 3, "adding %s AddSSTable [%s,%s) took %v", sz(len(item.sstBytes)), item.start, item.end, sendTime)
					return nil
				}

				err = pErr.GoError()
				// Retry on AmbiguousResult.
				if errors.HasType(err, (*roachpb.AmbiguousResultError)(nil)) {
					log.Warningf(ctx, "addsstable [%s,%s) attempt %d failed: %+v", start, end, i, err)
					continue
				}
				// This range has split -- we need to split the SST to try again.
				if m := (*roachpb.RangeKeyMismatchError)(nil); errors.As(err, &m) {
					// TODO(andrei): We just use the first of m.Ranges; presumably we
					// should be using all of them to avoid further retries.
					mr, err := m.MismatchedRange()
					if err != nil {
						return err
					}
					split := mr.Desc.EndKey.AsRawKey()
					log.Infof(ctx, "SSTable cannot be added spanning range bounds %v, retrying...", split)
					left, right, err := createSplitSSTable(ctx, item.start, split, iter, b.settings)
					if err != nil {
						return err
					}

					right.stats, err = storage.ComputeStatsForRange(
						iter, right.start, right.end, sendStart.Unix(),
					)
					if err != nil {
						return err
					}
					left.stats = item.stats
					left.stats.Subtract(right.stats)

					// Add more work.
					work = append([]*sstSpan{left, right}, work...)
					return nil
				}
			}
			return err
		}(); err != nil {
			return errors.Wrapf(err, "addsstable [%s,%s)", item.start, item.end)
		}
		// explicitly deallocate SST. This will not deallocate the
		// top level SST which is kept around to iterate over.
		item.sstBytes = nil
	}
	b.stats.splitRetries += files - 1
	log.VEventf(ctx, 3, "AddSSTable [%v, %v) added %d files and took %v", start, end, files, timeutil.Since(sendStart))
	return nil
}

// createSplitSSTable is a helper for splitting up SSTs. The iterator
// passed in is over the top level SST passed into AddSSTTable().
func createSplitSSTable(
	ctx context.Context,
	start, splitKey roachpb.Key,
	iter storage.SimpleMVCCIterator,
	settings *cluster.Settings,
) (*sstSpan, *sstSpan, error) {
	sstFile := &storage.MemFile{}
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

			left = &sstSpan{start: first, end: last.PrefixEnd(), sstBytes: sstFile.Data()}
			*sstFile = storage.MemFile{}
			w = storage.MakeIngestionSSTWriter(ctx, settings, sstFile)
			split = true
			first = nil
			last = nil
		}

		if len(first) == 0 {
			first = append(first[:0], key.Key...)
		}
		last = append(last[:0], key.Key...)

		if err := w.Put(key, iter.UnsafeValue()); err != nil {
			return nil, nil, err
		}

		iter.Next()
	}

	err := w.Finish()
	if err != nil {
		return nil, nil, err
	}
	right = &sstSpan{start: first, end: last.PrefixEnd(), sstBytes: sstFile.Data()}
	return left, right, nil
}
