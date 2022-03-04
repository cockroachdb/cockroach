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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var (
	tooSmallSSTSize = settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"kv.bulk_io_write.small_write_size",
		"size below which a 'bulk' write will be performed as a normal write instead",
		400*1<<10, // 400 Kib
	)

	splitAfter = settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"bulkio.ingest.scatter_after_size",
		"amount of data added to any one range after which a new range should be split off and scattered",
		48<<20,
	)

	ingestDelay = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"bulkio.ingest.flush_delay",
		"amount of time to wait before sending a file to the KV/Storage layer to ingest",
		0,
		settings.NonNegativeDuration,
	)
)

type sz int64

func (b sz) String() string {
	return redact.StringWithoutMarkers(b)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (b sz) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(humanizeutil.IBytes(int64(b)))
}

// SSTBatcher is a helper for bulk-adding many KVs in chunks via AddSSTable. An
// SSTBatcher can be handed KVs repeatedly and will make them into SSTs that are
// added when they reach the configured size, tracking the total added rows,
// bytes, etc. If configured with a non-nil, populated range cache, it will use
// it to attempt to flush SSTs before they cross range boundaries to minimize
// expensive on-split retries.
type SSTBatcher struct {
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
	totalRows   roachpb.BulkOpSummary
	flushCounts struct {
		total   int
		split   int
		sstSize int
		files   int // a single flush might create multiple files.

		scatterMoved int64

		sendWait    time.Duration
		splitWait   time.Duration
		scatterWait time.Duration
	}
	// Tracking for if we have "filled" a range in case we want to split/scatter.
	flushedToCurrentRange int64
	lastFlushKey          []byte
	maxWriteTS            hlc.Timestamp

	// The rest of the fields are per-batch and are reset via Reset() before each
	// batch is started.
	sstWriter       storage.SSTWriter
	sstFile         *storage.MemFile
	batchStartKey   []byte
	batchEndKey     []byte
	batchEndValue   []byte
	flushKeyChecked bool
	flushKey        roachpb.Key
	// stores on-the-fly stats for the SST if disallowShadowingBelow is set.
	ms enginepb.MVCCStats
	// rows written in the current batch.
	rowCounter storage.RowCounter
}

// MakeSSTBatcher makes a ready-to-use SSTBatcher.
func MakeSSTBatcher(
	ctx context.Context,
	db *kv.DB,
	settings *cluster.Settings,
	disallowShadowingBelow hlc.Timestamp,
	writeAtBatchTs bool,
) (*SSTBatcher, error) {
	b := &SSTBatcher{
		db:                     db,
		settings:               settings,
		disallowShadowingBelow: disallowShadowingBelow,
		writeAtBatchTS:         writeAtBatchTs,
	}
	err := b.Reset(ctx)
	return b, err
}

// MakeStreamSSTBatcher creates a batcher configured to ingest duplicate keys
// that might be received from a cluster to cluster stream.
func MakeStreamSSTBatcher(
	ctx context.Context, db *kv.DB, settings *cluster.Settings,
) (*SSTBatcher, error) {
	b := &SSTBatcher{db: db, settings: settings, ingestAll: true}
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
// keys -- like RESTORE where we want the restored data to look the like backup.
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
				log.VEventf(ctx, 3, "building sstable that will flush before %v", b.flushKey)
			} else {
				log.VEventf(ctx, 3, "no cached range desc available to determine sst flush key")
			}
		}
	}

	if b.flushKey != nil && b.flushKey.Compare(nextKey) <= 0 {
		if err := b.doFlush(ctx, rangeFlush, nil); err != nil {
			return err
		}
		return b.Reset(ctx)
	}

	if b.sstWriter.DataSize >= ingestFileSize(b.settings) {
		if err := b.doFlush(ctx, sizeFlush, nextKey); err != nil {
			return err
		}
		return b.Reset(ctx)
	}
	return nil
}

// Flush sends the current batch, if any.
func (b *SSTBatcher) Flush(ctx context.Context) error {
	if err := b.doFlush(ctx, manualFlush, nil); err != nil {
		return err
	}
	if !b.maxWriteTS.IsEmpty() {
		log.VEventf(ctx, 1, "waiting until max write time %s", b.maxWriteTS)
		return b.db.Clock().SleepUntil(ctx, b.maxWriteTS)
	}
	return nil
}

func (b *SSTBatcher) doFlush(ctx context.Context, reason int, nextKey roachpb.Key) error {
	if b.sstWriter.DataSize == 0 {
		return nil
	}
	b.flushCounts.total++

	if delay := ingestDelay.Get(&b.settings.SV); delay != 0 {
		if delay > time.Second || log.V(1) {
			log.Infof(ctx, "delaying %s before flushing ingestion buffer...", delay)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	hour := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Hour).UnixNano()}

	start := roachpb.Key(append([]byte(nil), b.batchStartKey...))
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(append([]byte(nil), b.batchEndKey...)).Next()

	size := b.sstWriter.DataSize
	if reason == sizeFlush {
		log.VEventf(ctx, 3, "flushing %s SST due to size > %s", sz(size), sz(ingestFileSize(b.settings)))
		b.flushCounts.sstSize++

		// On first flush, if it is due to size, we introduce one split at the start
		// of our span, since size means we didn't already hit one. When adders have
		// non-overlapping keyspace this split partitions off "our" target space for
		// future splitting/scattering, while if they don't, doing this only once
		// minimizes impact on other adders (e.g. causing extra SST splitting).
		//
		// We only do this splitting if the caller expects the sst_batcher to
		// split and scatter the data as it ingests it i.e. splitAfter > 0.
		if b.flushCounts.total == 1 && splitAfter.Get(&b.settings.SV) > 0 && !b.initialSplitDone {
			if splitAt, err := keys.EnsureSafeSplitKey(start); err != nil {
				log.Warningf(ctx, "failed to generate split key to separate ingestion span: %v", err)
			} else {
				if log.V(1) {
					log.Infof(ctx, "splitting on first flush to separate ingestion span using key %v", start)
				}
				// NB: Passing 'hour' here is technically illegal until 19.2 is
				// active, but the value will be ignored before that, and we don't
				// have access to the cluster version here.
				reply, err := b.db.SplitAndScatter(ctx, splitAt, hour)
				if err != nil {
					log.Warningf(ctx, "failed ot split at first key to separate ingestion span: %v", err)
				} else {
					b.flushCounts.splitWait += reply.Timing.Split
					b.flushCounts.scatterWait += reply.Timing.Scatter
					if reply.Stats != nil {
						b.flushCounts.scatterMoved += reply.Stats.Total()
					}
				}
			}
		}
	} else if reason == rangeFlush {
		log.VEventf(ctx, 3, "flushing %s SST due to range boundary %s", sz(size), b.flushKey)
		b.flushCounts.split++
	}

	err := b.sstWriter.Finish()
	if err != nil {
		return errors.Wrapf(err, "finishing constructed sstable")
	}

	// If the stats have been computed on-the-fly, set the last updated time
	// before ingesting the SST.
	if (b.ms != enginepb.MVCCStats{}) {
		b.ms.LastUpdateNanos = timeutil.Now().UnixNano()
	}

	beforeSend := timeutil.Now()
	writeTS, files, err := AddSSTable(ctx, b.db, start, end, b.sstFile.Data(), b.disallowShadowingBelow, b.ms, b.settings, b.batchTS, b.writeAtBatchTS)
	if err != nil {
		return err
	}
	b.flushCounts.sendWait += timeutil.Since(beforeSend)
	b.maxWriteTS.Forward(writeTS)

	b.flushCounts.files += files
	if b.flushKey != nil {
		// If the flush-before key hasn't changed we know we don't think we passed
		// a range boundary, and if the files-added count is 1 we didn't hit an
		// unexpected split either, so assume we added to the same range.
		if reason == sizeFlush && bytes.Equal(b.flushKey, b.lastFlushKey) && files == 1 {
			b.flushedToCurrentRange += size
		} else {
			// Assume we started adding to new different range with this SST.
			b.lastFlushKey = append(b.lastFlushKey[:0], b.flushKey...)
			b.flushedToCurrentRange = size
		}
		if splitSize := splitAfter.Get(&b.settings.SV); splitSize > 0 {
			if b.flushedToCurrentRange > splitSize && nextKey != nil {
				if splitAt, err := keys.EnsureSafeSplitKey(nextKey); err != nil {
					log.Warningf(ctx, "%v", err)
				} else {
					log.VEventf(ctx, 2, "%s added since last split, splitting/scattering for next range at %v", sz(b.flushedToCurrentRange), end)
					// NB: Passing 'hour' here is technically illegal until 19.2 is
					// active, but the value will be ignored before that, and we don't
					// have access to the cluster version here.
					reply, err := b.db.SplitAndScatter(ctx, splitAt, hour)
					if err != nil {
						log.Warningf(ctx, "failed to split and scatter during ingest: %+v", err)
					}
					b.flushCounts.splitWait += reply.Timing.Split
					b.flushCounts.scatterWait += reply.Timing.Scatter
					if reply.Stats != nil {
						b.flushCounts.scatterMoved += reply.Stats.Total()
					}
				}
				b.flushedToCurrentRange = 0
			}
		}
	}

	b.rowCounter.DataSize += b.sstWriter.DataSize
	b.totalRows.Add(b.rowCounter.BulkOpSummary)
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
	start, end             roachpb.Key
	sstBytes               []byte
	disallowShadowingBelow hlc.Timestamp
	stats                  enginepb.MVCCStats
}

// AddSSTable retries db.AddSSTable if retryable errors occur, including if the
// SST spans a split, in which case it is iterated and split into two SSTs, one
// for each side of the split in the error, and each are retried.
func AddSSTable(
	ctx context.Context,
	db *kv.DB,
	start, end roachpb.Key,
	sstBytes []byte,
	disallowShadowingBelow hlc.Timestamp,
	ms enginepb.MVCCStats,
	settings *cluster.Settings,
	batchTs hlc.Timestamp,
	writeAtBatchTs bool,
) (hlc.Timestamp, int, error) {
	var files int
	var maxTs hlc.Timestamp

	now := timeutil.Now()
	iter, err := storage.NewMemSSTIterator(sstBytes, true)
	if err != nil {
		return hlc.Timestamp{}, 0, err
	}
	defer iter.Close()

	var stats enginepb.MVCCStats
	if (ms == enginepb.MVCCStats{}) {
		stats, err = storage.ComputeStatsForRange(iter, start, end, now.UnixNano())
		if err != nil {
			return hlc.Timestamp{}, 0, errors.Wrapf(err, "computing stats for SST [%s, %s)", start, end)
		}
	} else {
		stats = ms
	}

	work := []*sstSpan{{start: start, end: end, sstBytes: sstBytes, disallowShadowingBelow: disallowShadowingBelow, stats: stats}}
	const maxAddSSTableRetries = 10
	for len(work) > 0 {
		item := work[0]
		work = work[1:]
		if err := func() error {
			var err error
			for i := 0; i < maxAddSSTableRetries; i++ {
				log.VEventf(ctx, 2, "sending %s AddSSTable [%s,%s)", sz(len(item.sstBytes)), item.start, item.end)
				before := timeutil.Now()
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
				if settings != nil && int64(len(item.sstBytes)) < tooSmallSSTSize.Get(&settings.SV) {
					log.VEventf(ctx, 2, "ingest data is too small (%d keys/%d bytes) for SSTable, adding via regular batch", item.stats.KeyCount, len(item.sstBytes))
					ingestAsWriteBatch = true
				}

				if writeAtBatchTs {
					var writeTs hlc.Timestamp
					// This will fail if the range has split but we'll check for that below.
					writeTs, err = db.AddSSTableAtBatchTimestamp(ctx, item.start, item.end, item.sstBytes,
						false /* disallowConflicts */, !item.disallowShadowingBelow.IsEmpty(),
						item.disallowShadowingBelow, &item.stats, ingestAsWriteBatch, batchTs)
					if err == nil {
						maxTs.Forward(writeTs)
					}
				} else {
					// This will fail if the range has split but we'll check for that below.
					err = db.AddSSTable(ctx, item.start, item.end, item.sstBytes, false, /* disallowConflicts */
						!item.disallowShadowingBelow.IsEmpty(), item.disallowShadowingBelow, &item.stats,
						ingestAsWriteBatch, batchTs)
				}
				if err == nil {
					log.VEventf(ctx, 3, "adding %s AddSSTable [%s,%s) took %v", sz(len(item.sstBytes)), item.start, item.end, timeutil.Since(before))
					return nil
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
					left, right, err := createSplitSSTable(ctx, db, item.start, split, item.disallowShadowingBelow, iter, settings)
					if err != nil {
						return err
					}

					right.stats, err = storage.ComputeStatsForRange(
						iter, right.start, right.end, now.UnixNano(),
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
				// Retry on AmbiguousResult.
				if errors.HasType(err, (*roachpb.AmbiguousResultError)(nil)) {
					log.Warningf(ctx, "addsstable [%s,%s) attempt %d failed: %+v", start, end, i, err)
					continue
				}
			}
			return errors.Wrapf(err, "addsstable [%s,%s)", item.start, item.end)
		}(); err != nil {
			return maxTs, files, err
		}
		files++
		// explicitly deallocate SST. This will not deallocate the
		// top level SST which is kept around to iterate over.
		item.sstBytes = nil
	}
	log.VEventf(ctx, 3, "AddSSTable [%v, %v) added %d files and took %v", start, end, files, timeutil.Since(now))
	return maxTs, files, nil
}

// createSplitSSTable is a helper for splitting up SSTs. The iterator
// passed in is over the top level SST passed into AddSSTTable().
func createSplitSSTable(
	ctx context.Context,
	db *kv.DB,
	start, splitKey roachpb.Key,
	disallowShadowingBelow hlc.Timestamp,
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
			left = &sstSpan{
				start:                  first,
				end:                    last.PrefixEnd(),
				sstBytes:               sstFile.Data(),
				disallowShadowingBelow: disallowShadowingBelow,
			}
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
	right = &sstSpan{
		start:                  first,
		end:                    last.PrefixEnd(),
		sstBytes:               sstFile.Data(),
		disallowShadowingBelow: disallowShadowingBelow,
	}
	return left, right, nil
}
