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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type sz int64

func (b sz) String() string {
	return humanizeutil.IBytes(int64(b))
}

// SSTBatcher is a helper for bulk-adding many KVs in chunks via AddSSTable. An
// SSTBatcher can be handed KVs repeatedly and will make them into SSTs that are
// added when they reach the configured size, tracking the total added rows,
// bytes, etc. If configured with a non-nil, populated range cache, it will use
// it to attempt to flush SSTs before they cross range boundaries to minimize
// expensive on-split retries.
type SSTBatcher struct {
	db         sender
	rc         *kv.RangeDescriptorCache
	maxSize    uint64
	splitAfter uint64

	// allows ingestion of keys where the MVCC.Key would shadow an existing row.
	disallowShadowing bool
	// skips duplicate keys (iff they are buffered together). This is true when
	// used to backfill an inverted index. An array in JSONB with multiple values
	// which are the same, will all correspond to the same kv in the inverted
	// index. The method which generates these kvs does not dedup, thus we rely on
	// the SSTBatcher to dedup them (by skipping), rather than throwing a
	// DuplicateKeyError. This is also true when used with IMPORT. Import
	// generally prohibits the ingestion of KVs which will shadow existing data,
	// with the exception of duplicates having the same value and timestamp. To
	// maintain uniform behavior, duplicates in the same batch with equal values
	// will not raise a DuplicateKeyError.
	skipDuplicates bool

	// The rest of the fields accumulated state as opposed to configuration. Some,
	// like totalRows, are accumulated _across_ batches and are not reset between
	// batches when Reset() is called.
	totalRows   roachpb.BulkOpSummary
	flushCounts struct {
		total   int
		split   int
		sstSize int
		files   int // a single flush might create multiple files.

		sendWait  time.Duration
		splitWait time.Duration
	}
	// Tracking for if we have "filled" a range in case we want to split/scatter.
	flushedToCurrentRange uint64
	lastFlushKey          []byte

	// The rest of the fields are per-batch and are reset via Reset() before each
	// batch is started.
	sstWriter       SSTWriter
	batchStartKey   []byte
	batchEndKey     []byte
	batchEndValue   []byte
	flushKeyChecked bool
	flushKey        roachpb.Key
	// stores on-the-fly stats for the SST if disallowShadowing is true.
	ms enginepb.MVCCStats
	// rows written in the current batch.
	rowCounter RowCounter
}

// MakeSSTBatcher makes a ready-to-use SSTBatcher.
func MakeSSTBatcher(ctx context.Context, db sender, flushBytes uint64) (*SSTBatcher, error) {
	b := &SSTBatcher{db: db, maxSize: flushBytes, disallowShadowing: true}
	err := b.Reset()
	return b, err
}

func (b *SSTBatcher) updateMVCCStats(key engine.MVCCKey, value []byte) {
	metaKeySize := int64(len(key.Key)) + 1
	metaValSize := int64(0)
	b.ms.LiveBytes += metaKeySize
	b.ms.LiveCount++
	b.ms.KeyBytes += metaKeySize
	b.ms.ValBytes += metaValSize
	b.ms.KeyCount++

	totalBytes := int64(len(value)) + engine.MVCCVersionTimestampSize
	b.ms.LiveBytes += totalBytes
	b.ms.KeyBytes += engine.MVCCVersionTimestampSize
	b.ms.ValBytes += int64(len(value))
	b.ms.ValCount++
}

// AddMVCCKey adds a key+timestamp/value pair to the batch (flushing if needed).
// This is only for callers that want to control the timestamp on individual
// keys -- like RESTORE where we want the restored data to look the like backup.
// Keys must be added in order.
func (b *SSTBatcher) AddMVCCKey(ctx context.Context, key engine.MVCCKey, value []byte) error {
	if len(b.batchEndKey) > 0 && bytes.Equal(b.batchEndKey, key.Key) {
		if b.skipDuplicates && bytes.Equal(b.batchEndValue, value) {
			return nil
		}

		var err storagebase.DuplicateKeyError
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
	if b.disallowShadowing {
		b.updateMVCCStats(key, value)
	}

	return b.sstWriter.Add(engine.MVCCKeyValue{Key: key, Value: value})
}

// Reset clears all state in the batcher and prepares it for reuse.
func (b *SSTBatcher) Reset() error {
	b.sstWriter.Close()
	b.sstWriter = MakeSSTWriter()
	b.batchStartKey = b.batchStartKey[:0]
	b.batchEndKey = b.batchEndKey[:0]
	b.batchEndValue = b.batchEndValue[:0]
	b.flushKey = nil
	b.flushKeyChecked = false
	b.ms.Reset()

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
			r, err := b.rc.GetCachedRangeDescriptor(k, false /* inverted */)
			if err != nil {
				log.Warningf(ctx, "failed to determine where to split SST: %+v", err)
			} else if r != nil {
				b.flushKey = r.EndKey.AsRawKey()
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
		return b.Reset()
	}

	if b.sstWriter.DataSize >= b.maxSize {
		if err := b.doFlush(ctx, sizeFlush, nextKey); err != nil {
			return err
		}
		return b.Reset()
	}
	return nil
}

// Flush sends the current batch, if any.
func (b *SSTBatcher) Flush(ctx context.Context) error {
	return b.doFlush(ctx, manualFlush, nil)
}

func (b *SSTBatcher) doFlush(ctx context.Context, reason int, nextKey roachpb.Key) error {
	if b.sstWriter.DataSize == 0 {
		return nil
	}
	b.flushCounts.total++

	hour := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Hour).UnixNano()}

	start := roachpb.Key(append([]byte(nil), b.batchStartKey...))
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(append([]byte(nil), b.batchEndKey...)).Next()

	size := b.sstWriter.DataSize
	if reason == sizeFlush {
		log.VEventf(ctx, 3, "flushing %s SST due to size > %s", sz(size), sz(b.maxSize))
		b.flushCounts.sstSize++

		// On first flush, if it is due to size, we introduce one split at the start
		// of our span, since size means we didn't already hit one. When adders have
		// non-overlapping keyspace this split partitions off "our" target space for
		// future splitting/scattering, while if they don't, doing this only once
		// minimizes impact on other adders (e.g. causing extra SST splitting).
		if b.flushCounts.total == 1 {
			if splitAt, err := keys.EnsureSafeSplitKey(start); err != nil {
				log.Warning(ctx, err)
			} else {
				// NB: Passing 'hour' here is technically illegal until 19.2 is
				// active, but the value will be ignored before that, and we don't
				// have access to the cluster version here.
				if err := b.db.SplitAndScatter(ctx, splitAt, hour); err != nil {
					log.Warning(ctx, err)
				}
			}
		}
	} else if reason == rangeFlush {
		log.VEventf(ctx, 3, "flushing %s SST due to range boundary %s", sz(size), b.flushKey)
		b.flushCounts.split++
	}

	sstBytes, err := b.sstWriter.Finish()
	if err != nil {
		return errors.Wrapf(err, "finishing constructed sstable")
	}

	// If the stats have been computed on-the-fly, set the last updated time
	// before ingesting the SST.
	if (b.ms != enginepb.MVCCStats{}) {
		b.ms.LastUpdateNanos = timeutil.Now().UnixNano()
	}

	beforeSend := timeutil.Now()
	files, err := AddSSTable(ctx, b.db, start, end, sstBytes, b.disallowShadowing, b.ms)
	if err != nil {
		return err
	}
	b.flushCounts.sendWait += timeutil.Since(beforeSend)

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
		if b.splitAfter > 0 && b.flushedToCurrentRange > b.splitAfter && nextKey != nil {
			if splitAt, err := keys.EnsureSafeSplitKey(nextKey); err != nil {
				log.Warning(ctx, err)
			} else {
				beforeSplit := timeutil.Now()

				log.VEventf(ctx, 2, "%s added since last split, splitting/scattering for next range at %v", sz(b.flushedToCurrentRange), end)
				// NB: Passing 'hour' here is technically illegal until 19.2 is
				// active, but the value will be ignored before that, and we don't
				// have access to the cluster version here.
				if err := b.db.SplitAndScatter(ctx, splitAt, hour); err != nil {
					log.Warningf(ctx, "failed to split and scatter during ingest: %+v", err)
				}
				b.flushCounts.splitWait += timeutil.Since(beforeSplit)
			}
			b.flushedToCurrentRange = 0
		}
	}

	b.totalRows.Add(b.rowCounter.BulkOpSummary)
	b.totalRows.DataSize += int64(b.sstWriter.DataSize)
	return nil
}

// Close closes the underlying SST builder.
func (b *SSTBatcher) Close() {
	b.sstWriter.Close()
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *SSTBatcher) GetSummary() roachpb.BulkOpSummary {
	return b.totalRows
}

type sender interface {
	AddSSTable(
		ctx context.Context, begin, end interface{}, data []byte, disallowShadowing bool, stats *enginepb.MVCCStats,
	) error
	SplitAndScatter(ctx context.Context, key roachpb.Key, expirationTime hlc.Timestamp) error
}

type sstSpan struct {
	start, end        roachpb.Key
	sstBytes          []byte
	disallowShadowing bool
	stats             enginepb.MVCCStats
}

// AddSSTable retries db.AddSSTable if retryable errors occur, including if the
// SST spans a split, in which case it is iterated and split into two SSTs, one
// for each side of the split in the error, and each are retried.
func AddSSTable(
	ctx context.Context,
	db sender,
	start, end roachpb.Key,
	sstBytes []byte,
	disallowShadowing bool,
	ms enginepb.MVCCStats,
) (int, error) {
	var files int
	now := timeutil.Now()
	iter, err := engine.NewMemSSTIterator(sstBytes, true)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var stats enginepb.MVCCStats
	if (ms == enginepb.MVCCStats{}) {
		stats, err = engine.ComputeStatsGo(
			iter, engine.MVCCKey{Key: start}, engine.MVCCKey{Key: end}, now.UnixNano(),
		)
		if err != nil {
			return 0, errors.Wrapf(err, "computing stats for SST [%s, %s)", start, end)
		}
	} else {
		stats = ms
	}

	work := []*sstSpan{{start: start, end: end, sstBytes: sstBytes, disallowShadowing: disallowShadowing, stats: stats}}
	const maxAddSSTableRetries = 10
	for len(work) > 0 {
		item := work[0]
		work = work[1:]
		if err := func() error {
			var err error
			for i := 0; i < maxAddSSTableRetries; i++ {
				log.VEventf(ctx, 2, "sending %s AddSSTable [%s,%s)", sz(len(item.sstBytes)), item.start, item.end)
				before := timeutil.Now()
				// This will fail if the range has split but we'll check for that below.
				err = db.AddSSTable(ctx, item.start, item.end, item.sstBytes, item.disallowShadowing, &item.stats)
				if err == nil {
					log.VEventf(ctx, 3, "adding %s AddSSTable [%s,%s) took %v", sz(len(item.sstBytes)), item.start, item.end, timeutil.Since(before))
					return nil
				}
				// This range has split -- we need to split the SST to try again.
				if m, ok := errors.Cause(err).(*roachpb.RangeKeyMismatchError); ok {
					split := m.MismatchedRange.EndKey.AsRawKey()
					log.Infof(ctx, "SSTable cannot be added spanning range bounds %v, retrying...", split)
					left, right, err := createSplitSSTable(ctx, db, item.start, split, item.disallowShadowing, iter)
					if err != nil {
						return err
					}

					right.stats, err = engine.ComputeStatsGo(
						iter, engine.MVCCKey{Key: right.start}, engine.MVCCKey{Key: right.end}, now.UnixNano(),
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
				if _, ok := err.(*roachpb.AmbiguousResultError); ok {
					log.Warningf(ctx, "addsstable [%s,%s) attempt %d failed: %+v", start, end, i, err)
					continue
				}
			}
			return errors.Wrapf(err, "addsstable [%s,%s)", item.start, item.end)
		}(); err != nil {
			return files, err
		}
		files++
		// explicitly deallocate SST. This will not deallocate the
		// top level SST which is kept around to iterate over.
		item.sstBytes = nil
	}
	log.VEventf(ctx, 3, "AddSSTable [%v, %v) added %d files and took %v", start, end, files, timeutil.Since(now))
	return files, nil
}

// createSplitSSTable is a helper for splitting up SSTs. The iterator
// passed in is over the top level SST passed into AddSSTTable().
func createSplitSSTable(
	ctx context.Context,
	db sender,
	start, splitKey roachpb.Key,
	disallowShadowing bool,
	iter engine.SimpleIterator,
) (*sstSpan, *sstSpan, error) {
	w := MakeSSTWriter()
	defer w.Close()

	split := false
	var first, last roachpb.Key
	var left, right *sstSpan

	iter.Seek(engine.MVCCKey{Key: start})
	for {
		if ok, err := iter.Valid(); err != nil {
			return nil, nil, err
		} else if !ok {
			break
		}

		key := iter.UnsafeKey()

		if !split && key.Key.Compare(splitKey) >= 0 {
			res, err := w.Finish()
			if err != nil {
				return nil, nil, err
			}
			left = &sstSpan{
				start:             first,
				end:               last.PrefixEnd(),
				sstBytes:          res,
				disallowShadowing: disallowShadowing,
			}
			w = MakeSSTWriter()
			split = true
			first = nil
			last = nil
		}

		if len(first) == 0 {
			first = append(first[:0], key.Key...)
		}
		last = append(last[:0], key.Key...)

		if err := w.Add(engine.MVCCKeyValue{Key: key, Value: iter.UnsafeValue()}); err != nil {
			return nil, nil, err
		}

		iter.Next()
	}

	res, err := w.Finish()
	if err != nil {
		return nil, nil, err
	}
	right = &sstSpan{
		start:             first,
		end:               last.PrefixEnd(),
		sstBytes:          res,
		disallowShadowing: disallowShadowing,
	}
	return left, right, nil
}
