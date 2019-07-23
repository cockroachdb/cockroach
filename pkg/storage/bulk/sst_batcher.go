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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	db sender

	flushKeyChecked bool
	flushKey        roachpb.Key
	rc              *kv.RangeDescriptorCache

	// skips duplicates (iff they are buffered together).
	skipDuplicates bool

	maxSize int64
	// rows written in the current batch.
	rowCounter RowCounter
	totalRows  roachpb.BulkOpSummary

	sstWriter     engine.RocksDBSstFileWriter
	batchStartKey []byte
	batchEndKey   []byte

	flushCounts struct {
		total   int
		split   int
		sstSize int
	}

	// allows ingestion of keys where the MVCC.Key would shadow an exisiting row.
	disallowShadowing bool
}

// MakeSSTBatcher makes a ready-to-use SSTBatcher.
func MakeSSTBatcher(ctx context.Context, db sender, flushBytes int64) (*SSTBatcher, error) {
	b := &SSTBatcher{db: db, maxSize: flushBytes}
	err := b.Reset()
	return b, err
}

// AddMVCCKey adds a key+timestamp/value pair to the batch (flushing if needed).
// This is only for callers that want to control the timestamp on individual
// keys -- like RESTORE where we want the restored data to look the like backup.
// Keys must be added in order.
func (b *SSTBatcher) AddMVCCKey(ctx context.Context, key engine.MVCCKey, value []byte) error {
	if len(b.batchEndKey) > 0 && bytes.Equal(b.batchEndKey, key.Key) {
		if b.skipDuplicates {
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
	if b.shouldFlush(ctx, key.Key) {
		if err := b.Flush(ctx); err != nil {
			return err
		}
		if err := b.Reset(); err != nil {
			return err
		}
	}

	// Update the range currently represented in this batch, as necessary.
	if len(b.batchStartKey) == 0 {
		b.batchStartKey = append(b.batchStartKey[:0], key.Key...)
	}
	b.batchEndKey = append(b.batchEndKey[:0], key.Key...)

	if err := b.rowCounter.Count(key.Key); err != nil {
		return err
	}
	return b.sstWriter.Add(engine.MVCCKeyValue{Key: key, Value: value})
}

// Reset clears all state in the batcher and prepares it for reuse.
func (b *SSTBatcher) Reset() error {
	b.sstWriter.Close()
	w, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	b.sstWriter = w
	b.batchStartKey = b.batchStartKey[:0]
	b.batchEndKey = b.batchEndKey[:0]
	b.flushKey = nil
	b.flushKeyChecked = false

	b.rowCounter.BulkOpSummary.Reset()
	return nil
}

func (b *SSTBatcher) shouldFlush(ctx context.Context, nextKey roachpb.Key) bool {
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

	size := b.sstWriter.DataSize

	if b.flushKey != nil && b.flushKey.Compare(nextKey) <= 0 {
		log.VEventf(ctx, 3, "flushing %s SST due to range boundary %s", sz(size), b.flushKey)
		b.flushCounts.split++
		return true
	}

	if size >= b.maxSize {
		log.VEventf(ctx, 3, "flushing %s SST due to size > %s", sz(size), sz(b.maxSize))
		b.flushCounts.sstSize++
		return true
	}
	return false
}

// Flush sends the current batch, if any.
func (b *SSTBatcher) Flush(ctx context.Context) error {
	if b.sstWriter.DataSize == 0 {
		return nil
	}
	b.flushCounts.total++

	start := roachpb.Key(append([]byte(nil), b.batchStartKey...))
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(append([]byte(nil), b.batchEndKey...)).Next()

	sstBytes, err := b.sstWriter.Finish()
	if err != nil {
		return errors.Wrapf(err, "finishing constructed sstable")
	}
	if err := AddSSTable(ctx, b.db, start, end, sstBytes, b.disallowShadowing); err != nil {
		return err
	}
	b.totalRows.Add(b.rowCounter.BulkOpSummary)
	b.totalRows.DataSize += b.sstWriter.DataSize
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
	AddSSTable(ctx context.Context, begin, end interface{}, data []byte, disallowShadowing bool) error
}

type sstSpan struct {
	start, end        roachpb.Key
	sstBytes          []byte
	disallowShadowing bool
}

// AddSSTable retries db.AddSSTable if retryable errors occur, including if the
// SST spans a split, in which case it is iterated and split into two SSTs, one
// for each side of the split in the error, and each are retried.
func AddSSTable(
	ctx context.Context, db sender, start, end roachpb.Key, sstBytes []byte, disallowShadowing bool,
) error {
	work := []*sstSpan{{start: start, end: end, sstBytes: sstBytes, disallowShadowing: disallowShadowing}}
	// Create an iterator that iterates over the top level SST to produce all the splits.
	var iter engine.SimpleIterator
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()
	const maxAddSSTableRetries = 10
	for len(work) > 0 {
		item := work[0]
		work = work[1:]
		if err := func() error {
			var err error
			for i := 0; i < maxAddSSTableRetries; i++ {
				log.VEventf(ctx, 2, "sending %s AddSSTable [%s,%s)", sz(len(sstBytes)), start, end)
				// This will fail if the range has split but we'll check for that below.
				err = db.AddSSTable(ctx, item.start, item.end, item.sstBytes, item.disallowShadowing)
				if err == nil {
					return nil
				}
				// This range has split -- we need to split the SST to try again.
				if m, ok := errors.Cause(err).(*roachpb.RangeKeyMismatchError); ok {
					if iter == nil {
						iter, err = engine.NewMemSSTIterator(sstBytes, false)
						if err != nil {
							return err
						}
					}
					split := m.MismatchedRange.EndKey.AsRawKey()
					log.Infof(ctx, "SSTable cannot be added spanning range bounds %v, retrying...", split)
					left, right, err := createSplitSSTable(ctx, db, item.start, split, item.disallowShadowing, iter)
					if err != nil {
						return err
					}
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
			return err
		}
		// explicitly deallocate SST. This will not deallocate the
		// top level SST which is kept around to iterate over.
		item.sstBytes = nil
	}

	return nil
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
	w, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return nil, nil, err
	}
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

			w.Close()
			w, err = engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return nil, nil, err
			}

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
