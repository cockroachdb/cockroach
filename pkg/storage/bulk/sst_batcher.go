// Copyright 2017 The Cockroach Authors.
//
/// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package bulk

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// FixedTimestampSSTBatcher is a wrapper for SSTBatcher that assigns a fixed
// timestamp to all the added keys.
type FixedTimestampSSTBatcher struct {
	timestamp hlc.Timestamp
	SSTBatcher
}

// MakeFixedTimestampSSTBatcher makes a ready-to-use SSTBatcher
func MakeFixedTimestampSSTBatcher(
	ctx context.Context, db *client.DB, flushBytes int64, timestamp hlc.Timestamp,
) (*FixedTimestampSSTBatcher, error) {
	b := &FixedTimestampSSTBatcher{timestamp, SSTBatcher{db: db, maxSize: flushBytes}}
	err := b.reset()
	return b, err
}

// Add a key/value pair with the batcher's timestamp, flushing if needed.
// Keys must be added in order.
func (b *FixedTimestampSSTBatcher) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	return b.AddMVCCKey(ctx, engine.MVCCKey{Key: key, Timestamp: b.timestamp}, value)
}

// SSTBatcher is a helper for bulk-adding many KVs in chunks via AddSSTable. An
// SSTBatcher can be handed KVs repeatedly and will make them into SSTs that are
// added when they reach the configured size, tracking the total added rows,
// bytes, etc.
//
// TODO(dt): this currently handles adding keys that span multiple ranges less
// than ideally -- if a flush fails due to spanning a range boundary, it will
// have to regenerate SSTs for each side of the reported split and retry. This
// could in worst-case result in many expensive regeneration/retry loops.
// Ideally the batcher should be aware of split points and try to flush before
// crossing a split boundary (though it will still need to handle a mismatched
// error on new splits).
type SSTBatcher struct {
	db *client.DB

	maxSize int64
	// rows written in the current batch.
	rowCounter RowCounter
	totalRows  roachpb.BulkOpSummary

	sstWriter     engine.RocksDBSstFileWriter
	batchStartKey []byte
	batchEndKey   []byte
}

// MakeSSTBatcher makes a ready-to-use SSTBatcher.
func MakeSSTBatcher(ctx context.Context, db *client.DB, flushBytes int64) (*SSTBatcher, error) {
	b := &SSTBatcher{db: db, maxSize: flushBytes}
	err := b.reset()
	return b, err
}

// AddMVCCKey adds a key+timestamp/value pair to the batch (flushing if needed).
// This is only for callers that want to control the timestamp on individual
// keys -- like RESTORE where we want the restored data to look the like backup.
// Keys must be added in order.
func (b *SSTBatcher) AddMVCCKey(ctx context.Context, key engine.MVCCKey, value []byte) error {
	// Check if we need to flush current batch *before* adding the next k/v --
	// the batcher may want to flush the keys it already has, either because it
	// is full or because it wants this key in a separate batch due to splits.
	if b.shouldFlush(key.Key) {
		if err := b.Flush(ctx); err != nil {
			return err
		}
		if err := b.reset(); err != nil {
			return err
		}
	}

	// Update the range currently represented in this batch, as necessary.
	if len(b.batchStartKey) == 0 || bytes.Compare(key.Key, b.batchStartKey) < 0 {
		b.batchStartKey = append(b.batchStartKey[:0], key.Key...)
	}
	if len(b.batchEndKey) == 0 || bytes.Compare(key.Key, b.batchEndKey) > 0 {
		b.batchEndKey = append(b.batchEndKey[:0], key.Key...)
	}
	if err := b.rowCounter.Count(key.Key); err != nil {
		return err
	}
	return b.sstWriter.Add(engine.MVCCKeyValue{Key: key, Value: value})
}

func (b *SSTBatcher) reset() error {
	b.sstWriter.Close()
	w, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	b.sstWriter = w
	b.batchStartKey = b.batchStartKey[:0]
	b.batchEndKey = b.batchEndKey[:0]
	b.rowCounter.BulkOpSummary.Reset()
	return nil
}

func (b *SSTBatcher) shouldFlush(nextKey roachpb.Key) bool {
	return b.sstWriter.DataSize >= b.maxSize
}

// Flush sends the current batch, if any.
func (b *SSTBatcher) Flush(ctx context.Context) error {
	if b.sstWriter.DataSize == 0 {
		return nil
	}
	start := roachpb.Key(append([]byte(nil), b.batchStartKey...))
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(append([]byte(nil), b.batchEndKey...)).Next()

	sstBytes, err := b.sstWriter.Finish()
	if err != nil {
		return errors.Wrapf(err, "finishing constructed sstable")
	}
	if err := AddSSTable(ctx, b.db, start, end, sstBytes); err != nil {
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

// AddSSTable retries db.AddSSTable if retryable errors occur, including if the
// SST spans a split, in which case it is iterated and split into two SSTs, one
// for each side of the split in the error, and each are retried.
func AddSSTable(ctx context.Context, db *client.DB, start, end roachpb.Key, sstBytes []byte) error {
	const maxAddSSTableRetries = 10
	var err error
	for i := 0; i < maxAddSSTableRetries; i++ {
		log.VEventf(ctx, 2, "sending AddSSTable [%s,%s)", start, end)
		// This will fail if the range has split but we'll check for that below.
		err = db.AddSSTable(ctx, start, end, sstBytes)
		if err == nil {
			return nil
		}
		// This range has split -- we need to split the SST to try again.
		if m, ok := errors.Cause(err).(*roachpb.RangeKeyMismatchError); ok {
			return addSplitSSTable(ctx, db, sstBytes, start, m.MismatchedRange.EndKey.AsRawKey())
		}
		// Retry on AmbiguousResult.
		if _, ok := err.(*roachpb.AmbiguousResultError); ok {
			log.Warningf(ctx, "addsstable [%s,%s) attempt %d failed: %+v", start, end, i, err)
			continue
		}
	}
	return errors.Wrapf(err, "addsstable [%s,%s)", start, end)
}

// addSplitSSTable is a helper for splitting up and retrying AddSStable calls.
func addSplitSSTable(
	ctx context.Context, db *client.DB, sstBytes []byte, start, splitKey roachpb.Key,
) error {
	iter, err := engine.NewMemSSTIterator(sstBytes, false)
	if err != nil {
		return err
	}
	defer iter.Close()

	w, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return err
	}
	defer w.Close()

	split := false
	var first, last roachpb.Key

	iter.Seek(engine.MVCCKey{Key: start})
	for {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		key := iter.UnsafeKey()

		if !split && key.Key.Compare(splitKey) >= 0 {
			res, err := w.Finish()
			if err != nil {
				return err
			}
			if err := AddSSTable(ctx, db, first, last.PrefixEnd(), res); err != nil {
				return err
			}
			w.Close()
			w, err = engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return err
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
			return err
		}

		iter.Next()
	}

	res, err := w.Finish()
	if err != nil {
		return err
	}
	return AddSSTable(ctx, db, first, last.PrefixEnd(), res)
}
