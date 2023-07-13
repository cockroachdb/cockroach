// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
)

// MultiSSTWriter may be used to construct a series of non-overlapping sstables
// from a stream of keys. The user may specify a set of fixed boundaries such
// that no sstable produced will span the boundaries.
type MultiSSTWriter struct {
	// It is up to the caller to provide an SSTWriter to be used for creating a new file.
	getNewSSTFile func(ctx context.Context) (SSTWriter, error)
	onEnterSpan   func(ctsx context.Context, span roachpb.Span, writer *MultiSSTWriter) error
	currSST       SSTWriter
	keySpans      []roachpb.Span
	currSpan      int
	// The total size of SST data. Updated on SST finalization.
	dataSize      int64
	tableMetadata []*sstable.WriterMetadata
	// Used for fragmenting range keys.
	rangeKeyFrag rangekey.Fragmenter
	// Used for fragmenting range deletes.
	rangeDelFrag rangekey.Fragmenter
	// The max size of a sstable file.
	maxSize int64
	err     error
}

func NewMultiSSTWriter(
	ctx context.Context,
	getNewSSTFile func(ctx context.Context) (SSTWriter, error),
	onEnterSpan func(ctx context.Context, span roachpb.Span, w *MultiSSTWriter) error,
	keySpans []roachpb.Span,
	maxSize int64,
) (MultiSSTWriter, error) {
	if len(keySpans) == 0 {
		return MultiSSTWriter{}, errors.New("Cannot call NewMultiSSTWriter without any key spans.")
	}

	msstw := MultiSSTWriter{
		getNewSSTFile: getNewSSTFile,
		keySpans:      keySpans,
		maxSize:       maxSize,
		onEnterSpan:   onEnterSpan,
	}
	msstw.rangeKeyFrag = rangekey.Fragmenter{
		Cmp:    EngineComparer.Compare,
		Format: EngineComparer.FormatKey,
		Emit: func(span rangekey.Span) {
			// Once a span is emitted we can write all of its keys as we are guaranteed that there will be no overlap.
			for _, key := range span.Keys {
				if err := msstw.currSST.PutEngineRangeKey(span.Start, span.End, key.Suffix, key.Value); err != nil {
					msstw.err = err
					break
				}
			}
		},
	}

	msstw.rangeDelFrag = rangekey.Fragmenter{
		Cmp:    EngineComparer.Compare,
		Format: EngineComparer.FormatKey,
		Emit: func(span rangekey.Span) {
			// Once a span is emitted we can clear that range.
			if err := msstw.currSST.ClearRawRange(span.Start, span.End, true, false); err != nil {
				msstw.err = err
			}
		},
	}

	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}

	if err := msstw.onEnterSpan(ctx, msstw.keySpans[msstw.currSpan], &msstw); err != nil {
		return msstw, err
	}

	return msstw, nil
}

func (msstw *MultiSSTWriter) initSST(ctx context.Context) error {
	if msstw.err != nil {
		return msstw.err
	}
	newSSTFile, err := msstw.getNewSSTFile(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create new sst file writer")
	}
	msstw.currSST = newSSTFile
	return nil
}

func (msstw *MultiSSTWriter) TableMetadata() []*sstable.WriterMetadata {
	return msstw.tableMetadata
}

func (msstw *MultiSSTWriter) finalizeSST(incrementSpan bool) error {
	if msstw.err != nil {
		return msstw.err
	}

	if err := msstw.currSST.Finish(); err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}

	msstw.tableMetadata = append(msstw.tableMetadata, msstw.currSST.Meta)
	msstw.dataSize += msstw.currSST.DataSize
	if incrementSpan {
		msstw.currSpan++
	}
	msstw.currSST = SSTWriter{}
	return nil
}

func (msstw *MultiSSTWriter) truncateAndFlushBuffered(key []byte) {
	msstw.rangeKeyFrag.TruncateAndFlushTo(key)
	msstw.rangeDelFrag.TruncateAndFlushTo(key)
}

func (msstw *MultiSSTWriter) updateCurrSST(ctx context.Context, key []byte) error {
	// Ensure that the current sstable's bounds reflects the current span.
	for msstw.keySpans[msstw.currSpan].EndKey.Compare(key) <= 0 {
		msstw.truncateAndFlushBuffered(msstw.keySpans[msstw.currSpan].EndKey)
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(true); err != nil {
			return err
		}

		if msstw.currSpan >= len(msstw.keySpans) {
			break
		}

		if err := msstw.initSST(ctx); err != nil {
			return err
		}

		// Since we've moved on to a new span, we must initialize the new span.
		if msstw.currSpan < len(msstw.keySpans) {
			if err := msstw.onEnterSpan(ctx, msstw.keySpans[msstw.currSpan], msstw); err != nil {
				return err
			}
		}
	}

	// If we have reached our sst file limit, flush all buffered keys and create a new sst file.
	// Note: the span is not incremented in this case.
	// TODO (rahul): include buffered keys size in this check
	if msstw.currSST.DataSize >= msstw.maxSize {
		msstw.truncateAndFlushBuffered(key)

		if err := msstw.finalizeSST(false); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (msstw *MultiSSTWriter) Put(ctx context.Context, key EngineKey, value []byte) error {
	if msstw.err != nil {
		return msstw.err
	}

	if msstw.keySpans[msstw.currSpan].Key.Compare(key.Key) > 0 {
		return errors.AssertionFailedf("client error: expected %s to fall in one of %s", key.Key, msstw.keySpans)
	}

	if err := msstw.updateCurrSST(ctx, key.Key); err != nil {
		return errors.Wrap(err, "failed to update the current sst")
	}

	if err := msstw.currSST.PutEngineKey(key, value); err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}

	return nil
}

func (msstw *MultiSSTWriter) PutRangeKeyDel(ctx context.Context, start, end roachpb.Key) error {
	if msstw.err != nil {
		return msstw.err
	}
	if start.Compare(end) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}

	if err := msstw.updateCurrSST(ctx, start); err != nil {
		return errors.Wrap(err, "failed to update the current sst")
	}

	// RangeKeyDelete cannot be written directly, must be buffered.
	msstw.rangeKeyFrag.Add(rangekey.Span{
		Start: start,
		End:   end,
		Keys: []rangekey.Key{
			{Trailer: uint64(pebble.InternalKeyKindRangeKeyDelete)},
		},
	})

	return nil
}

func (msstw *MultiSSTWriter) PutRangeKey(
	ctx context.Context, start, end roachpb.Key, suffix []byte, value []byte,
) error {
	if msstw.err != nil {
		return msstw.err
	}
	if start.Compare(end) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}

	if err := msstw.updateCurrSST(ctx, start); err != nil {
		return errors.Wrap(err, "failed to update the current sst")
	}

	// RangeKey cannot be written directly, must be buffered.
	msstw.rangeKeyFrag.Add(rangekey.Span{
		Start: start,
		End:   end,
		Keys: []rangekey.Key{
			{Suffix: suffix, Value: value},
		},
	})

	return nil
}

func (msstw *MultiSSTWriter) PutRangeDel(ctx context.Context, start, end roachpb.Key) error {
	if msstw.err != nil {
		return msstw.err
	}

	if start.Compare(end) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}

	if err := msstw.updateCurrSST(ctx, start); err != nil {
		return errors.Wrap(err, "failed to update the current sst")
	}

	// RangeDel cannot be written directly, must be buffered.
	msstw.rangeDelFrag.Add(rangekey.Span{
		Start: start,
		End:   end,
		Keys: []rangekey.Key{
			{Trailer: uint64(pebble.InternalKeyKindRangeDelete)},
		},
	})

	return nil
}

func (msstw *MultiSSTWriter) Finish(ctx context.Context) (int64, error) {
	if msstw.err != nil {
		return 0, msstw.err
	}

	if msstw.currSpan < len(msstw.keySpans) {
		for {
			msstw.truncateAndFlushBuffered(msstw.keySpans[msstw.currSpan].EndKey)
			if err := msstw.finalizeSST(true); err != nil {
				return 0, err
			}

			if msstw.currSpan >= len(msstw.keySpans) {
				break
			}

			if err := msstw.initSST(ctx); err != nil {
				return 0, err
			}

			// Since we've moved on to a new span, we must initialize the new span.
			if msstw.currSpan < len(msstw.keySpans) {
				if err := msstw.onEnterSpan(ctx, msstw.keySpans[msstw.currSpan], msstw); err != nil {
					return 0, err
				}
			}
		}
	}
	msstw.rangeDelFrag.Finish()
	msstw.rangeKeyFrag.Finish()
	return msstw.dataSize, nil
}
