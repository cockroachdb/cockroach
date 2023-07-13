// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
)

// MultiSSTWriter may be used to construct a series of non-overlapping sstables
// from a stream of keys. The user may specify a set of fixed boundaries such
// that no sstable produced will span the boundaries.
type MultiSSTWriter struct {
	// It is up to the caller to provide an SSTWriter to be used for creating a new file.
	getNewSSTFile func(ctx context.Context) (SSTWriter, error)
	// uponFinish is called once an
	beforeSSTFinalize func(start, end roachpb.Key) error
	currSST           SSTWriter
	keySpans          []roachpb.Span
	currSpan          int
	// The total size of SST data. Updated on SST finalization.
	dataSize      int64
	tableMetadata []*sstable.WriterMetadata
	// Used for fragmenting range keys.
	rangeKeyFrag keyspan.Fragmenter
	// Used for fragmenting range deletes.
	rangeDelFrag keyspan.Fragmenter
	// The max size of a sstable file.
	maxSize int64
}

func NewMultiSSTWriter(
	ctx context.Context,
	getNewSSTFile func(ctx context.Context) (SSTWriter, error),
	keySpans []roachpb.Span,
	maxSize int64,
) (MultiSSTWriter, error) {
	msstw := MultiSSTWriter{
		getNewSSTFile: getNewSSTFile,
		keySpans:      keySpans,
		maxSize:       maxSize,
	}
	msstw.rangeKeyFrag = keyspan.Fragmenter{
		Cmp:    EngineComparer.Compare,
		Format: EngineComparer.FormatKey,
		Emit: func(span rangekey.Span) {
			// Once a span is emitted we can write all of its keys as we are guaranteed that there will be no overlap.
			for _, key := range span.Keys {
				if err := msstw.currSST.PutEngineRangeKey(span.Start, span.End, key.Suffix, key.Value); err != nil {
					log.Warningf(ctx, "%v", err)
				}
			}
		},
	}

	msstw.rangeDelFrag = keyspan.Fragmenter{
		Cmp:    EngineComparer.Compare,
		Format: EngineComparer.FormatKey,
		Emit: func(span rangekey.Span) {
			// Once a span is emitted we can clear that range.
			if err := msstw.currSST.ClearRawRange(span.Start, span.End, true, false); err != nil {
				log.Warningf(ctx, "%v", err)
			}
		},
	}

	msstw.clearCurrSpan()

	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}
	return msstw, nil
}

func (msstw *MultiSSTWriter) initSST(ctx context.Context) error {
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

func (msstw *MultiSSTWriter) clearCurrSpan() {
	if msstw.currSpan >= len(msstw.keySpans) {
		return
	}
	// Add a range delete and range key delete that covers the current span.
	msstw.rangeKeyFrag.Add(keyspan.Span{
		Start: msstw.keySpans[msstw.currSpan].Key,
		End:   msstw.keySpans[msstw.currSpan].EndKey,
		Keys: []keyspan.Key{
			{Trailer: uint64(pebble.InternalKeyKindRangeDelete)},
		},
	})
	msstw.rangeDelFrag.Add(keyspan.Span{
		Start: msstw.keySpans[msstw.currSpan].Key,
		End:   msstw.keySpans[msstw.currSpan].EndKey,
		Keys: []keyspan.Key{
			{Trailer: uint64(pebble.InternalKeyKindDelete)},
		},
	})
}

func (msstw *MultiSSTWriter) finalizeSST(incrementSpan bool) error {
	if msstw.currSST.fw == nil {
		return errors.New("cannot close already closed writer.")
	}
	if err := msstw.currSST.fw.Close(); err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}
	metaData, err := msstw.currSST.fw.Metadata()
	if err != nil {
		return errors.Wrap(err, "failed to retrieve finished sst metadata")
	}
	msstw.currSST.fw = nil
	msstw.tableMetadata = append(msstw.tableMetadata, metaData)
	msstw.dataSize += msstw.currSST.DataSize
	msstw.currSST.Close()
	if incrementSpan {
		msstw.currSpan++
	}
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
		// Since our span has been incremented for the first time, we must clear the current span.
		msstw.clearCurrSpan()
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}

	// If we have reached our sst file limit, flush all buffered keys and create a new sst file.
	// Note: the span is not incremented in this case.
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
	if msstw.keySpans[msstw.currSpan].Key.Compare(key.Key) > 0 {
		return errors.AssertionFailedf("client error: expected %s to fall in one of %s", key.Key, msstw.keySpans)
	}

	if err := msstw.updateCurrSST(ctx, key.Key); err != nil {
		return errors.Wrap(err, "failed to create the required sst")
	}

	if err := msstw.currSST.PutEngineKey(key, value); err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}

	return nil
}

func (msstw *MultiSSTWriter) PutRangeKey(
	ctx context.Context, start, end roachpb.Key, suffix []byte, value []byte,
) error {
	if start.Compare(end) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}

	if err := msstw.updateCurrSST(ctx, start); err != nil {
		return errors.Wrap(err, "failed to create the required sst")
	}

	// RangeKey cannot be written directly, must be buffered.
	msstw.rangeKeyFrag.Add(keyspan.Span{
		Start: start,
		End:   end,
		Keys: []keyspan.Key{
			{Suffix: suffix, Value: value},
		},
	})

	return nil
}

func (msstw *MultiSSTWriter) PutRangeDel(ctx context.Context, start, end roachpb.Key) error {
	if start.Compare(end) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}

	if err := msstw.updateCurrSST(ctx, start); err != nil {
		return errors.Wrap(err, "failed to create the required sst")
	}

	// RangeDel cannot be written directly, must be buffered.
	msstw.rangeDelFrag.Add(keyspan.Span{
		Start: start,
		End:   end,
		Keys: []keyspan.Key{
			{Trailer: uint64(pebble.InternalKeyKindRangeDelete)},
		},
	})

	return nil
}

func (msstw *MultiSSTWriter) Finish(ctx context.Context) (int64, error) {
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
		}
	}
	return msstw.dataSize, nil
}

func (msstw *MultiSSTWriter) Close() {
	msstw.currSST.Close()
}
