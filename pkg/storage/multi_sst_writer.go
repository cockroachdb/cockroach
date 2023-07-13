// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

// MultiSSTWriter may be used to construct a series of non-overlapping sstables
// from a stream of keys. The user may specify a set of fixed boundaries such
// that no sstable produced will span the boundaries.
type MultiSSTWriter struct {
	// It is up to the caller to provide an SSTWriter to be used for creating a new file.
	getNewSSTFile func(ctx context.Context) (SSTWriter, error)
	currSST       SSTWriter
	keySpans      []roachpb.Span
	currSpan      int
	// The total size of SST data. Updated on SST finalization.
	dataSize      int64
	buffered      *bufferedKey
	tableMetadata []*sstable.WriterMetadata
	// The max size of a sstable file.
	maxSize int64
}

type bufferedKey struct {
	start  roachpb.Key
	end    roachpb.Key
	suffix []byte
	value  []byte
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
	if err := msstw.currSST.ClearRawRange(
		msstw.keySpans[msstw.currSpan].Key, msstw.keySpans[msstw.currSpan].EndKey,
		true /* pointKeys */, true, /* rangeKeys */
	); err != nil {
		msstw.currSST.Close()
		return errors.Wrap(err, "failed to clear range on sst file writer")
	}

	return nil
}

func (msstw *MultiSSTWriter) TableMetadata() []*sstable.WriterMetadata {
	return msstw.tableMetadata
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

func (msstw *MultiSSTWriter) maybeFlushBuffered() error {
	if msstw.buffered != nil {
		// Check that buffered range key is within in the bounds of the current span.
		if msstw.keySpans[msstw.currSpan].Key.Compare(msstw.buffered.start) > 0 ||
			msstw.keySpans[msstw.currSpan].EndKey.Compare(msstw.buffered.end) < 0 {
			return errors.AssertionFailedf("client error: expected %s to fall in one of %s",
				roachpb.Span{Key: msstw.buffered.start, EndKey: msstw.buffered.end}, msstw.keySpans)
		}
		if err := msstw.currSST.PutEngineRangeKey(msstw.buffered.start, msstw.buffered.end, msstw.buffered.suffix, msstw.buffered.value); err != nil {
			return errors.Wrap(err, "failed to put range key in sst")
		}
		msstw.buffered = nil
	}
	return nil
}

func (msstw *MultiSSTWriter) Put(ctx context.Context, key EngineKey, value []byte) error {
	for msstw.keySpans[msstw.currSpan].EndKey.Compare(key.Key) <= 0 {
		// If the point key to add is outside the current span flush the current range key.
		// Note: At most one range key flush happens inside this loop.
		if err := msstw.maybeFlushBuffered(); err != nil {
			return err
		}
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(true); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}

	if msstw.currSST.DataSize >= msstw.maxSize {
		if msstw.buffered != nil && msstw.buffered.end.Compare(key.Key) > 0 {
			// Buffered range key needs to be truncated. For example, we have buffered
			// the range key a-e however we run out of space at point key d, we must
			// split the range key into a-d and d-e.
			splitbuffered := &bufferedKey{
				start:  key.Key,
				end:    msstw.buffered.end,
				value:  msstw.buffered.value,
				suffix: msstw.buffered.suffix,
			}
			msstw.buffered.end = key.Key
			if err := msstw.maybeFlushBuffered(); err != nil {
				return err
			}
			msstw.buffered = splitbuffered
		} else {
			if err := msstw.maybeFlushBuffered(); err != nil {
				return err
			}
		}

		if err := msstw.finalizeSST(false); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}

	if msstw.keySpans[msstw.currSpan].Key.Compare(key.Key) > 0 {
		return errors.AssertionFailedf("client error: expected %s to fall in one of %s", key.Key, msstw.keySpans)
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

	// Since we have encountered a new range key, the previously buffered one can
	// be flushed (due to non-overlap).
	if err := msstw.maybeFlushBuffered(); err != nil {
		return err
	}

	for msstw.keySpans[msstw.currSpan].EndKey.Compare(start) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(true); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}

	if msstw.currSST.DataSize >= msstw.maxSize {
		// Finish the current SST and write to the file (do not move to the next key span).
		if err := msstw.finalizeSST(false); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}

	msstw.buffered = &bufferedKey{
		start:  start,
		end:    end,
		suffix: suffix,
		value:  value,
	}
	return nil
}

func (msstw *MultiSSTWriter) Finish(ctx context.Context) (int64, error) {
	if msstw.currSpan < len(msstw.keySpans) {
		if err := msstw.maybeFlushBuffered(); err != nil {
			return 0, err
		}
		for {
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
