// Copyright 2018 The Cockroach Authors.
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
)

const SSTSizeLimit = 128 << 20

// multiSSTWriter is a wrapper around an SSTWriter that handles chunking SSTs
// and persisting them to disk. Range key buffering and truncation is also
// handled by multiSSTWriter.
type multiSSTWriter struct {
	currSST  SSTWriter
	keySpans []roachpb.Span
	// It is up to the caller to provide an SSTWriter to be used for creating a new file.
	getNewSSTFile func(ctx context.Context) (SSTWriter, error)
	currSpan      int
	// The total size of SST data. Updated on SST finalization.
	dataSize    int64
	bufferedKey *bufferedRangeKey
}

type bufferedRangeKey struct {
	start  roachpb.Key
	end    roachpb.Key
	suffix []byte
	value  []byte
}

func NewMultiSSTWriter(
	ctx context.Context,
	getNewSSTFile func(ctx context.Context) (SSTWriter, error),
	keySpans []roachpb.Span,
) (multiSSTWriter, error) {
	msstw := multiSSTWriter{
		getNewSSTFile: getNewSSTFile,
		keySpans:      keySpans,
	}
	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}
	return msstw, nil
}

func (msstw *multiSSTWriter) initSST(ctx context.Context) error {
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

func (msstw *multiSSTWriter) finalizeSST(incrementSpan bool) error {
	err := msstw.currSST.Finish()
	if err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}
	msstw.dataSize += msstw.currSST.DataSize
	msstw.currSST.Close()
	if incrementSpan {
		msstw.currSpan++
	}
	return nil
}

func (msstw *multiSSTWriter) maybeFlushBufferedKey() error {
	if msstw.bufferedKey != nil {
		// Check that buffered range key is within in the bounds of the current span.
		if msstw.keySpans[msstw.currSpan].Key.Compare(msstw.bufferedKey.start) > 0 ||
			msstw.keySpans[msstw.currSpan].EndKey.Compare(msstw.bufferedKey.end) < 0 {
			return errors.AssertionFailedf("client error: expected %s to fall in one of %s",
				roachpb.Span{Key: msstw.bufferedKey.start, EndKey: msstw.bufferedKey.end}, msstw.keySpans)
		}
		if err := msstw.currSST.PutEngineRangeKey(msstw.bufferedKey.start, msstw.bufferedKey.end, msstw.bufferedKey.suffix, msstw.bufferedKey.value); err != nil {
			return errors.Wrap(err, "failed to put range key in sst")
		}
		msstw.bufferedKey = nil
	}
	return nil
}

func (msstw *multiSSTWriter) Put(ctx context.Context, key EngineKey, value []byte) error {
	if msstw.keySpans[msstw.currSpan].EndKey.Compare(key.Key) <= 0 {
		if err := msstw.maybeFlushBufferedKey(); err != nil {
			return err
		}

		for msstw.keySpans[msstw.currSpan].EndKey.Compare(key.Key) <= 0 {
			// Finish the current SST, write to the file, and move to the next key
			// range.
			if err := msstw.finalizeSST(true); err != nil {
				return err
			}
			if err := msstw.initSST(ctx); err != nil {
				return err
			}
		}
	}

	if msstw.currSST.DataSize >= SSTSizeLimit {
		if msstw.bufferedKey != nil && msstw.bufferedKey.end.Compare(key.Key) > 0 {
			// Buffered range key needs to be truncated. For example, we have buffered
			// the range key a-e however we run out of space at point key d, we must
			// split the range key into a-d and d-e.
			splitBufferedKey := &bufferedRangeKey{
				start:  key.Key,
				end:    msstw.bufferedKey.end,
				value:  msstw.bufferedKey.value,
				suffix: msstw.bufferedKey.suffix,
			}
			msstw.bufferedKey.end = key.Key
			if err := msstw.maybeFlushBufferedKey(); err != nil {
				return err
			}
			msstw.bufferedKey = splitBufferedKey
		} else {
			if err := msstw.maybeFlushBufferedKey(); err != nil {
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

func (msstw *multiSSTWriter) PutRangeKey(
	ctx context.Context, start, end roachpb.Key, suffix []byte, value []byte,
) error {
	if start.Compare(end) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}

	// Since we have encountered a new range key, the previously buffered one can
	// be flushed (due to non-overlap).
	if err := msstw.maybeFlushBufferedKey(); err != nil {
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

	if msstw.currSST.DataSize >= SSTSizeLimit {
		// Finish the current SST and write to the file (do not move to next key range).
		if err := msstw.finalizeSST(false); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}

	msstw.bufferedKey = &bufferedRangeKey{
		start:  start,
		end:    end,
		suffix: suffix,
		value:  value,
	}
	return nil
}

func (msstw *multiSSTWriter) Finish(ctx context.Context) (int64, error) {
	if msstw.currSpan < len(msstw.keySpans) {
		if err := msstw.maybeFlushBufferedKey(); err != nil {
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

func (msstw *multiSSTWriter) Close() {
	msstw.currSST.Close()
}
