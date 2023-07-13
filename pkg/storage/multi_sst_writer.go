package storage

// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// multiSSTWriter is a wrapper around an SSTWriter and SSTSnapshotStorageScratch
// that handles chunking SSTs and persisting them to disk.
type multiSSTWriter struct {
	// It is up to the caller to provide an SSTWriter to be used for creating a new file.
	getNewSSTFile func(ctx context.Context) (SSTWriter, error)
	currSST       SSTWriter
	keySpans      []roachpb.Span
	currSpan      int
	// The total size of SST data. Updated on SST finalization.
	dataSize int64
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

func (msstw *multiSSTWriter) finalizeSST() error {
	err := msstw.currSST.Finish()
	if err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}
	msstw.dataSize += msstw.currSST.DataSize
	msstw.currSpan++
	msstw.currSST.Close()
	return nil
}

func (msstw *multiSSTWriter) Put(ctx context.Context, key EngineKey, value []byte) error {
	for msstw.keySpans[msstw.currSpan].EndKey.Compare(key.Key) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(); err != nil {
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
	for msstw.keySpans[msstw.currSpan].EndKey.Compare(start) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	if msstw.keySpans[msstw.currSpan].Key.Compare(start) > 0 ||
		msstw.keySpans[msstw.currSpan].EndKey.Compare(end) < 0 {
		return errors.AssertionFailedf("client error: expected %s to fall in one of %s",
			roachpb.Span{Key: start, EndKey: end}, msstw.keySpans)
	}
	if err := msstw.currSST.PutEngineRangeKey(start, end, suffix, value); err != nil {
		return errors.Wrap(err, "failed to put range key in sst")
	}
	return nil
}

func (msstw *multiSSTWriter) Finish(ctx context.Context) (int64, error) {
	if msstw.currSpan < len(msstw.keySpans) {
		for {
			if err := msstw.finalizeSST(); err != nil {
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
