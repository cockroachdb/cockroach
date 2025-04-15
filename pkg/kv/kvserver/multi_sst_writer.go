// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangedel"
	"github.com/cockroachdb/pebble/rangekey"
)

// multiSSTWriter is a wrapper around an SSTWriter and SSTSnapshotStorageScratch
// that handles chunking SSTs and persisting them to disk.
type multiSSTWriter struct {
	st      *cluster.Settings
	scratch *SSTSnapshotStorageScratch
	currSST storage.SSTWriter
	// localKeySpans are key spans that are considered unsplittable across sstables, and
	// represent the range's range local key spans. In contrast, mvccKeySpan can be split
	// across multiple sstables if one of them exceeds maxSSTSize. The expectation is
	// that for large ranges, keys in mvccKeySpan will dominate in size compared to keys
	// in localKeySpans.
	localKeySpans []roachpb.Span
	mvccKeySpan   roachpb.Span
	// mvccSSTSpans reflects the actual split of the mvccKeySpan into constituent
	// sstables.
	mvccSSTSpans []storage.EngineKeyRange
	// currSpan is the index of the current span being written to. The first
	// len(localKeySpans) spans are localKeySpans, and the rest are mvccSSTSpans.
	// In a sense, currSpan indexes into a slice composed of
	// append(localKeySpans, mvccSSTSpans).
	currSpan int
	// The approximate size of the SST chunk to buffer in memory on the receiver
	// before flushing to disk.
	sstChunkSize int64
	// The total size of the key and value pairs (not the total size of the SSTs),
	// excluding currSST. Updated on SST finalization.
	dataSize int64
	// The total size of the SSTs, excluding currSST. Updated on SST finalization.
	sstSize int64
	// maxSSTSize is the maximum size to use for SSTs containing MVCC/user keys.
	// Once the sstable writer reaches this size, it will be finalized and a new
	// sstable will be created.
	maxSSTSize int64
	// rangeKeyFrag is used to fragment range keys across the SSTs. For each SST
	// (with the exception of the MVCC SST), it's initialized with a range key del
	// for the entire span, but the incoming stream of data may also contain new
	// range keys. As the start key of these incoming range keys increases, the
	// fragmenter emits fragmented range keys into the produced SST.
	rangeKeyFrag rangekey.Fragmenter
	// rangeDelFrag is like rangeKeyFrag, but for range deletions (i.e. operations
	// that simply clear out all keys in a span).
	rangeDelFrag rangedel.Fragmenter
}

func newMultiSSTWriter(
	ctx context.Context,
	st *cluster.Settings,
	scratch *SSTSnapshotStorageScratch,
	localKeySpans []roachpb.Span,
	mvccKeySpan roachpb.Span,
	sstChunkSize int64,
	rangeKeysInOrder bool,
) (*multiSSTWriter, error) {
	msstw := &multiSSTWriter{
		st:            st,
		scratch:       scratch,
		localKeySpans: localKeySpans,
		mvccKeySpan:   mvccKeySpan,
		mvccSSTSpans: []storage.EngineKeyRange{{
			Start: storage.EngineKey{Key: mvccKeySpan.Key},
			End:   storage.EngineKey{Key: mvccKeySpan.EndKey},
		}},
		sstChunkSize: sstChunkSize,
	}
	if rangeKeysInOrder {
		// We disable snapshot sstable splitting unless the sender has
		// specified in its snapshot header that it is sending range keys in
		// key order alongside point keys, as opposed to sending them at the end
		// of the snapshot. This is necessary to efficiently produce fragmented
		// snapshot sstables, as otherwise range keys will arrive out-of-order
		// wrt. point keys.
		msstw.maxSSTSize = MaxSnapshotSSTableSize.Get(&st.SV)
	}
	msstw.rangeKeyFrag = rangekey.Fragmenter{
		Cmp:    storage.EngineComparer.Compare,
		Format: storage.EngineComparer.FormatKey,
		Emit:   msstw.emitRangeKey,
	}
	msstw.rangeDelFrag = rangedel.Fragmenter{
		Cmp:    storage.EngineComparer.Compare,
		Format: storage.EngineComparer.FormatKey,
		Emit:   msstw.emitRangeDel,
	}

	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}
	return msstw, nil
}

// estimatedDataSize returns the approximation of the written bytes to SSTs
// (including currSST).
func (msstw *multiSSTWriter) estimatedDataSize() int64 {
	return msstw.sstSize + msstw.currSST.DataSize
}

func (msstw *multiSSTWriter) emitRangeKey(key rangekey.Span) {
	for i := range key.Keys {
		if err := msstw.currSST.PutInternalRangeKey(key.Start, key.End, key.Keys[i]); err != nil {
			panic(fmt.Sprintf("failed to put range key in sst: %s", err))
		}
	}
}

func (msstw *multiSSTWriter) emitRangeDel(key rangedel.Span) {
	if err := msstw.currSST.ClearRawEncodedRange(key.Start, key.End); err != nil {
		panic(fmt.Sprintf("failed to put range del in sst: %s", err))
	}
}

// currentSpan returns the current user-provided span that
// is being written to. Note that this does not account for
// mvcc keys being split across multiple sstables.
func (msstw *multiSSTWriter) currentSpan() roachpb.Span {
	if msstw.currSpanIsMVCCSpan() {
		return msstw.mvccKeySpan
	}
	return msstw.localKeySpans[msstw.currSpan]
}

func (msstw *multiSSTWriter) currSpanIsMVCCSpan() bool {
	if msstw.currSpan >= len(msstw.localKeySpans)+len(msstw.mvccSSTSpans) {
		panic("current span is out of bounds")
	}
	return msstw.currSpan >= len(msstw.localKeySpans)
}

func (msstw *multiSSTWriter) initSST(ctx context.Context) error {
	newSSTFile, err := msstw.scratch.NewFile(ctx, msstw.sstChunkSize)
	if err != nil {
		return errors.Wrap(err, "failed to create new sst file")
	}
	newSST := storage.MakeIngestionSSTWriter(ctx, msstw.st, newSSTFile)
	msstw.currSST = newSST

	// Add a RangeKeyDel as well as a range del for the entire bounds of the SST,
	// meaning upon ingestion any range and point keys existing in the span will
	// be deleted.
	// Note that the MVCC span will be excised on ingest, so this step is skipped
	// for it.
	if !msstw.currSpanIsMVCCSpan() {
		sp := msstw.currentSpan()
		startKey := storage.EngineKey{Key: sp.Key}.Encode()
		endKey := storage.EngineKey{Key: sp.EndKey}.Encode()
		msstw.rangeKeyFrag.Add(rangekey.Span{
			Start: startKey, End: endKey, Keys: []rangekey.Key{{
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeKeyDelete),
			}},
		})
		msstw.rangeDelFrag.Add(rangedel.Span{
			Start: startKey, End: endKey, Keys: []rangedel.Key{{
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeDelete),
			}}},
		)
	}
	return nil
}

// NB: when nextKey is non-nil, do not do anything in this function to cause
// nextKey at the caller to escape to the heap.
func (msstw *multiSSTWriter) finalizeSST(ctx context.Context, nextKey *storage.EngineKey) error {
	var currEngineSpan storage.EngineKeyRange
	if msstw.currSpanIsMVCCSpan() {
		currEngineSpan = msstw.mvccSSTSpans[msstw.currSpan-len(msstw.localKeySpans)]
	} else {
		cur := msstw.currentSpan()
		currEngineSpan = storage.EngineKeyRange{
			Start: storage.EngineKey{Key: cur.Key},
			End:   storage.EngineKey{Key: cur.EndKey},
		}

	}

	// If we're at the last span, call Finish on the fragmenters. If we're not at the
	// last span, call Truncate.
	if msstw.currSpan == len(msstw.localKeySpans)+len(msstw.mvccSSTSpans)-1 {
		msstw.rangeKeyFrag.Finish()
		msstw.rangeDelFrag.Finish()
	} else {
		msstw.rangeKeyFrag.Truncate(currEngineSpan.End.Encode())
		msstw.rangeDelFrag.Truncate(currEngineSpan.End.Encode())
	}

	err := msstw.currSST.Finish()
	if err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}
	if nextKey != nil {
		meta := msstw.currSST.Meta
		encodedNextKey := nextKey.Encode()
		// Use nextKeyCopy for the remainder of this function. Calling
		// errors.Errorf with nextKey caused it to escape to the heap in the
		// caller of finalizeSST (even when finalizeSST was not called), which was
		// costly.
		nextKeyCopy := *nextKey
		if meta.HasPointKeys && storage.EngineComparer.Compare(meta.LargestPoint.UserKey, encodedNextKey) > 0 {
			metaEndKey, ok := storage.DecodeEngineKey(meta.LargestPoint.UserKey)
			if !ok {
				return errors.Errorf("multiSSTWriter created overlapping ingestion sstables: sstable largest point key %s > next sstable start key %s",
					meta.LargestPoint.UserKey, nextKeyCopy)
			}
			return errors.Errorf("multiSSTWriter created overlapping ingestion sstables: sstable largest point key %s > next sstable start key %s",
				metaEndKey, nextKeyCopy)
		}
		if meta.HasRangeDelKeys && storage.EngineComparer.Compare(meta.LargestRangeDel.UserKey, encodedNextKey) > 0 {
			metaEndKey, ok := storage.DecodeEngineKey(meta.LargestRangeDel.UserKey)
			if !ok {
				return errors.Errorf("multiSSTWriter created overlapping ingestion sstables: sstable largest range del %s > next sstable start key %s",
					meta.LargestRangeDel.UserKey, nextKeyCopy)
			}
			return errors.Errorf("multiSSTWriter created overlapping ingestion sstables: sstable largest range del %s > next sstable start key %s",
				metaEndKey, nextKeyCopy)
		}
		if meta.HasRangeKeys && storage.EngineComparer.Compare(meta.LargestRangeKey.UserKey, encodedNextKey) > 0 {
			metaEndKey, ok := storage.DecodeEngineKey(meta.LargestRangeKey.UserKey)
			if !ok {
				return errors.Errorf("multiSSTWriter created overlapping ingestion sstables: sstable largest range key %s > next sstable start key %s",
					meta.LargestRangeKey.UserKey, nextKeyCopy)
			}
			return errors.Errorf("multiSSTWriter created overlapping ingestion sstables: sstable largest range key %s > next sstable start key %s",
				metaEndKey, nextKeyCopy)
		}
	}
	msstw.dataSize += msstw.currSST.DataSize
	msstw.sstSize += int64(msstw.currSST.Meta.Size)
	msstw.currSpan++
	msstw.currSST.Close()
	return nil
}

// rolloverSST rolls the underlying SST writer over to the appropriate SST
// writer for writing a point/range key at key. For point keys, endKey and key
// must equal each other.
func (msstw *multiSSTWriter) rolloverSST(
	ctx context.Context, key storage.EngineKey, endKey storage.EngineKey,
) error {
	for msstw.currentSpan().EndKey.Compare(key.Key) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(ctx, &key); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	currSpan := msstw.currentSpan()
	if currSpan.Key.Compare(key.Key) > 0 || currSpan.EndKey.Compare(endKey.Key) < 0 {
		if !key.Key.Equal(endKey.Key) {
			return errors.AssertionFailedf("client error: expected %s to fall in one of %s or %s",
				roachpb.Span{Key: key.Key, EndKey: endKey.Key}, msstw.localKeySpans, msstw.mvccKeySpan)
		}
		return errors.AssertionFailedf("client error: expected %s to fall in one of %s or %s", key, msstw.localKeySpans, msstw.mvccKeySpan)
	}
	if msstw.currSpanIsMVCCSpan() && msstw.maxSSTSize > 0 && msstw.currSST.DataSize > msstw.maxSSTSize {
		// We're in an MVCC / user keys span, and the current sstable has exceeded
		// the max size for MVCC sstables that we should be creating. Split this
		// sstable into smaller ones. We do this by splitting the mvccKeySpan
		// from [oldStartKey, oldEndKey) to [oldStartKey, key) and [key, oldEndKey).
		// The split spans are added to msstw.mvccSSTSpans.
		currSpan := &msstw.mvccSSTSpans[msstw.currSpan-len(msstw.localKeySpans)]
		if bytes.Equal(currSpan.Start.Key, key.Key) && bytes.Equal(currSpan.Start.Version, key.Version) {
			panic("unexpectedly reached max sstable size at start of an mvcc sstable span")
		}
		oldEndKey := currSpan.End
		currSpan.End = key.Copy()
		newSpan := storage.EngineKeyRange{Start: currSpan.End, End: oldEndKey}
		msstw.mvccSSTSpans = append(msstw.mvccSSTSpans, newSpan)
		if msstw.currSpan < len(msstw.localKeySpans)+len(msstw.mvccSSTSpans)-2 {
			// This should never happen; we only split sstables when we're at the end
			// of mvccSSTSpans.
			panic("unexpectedly split an earlier mvcc sstable span in multiSSTWriter")
		}
		if err := msstw.finalizeSST(ctx, &key); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (msstw *multiSSTWriter) Put(ctx context.Context, key storage.EngineKey, value []byte) error {
	if err := msstw.rolloverSST(ctx, key, key); err != nil {
		return err
	}
	if err := msstw.currSST.PutEngineKey(key, value); err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}
	return nil
}

func (msstw *multiSSTWriter) PutInternalPointKey(
	ctx context.Context, key []byte, kind pebble.InternalKeyKind, val []byte,
) error {
	decodedKey, ok := storage.DecodeEngineKey(key)
	if !ok {
		return errors.New("cannot decode engine key")
	}
	if err := msstw.rolloverSST(ctx, decodedKey, decodedKey); err != nil {
		return err
	}
	var err error
	switch kind {
	case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
		err = msstw.currSST.PutEngineKey(decodedKey, val)
	case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
		err = msstw.currSST.ClearEngineKey(decodedKey, storage.ClearOptions{ValueSizeKnown: false})
	default:
		err = errors.New("unexpected key kind")
	}
	if err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}
	return nil
}

func decodeRangeStartEnd(
	start, end []byte,
) (decodedStart, decodedEnd storage.EngineKey, err error) {
	var emptyKey storage.EngineKey
	decodedStart, ok := storage.DecodeEngineKey(start)
	if !ok {
		return emptyKey, emptyKey, errors.New("cannot decode start engine key")
	}
	decodedEnd, ok = storage.DecodeEngineKey(end)
	if !ok {
		return emptyKey, emptyKey, errors.New("cannot decode end engine key")
	}
	if decodedStart.Key.Compare(decodedEnd.Key) >= 0 {
		return emptyKey, emptyKey, errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}
	return decodedStart, decodedEnd, nil
}

func (msstw *multiSSTWriter) PutInternalRangeDelete(ctx context.Context, start, end []byte) error {
	decodedStart, decodedEnd, err := decodeRangeStartEnd(start, end)
	if err != nil {
		return err
	}
	if err := msstw.rolloverSST(ctx, decodedStart, decodedEnd); err != nil {
		return err
	}
	msstw.rangeDelFrag.Add(rangedel.Span{Start: start, End: end})
	return nil
}

func (msstw *multiSSTWriter) PutInternalRangeKey(
	ctx context.Context, start, end []byte, key rangekey.Key,
) error {
	return msstw.putRangeKey(ctx, storage.EngineKeyRange{}, [2][]byte{start, end}, key)
}

func (msstw *multiSSTWriter) putRangeKey(
	ctx context.Context, dec storage.EngineKeyRange, enc [2][]byte, key rangekey.Key,
) error {
	// We need both the encoded and decoded forms of the key range here. The caller
	// may supply either.
	haveDec, haveEnc := len(dec.End.Key) != 0, len(enc[1]) != 0
	switch {
	case !haveDec && !haveEnc:
		return errors.AssertionFailedf("key range must be specified either in encoded or decoded form")
	case !haveDec:
		ds, de, err := decodeRangeStartEnd(enc[0], enc[1])
		if err != nil {
			return err
		}
		dec = storage.EngineKeyRange{
			Start: ds,
			End:   de,
		}
	case !haveEnc:
		enc[0] = dec.Start.Encode()
		enc[1] = dec.End.Encode()
	}

	if k, ek := dec.Start.Key, dec.End.Key; k.Compare(ek) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", k, ek)
	}

	if err := msstw.rolloverSST(ctx, dec.Start, dec.End); err != nil {
		return err
	}

	msstw.rangeKeyFrag.Add(rangekey.Span{
		Start: enc[0],
		End:   enc[1],
		Keys:  []rangekey.Key{key},
	})
	return nil
}

func (msstw *multiSSTWriter) PutRangeKey(
	ctx context.Context, start, end roachpb.Key, suffix []byte, value []byte,
) error {
	return msstw.putRangeKey(
		ctx,
		storage.EngineKeyRange{
			Start: storage.EngineKey{Key: start},
			End:   storage.EngineKey{Key: end},
		},
		[2][]byte{}, // enc
		rangekey.Key{
			Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeKeySet),
			Suffix:  suffix,
			Value:   value,
		},
	)
}

func (msstw *multiSSTWriter) Finish(ctx context.Context) (int64, error) {
	if msstw.currSpan < (len(msstw.localKeySpans) + len(msstw.mvccSSTSpans)) {
		for {
			if err := msstw.finalizeSST(ctx, nil /* nextKey */); err != nil {
				return 0, err
			}
			if msstw.currSpan >= (len(msstw.localKeySpans) + len(msstw.mvccSSTSpans)) {
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
