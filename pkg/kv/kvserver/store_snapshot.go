// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/multiqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

const (
	// Messages that provide detail about why a snapshot was rejected.
	storeDrainingMsg = "store is draining"

	// IntersectingSnapshotMsg is part of the error message returned from
	// canAcceptSnapshotLocked and is exposed here so testing can rely on it.
	IntersectingSnapshotMsg = "snapshot intersects existing range"

	// tagSnapshotTiming is the tracing span tag that the *snapshotTimingTag
	// lives under.
	tagSnapshotTiming = "snapshot_timing_tag"
)

// MaxSnapshotSSTableSize is the maximum size of an sstable containing MVCC/user keys
// in a snapshot before we truncate and write a new snapshot sstable.
var MaxSnapshotSSTableSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_rebalance.max_sst_size",
	"maximum size of a rebalance or recovery SST size",
	int64(metamorphic.ConstantWithTestRange(
		"kv.snapshot_rebalance.max_sst_size",
		128<<20, /* defaultValue */
		32<<10,  /* metamorphic min */
		512<<20, /* metamorphic max */
	)), // 128 MB default
	settings.NonNegativeInt,
)

// snapshotMetrics contains metrics on the number and size of snapshots in
// progress or in the snapshot queue.
type snapshotMetrics struct {
	QueueLen        *metric.Gauge
	QueueSize       *metric.Gauge
	InProgress      *metric.Gauge
	TotalInProgress *metric.Gauge
}

// incomingSnapshotStream is the minimal interface on a GRPC stream required
// to receive a snapshot over the network.
type incomingSnapshotStream interface {
	Send(*kvserverpb.SnapshotResponse) error
	Recv() (*kvserverpb.SnapshotRequest, error)
}

// outgoingSnapshotStream is the minimal interface on a GRPC stream required
// to send a snapshot over the network.
type outgoingSnapshotStream interface {
	Send(*kvserverpb.SnapshotRequest) error
	Recv() (*kvserverpb.SnapshotResponse, error)
}

// snapshotRecordMetrics is a wrapper function that increments a set of metrics
// related to the number of snapshot bytes sent/received. The definer of the
// function specifies which metrics are incremented.
type snapshotRecordMetrics func(inc int64)

// kvBatchSnapshotStrategy is an implementation of snapshotStrategy that streams
// batches of KV pairs in the BatchRepr format.
type kvBatchSnapshotStrategy struct {
	status redact.RedactableString

	// The size of the batches of PUT operations to send to the receiver of the
	// snapshot. Only used on the sender side.
	batchSize int64
	// Limiter for sending KV batches. Only used on the sender side.
	limiter *rate.Limiter
	// Only used on the sender side.
	newWriteBatch func() storage.WriteBatch

	// The approximate size of the SST chunk to buffer in memory on the receiver
	// before flushing to disk. Only used on the receiver side.
	sstChunkSize int64
	// Only used on the receiver side.
	scratch   *SSTSnapshotStorageScratch
	st        *cluster.Settings
	clusterID uuid.UUID
}

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
	// The total size of the key and value pairs (not the total size of the
	// SSTs). Updated on SST finalization.
	dataSize int64
	// The total size of the SSTs.
	sstSize int64
	// Incremental count of number of bytes written to disk.
	writeBytes int64
	// if skipClearForMVCCSpan is true, the MVCC span is not ClearEngineRange()d in
	// the same sstable. We rely on the caller to take care of clearing this span
	// through a different process (eg. IngestAndExcise on pebble). Note that
	// having this bool to true also disables all range key fragmentation
	// and splitting of sstables in the mvcc span.
	skipClearForMVCCSpan bool
	// maxSSTSize is the maximum size to use for SSTs containing MVCC/user keys.
	// Once the sstable writer reaches this size, it will be finalized and a new
	// sstable will be created.
	maxSSTSize int64
	// rangeKeyFrag is used to fragment range keys across the mvcc key spans.
	rangeKeyFrag rangekey.Fragmenter
}

func newMultiSSTWriter(
	ctx context.Context,
	st *cluster.Settings,
	scratch *SSTSnapshotStorageScratch,
	localKeySpans []roachpb.Span,
	mvccKeySpan roachpb.Span,
	sstChunkSize int64,
	skipClearForMVCCSpan bool,
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
		sstChunkSize:         sstChunkSize,
		skipClearForMVCCSpan: skipClearForMVCCSpan,
	}
	if !skipClearForMVCCSpan && rangeKeysInOrder {
		// If skipClearForMVCCSpan is true, we don't split the MVCC span across
		// multiple sstables, as addClearForMVCCSpan could be called by the caller
		// at any time.
		//
		// We also disable snapshot sstable splitting unless the sender has
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

	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}
	return msstw, nil
}

func (msstw *multiSSTWriter) emitRangeKey(key rangekey.Span) {
	for i := range key.Keys {
		if err := msstw.currSST.PutInternalRangeKey(key.Start, key.End, key.Keys[i]); err != nil {
			panic(fmt.Sprintf("failed to put range key in sst: %s", err))
		}
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
	if !msstw.currSpanIsMVCCSpan() || (!msstw.skipClearForMVCCSpan && msstw.currSpan <= len(msstw.localKeySpans)) {
		// We're either in a local key span, or we're in the first MVCC sstable
		// span (before any splits). Add a RangeKeyDel for the whole span. If this
		// is the MVCC span, we don't need to keep re-adding it to the fragmenter
		// as the fragmenter will take care of splits. Note that currentSpan()
		// will return the entire mvcc span in the case we're at an MVCC span.
		startKey := storage.EngineKey{Key: msstw.currentSpan().Key}.Encode()
		endKey := storage.EngineKey{Key: msstw.currentSpan().EndKey}.Encode()
		trailer := pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeKeyDelete)
		s := rangekey.Span{Start: startKey, End: endKey, Keys: []rangekey.Key{{Trailer: trailer}}}
		msstw.rangeKeyFrag.Add(s)
	}
	return nil
}

// NB: when nextKey is non-nil, do not do anything in this function to cause
// nextKey at the caller to escape to the heap.
func (msstw *multiSSTWriter) finalizeSST(ctx context.Context, nextKey *storage.EngineKey) error {
	currSpan := msstw.currentSpan()
	if msstw.currSpanIsMVCCSpan() {
		// We're in the MVCC span (ie. MVCC / user keys). If skipClearForMVCCSpan
		// is true, we don't write a clearRange for the last span at all. Otherwise,
		// we need to write a clearRange for all keys leading up to the current key
		// we're writing.
		currEngineSpan := msstw.mvccSSTSpans[msstw.currSpan-len(msstw.localKeySpans)]
		if !msstw.skipClearForMVCCSpan {
			if err := msstw.currSST.ClearEngineRange(
				currEngineSpan.Start, currEngineSpan.End,
			); err != nil {
				msstw.currSST.Close()
				return errors.Wrap(err, "failed to clear range on sst file writer")
			}
		}
	} else {
		if err := msstw.currSST.ClearRawRange(
			currSpan.Key, currSpan.EndKey,
			true /* pointKeys */, false, /* rangeKeys */
		); err != nil {
			msstw.currSST.Close()
			return errors.Wrap(err, "failed to clear range on sst file writer")
		}
	}

	// If we're at the last span, call Finish on the fragmenter. If we're not at the
	// last span, call Truncate.
	if msstw.currSpan == len(msstw.localKeySpans)+len(msstw.mvccSSTSpans)-1 {
		msstw.rangeKeyFrag.Finish()
	} else {
		endKey := storage.EngineKey{Key: currSpan.EndKey}
		if msstw.currSpanIsMVCCSpan() {
			endKey = msstw.mvccSSTSpans[msstw.currSpan-len(msstw.localKeySpans)].End
		}
		msstw.rangeKeyFrag.Truncate(endKey.Encode())
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
	// Account for any additional bytes written other than the KV data.
	msstw.writeBytes += int64(msstw.currSST.Meta.Size) - msstw.currSST.DataSize
	msstw.dataSize += msstw.currSST.DataSize
	msstw.sstSize += int64(msstw.currSST.Meta.Size)
	msstw.currSpan++
	msstw.currSST.Close()
	return nil
}

// addClearForMVCCSpan allows us to explicitly add a deletion tombstone
// for the mvcc span in the msstw, if it was instantiated with the expectation
// that no tombstone was necessary.
func (msstw *multiSSTWriter) addClearForMVCCSpan() error {
	if !msstw.skipClearForMVCCSpan {
		// Nothing to do.
		return nil
	}
	if msstw.currSpan < len(msstw.localKeySpans) {
		// When we switch to the mvcc key span, we will just add a rangedel for it.
		// Set skipClearForMVCCSpan to false.
		msstw.skipClearForMVCCSpan = false
		return nil
	}
	if msstw.currSpan >= len(msstw.localKeySpans) {
		panic("cannot clearEngineRange if sst writer has moved past user keys")
	}
	panic("multiSSTWriter already added keys to sstable that cannot be deleted by a rangedel/rangekeydel within it")
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
	prevWriteBytes := msstw.currSST.EstimatedSize()
	if err := msstw.currSST.PutEngineKey(key, value); err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}
	msstw.writeBytes += int64(msstw.currSST.EstimatedSize() - prevWriteBytes)
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
	prevWriteBytes := msstw.currSST.EstimatedSize()
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
	msstw.writeBytes += int64(msstw.currSST.EstimatedSize() - prevWriteBytes)
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
	if !msstw.skipClearForMVCCSpan {
		panic("can only add internal range deletes to multiSSTWriter if skipClearForMVCCSpan is true")
	}
	decodedStart, decodedEnd, err := decodeRangeStartEnd(start, end)
	if err != nil {
		return err
	}
	if err := msstw.rolloverSST(ctx, decodedStart, decodedEnd); err != nil {
		return err
	}
	prevWriteBytes := msstw.currSST.EstimatedSize()
	if err := msstw.currSST.ClearRawEncodedRange(start, end); err != nil {
		return errors.Wrap(err, "failed to put range delete in sst")
	}
	msstw.writeBytes += int64(msstw.currSST.EstimatedSize() - prevWriteBytes)
	return nil
}

func (msstw *multiSSTWriter) PutInternalRangeKey(
	ctx context.Context, start, end []byte, key rangekey.Key,
) error {
	if !msstw.skipClearForMVCCSpan {
		panic("can only add internal range deletes to multiSSTWriter if skipClearForMVCCSpan is true")
	}
	decodedStart, decodedEnd, err := decodeRangeStartEnd(start, end)
	if err != nil {
		return err
	}
	if err := msstw.rolloverSST(ctx, decodedStart, decodedEnd); err != nil {
		return err
	}
	prevWriteBytes := msstw.currSST.EstimatedSize()
	if err := msstw.currSST.PutInternalRangeKey(start, end, key); err != nil {
		return errors.Wrap(err, "failed to put range key in sst")
	}
	msstw.writeBytes += int64(msstw.currSST.EstimatedSize() - prevWriteBytes)
	return nil
}

func (msstw *multiSSTWriter) PutRangeKey(
	ctx context.Context, start, end roachpb.Key, suffix []byte, value []byte,
) error {
	if start.Compare(end) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}
	if err := msstw.rolloverSST(ctx, storage.EngineKey{Key: start}, storage.EngineKey{Key: end}); err != nil {
		return err
	}
	if msstw.skipClearForMVCCSpan {
		prevWriteBytes := msstw.currSST.EstimatedSize()
		// Skip the fragmenter. See the comment in skipClearForMVCCSpan.
		if err := msstw.currSST.PutEngineRangeKey(start, end, suffix, value); err != nil {
			return errors.Wrap(err, "failed to put range key in sst")
		}
		msstw.writeBytes += int64(msstw.currSST.EstimatedSize() - prevWriteBytes)
		return nil
	}

	startKey, endKey := storage.EngineKey{Key: start}.Encode(), storage.EngineKey{Key: end}.Encode()
	startTrailer := pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeKeySet)
	msstw.rangeKeyFrag.Add(rangekey.Span{
		Start: startKey,
		End:   endKey,
		Keys:  []rangekey.Key{{Trailer: startTrailer, Suffix: suffix, Value: value}},
	})
	return nil
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

// snapshotTimingTag represents a lazy tracing span tag containing information
// on how long individual parts of a snapshot take. Individual stopwatches can
// be added to a snapshotTimingTag.
type snapshotTimingTag struct {
	mu struct {
		syncutil.Mutex
		stopwatches map[string]*timeutil.StopWatch
	}
}

// newSnapshotTimingTag creates a new snapshotTimingTag.
func newSnapshotTimingTag() *snapshotTimingTag {
	tag := snapshotTimingTag{}
	tag.mu.stopwatches = make(map[string]*timeutil.StopWatch)
	return &tag
}

// addStopwatch adds the given stopwatch to the tag's map of stopwatches.
func (tag *snapshotTimingTag) addStopwatch(name string) {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	tag.mu.stopwatches[name] = timeutil.NewStopWatch()
}

// start begins the stopwatch corresponding to name if the stopwatch
// exists and shouldRecord is true.
func (tag *snapshotTimingTag) start(name string) {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	if stopwatch, ok := tag.mu.stopwatches[name]; ok {
		stopwatch.Start()
	}
}

// stop ends the stopwatch corresponding to name if the stopwatch
// exists and shouldRecord is true.
func (tag *snapshotTimingTag) stop(name string) {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	if stopwatch, ok := tag.mu.stopwatches[name]; ok {
		stopwatch.Stop()
	}
}

// Render implements the tracing.LazyTag interface. It returns a map of each
// stopwatch's name to the stopwatch's elapsed time.
func (tag *snapshotTimingTag) Render() []attribute.KeyValue {
	tag.mu.Lock()
	defer tag.mu.Unlock()
	tags := make([]attribute.KeyValue, 0, len(tag.mu.stopwatches))
	for name, stopwatch := range tag.mu.stopwatches {
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(name),
			Value: attribute.StringValue(string(humanizeutil.Duration(stopwatch.Elapsed()))),
		})
	}
	return tags
}

// stubBackingHandle is a stub implementation of RemoteObjectBackingHandle
// that just wraps a RemoteObjectBacking. This is used by a snapshot receiver as
// it is on a different node than the one that created the original
// RemoteObjectBackingHandle, so the Close() function is a no-op.
type stubBackingHandle struct {
	backing objstorage.RemoteObjectBacking
}

// Get implements the RemoteObjectBackingHandle interface.
func (s stubBackingHandle) Get() (objstorage.RemoteObjectBacking, error) {
	return s.backing, nil
}

// Close implements the RemoteObjectBackingHandle interface.
func (s stubBackingHandle) Close() {
	// No-op.
}

var _ objstorage.RemoteObjectBackingHandle = &stubBackingHandle{}

// Receive implements the snapshotStrategy interface.
//
// NOTE: This function assumes that the point and range (e.g. MVCC range
// tombstone) KV pairs are sent grouped by the following key spans in order:
//
// 1. Replicated range-id local key span.
// 2. Range-local key span.
// 3. Two lock-table key spans (optional).
// 4. User key span.
//
// For each key span above, all point keys are sent first (in sorted order) and
// then all range keys (in sorted order), possibly mixed in the same batch.
// However, we currently only expect to see range keys in the user key span.
//
// This allows building individual SSTs per key span containing all point/range
// KVs for that key span, without the SSTs spanning across wide swaths of the
// key space across to the next key span.
func (kvSS *kvBatchSnapshotStrategy) Receive(
	ctx context.Context,
	s *Store,
	stream incomingSnapshotStream,
	header kvserverpb.SnapshotRequest_Header,
	recordBytesReceived snapshotRecordMetrics,
) (IncomingSnapshot, error) {
	if fn := s.cfg.TestingKnobs.BeforeRecvAcceptedSnapshot; fn != nil {
		fn()
	}
	snapshotCtx := ctx
	ctx, rSp := tracing.EnsureChildSpan(ctx, s.cfg.Tracer(), "receive snapshot data")
	defer rSp.Finish() // Ensure that the tracing span is closed, even if Receive errors.

	// These stopwatches allow us to time the various components of Receive().
	// - totalTime Stopwatch measures the total time spent within this function.
	// - sst Stopwatch measures the time it takes to write the data from the
	//   snapshot into SSTs
	// - recv Stopwatch records the amount of time spent waiting on the gRPC stream
	//   and receiving the data from the stream. NB: this value encapsulates wait
	//   time due to sender-side rate limiting
	timingTag := newSnapshotTimingTag()
	timingTag.addStopwatch("totalTime")
	timingTag.addStopwatch("sst")
	timingTag.addStopwatch("recv")
	if sp := tracing.SpanFromContext(ctx); sp != nil {
		sp.SetLazyTag(tagSnapshotTiming, timingTag)
	}

	timingTag.start("totalTime")

	// At the moment we'll write at most five SSTs.
	// TODO(jeffreyxiao): Re-evaluate as the default range size grows.
	keyRanges := rditer.MakeReplicatedKeySpans(header.State.Desc)

	doExcise := header.SharedReplicate || header.ExternalReplicate || storage.UseExciseForSnapshots.Get(&s.ClusterSettings().SV)
	if header.SharedReplicate && !s.cfg.SharedStorageEnabled {
		return noSnap, sendSnapshotError(ctx, s, stream, errors.New("cannot accept shared sstables"))
	}

	// We rely on the last keyRange passed into multiSSTWriter being the user key
	// span. If the sender signals that it can no longer do shared replication
	// (with a TransitionFromSharedToRegularReplicate = true), we will have to
	// switch to adding a rangedel for that span. Since multiSSTWriter acts on an
	// opaque slice of keyRanges, we just tell it to add a rangedel for the last
	// span. To avoid bugs, assert on the last span in keyRanges actually being
	// equal to the user key span.
	if doExcise {
		if !keyRanges[len(keyRanges)-1].Equal(header.State.Desc.KeySpan().AsRawSpanWithNoLocals()) {
			return noSnap, errors.AssertionFailedf("last span in multiSSTWriter did not equal the user key span: %s", keyRanges[len(keyRanges)-1].String())
		}
	}

	// TODO(aaditya): Remove once we support flushableIngests for shared and
	// external files in the engine.
	skipClearForMVCCSpan := doExcise && (header.SharedReplicate || header.ExternalReplicate)
	// The last key range is the user key span.
	localRanges := keyRanges[:len(keyRanges)-1]
	mvccRange := keyRanges[len(keyRanges)-1]
	msstw, err := newMultiSSTWriter(ctx, kvSS.st, kvSS.scratch, localRanges, mvccRange, kvSS.sstChunkSize, skipClearForMVCCSpan, header.RangeKeysInOrder)
	if err != nil {
		return noSnap, err
	}
	defer msstw.Close()

	log.Event(ctx, "waiting for snapshot batches to begin")

	var sharedSSTs []pebble.SharedSSTMeta
	var externalSSTs []pebble.ExternalFile
	var prevWriteBytes int64

	snapshotQ := s.cfg.KVAdmissionController.GetSnapshotQueue(s.StoreID())
	if snapshotQ == nil {
		log.Errorf(ctx, "unable to find snapshot queue for store: %s", s.StoreID())
	}
	// Using a nil pacer is effectively a noop if snapshot control is disabled.
	var pacer *admission.SnapshotPacer = nil
	if admission.DiskBandwidthForSnapshotIngest.Get(&s.cfg.Settings.SV) && snapshotQ != nil {
		pacer = admission.NewSnapshotPacer(snapshotQ)
	}

	for {
		timingTag.start("recv")
		req, err := stream.Recv()
		timingTag.stop("recv")
		if err != nil {
			return noSnap, err
		}
		if req.Header != nil {
			err := errors.New("client error: provided a header mid-stream")
			return noSnap, sendSnapshotError(snapshotCtx, s, stream, err)
		}
		if req.TransitionFromSharedToRegularReplicate {
			doExcise = false
			sharedSSTs = nil
			externalSSTs = nil
			if err := msstw.addClearForMVCCSpan(); err != nil {
				return noSnap, errors.Wrap(err, "adding tombstone for last span")
			}
		}

		if req.KVBatch != nil {
			recordBytesReceived(int64(len(req.KVBatch)))
			batchReader, err := storage.NewBatchReader(req.KVBatch)
			if err != nil {
				return noSnap, errors.Wrap(err, "failed to decode batch")
			}

			timingTag.start("sst")
			verifyCheckSum := snapshotChecksumVerification.Get(&s.ClusterSettings().SV)
			// All batch operations are guaranteed to be point key or range key puts.
			for batchReader.Next() {
				// TODO(lyang24): maybe avoid decoding engine key twice.
				// msstw calls (i.e. PutInternalPointKey) can use the decoded engine key here as input.

				writeBytes := msstw.writeBytes - prevWriteBytes
				// Calling nil pacer is a noop.
				if err := pacer.Pace(ctx, writeBytes, false /* final */); err != nil {
					return noSnap, errors.Wrapf(err, "snapshot admission pacer")
				}
				prevWriteBytes = msstw.writeBytes

				ek, err := batchReader.EngineKey()
				if err != nil {
					return noSnap, err
				}
				// Verify value checksum to catch data corruption.
				if verifyCheckSum {
					if err = ek.Verify(batchReader.Value()); err != nil {
						return noSnap, errors.Wrap(err, "verifying value checksum")
					}
				}
				switch batchReader.KeyKind() {
				case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
					if err := msstw.Put(ctx, ek, batchReader.Value()); err != nil {
						return noSnap, errors.Wrapf(err, "writing sst for raft snapshot")
					}
				case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
					if !doExcise {
						return noSnap, errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
					}
					if err := msstw.PutInternalPointKey(ctx, batchReader.Key(), batchReader.KeyKind(), nil); err != nil {
						return noSnap, errors.Wrapf(err, "writing sst for raft snapshot")
					}
				case pebble.InternalKeyKindRangeDelete:
					if !doExcise {
						return noSnap, errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
					}
					start := batchReader.Key()
					end, err := batchReader.EndKey()
					if err != nil {
						return noSnap, err
					}
					if err := msstw.PutInternalRangeDelete(ctx, start, end); err != nil {
						return noSnap, errors.Wrapf(err, "writing sst for raft snapshot")
					}

				case pebble.InternalKeyKindRangeKeyUnset, pebble.InternalKeyKindRangeKeyDelete:
					if !doExcise {
						return noSnap, errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
					}
					start := batchReader.Key()
					end, err := batchReader.EndKey()
					if err != nil {
						return noSnap, err
					}
					rangeKeys, err := batchReader.RawRangeKeys()
					if err != nil {
						return noSnap, err
					}
					for _, rkv := range rangeKeys {
						err := msstw.PutInternalRangeKey(ctx, start, end, rkv)
						if err != nil {
							return noSnap, errors.Wrapf(err, "writing sst for raft snapshot")
						}
					}
				case pebble.InternalKeyKindRangeKeySet:
					start := ek
					end, err := batchReader.EngineEndKey()
					if err != nil {
						return noSnap, err
					}
					rangeKeys, err := batchReader.EngineRangeKeys()
					if err != nil {
						return noSnap, err
					}
					for _, rkv := range rangeKeys {
						err := msstw.PutRangeKey(ctx, start.Key, end.Key, rkv.Version, rkv.Value)
						if err != nil {
							return noSnap, errors.Wrapf(err, "writing sst for raft snapshot")
						}
					}

				default:
					return noSnap, errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
				}
			}
			if batchReader.Error() != nil {
				return noSnap, err
			}
			timingTag.stop("sst")
		}
		if len(req.SharedTables) > 0 && doExcise {
			for i := range req.SharedTables {
				sst := req.SharedTables[i]
				pbToInternalKey := func(k *kvserverpb.SnapshotRequest_SharedTable_InternalKey) pebble.InternalKey {
					return pebble.InternalKey{UserKey: k.UserKey, Trailer: pebble.InternalKeyTrailer(k.Trailer)}
				}
				sharedSSTs = append(sharedSSTs, pebble.SharedSSTMeta{
					Backing:          stubBackingHandle{sst.Backing},
					Smallest:         pbToInternalKey(sst.Smallest),
					Largest:          pbToInternalKey(sst.Largest),
					SmallestRangeKey: pbToInternalKey(sst.SmallestRangeKey),
					LargestRangeKey:  pbToInternalKey(sst.LargestRangeKey),
					SmallestPointKey: pbToInternalKey(sst.SmallestPointKey),
					LargestPointKey:  pbToInternalKey(sst.LargestPointKey),
					Level:            uint8(sst.Level),
					Size:             sst.Size_,
				})
			}
		}
		if len(req.ExternalTables) > 0 && doExcise {
			for i := range req.ExternalTables {
				sst := req.ExternalTables[i]
				externalSSTs = append(externalSSTs, pebble.ExternalFile{
					Locator:           remote.Locator(sst.Locator),
					ObjName:           sst.ObjectName,
					StartKey:          sst.StartKey,
					EndKey:            sst.EndKey,
					EndKeyIsInclusive: sst.EndKeyIsInclusive,
					HasPointKey:       sst.HasPointKey,
					HasRangeKey:       sst.HasRangeKey,
					SyntheticPrefix:   sst.SyntheticPrefix,
					SyntheticSuffix:   sst.SyntheticSuffix,
					Level:             uint8(sst.Level),
					Size:              sst.Size_,
				})
			}
		}
		if req.Final {
			// We finished receiving all batches and log entries. It's possible that
			// we did not receive any key-value pairs for some of the key spans, but
			// we must still construct SSTs with range deletion tombstones to remove
			// the data.
			timingTag.start("sst")
			dataSize, err := msstw.Finish(ctx)
			sstSize := msstw.sstSize
			if err != nil {
				return noSnap, errors.Wrapf(err, "finishing sst for raft snapshot")
			}
			// Defensive call to account for any discrepancies. The SST sizes should
			// have been updated upon closing.
			additionalWrites := sstSize - msstw.writeBytes
			if err := pacer.Pace(ctx, additionalWrites, true /* final */); err != nil {
				return noSnap, errors.Wrapf(err, "snapshot admission pacer")
			}
			msstw.Close()
			timingTag.stop("sst")
			log.Eventf(ctx, "all data received from snapshot and all SSTs were finalized")
			var sharedSize int64
			for i := range sharedSSTs {
				sharedSize += int64(sharedSSTs[i].Size)
			}

			snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
			if err != nil {
				err = errors.Wrap(err, "client error: invalid snapshot")
				return noSnap, sendSnapshotError(snapshotCtx, s, stream, err)
			}

			inSnap := IncomingSnapshot{
				SnapUUID:                    snapUUID,
				SSTStorageScratch:           kvSS.scratch,
				FromReplica:                 header.RaftMessageRequest.FromReplica,
				Desc:                        header.State.Desc,
				DataSize:                    dataSize,
				SSTSize:                     sstSize,
				SharedSize:                  sharedSize,
				raftAppliedIndex:            header.State.RaftAppliedIndex,
				msgAppRespCh:                make(chan raftpb.Message, 1),
				sharedSSTs:                  sharedSSTs,
				externalSSTs:                externalSSTs,
				doExcise:                    doExcise,
				includesRangeDelForLastSpan: !skipClearForMVCCSpan,
				clearedSpans:                keyRanges,
			}

			timingTag.stop("totalTime")

			kvSS.status = redact.Sprintf("local ssts: %d, shared ssts: %d, external ssts: %d", len(kvSS.scratch.SSTs()), len(sharedSSTs), len(externalSSTs))
			return inSnap, nil
		}
	}
}

// Send implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Send(
	ctx context.Context,
	stream outgoingSnapshotStream,
	header kvserverpb.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	recordBytesSent snapshotRecordMetrics,
) (int64, error) {
	// bytesSent is updated as key-value batches are sent with sendBatch. It does
	// not reflect the log entries sent (which are never sent in newer versions of
	// CRDB, as of VersionUnreplicatedTruncatedState).
	var bytesSent int64
	var kvs, rangeKVs, sharedSSTCount, externalSSTCount int

	// These stopwatches allow us to time the various components of Send().
	// - totalTimeStopwatch measures the total time spent within this function.
	// - iterStopwatch measures how long it takes to read from the snapshot via
	//   iter.Next().
	// - sendStopwatch measure the time it takes to send the snapshot batch data
	//   over the network, excluding waits due to rate limiting.
	// - rateLimitStopwatch records the amount of time spent waiting in order to
	//   enforce the snapshot rate limit
	timingTag := newSnapshotTimingTag()
	timingTag.addStopwatch("totalTime")
	timingTag.addStopwatch("iter")
	timingTag.addStopwatch("send")
	timingTag.addStopwatch("rateLimit")
	if sp := tracing.SpanFromContext(ctx); sp != nil {
		log.Eventf(ctx, "found span %s", sp.OperationName())
		sp.SetLazyTag(tagSnapshotTiming, timingTag)
	}

	log.Event(ctx, "beginning to send batches of snapshot bytes")
	timingTag.start("totalTime")

	// Iterate over all keys (point keys and range keys) and stream out batches of
	// key-values.
	var b storage.WriteBatch
	var sharedSSTs []kvserverpb.SnapshotRequest_SharedTable
	var externalSSTs []kvserverpb.SnapshotRequest_ExternalTable
	var transitionFromSharedToRegularReplicate bool
	defer func() {
		if b != nil {
			b.Close()
		}
	}()

	flushBatch := func() error {
		if err := kvSS.sendBatch(ctx, stream, b, sharedSSTs, externalSSTs, transitionFromSharedToRegularReplicate, timingTag); err != nil {
			return err
		}
		bLen := int64(b.Len())
		bytesSent += bLen
		recordBytesSent(bLen)
		b.Close()
		b = nil
		sharedSSTs = sharedSSTs[:0]
		externalSSTs = externalSSTs[:0]
		transitionFromSharedToRegularReplicate = false
		return nil
	}

	maybeFlushBatch := func() error {
		if int64(b.Len()) >= kvSS.batchSize {
			return flushBatch()
		}
		return nil
	}

	// If snapshots containing shared files are allowed, and this range is a
	// non-system range, take advantage of shared storage to minimize the amount
	// of data we're iterating on and sending over the network.
	sharedReplicate := header.SharedReplicate && rditer.IterateReplicaKeySpansShared != nil
	externalReplicate := header.ExternalReplicate && rditer.IterateReplicaKeySpansShared != nil
	replicatedFilter := rditer.ReplicatedSpansAll
	if sharedReplicate || externalReplicate {
		replicatedFilter = rditer.ReplicatedSpansExcludeUser
	}

	iterateRKSpansVisitor := func(iter storage.EngineIterator, _ roachpb.Span) error {
		timingTag.start("iter")
		defer timingTag.stop("iter")

		var err error
		for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
			hasPoint, hasRange := iter.HasPointAndRange()
			if hasRange && iter.RangeKeyChanged() {
				bounds, err := iter.EngineRangeBounds()
				if err != nil {
					return err
				}
				for _, rkv := range iter.EngineRangeKeys() {
					rangeKVs++
					if b == nil {
						b = kvSS.newWriteBatch()
					}
					err := b.PutEngineRangeKey(bounds.Key, bounds.EndKey, rkv.Version, rkv.Value)
					if err != nil {
						return err
					}
					if err = maybeFlushBatch(); err != nil {
						return err
					}
				}
			}
			if hasPoint {
				kvs++
				if b == nil {
					b = kvSS.newWriteBatch()
				}
				key, err := iter.UnsafeEngineKey()
				if err != nil {
					return err
				}
				v, err := iter.UnsafeValue()
				if err != nil {
					return err
				}
				if err = b.PutEngineKey(key, v); err != nil {
					return err
				}
				if err = maybeFlushBatch(); err != nil {
					return err
				}
			}
		}
		return err
	}
	err := rditer.IterateReplicaKeySpans(ctx, snap.State.Desc, snap.EngineSnap, true, /* replicatedOnly */
		replicatedFilter, iterateRKSpansVisitor)
	if err != nil {
		return 0, err
	}

	var valBuf []byte
	if sharedReplicate || externalReplicate {
		var sharedVisitor func(sst *pebble.SharedSSTMeta) error
		if sharedReplicate {
			sharedVisitor = func(sst *pebble.SharedSSTMeta) error {
				sharedSSTCount++
				snap.sharedBackings = append(snap.sharedBackings, sst.Backing)
				backing, err := sst.Backing.Get()
				if err != nil {
					return err
				}
				ikeyToPb := func(ik pebble.InternalKey) *kvserverpb.SnapshotRequest_SharedTable_InternalKey {
					return &kvserverpb.SnapshotRequest_SharedTable_InternalKey{
						UserKey: ik.UserKey,
						Trailer: uint64(ik.Trailer),
					}
				}
				sharedSSTs = append(sharedSSTs, kvserverpb.SnapshotRequest_SharedTable{
					Backing:          backing,
					Smallest:         ikeyToPb(sst.Smallest),
					Largest:          ikeyToPb(sst.Largest),
					SmallestRangeKey: ikeyToPb(sst.SmallestRangeKey),
					LargestRangeKey:  ikeyToPb(sst.LargestRangeKey),
					SmallestPointKey: ikeyToPb(sst.SmallestPointKey),
					LargestPointKey:  ikeyToPb(sst.LargestPointKey),
					Level:            int32(sst.Level),
					Size_:            sst.Size,
				})
				return nil
			}
		}
		var externalVisitor func(sst *pebble.ExternalFile) error
		if externalReplicate {
			externalVisitor = func(sst *pebble.ExternalFile) error {
				externalSSTCount++
				externalSSTs = append(externalSSTs, kvserverpb.SnapshotRequest_ExternalTable{
					Locator:           []byte(sst.Locator),
					ObjectName:        sst.ObjName,
					Size_:             sst.Size,
					StartKey:          sst.StartKey,
					EndKey:            sst.EndKey,
					EndKeyIsInclusive: sst.EndKeyIsInclusive,
					HasPointKey:       sst.HasPointKey,
					HasRangeKey:       sst.HasRangeKey,
					SyntheticPrefix:   sst.SyntheticPrefix,
					SyntheticSuffix:   sst.SyntheticSuffix,
					Level:             int32(sst.Level),
				})
				return nil
			}
		}
		err := rditer.IterateReplicaKeySpansShared(ctx, snap.State.Desc, kvSS.st, kvSS.clusterID, snap.EngineSnap, func(key *pebble.InternalKey, value pebble.LazyValue, _ pebble.IteratorLevel) error {
			kvs++
			if b == nil {
				b = kvSS.newWriteBatch()
			}
			var val []byte
			switch key.Kind() {
			case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete, pebble.InternalKeyKindMerge:
				var callerOwned bool
				var err error
				val, callerOwned, err = value.Value(valBuf)
				if err != nil {
					return err
				}
				if callerOwned && val != nil {
					valBuf = val[:0]
				}
			}
			if err := b.PutInternalPointKey(key, val); err != nil {
				return err
			}
			return maybeFlushBatch()
		}, func(start, end []byte, seqNum pebble.SeqNum) error {
			kvs++
			if b == nil {
				b = kvSS.newWriteBatch()
			}
			if err := b.ClearRawEncodedRange(start, end); err != nil {
				return err
			}
			return maybeFlushBatch()
		}, func(start, end []byte, keys []rangekey.Key) error {
			if b == nil {
				b = kvSS.newWriteBatch()
			}
			for i := range keys {
				rangeKVs++
				err := b.PutInternalRangeKey(start, end, keys[i])
				if err != nil {
					return err
				}
			}
			return maybeFlushBatch()
		}, sharedVisitor, externalVisitor)
		if err != nil && errors.Is(err, pebble.ErrInvalidSkipSharedIteration) {
			transitionFromSharedToRegularReplicate = true
			err = rditer.IterateReplicaKeySpans(ctx, snap.State.Desc, snap.EngineSnap, true, /* replicatedOnly */
				rditer.ReplicatedSpansUserOnly, iterateRKSpansVisitor)
		}
		if err != nil {
			return 0, err
		}
	}
	if b != nil {
		if err = flushBatch(); err != nil {
			return 0, err
		}
	}

	timingTag.stop("totalTime")
	log.Eventf(ctx, "finished sending snapshot batches, sent a total of %d bytes", bytesSent)

	kvSS.status = redact.Sprintf("kvs=%d rangeKVs=%d sharedSSTs=%d, externalSSTs=%d", kvs, rangeKVs, sharedSSTCount, externalSSTCount)
	return bytesSent, nil
}

func (kvSS *kvBatchSnapshotStrategy) sendBatch(
	ctx context.Context,
	stream outgoingSnapshotStream,
	batch storage.WriteBatch,
	sharedSSTs []kvserverpb.SnapshotRequest_SharedTable,
	externalSSTs []kvserverpb.SnapshotRequest_ExternalTable,
	transitionToRegularReplicate bool,
	timerTag *snapshotTimingTag,
) error {
	timerTag.start("rateLimit")
	err := kvSS.limiter.WaitN(ctx, 1)
	timerTag.stop("rateLimit")
	if err != nil {
		return err
	}
	timerTag.start("send")
	res := stream.Send(&kvserverpb.SnapshotRequest{
		KVBatch:                                batch.Repr(),
		SharedTables:                           sharedSSTs,
		ExternalTables:                         externalSSTs,
		TransitionFromSharedToRegularReplicate: transitionToRegularReplicate,
	})
	timerTag.stop("send")
	return res
}

// Status implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Status() redact.RedactableString {
	return kvSS.status
}

// Close implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Close(ctx context.Context) {
	if kvSS.scratch != nil {
		// A failure to clean up the storage is benign except that it will leak
		// disk space (which is reclaimed on node restart). It is unexpected
		// though, so log a warning.
		if err := kvSS.scratch.Close(); err != nil {
			log.Warningf(ctx, "error closing kvBatchSnapshotStrategy: %v", err)
		}
	}
}

// reserveReceiveSnapshot reserves space for this snapshot which will attempt to
// prevent overload of system resources as this snapshot is being sent.
// Snapshots are often sent in bulk (due to operations like store decommission)
// so it is necessary to prevent snapshot transfers from overly impacting
// foreground traffic.
func (s *Store) reserveReceiveSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header,
) (_cleanup func(), _err error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, s.cfg.Tracer(), "reserveReceiveSnapshot")
	defer sp.Finish()

	return s.throttleSnapshot(ctx,
		s.snapshotApplyQueue,
		int(header.SenderQueueName),
		header.SenderQueuePriority,
		-1,
		header.RangeSize,
		header.RaftMessageRequest.RangeID,
		snapshotMetrics{
			s.metrics.RangeSnapshotRecvQueueLength,
			s.metrics.RangeSnapshotRecvQueueSize,
			s.metrics.RangeSnapshotRecvInProgress,
			s.metrics.RangeSnapshotRecvTotalInProgress,
		},
	)
}

// reserveSendSnapshot throttles outgoing snapshots.
func (s *Store) reserveSendSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest, rangeSize int64,
) (_cleanup func(), _err error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, s.cfg.Tracer(), "reserveSendSnapshot")
	defer sp.Finish()
	if fn := s.cfg.TestingKnobs.BeforeSendSnapshotThrottle; fn != nil {
		fn()
	}

	return s.throttleSnapshot(ctx,
		s.snapshotSendQueue,
		int(req.SenderQueueName),
		req.SenderQueuePriority,
		req.QueueOnDelegateLen,
		rangeSize,
		req.RangeID,
		snapshotMetrics{
			s.metrics.RangeSnapshotSendQueueLength,
			s.metrics.RangeSnapshotSendQueueSize,
			s.metrics.RangeSnapshotSendInProgress,
			s.metrics.RangeSnapshotSendTotalInProgress,
		},
	)
}

// throttleSnapshot is a helper function to throttle snapshot sending and
// receiving. The returned closure is used to cleanup the reservation and
// release its resources.
func (s *Store) throttleSnapshot(
	ctx context.Context,
	snapshotQueue *multiqueue.MultiQueue,
	requestSource int,
	requestPriority float64,
	maxQueueLength int64,
	rangeSize int64,
	rangeID roachpb.RangeID,
	snapshotMetrics snapshotMetrics,
) (cleanup func(), funcErr error) {

	tBegin := timeutil.Now()
	var permit *multiqueue.Permit
	// Empty snapshots are exempt from rate limits because they're so cheap to
	// apply. This vastly speeds up rebalancing any empty ranges created by a
	// RESTORE or manual SPLIT AT, since it prevents these empty snapshots from
	// getting stuck behind large snapshots managed by the replicate queue.
	if rangeSize != 0 || s.cfg.TestingKnobs.ThrottleEmptySnapshots {
		task, err := snapshotQueue.Add(requestSource, requestPriority, maxQueueLength)
		if err != nil {
			return nil, err
		}
		// After this point, the task is on the queue, so any future errors need to
		// be handled by cancelling the task to release the permit.
		defer func() {
			if funcErr != nil {
				snapshotQueue.Cancel(task)
			}
		}()

		// Total bytes of snapshots waiting in the snapshot queue
		snapshotMetrics.QueueSize.Inc(rangeSize)
		defer snapshotMetrics.QueueSize.Dec(rangeSize)
		// Total number of snapshots waiting in the snapshot queue
		snapshotMetrics.QueueLen.Inc(1)
		defer snapshotMetrics.QueueLen.Dec(1)

		queueCtx := ctx
		if deadline, ok := queueCtx.Deadline(); ok {
			// Enforce a more strict timeout for acquiring the snapshot reservation to
			// ensure that if the reservation is acquired, the snapshot has sufficient
			// time to complete. See the comment on snapshotReservationQueueTimeoutFraction
			// and TestReserveSnapshotQueueTimeout.
			timeoutFrac := snapshotReservationQueueTimeoutFraction.Get(&s.ClusterSettings().SV)
			timeout := time.Duration(timeoutFrac * float64(timeutil.Until(deadline)))
			var cancel func()
			queueCtx, cancel = context.WithTimeout(queueCtx, timeout) // nolint:context
			defer cancel()
		}
		select {
		case permit = <-task.GetWaitChan():
			// Got a spot in the snapshotQueue, continue with sending the snapshot.
			if fn := s.cfg.TestingKnobs.AfterSnapshotThrottle; fn != nil {
				fn()
			}
			log.Event(ctx, "acquired spot in the snapshot snapshotQueue")
		case <-queueCtx.Done():
			// We need to cancel the task so that it doesn't ever get a permit.
			if err := ctx.Err(); err != nil {
				return nil, errors.Wrap(err, "acquiring snapshot reservation")
			}
			return nil, errors.Wrapf(
				queueCtx.Err(),
				"giving up during snapshot reservation due to cluster setting %q",
				snapshotReservationQueueTimeoutFraction.Name(),
			)
		case <-s.stopper.ShouldQuiesce():
			return nil, errors.Errorf("stopped")
		}

		// Counts non-empty in-progress snapshots.
		snapshotMetrics.InProgress.Inc(1)
	}
	// Counts all in-progress snapshots.
	snapshotMetrics.TotalInProgress.Inc(1)

	// The choice here is essentially arbitrary, but with a default range size of 128mb-512mb and the
	// Raft snapshot rate limiting of 32mb/s, we expect to spend less than 16s per snapshot.
	// which is what we want to log.
	const snapshotReservationWaitWarnThreshold = 32 * time.Second
	elapsed := timeutil.Since(tBegin)
	// NB: this log message is skipped in test builds as many tests do not mock
	// all of the objects being logged.
	if elapsed > snapshotReservationWaitWarnThreshold && !buildutil.CrdbTestBuild {
		log.Infof(
			ctx,
			"waited for %.1fs to acquire snapshot reservation to r%d",
			elapsed.Seconds(),
			rangeID,
		)
	}

	s.metrics.ReservedReplicaCount.Inc(1)
	s.metrics.Reserved.Inc(rangeSize)
	return func() {
		s.metrics.ReservedReplicaCount.Dec(1)
		s.metrics.Reserved.Dec(rangeSize)
		snapshotMetrics.TotalInProgress.Dec(1)

		if rangeSize != 0 || s.cfg.TestingKnobs.ThrottleEmptySnapshots {
			snapshotMetrics.InProgress.Dec(1)
			snapshotQueue.Release(permit)
		}
	}, nil
}

// canAcceptSnapshotLocked returns (_, nil) if the snapshot can be applied to
// this store's replica (i.e. the snapshot is not from an older incarnation of
// the replica) and a placeholder that can be (but is not yet) added to the
// replicasByKey map (if necessary).
//
// Both the store mu and the raft mu for the existing replica (which must exist)
// must be held.
func (s *Store) canAcceptSnapshotLocked(
	ctx context.Context, snapHeader *kvserverpb.SnapshotRequest_Header,
) (*ReplicaPlaceholder, error) {
	// TODO(tbg): see the comment on desc.Generation for what seems to be a much
	// saner way to handle overlap via generational semantics.
	desc := *snapHeader.State.Desc

	// First, check for an existing Replica.
	existingRepl, ok := s.mu.replicasByRangeID.Load(desc.RangeID)
	if !ok {
		return nil, errors.Errorf("canAcceptSnapshotLocked requires a replica present")
	}
	// The raftMu is held which allows us to use the existing replica as a
	// placeholder when we decide that the snapshot can be applied. As long as the
	// caller releases the raftMu only after feeding the snapshot into the
	// replica, this is safe. This is true even when the snapshot spans a merge,
	// because we will be guaranteed to have the subsumed (initialized) Replicas
	// in place as well. This is because they are present when the merge first
	// commits, and cannot have been replicaGC'ed yet (see replicaGCQueue.process).
	existingRepl.raftMu.AssertHeld()

	existingRepl.mu.RLock()
	existingDesc := existingRepl.shMu.state.Desc
	existingIsInitialized := existingDesc.IsInitialized()
	existingDestroyStatus := existingRepl.mu.destroyStatus
	existingRepl.mu.RUnlock()

	if existingIsInitialized {
		// Regular Raft snapshots can't be refused at this point,
		// even if they widen the existing replica. See the comments
		// in Replica.maybeAcquireSnapshotMergeLock for how this is
		// made safe.
		//
		// NB: The snapshot must be intended for this replica as
		// withReplicaForRequest ensures that requests with a non-zero replica
		// id are passed to a replica with a matching id.
		return nil, nil
	}

	// If we are not alive then we should not apply a snapshot as our removal
	// is imminent.
	if existingDestroyStatus.Removed() {
		return nil, existingDestroyStatus.err
	}

	// We have a key span [desc.StartKey,desc.EndKey) which we want to apply a
	// snapshot for. Is there a conflicting existing placeholder or an
	// overlapping range?
	if err := s.checkSnapshotOverlapLocked(ctx, snapHeader); err != nil {
		return nil, err
	}

	placeholder := &ReplicaPlaceholder{
		rangeDesc: desc,
	}
	return placeholder, nil
}

// checkSnapshotOverlapLocked returns an error if the snapshot overlaps an
// existing replica or placeholder. Any replicas that do overlap have a good
// chance of being abandoned, so they're proactively handed to the replica GC
// queue.
func (s *Store) checkSnapshotOverlapLocked(
	ctx context.Context, snapHeader *kvserverpb.SnapshotRequest_Header,
) error {
	desc := *snapHeader.State.Desc

	// NB: this check seems redundant since placeholders are also represented in
	// replicasByKey (and thus returned in getOverlappingKeyRangeLocked).
	if exRng, ok := s.mu.replicaPlaceholders[desc.RangeID]; ok {
		return errors.Mark(errors.Errorf(
			"%s: canAcceptSnapshotLocked: cannot add placeholder, have an existing placeholder %s %v",
			s, exRng, snapHeader.RaftMessageRequest.FromReplica),
			errMarkSnapshotError)
	}

	// TODO(benesch): consider discovering and GC'ing *all* overlapping ranges,
	// not just the first one that getOverlappingKeyRangeLocked happens to return.
	if it := s.getOverlappingKeyRangeLocked(&desc); it.item != nil {
		// We have a conflicting range, so we must block the snapshot.
		// When such a conflict exists, it will be resolved by one range
		// either being split or garbage collected.
		exReplica, err := s.GetReplica(it.Desc().RangeID)
		msg := IntersectingSnapshotMsg
		if err != nil {
			log.Warningf(ctx, "unable to look up overlapping replica on %s: %v", exReplica, err)
		} else {
			inactive := func(r *Replica) bool {
				if r.RaftStatus() == nil {
					return true
				}
				// TODO(benesch): this check does not detect inactivity on
				// replicas with epoch-based leases. Since the validity of an
				// epoch-based lease is tied to the owning node's liveness, the
				// lease can be valid well after the leader of the range has cut
				// off communication with this replica. Expiration based leases,
				// by contrast, will expire quickly if the leader of the range
				// stops sending this replica heartbeats.
				return !r.CurrentLeaseStatus(ctx).IsValid()
			}
			// We unconditionally send this replica through the replica GC queue. It's
			// reasonably likely that the replica GC queue will do nothing because the
			// replica needs to split instead, but better to err on the side of
			// queueing too frequently. Blocking Raft snapshots for too long can wedge
			// a cluster, and if the replica does need to be GC'd, this might be the
			// only code path that notices in a timely fashion.
			//
			// We're careful to avoid starving out other replicas in the replica GC
			// queue by queueing at a low priority unless we can prove that the range
			// is inactive and thus unlikely to be about to process a split.
			gcPriority := replicaGCPriorityDefault
			if inactive(exReplica) {
				gcPriority = replicaGCPrioritySuspect
			}

			msg += "; initiated GC:"
			s.replicaGCQueue.AddAsync(ctx, exReplica, gcPriority)
		}
		return errors.Mark(
			errors.Errorf("%s %v (incoming %v)", msg, exReplica, snapHeader.State.Desc.RSpan()), // exReplica can be nil
			errMarkSnapshotError,
		)
	}
	return nil
}

// getLocalityComparison takes two nodeIDs as input and returns the locality
// comparison result between their corresponding nodes. This result indicates
// whether the two nodes are located in different regions or zones.
func (s *Store) getLocalityComparison(
	ctx context.Context, fromNodeID roachpb.NodeID, toNodeID roachpb.NodeID,
) roachpb.LocalityComparisonType {
	firstLocality := s.cfg.StorePool.GetNodeLocality(fromNodeID)
	secLocality := s.cfg.StorePool.GetNodeLocality(toNodeID)
	comparisonResult, regionValid, zoneValid := firstLocality.CompareWithLocality(secLocality)
	if !regionValid {
		log.VEventf(ctx, 5, "unable to determine if the given nodes are cross region")
	}
	if !zoneValid {
		log.VEventf(ctx, 5, "unable to determine if the given nodes are cross zone")
	}
	return comparisonResult
}

// receiveSnapshot receives an incoming snapshot via a pre-opened GRPC stream.
func (s *Store) receiveSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header, stream incomingSnapshotStream,
) error {
	// Draining nodes will generally not be rebalanced to (see the filtering that
	// happens in getStoreListFromIDsLocked()), but in case they are, they should
	// reject the incoming rebalancing snapshots.
	if s.IsDraining() {
		switch t := header.SenderQueueName; t {
		case kvserverpb.SnapshotRequest_RAFT_SNAPSHOT_QUEUE:
			// We can not reject Raft snapshots because draining nodes may have
			// replicas in `StateSnapshot` that need to catch up.
		case kvserverpb.SnapshotRequest_REPLICATE_QUEUE:
			// Only reject if these are "rebalance" snapshots, not "recovery"
			// snapshots. We use the priority 0 to differentiate the types.
			if header.SenderQueuePriority == 0 {
				return sendSnapshotError(ctx, s, stream, errors.New(storeDrainingMsg))
			}
		case kvserverpb.SnapshotRequest_OTHER:
			return sendSnapshotError(ctx, s, stream, errors.New(storeDrainingMsg))
		default:
			// If this a new snapshot type that this cockroach version does not know
			// about, we let it through.
		}
	}

	if fn := s.cfg.TestingKnobs.ReceiveSnapshot; fn != nil {
		if err := fn(ctx, header); err != nil {
			// NB: we intentionally don't mark this error as errMarkSnapshotError so
			// that we don't end up retrying injected errors in tests.
			return sendSnapshotError(ctx, s, stream, err)
		}
	}

	// Defensive check that any snapshot contains this store in the	descriptor.
	storeID := s.StoreID()
	if _, ok := header.State.Desc.GetReplicaDescriptor(storeID); !ok {
		return errors.AssertionFailedf(
			`snapshot from queue %s was sent to s%d which did not contain it as a replica: %s`,
			header.SenderQueueName, storeID, header.State.Desc.Replicas())
	}

	cleanup, err := s.reserveReceiveSnapshot(ctx, header)
	if err != nil {
		return err
	}
	defer cleanup()

	// The comment on ReplicaPlaceholder motivates and documents
	// ReplicaPlaceholder semantics. Please be familiar with them
	// before making any changes.
	var placeholder *ReplicaPlaceholder
	if pErr := s.withReplicaForRequest(
		ctx, &header.RaftMessageRequest, func(ctx context.Context, r *Replica,
		) *kvpb.Error {
			var err error
			s.mu.Lock()
			defer s.mu.Unlock()
			placeholder, err = s.canAcceptSnapshotLocked(ctx, header)
			if err != nil {
				return kvpb.NewError(err)
			}
			if placeholder != nil {
				if err := s.addPlaceholderLocked(placeholder); err != nil {
					return kvpb.NewError(err)
				}
			}
			return nil
		}); pErr != nil {
		log.Infof(ctx, "cannot accept snapshot: %s", pErr)
		return sendSnapshotError(ctx, s, stream, pErr.GoError())
	}

	defer func() {
		if placeholder != nil {
			// Remove the placeholder, if it's still there. Most of the time it will
			// have been filled and this is a no-op.
			if _, err := s.removePlaceholder(ctx, placeholder, removePlaceholderFailed); err != nil {
				log.Fatalf(ctx, "unable to remove placeholder: %s", err)
			}
		}
	}()

	snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
	if err != nil {
		err = errors.Wrap(err, "invalid snapshot")
		return sendSnapshotError(ctx, s, stream, err)
	}

	ss := &kvBatchSnapshotStrategy{
		scratch:      s.sstSnapshotStorage.NewScratchSpace(header.State.Desc.RangeID, snapUUID),
		sstChunkSize: snapshotSSTWriteSyncRate.Get(&s.cfg.Settings.SV),
		st:           s.ClusterSettings(),
		clusterID:    s.ClusterID(),
	}
	defer ss.Close(ctx)

	if err := stream.Send(&kvserverpb.SnapshotResponse{Status: kvserverpb.SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "accepted snapshot reservation for r%d", header.State.Desc.RangeID)
	}

	comparisonResult := s.getLocalityComparison(ctx,
		header.RaftMessageRequest.FromReplica.NodeID, header.RaftMessageRequest.ToReplica.NodeID)

	recordBytesReceived := func(inc int64) {
		s.metrics.RangeSnapshotRcvdBytes.Inc(inc)
		s.metrics.updateCrossLocalityMetricsOnSnapshotRcvd(comparisonResult, inc)

		// This logic for metrics should match what is in replica_command.
		if header.SenderQueueName == kvserverpb.SnapshotRequest_RAFT_SNAPSHOT_QUEUE {
			s.metrics.RangeSnapshotRecoveryRcvdBytes.Inc(inc)
		} else if header.SenderQueueName == kvserverpb.SnapshotRequest_OTHER {
			s.metrics.RangeSnapshotRebalancingRcvdBytes.Inc(inc)
		} else {
			// Replicate queue does both types, so split based on priority.
			// See AllocatorAction.Priority
			if header.SenderQueuePriority > 0 {
				s.metrics.RangeSnapshotUpreplicationRcvdBytes.Inc(inc)
			} else {
				s.metrics.RangeSnapshotRebalancingRcvdBytes.Inc(inc)
			}
		}
	}
	inSnap, err := ss.Receive(ctx, s, stream, *header, recordBytesReceived)
	if err != nil {
		return err
	}
	inSnap.placeholder = placeholder

	// Use a background context for applying the snapshot, as handleRaftReady is
	// not prepared to deal with arbitrary context cancellation. Also, we've
	// already received the entire snapshot here, so there's no point in
	// abandoning application half-way through if the caller goes away.
	applyCtx := s.AnnotateCtx(context.Background())
	msgAppResp, pErr := s.processRaftSnapshotRequest(applyCtx, header, inSnap)
	if pErr != nil {
		err := pErr.GoError()
		// We mark this error as a snapshot error which will be interpreted by the
		// sender as this being a retriable error, see isSnapshotError().
		err = errors.Mark(err, errMarkSnapshotError)
		err = errors.Wrap(err, "failed to apply snapshot")
		return sendSnapshotError(ctx, s, stream, err)
	}
	return stream.Send(&kvserverpb.SnapshotResponse{
		Status:         kvserverpb.SnapshotResponse_APPLIED,
		CollectedSpans: tracing.SpanFromContext(ctx).GetConfiguredRecording(),
		MsgAppResp:     msgAppResp,
	})
}

// sendSnapshotError sends an error response back to the sender of this snapshot
// to signify that it can not accept this snapshot. Internally it increments the
// statistic tracking how many invalid snapshots it received.
func sendSnapshotError(
	ctx context.Context, s *Store, stream incomingSnapshotStream, err error,
) error {
	s.metrics.RangeSnapshotRecvFailed.Inc(1)
	resp := snapRespErr(err)
	resp.CollectedSpans = tracing.SpanFromContext(ctx).GetConfiguredRecording()

	return stream.Send(resp)
}

func snapRespErr(err error) *kvserverpb.SnapshotResponse {
	return &kvserverpb.SnapshotResponse{
		Status:            kvserverpb.SnapshotResponse_ERROR,
		EncodedError:      errors.EncodeError(context.Background(), err),
		DeprecatedMessage: err.Error(),
	}
}

func maybeHandleDeprecatedSnapErr(deprecated bool, err error) error {
	if !deprecated {
		return err
	}
	return errors.Mark(err, errMarkSnapshotError)
}

// SnapshotStorePool narrows StorePool to make sendSnapshotUsingDelegate easier to test.
type SnapshotStorePool interface {
	Throttle(reason storepool.ThrottleReason, why string, toStoreID roachpb.StoreID)
}

// minSnapshotRate defines the minimum value that the rate limit for rebalance
// and recovery snapshots can be configured to. Any value below this lower bound
// is considered unsafe for use, as it can lead to excessively long-running
// snapshots. The sender of Raft snapshots holds resources (e.g. LSM snapshots,
// LSM iterators until #75824 is addressed) and blocks Raft log truncation, so
// it is not safe to let a single snapshot run for an unlimited period of time.
//
// The value was chosen based on a maximum range size of 512mb and a desire to
// prevent a single snapshot for running for more than 10 minutes. With a rate
// limit of 1mb/s, a 512mb snapshot will take just under 9 minutes to send.
const minSnapshotRate = 1 << 20 // 1mb/s

// rebalanceSnapshotRate is the rate at which all snapshots are sent.
var rebalanceSnapshotRate = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_rebalance.max_rate",
	"the rate limit (bytes/sec) to use for rebalance and upreplication snapshots",
	32<<20, // 32mb/s
	settings.ByteSizeWithMinimum(minSnapshotRate),
	settings.WithPublic,
)

// snapshotSenderBatchSize is the size that key-value batches are allowed to
// grow to during Range snapshots before being sent to the receiver. This limit
// places an upper-bound on the memory footprint of the sender of a Range
// snapshot. It is also the granularity of rate limiting.
var snapshotSenderBatchSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_sender.batch_size",
	"size of key-value batches sent over the network during snapshots",
	256<<10, // 256 KB
	settings.PositiveInt,
)

// snapshotReservationQueueTimeoutFraction is the maximum fraction of a Range
// snapshot's total timeout that it is allowed to spend queued on the receiver
// waiting for a reservation.
//
// Enforcement of this snapshotApplyQueue-scoped timeout is intended to prevent
// starvation of snapshots in cases where a queue of snapshots waiting for
// reservations builds and no single snapshot acquires the semaphore with
// sufficient time to complete, but each holds the semaphore long enough to
// ensure that later snapshots in the queue encounter this same situation. This
// is a case of FIFO queuing + timeouts leading to starvation. By rejecting
// snapshot attempts earlier, we ensure that those that do acquire the semaphore
// have sufficient time to complete.
//
// Consider the following motivating example:
//
// With a 60s timeout set by the snapshotQueue/replicateQueue for each snapshot,
// 45s needed to actually stream the data, and a willingness to wait for as long
// as it takes to get the reservation (i.e. this fraction = 1.0) there can be
// starvation. Each snapshot spends so much time waiting for the reservation
// that it will itself fail during sending, while the next snapshot wastes
// enough time waiting for us that it will itself fail, ad infinitum:
//
//	t   | snap1 snap2 snap3 snap4 snap5 ...
//	----+------------------------------------
//	0   | send
//	15  |       queue queue
//	30  |                   queue
//	45  | ok    send
//	60  |                         queue
//	75  |       fail  fail  send
//	90  |                   fail  send
//	105 |
//	120 |                         fail
//	135 |
//
// If we limit the amount of time we are willing to wait for a reservation to
// something that is small enough to, on success, give us enough time to
// actually stream the data, no starvation can occur. For example, with a 60s
// timeout, 45s needed to stream the data, we can wait at most 15s for a
// reservation and still avoid starvation:
//
//	t   | snap1 snap2 snap3 snap4 snap5 ...
//	----+------------------------------------
//	0   | send
//	15  |       queue queue
//	30  |       fail  fail  send
//	45  |
//	60  | ok                      queue
//	75  |                   ok    send
//	90  |
//	105 |
//	120 |                         ok
//	135 |
//
// In practice, the snapshot reservation logic (reserveReceiveSnapshot) doesn't know
// how long sending the snapshot will actually take. But it knows the timeout it
// has been given by the snapshotQueue/replicateQueue, which serves as an upper
// bound, under the assumption that snapshots can make progress in the absence
// of starvation.
//
// Without the reservation timeout fraction, if the product of the number of
// concurrent snapshots and the average streaming time exceeded this timeout,
// the starvation scenario could occur, since the average queuing time would
// exceed the timeout. With the reservation limit, progress will be made as long
// as the average streaming time is less than the guaranteed processing time for
// any snapshot that succeeds in acquiring a reservation:
//
//	guaranteed_processing_time = (1 - reservation_queue_timeout_fraction) x timeout
//
// The timeout for the snapshot and replicate queues bottoms out at 60s (by
// default, see kv.queue.process.guaranteed_time_budget). Given a default
// reservation queue timeout fraction of 0.4, this translates to a guaranteed
// processing time of 36s for any snapshot attempt that manages to acquire a
// reservation. This means that a 512MiB snapshot will succeed if sent at a rate
// of 14MiB/s or above.
//
// Lower configured snapshot rate limits quickly lead to a much higher timeout
// since we apply a liberal multiplier (permittedRangeScanSlowdown). Concretely,
// we move past the 1-minute timeout once the rate limit is set to anything less
// than 10*range_size/guaranteed_budget(in MiB/s), which comes out to ~85MiB/s
// for a 512MiB range and the default 1m budget. In other words, the queue uses
// sumptuous timeouts, and so we'll also be excessively lenient with how long
// we're willing to wait for a reservation (but not to the point of allowing the
// starvation scenario). As long as the nodes between the cluster can transfer
// at around ~14MiB/s, even a misconfiguration of the rate limit won't cause
// issues and where it does, the setting can be set to 1.0, effectively
// reverting to the old behavior.
var snapshotReservationQueueTimeoutFraction = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.snapshot_receiver.reservation_queue_timeout_fraction",
	"the fraction of a snapshot's total timeout that it is allowed to spend "+
		"queued on the receiver waiting for a reservation",
	0.4,
	settings.FloatInRange(0.25, 1.0),
)

// snapshotSSTWriteSyncRate is the size of chunks to write before fsync-ing.
// The default of 2 MiB was chosen to be in line with the behavior in bulk-io.
// See sstWriteSyncRate.
var snapshotSSTWriteSyncRate = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_sst.sync_size",
	"threshold after which snapshot SST writes must fsync",
	kvserverbase.BulkIOWriteBurst,
	settings.PositiveInt,
)

// snapshotChecksumVerification enables/disables value checksums verification
// when receiving snapshots.
var snapshotChecksumVerification = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.snapshot_receiver.checksum_verification.enabled",
	"verify value checksums on receiving a raft snapshot",
	true,
)

// SendEmptySnapshot creates an OutgoingSnapshot for the input range
// descriptor and seeds it with an empty range. Then, it sends this
// snapshot to the replica specified in the input.
func SendEmptySnapshot(
	ctx context.Context,
	clusterID uuid.UUID,
	st *cluster.Settings,
	tracer *tracing.Tracer,
	cc *grpc.ClientConn,
	now hlc.Timestamp,
	desc roachpb.RangeDescriptor,
	to roachpb.ReplicaDescriptor,
) error {
	// Create an engine to use as a buffer for the empty snapshot.
	eng, err := storage.Open(
		context.Background(),
		storage.InMemory(),
		cluster.MakeClusterSettings(),
		storage.CacheSize(1<<20 /* 1 MiB */),
		storage.MaxSizeBytes(512<<20 /* 512 MiB */))
	if err != nil {
		return err
	}
	defer eng.Close()

	var ms enginepb.MVCCStats
	// Seed an empty range into the new engine.
	if err := storage.MVCCPutProto(
		ctx, eng, keys.RangeDescriptorKey(desc.StartKey), now, &desc, storage.MVCCWriteOptions{Stats: &ms},
	); err != nil {
		return err
	}

	ms, err = stateloader.WriteInitialReplicaState(
		ctx,
		eng,
		ms,
		desc,
		roachpb.Lease{},
		hlc.Timestamp{}, // gcThreshold
		roachpb.GCHint{},
		st.Version.ActiveVersionOrEmpty(ctx).Version,
	)
	if err != nil {
		return err
	}

	// Use stateloader to load state out of memory from the previously created engine.
	sl := stateloader.Make(desc.RangeID)
	state, err := sl.Load(ctx, eng, &desc)
	if err != nil {
		return err
	}
	// See comment on DeprecatedUsingAppliedStateKey for why we need to set this
	// explicitly for snapshots going out to followers.
	state.DeprecatedUsingAppliedStateKey = true

	snapUUID := uuid.NewV4()

	// The snapshot must use a Pebble snapshot, since it requires consistent
	// iterators.
	//
	// NB: Using a regular snapshot as opposed to an EventuallyFileOnlySnapshot
	// is alright here as there should be no keys in this span to begin with,
	// and this snapshot should be very short-lived.
	engSnapshot := eng.NewSnapshot()

	// Create an OutgoingSnapshot to send.
	outgoingSnap, err := snapshot(
		ctx,
		snapUUID,
		sl,
		engSnapshot,
		desc.StartKey,
	)
	if err != nil {
		// Close() is not idempotent, and will be done by outgoingSnap.Close() if
		// the snapshot was successfully created.
		engSnapshot.Close()
		return err
	}
	defer outgoingSnap.Close()

	// From and to replica descriptors are the same because we have
	// to send the snapshot from a member of the range descriptor.
	// Sending it from the current replica ensures that. Otherwise,
	// it would be a malformed request if it came from a non-member.
	from := to
	req := kvserverpb.RaftMessageRequest{
		RangeID:     desc.RangeID,
		FromReplica: from,
		ToReplica:   to,
		Message: raftpb.Message{
			Type:     raftpb.MsgSnap,
			To:       raftpb.PeerID(to.ReplicaID),
			From:     raftpb.PeerID(from.ReplicaID),
			Term:     outgoingSnap.RaftSnap.Metadata.Term,
			Snapshot: &outgoingSnap.RaftSnap,
		},
	}

	header := kvserverpb.SnapshotRequest_Header{
		State:              state,
		RaftMessageRequest: req,
		RangeSize:          ms.Total(),
		RangeKeysInOrder:   true,
	}

	stream, err := NewMultiRaftClient(cc).RaftSnapshot(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := stream.CloseSend(); err != nil {
			log.Warningf(ctx, "failed to close snapshot stream: %+v", err)
		}
	}()

	if _, err := sendSnapshot(
		ctx,
		clusterID,
		st,
		tracer,
		stream,
		noopStorePool{},
		header,
		&outgoingSnap,
		eng.NewWriteBatch,
		func() {},
		nil, /* recordBytesSent */
	); err != nil {
		return err
	}
	return nil
}

// noopStorePool is a hollowed out StorePool that does not throttle. It's used in recovery scenarios.
type noopStorePool struct{}

func (n noopStorePool) Throttle(storepool.ThrottleReason, string, roachpb.StoreID) {}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	clusterID uuid.UUID,
	st *cluster.Settings,
	tracer *tracing.Tracer,
	stream outgoingSnapshotStream,
	storePool SnapshotStorePool,
	header kvserverpb.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newWriteBatch func() storage.WriteBatch,
	sent func(),
	recordBytesSent snapshotRecordMetrics,
) (*kvserverpb.SnapshotResponse, error) {
	if recordBytesSent == nil {
		// NB: Some tests and an offline tool (ResetQuorum) call into `sendSnapshotUsingDelegate`
		// with a nil metrics tracking function. We pass in a fake metrics tracking function here that isn't
		// hooked up to anything.
		recordBytesSent = func(inc int64) {}
	}
	ctx, sp := tracing.EnsureChildSpan(ctx, tracer, "sending snapshot")
	defer sp.Finish()

	start := timeutil.Now()
	to := header.RaftMessageRequest.ToReplica
	if err := stream.Send(&kvserverpb.SnapshotRequest{Header: &header}); err != nil {
		return nil, err
	}
	log.Event(ctx, "sent SNAPSHOT_REQUEST message to server")
	// Wait until we get a response from the server. The recipient may queue us
	// (only a limited number of snapshots are allowed concurrently) or flat-out
	// reject the snapshot. After the initial message exchange, we'll go and send
	// the actual snapshot (if not rejected).
	resp, err := stream.Recv()
	if err != nil {
		storePool.Throttle(storepool.ThrottleFailed, err.Error(), to.StoreID)
		return nil, err
	}
	switch resp.Status {
	case kvserverpb.SnapshotResponse_ERROR:
		sp.ImportRemoteRecording(resp.CollectedSpans)
		storePool.Throttle(storepool.ThrottleFailed, resp.DeprecatedMessage, to.StoreID)
		return nil, errors.Wrapf(maybeHandleDeprecatedSnapErr(resp.Error()), "%s: remote couldn't accept %s", to, snap)
	case kvserverpb.SnapshotResponse_ACCEPTED:
		// This is the response we're expecting. Continue with snapshot sending.
		log.Event(ctx, "received SnapshotResponse_ACCEPTED message from server")
	default:
		err := errors.Errorf("%s: server sent an invalid status while negotiating %s: %s",
			to, snap, resp.Status)
		storePool.Throttle(storepool.ThrottleFailed, err.Error(), to.StoreID)
		return nil, err
	}

	durQueued := timeutil.Since(start)
	start = timeutil.Now()

	// Consult cluster settings to determine rate limits and batch sizes.
	targetRate := rate.Limit(rebalanceSnapshotRate.Get(&st.SV))
	batchSize := snapshotSenderBatchSize.Get(&st.SV)

	// Convert the bytes/sec rate limit to batches/sec.
	//
	// TODO(peter): Using bytes/sec for rate limiting seems more natural but has
	// practical difficulties. We either need to use a very large burst size
	// which seems to disable the rate limiting, or call WaitN in smaller than
	// burst size chunks which caused excessive slowness in testing. Would be
	// nice to figure this out, but the batches/sec rate limit works for now.
	limiter := rate.NewLimiter(targetRate/rate.Limit(batchSize), 1 /* burst size */)

	ss := &kvBatchSnapshotStrategy{
		batchSize:     batchSize,
		limiter:       limiter,
		newWriteBatch: newWriteBatch,
		st:            st,
		clusterID:     clusterID,
	}

	// Record timings for snapshot send if kv.trace.snapshot.enable_threshold is enabled
	numBytesSent, err := ss.Send(ctx, stream, header, snap, recordBytesSent)
	if err != nil {
		return nil, err
	}
	durSent := timeutil.Since(start)

	// Notify the sent callback before the final snapshot request is sent so that
	// the snapshots generated metric gets incremented before the snapshot is
	// applied.
	sent()
	if err := stream.Send(&kvserverpb.SnapshotRequest{Final: true}); err != nil {
		return nil, err
	}
	log.KvDistribution.Infof(
		ctx,
		"streamed %s to %s with %s in %.2fs @ %s/s: %s, rate-limit: %s/s, queued: %.2fs",
		snap,
		to,
		humanizeutil.IBytes(numBytesSent),
		durSent.Seconds(),
		humanizeutil.IBytes(int64(float64(numBytesSent)/durSent.Seconds())),
		ss.Status(),
		humanizeutil.IBytes(int64(targetRate)),
		durQueued.Seconds(),
	)

	resp, err = stream.Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "%s: remote failed to apply snapshot", to)
	}
	sp.ImportRemoteRecording(resp.CollectedSpans)
	// NB: wait for EOF which ensures that all processing on the server side has
	// completed (such as defers that might be run after the previous message was
	// received).
	if unexpectedResp, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return nil, errors.Wrapf(err, "%s: expected EOF, got resp=%v with error", to, unexpectedResp)
		}
		return nil, errors.Newf("%s: expected EOF, got resp=%v", to, unexpectedResp)
	}
	switch resp.Status {
	case kvserverpb.SnapshotResponse_ERROR:
		return nil, errors.Wrapf(
			maybeHandleDeprecatedSnapErr(resp.Error()), "%s: remote failed to apply snapshot", to,
		)
	case kvserverpb.SnapshotResponse_APPLIED:
		return resp, nil
	default:
		return nil, errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status,
		)
	}
}
