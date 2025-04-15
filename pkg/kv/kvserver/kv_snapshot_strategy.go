// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/redact"
	"golang.org/x/time/rate"
)

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
	if !keyRanges[len(keyRanges)-1].Equal(header.State.Desc.KeySpan().AsRawSpanWithNoLocals()) {
		return noSnap, errors.AssertionFailedf("last span in multiSSTWriter did not equal the user key span: %s", keyRanges[len(keyRanges)-1].String())
	}

	// The last key range is the user key span.
	localRanges := keyRanges[:len(keyRanges)-1]
	mvccRange := keyRanges[len(keyRanges)-1]
	msstw, err := newMultiSSTWriter(ctx, kvSS.st, kvSS.scratch, localRanges, mvccRange, kvSS.sstChunkSize, header.RangeKeysInOrder)
	if err != nil {
		return noSnap, err
	}
	defer msstw.Close()

	log.Event(ctx, "waiting for snapshot batches to begin")

	var sharedSSTs []pebble.SharedSSTMeta
	var externalSSTs []pebble.ExternalFile
	var prevBytesEstimate int64

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
			sharedSSTs = nil
			externalSSTs = nil
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

				bytesEstimate := msstw.estimatedDataSize()
				delta := bytesEstimate - prevBytesEstimate
				// Calling nil pacer is a noop.
				if err := pacer.Pace(ctx, delta, false /* final */); err != nil {
					return noSnap, errors.Wrapf(err, "snapshot admission pacer")
				}
				prevBytesEstimate = bytesEstimate

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

				if err := kvSS.readOneToBatch(ctx, ek, header.SharedReplicate, batchReader, msstw); err != nil {
					return noSnap, err
				}
			}
			if batchReader.Error() != nil {
				return noSnap, err
			}
			timingTag.stop("sst")
		}

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
			additionalWrites := sstSize - prevBytesEstimate
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
				SnapUUID:          snapUUID,
				SSTStorageScratch: kvSS.scratch,
				FromReplica:       header.RaftMessageRequest.FromReplica,
				Desc:              header.State.Desc,
				DataSize:          dataSize,
				SSTSize:           sstSize,
				SharedSize:        sharedSize,
				raftAppliedIndex:  header.State.RaftAppliedIndex,
				msgAppRespCh:      make(chan raftpb.Message, 1),
				sharedSSTs:        sharedSSTs,
				externalSSTs:      externalSSTs,
				clearedSpans:      keyRanges,
			}

			timingTag.stop("totalTime")

			kvSS.status = redact.Sprintf("local ssts: %d, shared ssts: %d, external ssts: %d", len(kvSS.scratch.SSTs()), len(sharedSSTs), len(externalSSTs))
			return inSnap, nil
		}
	}
}

func (kvSS *kvBatchSnapshotStrategy) readOneToBatch(
	ctx context.Context,
	ek storage.EngineKey,
	shared bool, // may receive shared SSTs
	batchReader *storage.BatchReader,
	msstw *multiSSTWriter,
) error {
	switch batchReader.KeyKind() {
	case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
		if err := msstw.Put(ctx, ek, batchReader.Value()); err != nil {
			return errors.Wrapf(err, "writing sst for raft snapshot")
		}
	case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
		if !shared {
			return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
		}
		if err := msstw.PutInternalPointKey(ctx, batchReader.Key(), batchReader.KeyKind(), nil); err != nil {
			return errors.Wrapf(err, "writing sst for raft snapshot")
		}
	case pebble.InternalKeyKindRangeDelete:
		if !shared {
			return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
		}
		start := batchReader.Key()
		end, err := batchReader.EndKey()
		if err != nil {
			return err
		}
		if err := msstw.PutInternalRangeDelete(ctx, start, end); err != nil {
			return errors.Wrapf(err, "writing sst for raft snapshot")
		}

	case pebble.InternalKeyKindRangeKeyUnset, pebble.InternalKeyKindRangeKeyDelete:
		if !shared {
			return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
		}
		start := batchReader.Key()
		end, err := batchReader.EndKey()
		if err != nil {
			return err
		}
		rangeKeys, err := batchReader.RawRangeKeys()
		if err != nil {
			return err
		}
		for _, rkv := range rangeKeys {
			err := msstw.PutInternalRangeKey(ctx, start, end, rkv)
			if err != nil {
				return errors.Wrapf(err, "writing sst for raft snapshot")
			}
		}
	case pebble.InternalKeyKindRangeKeySet:
		start := ek
		end, err := batchReader.EngineEndKey()
		if err != nil {
			return err
		}
		rangeKeys, err := batchReader.EngineRangeKeys()
		if err != nil {
			return err
		}
		for _, rkv := range rangeKeys {
			err := msstw.PutRangeKey(ctx, start.Key, end.Key, rkv.Version, rkv.Value)
			if err != nil {
				return errors.Wrapf(err, "writing sst for raft snapshot")
			}
		}
	default:
		return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
	}
	return nil
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
		kvsBefore := kvs
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
			// IterateReplicaKeySpansShared will return ErrInvalidSkipSharedIteration
			// before visiting user keys. This is a subtle contract.
			// See also:
			//
			// https://cockroachlabs.slack.com/archives/CAC6K3SLU/p1741360036808799?thread_ts=1741356670.269679&cid=CAC6K3S
			if kvsBefore != kvs {
				return 0, errors.AssertionFailedf(
					"unable to transition from shared to regular replicate: %d user keys were already sent",
					kvs-kvsBefore,
				)
			}
			// TODO(tbg): the shared and external visitors never flush, so at this
			// point the receiver hasn't gotten any shared/external SST references
			// sent to it. This implies that an explicit transition is unnecessary
			// and we can remove this.
			//
			// See: https://github.com/cockroachdb/cockroach/issues/142673
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
