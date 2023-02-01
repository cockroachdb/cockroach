// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/multiqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/raft/v3/raftpb"
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

	// DefaultSnapshotSendLimit is the max number of snapshots concurrently sent.
	// See server.KVConfig for more info.
	DefaultSnapshotSendLimit = 2

	// DefaultSnapshotApplyLimit is the number of snapshots concurrently applied.
	// See server.KVConfig for more info.
	DefaultSnapshotApplyLimit = 1
)

// snapshotPrioritizationEnabled will allow the sender and receiver of snapshots
// to prioritize the snapshots. If disabled, the behavior will be FIFO on both
// send and receive sides.
var snapshotPrioritizationEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.snapshot_prioritization.enabled",
	"if true, then prioritize enqueued snapshots on both the send or receive sides",
	true,
)

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

// snapshotStrategy is an approach to sending and receiving Range snapshots.
// Each implementation corresponds to a SnapshotRequest_Strategy, and it is
// expected that the implementation that matches the Strategy specified in the
// snapshot header will always be used.
type snapshotStrategy interface {
	// Receive streams SnapshotRequests in from the provided stream and
	// constructs an IncomingSnapshot.
	Receive(
		context.Context,
		incomingSnapshotStream,
		kvserverpb.SnapshotRequest_Header,
		snapshotRecordMetrics,
	) (IncomingSnapshot, error)

	// Send streams SnapshotRequests created from the OutgoingSnapshot in to the
	// provided stream. On nil error, the number of bytes sent is returned.
	Send(
		context.Context,
		outgoingSnapshotStream,
		kvserverpb.SnapshotRequest_Header,
		*OutgoingSnapshot,
		snapshotRecordMetrics,
	) (int64, error)

	// Status provides a status report on the work performed during the
	// snapshot. Only valid if the strategy succeeded.
	Status() redact.RedactableString

	// Close cleans up any resources associated with the snapshot strategy.
	Close(context.Context)
}

func assertStrategy(
	ctx context.Context,
	header kvserverpb.SnapshotRequest_Header,
	expect kvserverpb.SnapshotRequest_Strategy,
) {
	if header.Strategy != expect {
		log.Fatalf(ctx, "expected strategy %s, found strategy %s", expect, header.Strategy)
	}
}

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
	newBatch func() storage.Batch

	// The approximate size of the SST chunk to buffer in memory on the receiver
	// before flushing to disk. Only used on the receiver side.
	sstChunkSize int64
	// Only used on the receiver side.
	scratch *SSTSnapshotStorageScratch
	st      *cluster.Settings
}

// multiSSTWriter is a wrapper around an SSTWriter and SSTSnapshotStorageScratch
// that handles chunking SSTs and persisting them to disk.
type multiSSTWriter struct {
	st       *cluster.Settings
	scratch  *SSTSnapshotStorageScratch
	currSST  storage.SSTWriter
	keySpans []roachpb.Span
	currSpan int
	// The approximate size of the SST chunk to buffer in memory on the receiver
	// before flushing to disk.
	sstChunkSize int64
	// The total size of SST data. Updated on SST finalization.
	dataSize int64
}

func newMultiSSTWriter(
	ctx context.Context,
	st *cluster.Settings,
	scratch *SSTSnapshotStorageScratch,
	keySpans []roachpb.Span,
	sstChunkSize int64,
) (multiSSTWriter, error) {
	msstw := multiSSTWriter{
		st:           st,
		scratch:      scratch,
		keySpans:     keySpans,
		sstChunkSize: sstChunkSize,
	}
	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}
	return msstw, nil
}

func (msstw *multiSSTWriter) initSST(ctx context.Context) error {
	newSSTFile, err := msstw.scratch.NewFile(ctx, msstw.sstChunkSize)
	if err != nil {
		return errors.Wrap(err, "failed to create new sst file")
	}
	newSST := storage.MakeIngestionSSTWriter(ctx, msstw.st, newSSTFile)
	msstw.currSST = newSST
	if err := msstw.currSST.ClearRawRange(
		msstw.keySpans[msstw.currSpan].Key, msstw.keySpans[msstw.currSpan].EndKey,
		true /* pointKeys */, true, /* rangeKeys */
	); err != nil {
		msstw.currSST.Close()
		return errors.Wrap(err, "failed to clear range on sst file writer")
	}
	return nil
}

func (msstw *multiSSTWriter) finalizeSST(ctx context.Context) error {
	err := msstw.currSST.Finish()
	if err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}
	msstw.dataSize += msstw.currSST.DataSize
	msstw.currSpan++
	msstw.currSST.Close()
	return nil
}

func (msstw *multiSSTWriter) Put(ctx context.Context, key storage.EngineKey, value []byte) error {
	for msstw.keySpans[msstw.currSpan].EndKey.Compare(key.Key) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(ctx); err != nil {
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
		if err := msstw.finalizeSST(ctx); err != nil {
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
			if err := msstw.finalizeSST(ctx); err != nil {
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
	stream incomingSnapshotStream,
	header kvserverpb.SnapshotRequest_Header,
	recordBytesReceived snapshotRecordMetrics,
) (IncomingSnapshot, error) {
	assertStrategy(ctx, header, kvserverpb.SnapshotRequest_KV_BATCH)

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
	msstw, err := newMultiSSTWriter(ctx, kvSS.st, kvSS.scratch, keyRanges, kvSS.sstChunkSize)
	if err != nil {
		return noSnap, err
	}
	defer msstw.Close()

	log.Event(ctx, "waiting for snapshot batches to begin")

	for {
		timingTag.start("recv")
		req, err := stream.Recv()
		timingTag.stop("recv")
		if err != nil {
			return noSnap, err
		}
		if req.Header != nil {
			err := errors.New("client error: provided a header mid-stream")
			return noSnap, sendSnapshotError(stream, err)
		}

		if req.KVBatch != nil {
			recordBytesReceived(int64(len(req.KVBatch)))
			batchReader, err := storage.NewPebbleBatchReader(req.KVBatch)
			if err != nil {
				return noSnap, errors.Wrap(err, "failed to decode batch")
			}

			timingTag.start("sst")
			// All batch operations are guaranteed to be point key or range key puts.
			for batchReader.Next() {
				switch batchReader.BatchType() {
				case storage.BatchTypeValue:
					key, err := batchReader.EngineKey()
					if err != nil {
						return noSnap, err
					}
					if err := msstw.Put(ctx, key, batchReader.Value()); err != nil {
						return noSnap, errors.Wrapf(err, "writing sst for raft snapshot")
					}

				case storage.BatchTypeRangeKeySet:
					start, err := batchReader.EngineKey()
					if err != nil {
						return noSnap, err
					}
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
					return noSnap, errors.AssertionFailedf("unexpected batch entry type %d", batchReader.BatchType())
				}
			}
			timingTag.stop("sst")
		}
		if req.Final {
			// We finished receiving all batches and log entries. It's possible that
			// we did not receive any key-value pairs for some of the key spans, but
			// we must still construct SSTs with range deletion tombstones to remove
			// the data.
			timingTag.start("sst")
			dataSize, err := msstw.Finish(ctx)
			if err != nil {
				return noSnap, errors.Wrapf(err, "finishing sst for raft snapshot")
			}
			msstw.Close()
			timingTag.stop("sst")
			log.Eventf(ctx, "all data received from snapshot and all SSTs were finalized")

			snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
			if err != nil {
				err = errors.Wrap(err, "client error: invalid snapshot")
				return noSnap, sendSnapshotError(stream, err)
			}

			inSnap := IncomingSnapshot{
				SnapUUID:          snapUUID,
				SSTStorageScratch: kvSS.scratch,
				FromReplica:       header.RaftMessageRequest.FromReplica,
				Desc:              header.State.Desc,
				DataSize:          dataSize,
				snapType:          header.Type,
				raftAppliedIndex:  header.State.RaftAppliedIndex,
			}

			timingTag.stop("totalTime")

			kvSS.status = redact.Sprintf("ssts: %d", len(kvSS.scratch.SSTs()))
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
	assertStrategy(ctx, header, kvserverpb.SnapshotRequest_KV_BATCH)
	// bytesSent is updated as key-value batches are sent with sendBatch. It does
	// not reflect the log entries sent (which are never sent in newer versions of
	// CRDB, as of VersionUnreplicatedTruncatedState).
	var bytesSent int64
	var kvs, rangeKVs int

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
	var b storage.Batch
	defer func() {
		if b != nil {
			b.Close()
		}
	}()

	flushBatch := func() error {
		if err := kvSS.sendBatch(ctx, stream, b, timingTag); err != nil {
			return err
		}
		bLen := int64(b.Len())
		bytesSent += bLen
		recordBytesSent(bLen)
		b.Close()
		b = nil
		return nil
	}

	maybeFlushBatch := func() error {
		if int64(b.Len()) >= kvSS.batchSize {
			return flushBatch()
		}
		return nil
	}

	err := rditer.IterateReplicaKeySpans(snap.State.Desc, snap.EngineSnap, true, /* replicatedOnly */
		func(iter storage.EngineIterator, _ roachpb.Span, keyType storage.IterKeyType) error {
			timingTag.start("iter")
			defer timingTag.stop("iter")

			var err error
			switch keyType {
			case storage.IterKeyTypePointsOnly:
				for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
					kvs++
					if b == nil {
						b = kvSS.newBatch()
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

			case storage.IterKeyTypeRangesOnly:
				for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
					bounds, err := iter.EngineRangeBounds()
					if err != nil {
						return err
					}
					for _, rkv := range iter.EngineRangeKeys() {
						rangeKVs++
						if b == nil {
							b = kvSS.newBatch()
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

			default:
				return errors.AssertionFailedf("unexpected key type %v", keyType)
			}
			return err
		})
	if err != nil {
		return 0, err
	}
	if b != nil {
		if err = flushBatch(); err != nil {
			return 0, err
		}
	}

	timingTag.stop("totalTime")
	log.Eventf(ctx, "finished sending snapshot batches, sent a total of %d bytes", bytesSent)

	kvSS.status = redact.Sprintf("kvs=%d rangeKVs=%d", kvs, rangeKVs)
	return bytesSent, nil
}

func (kvSS *kvBatchSnapshotStrategy) sendBatch(
	ctx context.Context,
	stream outgoingSnapshotStream,
	batch storage.Batch,
	timerTag *snapshotTimingTag,
) error {
	timerTag.start("rateLimit")
	err := kvSS.limiter.WaitN(ctx, 1)
	timerTag.stop("rateLimit")
	if err != nil {
		return err
	}
	timerTag.start("send")
	res := stream.Send(&kvserverpb.SnapshotRequest{KVBatch: batch.Repr()})
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

	return s.throttleSnapshot(ctx, s.snapshotApplyQueue,
		int(header.SenderQueueName), header.SenderQueuePriority,
		-1,
		header.RangeSize,
		header.RaftMessageRequest.RangeID,
		s.metrics.RangeSnapshotRecvQueueLength,
		s.metrics.RangeSnapshotRecvInProgress, s.metrics.RangeSnapshotRecvTotalInProgress,
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

	return s.throttleSnapshot(ctx, s.snapshotSendQueue,
		int(req.SenderQueueName),
		req.SenderQueuePriority,
		req.QueueOnDelegateLen,
		rangeSize,
		req.RangeID,
		s.metrics.RangeSnapshotSendQueueLength,
		s.metrics.RangeSnapshotSendInProgress, s.metrics.RangeSnapshotSendTotalInProgress,
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
	waitingSnapshotMetric, inProgressSnapshotMetric, totalInProgressSnapshotMetric *metric.Gauge,
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

		waitingSnapshotMetric.Inc(1)
		defer waitingSnapshotMetric.Dec(1)
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
			if fn := s.cfg.TestingKnobs.AfterSendSnapshotThrottle; fn != nil {
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
				"giving up during snapshot reservation due to %q",
				snapshotReservationQueueTimeoutFraction.Key(),
			)
		case <-s.stopper.ShouldQuiesce():
			return nil, errors.Errorf("stopped")
		}

		// Counts non-empty in-progress snapshots.
		inProgressSnapshotMetric.Inc(1)
	}
	// Counts all in-progress snapshots.
	totalInProgressSnapshotMetric.Inc(1)

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
		totalInProgressSnapshotMetric.Dec(1)

		if rangeSize != 0 || s.cfg.TestingKnobs.ThrottleEmptySnapshots {
			inProgressSnapshotMetric.Dec(1)
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
	existingDesc := existingRepl.mu.state.Desc
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

// receiveSnapshot receives an incoming snapshot via a pre-opened GRPC stream.
func (s *Store) receiveSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header, stream incomingSnapshotStream,
) error {
	sp := tracing.SpanFromContext(ctx)

	// Draining nodes will generally not be rebalanced to (see the filtering that
	// happens in getStoreListFromIDsLocked()), but in case they are, they should
	// reject the incoming rebalancing snapshots.
	if s.IsDraining() {
		switch t := header.Priority; t {
		case kvserverpb.SnapshotRequest_RECOVERY:
			// We can not reject Raft snapshots because draining nodes may have
			// replicas in `StateSnapshot` that need to catch up.
			//
			// TODO(aayush): We also do not reject snapshots sent to replace dead
			// replicas here, but draining stores are still filtered out in
			// getStoreListFromIDsLocked(). Is that sound? Don't we want to
			// upreplicate to draining nodes if there are no other candidates?
		case kvserverpb.SnapshotRequest_REBALANCE:
			return sendSnapshotError(stream, errors.New(storeDrainingMsg))
		default:
			// If this a new snapshot type that this cockroach version does not know
			// about, we let it through.
		}
	}

	if fn := s.cfg.TestingKnobs.ReceiveSnapshot; fn != nil {
		if err := fn(header); err != nil {
			// NB: we intentionally don't mark this error as errMarkSnapshotError so
			// that we don't end up retrying injected errors in tests.
			return sendSnapshotError(stream, err)
		}
	}

	// Defensive check that any snapshot contains this store in the	descriptor.
	storeID := s.StoreID()
	if _, ok := header.State.Desc.GetReplicaDescriptor(storeID); !ok {
		return errors.AssertionFailedf(
			`snapshot of type %s was sent to s%d which did not contain it as a replica: %s`,
			header.Type, storeID, header.State.Desc.Replicas())
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
		) *roachpb.Error {
			var err error
			s.mu.Lock()
			defer s.mu.Unlock()
			placeholder, err = s.canAcceptSnapshotLocked(ctx, header)
			if err != nil {
				return roachpb.NewError(err)
			}
			if placeholder != nil {
				if err := s.addPlaceholderLocked(placeholder); err != nil {
					return roachpb.NewError(err)
				}
			}
			return nil
		}); pErr != nil {
		log.Infof(ctx, "cannot accept snapshot: %s", pErr)
		return sendSnapshotError(stream, pErr.GoError())
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

	// Determine which snapshot strategy the sender is using to send this
	// snapshot. If we don't know how to handle the specified strategy, return
	// an error.
	var ss snapshotStrategy
	switch header.Strategy {
	case kvserverpb.SnapshotRequest_KV_BATCH:
		snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
		if err != nil {
			err = errors.Wrap(err, "invalid snapshot")
			return sendSnapshotError(stream, err)
		}

		ss = &kvBatchSnapshotStrategy{
			scratch:      s.sstSnapshotStorage.NewScratchSpace(header.State.Desc.RangeID, snapUUID),
			sstChunkSize: snapshotSSTWriteSyncRate.Get(&s.cfg.Settings.SV),
			st:           s.ClusterSettings(),
		}
		defer ss.Close(ctx)
	default:
		return sendSnapshotError(stream,
			errors.Errorf("%s,r%d: unknown snapshot strategy: %s",
				s, header.State.Desc.RangeID, header.Strategy),
		)
	}

	if err := stream.Send(&kvserverpb.SnapshotResponse{Status: kvserverpb.SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "accepted snapshot reservation for r%d", header.State.Desc.RangeID)
	}

	recordBytesReceived := func(inc int64) {
		s.metrics.RangeSnapshotRcvdBytes.Inc(inc)

		switch header.Priority {
		case kvserverpb.SnapshotRequest_RECOVERY:
			s.metrics.RangeSnapshotRecoveryRcvdBytes.Inc(inc)
		case kvserverpb.SnapshotRequest_REBALANCE:
			s.metrics.RangeSnapshotRebalancingRcvdBytes.Inc(inc)
		default:
			// If a snapshot is not a RECOVERY or REBALANCE snapshot, it must be of
			// type UNKNOWN.
			s.metrics.RangeSnapshotUnknownRcvdBytes.Inc(inc)
		}
	}
	ctx, rSp := tracing.EnsureChildSpan(ctx, s.cfg.Tracer(), "receive snapshot data")
	defer rSp.Finish() // Ensure that the tracing span is closed, even if ss.Receive errors
	inSnap, err := ss.Receive(ctx, stream, *header, recordBytesReceived)
	if err != nil {
		return err
	}
	inSnap.placeholder = placeholder

	rec := sp.GetConfiguredRecording()

	// Use a background context for applying the snapshot, as handleRaftReady is
	// not prepared to deal with arbitrary context cancellation. Also, we've
	// already received the entire snapshot here, so there's no point in
	// abandoning application half-way through if the caller goes away.
	applyCtx := s.AnnotateCtx(context.Background())
	if pErr := s.processRaftSnapshotRequest(applyCtx, header, inSnap); pErr != nil {
		err := pErr.GoError()
		// We mark this error as a snapshot error which will be interpreted by the
		// sender as this being a retriable error, see isSnapshotError().
		err = errors.Mark(err, errMarkSnapshotError)
		err = errors.Wrap(err, "failed to apply snapshot")
		return sendSnapshotErrorWithTrace(stream, err, rec)
	}
	return stream.Send(&kvserverpb.SnapshotResponse{
		Status:         kvserverpb.SnapshotResponse_APPLIED,
		CollectedSpans: rec,
	})
}

func sendSnapshotError(stream incomingSnapshotStream, err error) error {
	return sendSnapshotErrorWithTrace(stream, err, nil /* trace */)
}

func sendSnapshotErrorWithTrace(
	stream incomingSnapshotStream, err error, trace tracingpb.Recording,
) error {
	resp := snapRespErr(err)
	resp.CollectedSpans = trace
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

// rebalanceSnapshotRate is the rate at which snapshots can be sent in the
// context of up-replication or rebalancing (i.e. any snapshot that was not
// requested by raft itself, to which `kv.snapshot_recovery.max_rate` applies).
var rebalanceSnapshotRate = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_rebalance.max_rate",
	"the rate limit (bytes/sec) to use for rebalance and upreplication snapshots",
	32<<20, // 32mb/s
	func(v int64) error {
		if v < minSnapshotRate {
			return errors.Errorf("snapshot rate cannot be set to a value below %s: %s",
				humanizeutil.IBytes(minSnapshotRate), humanizeutil.IBytes(v))
		}
		return nil
	},
).WithPublic()

// recoverySnapshotRate is the rate at which Raft-initiated snapshot can be
// sent. Ideally, one would never see a Raft-initiated snapshot; we'd like all
// replicas to start out as learners or via splits, and to never be cut off from
// the log. However, it has proved unfeasible to completely get rid of them.
//
// TODO(tbg): The existence of this rate, separate from rebalanceSnapshotRate,
// does not make a whole lot of sense. Both sources of snapshots compete thanks
// to a semaphore at the receiver, and so the slower one ultimately determines
// the pace at which things can move along.
var recoverySnapshotRate = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_recovery.max_rate",
	"the rate limit (bytes/sec) to use for recovery snapshots",
	32<<20, // 32mb/s
	func(v int64) error {
		if v < minSnapshotRate {
			return errors.Errorf("snapshot rate cannot be set to a value below %s: %s",
				humanizeutil.IBytes(minSnapshotRate), humanizeutil.IBytes(v))
		}
		return nil
	},
).WithPublic()

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
	func(v float64) error {
		const min, max = 0.25, 1.0
		if v < min {
			return errors.Errorf("cannot set to a value less than %f: %f", min, v)
		} else if v > max {
			return errors.Errorf("cannot set to a value greater than %f: %f", max, v)
		}
		return nil
	},
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

func snapshotRateLimit(
	st *cluster.Settings, priority kvserverpb.SnapshotRequest_Priority,
) (rate.Limit, error) {
	switch priority {
	case kvserverpb.SnapshotRequest_RECOVERY:
		return rate.Limit(recoverySnapshotRate.Get(&st.SV)), nil
	case kvserverpb.SnapshotRequest_REBALANCE:
		return rate.Limit(rebalanceSnapshotRate.Get(&st.SV)), nil
	default:
		return 0, errors.Errorf("unknown snapshot priority: %s", priority)
	}
}

// SendEmptySnapshot creates an OutgoingSnapshot for the input range
// descriptor and seeds it with an empty range. Then, it sends this
// snapshot to the replica specified in the input.
func SendEmptySnapshot(
	ctx context.Context,
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
		storage.MaxSize(512<<20 /* 512 MiB */))
	if err != nil {
		return err
	}
	defer eng.Close()

	var ms enginepb.MVCCStats
	// Seed an empty range into the new engine.
	if err := storage.MVCCPutProto(
		ctx, eng, &ms, keys.RangeDescriptorKey(desc.StartKey), now, hlc.ClockTimestamp{}, nil /* txn */, &desc,
	); err != nil {
		return err
	}

	supportsGCHints := st.Version.IsActive(ctx, clusterversion.V22_2GCHintInReplicaState)
	// SendEmptySnapshot is only used by the cockroach debug reset-quorum tool.
	// It is experimental and unlikely to be used in cluster versions that are
	// older than GCHintInReplicaState. We do not want the cluster version to
	// fully dictate the value of the supportsGCHints parameter, since if this
	// node's view of the version is stale we could regress to a state before the
	// migration. Instead, we return an error if the cluster version is old.
	if !supportsGCHints {
		return errors.Errorf("cluster version is too old %s",
			st.Version.ActiveVersionOrEmpty(ctx))
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
		supportsGCHints, /* 22.2: GCHintInReplicaState */
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

	hs, err := sl.LoadHardState(ctx, eng)
	if err != nil {
		return err
	}

	snapUUID, err := uuid.NewV4()
	if err != nil {
		return err
	}

	// The snapshot must use a Pebble snapshot, since it requires consistent
	// iterators.
	engSnapshot := eng.NewSnapshot()

	// Create an OutgoingSnapshot to send.
	outgoingSnap, err := snapshot(
		ctx,
		snapUUID,
		sl,
		// TODO(tbg): We may want a separate SnapshotRequest type
		// for recovery that always goes through by bypassing all throttling
		// so they cannot be declined. We don't want our operation to be held
		// up behind a long running snapshot. We want this to go through
		// quickly.
		kvserverpb.SnapshotRequest_VIA_SNAPSHOT_QUEUE,
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
			To:       uint64(to.ReplicaID),
			From:     uint64(from.ReplicaID),
			Term:     hs.Term,
			Snapshot: &outgoingSnap.RaftSnap,
		},
	}

	header := kvserverpb.SnapshotRequest_Header{
		State:                                state,
		RaftMessageRequest:                   req,
		RangeSize:                            ms.Total(),
		Priority:                             kvserverpb.SnapshotRequest_RECOVERY,
		Strategy:                             kvserverpb.SnapshotRequest_KV_BATCH,
		Type:                                 kvserverpb.SnapshotRequest_VIA_SNAPSHOT_QUEUE,
		DeprecatedUnreplicatedTruncatedState: true,
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

	return sendSnapshot(
		ctx,
		st,
		tracer,
		stream,
		noopStorePool{},
		header,
		&outgoingSnap,
		eng.NewBatch,
		func() {},
		nil, /* recordBytesSent */
	)
}

// noopStorePool is a hollowed out StorePool that does not throttle. It's used in recovery scenarios.
type noopStorePool struct{}

func (n noopStorePool) Throttle(storepool.ThrottleReason, string, roachpb.StoreID) {}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	st *cluster.Settings,
	tracer *tracing.Tracer,
	stream outgoingSnapshotStream,
	storePool SnapshotStorePool,
	header kvserverpb.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() storage.Batch,
	sent func(),
	recordBytesSent snapshotRecordMetrics,
) error {
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
		return err
	}
	log.Event(ctx, "sent SNAPSHOT_REQUEST message to server")
	// Wait until we get a response from the server. The recipient may queue us
	// (only a limited number of snapshots are allowed concurrently) or flat-out
	// reject the snapshot. After the initial message exchange, we'll go and send
	// the actual snapshot (if not rejected).
	resp, err := stream.Recv()
	if err != nil {
		storePool.Throttle(storepool.ThrottleFailed, err.Error(), to.StoreID)
		return err
	}
	switch resp.Status {
	case kvserverpb.SnapshotResponse_ERROR:
		sp.ImportRemoteRecording(resp.CollectedSpans)
		storePool.Throttle(storepool.ThrottleFailed, resp.DeprecatedMessage, to.StoreID)
		return errors.Wrapf(maybeHandleDeprecatedSnapErr(resp.Error()), "%s: remote couldn't accept %s", to, snap)
	case kvserverpb.SnapshotResponse_ACCEPTED:
		// This is the response we're expecting. Continue with snapshot sending.
		log.Event(ctx, "received SnapshotResponse_ACCEPTED message from server")
	default:
		err := errors.Errorf("%s: server sent an invalid status while negotiating %s: %s",
			to, snap, resp.Status)
		storePool.Throttle(storepool.ThrottleFailed, err.Error(), to.StoreID)
		return err
	}

	durQueued := timeutil.Since(start)
	start = timeutil.Now()

	// Consult cluster settings to determine rate limits and batch sizes.
	targetRate, err := snapshotRateLimit(st, header.Priority)
	if err != nil {
		return errors.Wrapf(err, "%s", to)
	}
	batchSize := snapshotSenderBatchSize.Get(&st.SV)

	// Convert the bytes/sec rate limit to batches/sec.
	//
	// TODO(peter): Using bytes/sec for rate limiting seems more natural but has
	// practical difficulties. We either need to use a very large burst size
	// which seems to disable the rate limiting, or call WaitN in smaller than
	// burst size chunks which caused excessive slowness in testing. Would be
	// nice to figure this out, but the batches/sec rate limit works for now.
	limiter := rate.NewLimiter(targetRate/rate.Limit(batchSize), 1 /* burst size */)

	// Create a snapshotStrategy based on the desired snapshot strategy.
	var ss snapshotStrategy
	switch header.Strategy {
	case kvserverpb.SnapshotRequest_KV_BATCH:
		ss = &kvBatchSnapshotStrategy{
			batchSize: batchSize,
			limiter:   limiter,
			newBatch:  newBatch,
			st:        st,
		}
	default:
		log.Fatalf(ctx, "unknown snapshot strategy: %s", header.Strategy)
	}

	// Record timings for snapshot send if kv.trace.snapshot.enable_threshold is enabled
	numBytesSent, err := ss.Send(ctx, stream, header, snap, recordBytesSent)
	if err != nil {
		return err
	}
	durSent := timeutil.Since(start)

	// Notify the sent callback before the final snapshot request is sent so that
	// the snapshots generated metric gets incremented before the snapshot is
	// applied.
	sent()
	if err := stream.Send(&kvserverpb.SnapshotRequest{Final: true}); err != nil {
		return err
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
		return errors.Wrapf(err, "%s: remote failed to apply snapshot", to)
	}
	sp.ImportRemoteRecording(resp.CollectedSpans)
	// NB: wait for EOF which ensures that all processing on the server side has
	// completed (such as defers that might be run after the previous message was
	// received).
	if unexpectedResp, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return errors.Wrapf(err, "%s: expected EOF, got resp=%v with error", to, unexpectedResp)
		}
		return errors.Newf("%s: expected EOF, got resp=%v", to, unexpectedResp)
	}
	switch resp.Status {
	case kvserverpb.SnapshotResponse_ERROR:
		return errors.Wrapf(
			maybeHandleDeprecatedSnapErr(resp.Error()), "%s: remote failed to apply snapshot", to,
		)
	case kvserverpb.SnapshotResponse_APPLIED:
		return nil
	default:
		return errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status,
		)
	}
}
