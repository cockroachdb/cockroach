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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	enginepb "github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

const (
	// Messages that provide detail about why a snapshot was rejected.
	storeDrainingMsg = "store is draining"

	// IntersectingSnapshotMsg is part of the error message returned from
	// canAcceptSnapshotLocked and is exposed here so testing can rely on it.
	IntersectingSnapshotMsg = "snapshot intersects existing range"
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

// snapshotStrategy is an approach to sending and receiving Range snapshots.
// Each implementation corresponds to a SnapshotRequest_Strategy, and it is
// expected that the implementation that matches the Strategy specified in the
// snapshot header will always be used.
type snapshotStrategy interface {
	// Receive streams SnapshotRequests in from the provided stream and
	// constructs an IncomingSnapshot.
	Receive(context.Context, incomingSnapshotStream, kvserverpb.SnapshotRequest_Header) (IncomingSnapshot, error)

	// Send streams SnapshotRequests created from the OutgoingSnapshot in to the
	// provided stream. On nil error, the number of bytes sent is returned.
	Send(context.Context, outgoingSnapshotStream, kvserverpb.SnapshotRequest_Header, *OutgoingSnapshot) (int64, error)

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

// Separated locks and snapshots send/receive:
// When running in a mixed version cluster with 21.1 and 20.2, snapshots sent
// by 21.1 nodes will attempt to read the lock table key space and send any
// keys in it. But there will be none, so 20.2 nodes receiving such snapshots
// are fine. A 21.1 node receiving a snapshot will construct SSTs for the lock
// table key range which will only contain ClearRange for those ranges.
//
// When the cluster transitions to clusterversion.SeparatedLocks, the nodes
// that see that transition can immediately start writing separated
// intents/locks. Since the 21.1 nodes that have not seen that transition are
// always ready to handle separated intents, including receiving them in
// snapshots, the cluster will behave correctly despite nodes seeing this
// state transition at different times.

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

// multiSSTWriter is a wrapper around RocksDBSstFileWriter and
// SSTSnapshotStorageScratch that handles chunking SSTs and persisting them to
// disk.
type multiSSTWriter struct {
	st        *cluster.Settings
	scratch   *SSTSnapshotStorageScratch
	currSST   storage.SSTWriter
	keyRanges []rditer.KeyRange
	currRange int
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
	keyRanges []rditer.KeyRange,
	sstChunkSize int64,
) (multiSSTWriter, error) {
	msstw := multiSSTWriter{
		st:           st,
		scratch:      scratch,
		keyRanges:    keyRanges,
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
		msstw.keyRanges[msstw.currRange].Start, msstw.keyRanges[msstw.currRange].End); err != nil {
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
	msstw.currRange++
	msstw.currSST.Close()
	return nil
}

func (msstw *multiSSTWriter) Put(ctx context.Context, key storage.EngineKey, value []byte) error {
	for msstw.keyRanges[msstw.currRange].End.Compare(key.Key) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(ctx); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	if msstw.keyRanges[msstw.currRange].Start.Compare(key.Key) > 0 {
		return errors.AssertionFailedf("client error: expected %s to fall in one of %s", key.Key, msstw.keyRanges)
	}
	if err := msstw.currSST.PutEngineKey(key, value); err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}
	return nil
}

func (msstw *multiSSTWriter) Finish(ctx context.Context) (int64, error) {
	if msstw.currRange < len(msstw.keyRanges) {
		for {
			if err := msstw.finalizeSST(ctx); err != nil {
				return 0, err
			}
			if msstw.currRange >= len(msstw.keyRanges) {
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

// Receive implements the snapshotStrategy interface.
//
// NOTE: This function assumes that the key-value pairs are sent in sorted
// order. The key-value pairs are sent in the following sorted order:
//
// 1. Replicated range-id local key range
// 2. Range-local key range
// 3. Two lock-table key ranges (optional)
// 4. User key range
func (kvSS *kvBatchSnapshotStrategy) Receive(
	ctx context.Context, stream incomingSnapshotStream, header kvserverpb.SnapshotRequest_Header,
) (IncomingSnapshot, error) {
	assertStrategy(ctx, header, kvserverpb.SnapshotRequest_KV_BATCH)

	// At the moment we'll write at most five SSTs.
	// TODO(jeffreyxiao): Re-evaluate as the default range size grows.
	keyRanges := rditer.MakeReplicatedKeyRanges(header.State.Desc)
	msstw, err := newMultiSSTWriter(ctx, kvSS.st, kvSS.scratch, keyRanges, kvSS.sstChunkSize)
	if err != nil {
		return noSnap, err
	}
	defer msstw.Close()

	for {
		req, err := stream.Recv()
		if err != nil {
			return noSnap, err
		}
		if req.Header != nil {
			err := errors.New("client error: provided a header mid-stream")
			return noSnap, sendSnapshotError(stream, err)
		}

		if req.KVBatch != nil {
			batchReader, err := storage.NewRocksDBBatchReader(req.KVBatch)
			if err != nil {
				return noSnap, errors.Wrap(err, "failed to decode batch")
			}
			// All operations in the batch are guaranteed to be puts.
			for batchReader.Next() {
				if batchReader.BatchType() != storage.BatchTypeValue {
					return noSnap, errors.AssertionFailedf("expected type %d, found type %d", storage.BatchTypeValue, batchReader.BatchType())
				}
				key, err := batchReader.EngineKey()
				if err != nil {
					return noSnap, errors.Wrap(err, "failed to decode mvcc key")
				}
				if err := msstw.Put(ctx, key, batchReader.Value()); err != nil {
					return noSnap, errors.Wrapf(err, "writing sst for raft snapshot")
				}
			}
		}
		if req.Final {
			// We finished receiving all batches and log entries. It's possible that
			// we did not receive any key-value pairs for some of the key ranges, but
			// we must still construct SSTs with range deletion tombstones to remove
			// the data.
			dataSize, err := msstw.Finish(ctx)
			if err != nil {
				return noSnap, errors.Wrapf(err, "finishing sst for raft snapshot")
			}
			msstw.Close()

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

			kvSS.status = redact.Sprintf("ssts: %d", len(kvSS.scratch.SSTs()))
			return inSnap, nil
		}
	}
}

// errMalformedSnapshot indicates that the snapshot in question is malformed,
// for e.g. missing raft log entries.
var errMalformedSnapshot = errors.New("malformed snapshot generated")

// Send implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Send(
	ctx context.Context,
	stream outgoingSnapshotStream,
	header kvserverpb.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
) (int64, error) {
	assertStrategy(ctx, header, kvserverpb.SnapshotRequest_KV_BATCH)

	// bytesSent is updated as key-value batches are sent with sendBatch. It
	// does not reflect the log entries sent (which are never sent in newer
	// versions of CRDB, as of VersionUnreplicatedTruncatedState).
	bytesSent := int64(0)

	// Iterate over all keys using the provided iterator and stream out batches
	// of key-values.
	kvs := 0
	var b storage.Batch
	defer func() {
		if b != nil {
			b.Close()
		}
	}()
	for iter := snap.Iter; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return 0, err
		} else if !ok {
			break
		}
		kvs++
		unsafeKey := iter.UnsafeKey()
		unsafeValue := iter.UnsafeValue()
		if b == nil {
			b = kvSS.newBatch()
		}
		if err := b.PutEngineKey(unsafeKey, unsafeValue); err != nil {
			return 0, err
		}

		if bLen := int64(b.Len()); bLen >= kvSS.batchSize {
			if err := kvSS.sendBatch(ctx, stream, b); err != nil {
				return 0, err
			}
			bytesSent += bLen
			b.Close()
			b = nil
		}
	}
	if b != nil {
		if err := kvSS.sendBatch(ctx, stream, b); err != nil {
			return 0, err
		}
		bytesSent += int64(b.Len())
	}

	kvSS.status = redact.Sprintf("kv pairs: %d", kvs)
	return bytesSent, nil
}

func (kvSS *kvBatchSnapshotStrategy) sendBatch(
	ctx context.Context, stream outgoingSnapshotStream, batch storage.Batch,
) error {
	if err := kvSS.limiter.WaitN(ctx, 1); err != nil {
		return err
	}
	return stream.Send(&kvserverpb.SnapshotRequest{KVBatch: batch.Repr()})
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
		if err := kvSS.scratch.Clear(); err != nil {
			log.Warningf(ctx, "error closing kvBatchSnapshotStrategy: %v", err)
		}
	}
}

// reserveSnapshot throttles incoming snapshots. The returned closure is used
// to cleanup the reservation and release its resources.
func (s *Store) reserveSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header,
) (_cleanup func(), _err error) {
	tBegin := timeutil.Now()

	// Empty snapshots are exempt from rate limits because they're so cheap to
	// apply. This vastly speeds up rebalancing any empty ranges created by a
	// RESTORE or manual SPLIT AT, since it prevents these empty snapshots from
	// getting stuck behind large snapshots managed by the replicate queue.
	if header.RangeSize != 0 {
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
		case s.snapshotApplySem <- struct{}{}:
		case <-queueCtx.Done():
			if err := ctx.Err(); err != nil {
				return nil, errors.Wrap(err, "acquiring snapshot reservation")
			}
			return nil, errors.Wrapf(queueCtx.Err(),
				"giving up during snapshot reservation due to %q",
				snapshotReservationQueueTimeoutFraction.Key())
		case <-s.stopper.ShouldQuiesce():
			return nil, errors.Errorf("stopped")
		}
	}

	// The choice here is essentially arbitrary, but with a default range size of 128mb-512mb and the
	// Raft snapshot rate limiting of 32mb/s, we expect to spend less than 16s per snapshot.
	// which is what we want to log.
	const snapshotReservationWaitWarnThreshold = 32 * time.Second
	if elapsed := timeutil.Since(tBegin); elapsed > snapshotReservationWaitWarnThreshold {
		replDesc, _ := header.State.Desc.GetReplicaDescriptor(s.StoreID())
		log.Infof(
			ctx,
			"waited for %.1fs to acquire snapshot reservation to r%d/%d",
			elapsed.Seconds(),
			header.State.Desc.RangeID,
			replDesc.ReplicaID,
		)
	}

	s.metrics.ReservedReplicaCount.Inc(1)
	s.metrics.Reserved.Inc(header.RangeSize)
	return func() {
		s.metrics.ReservedReplicaCount.Dec(1)
		s.metrics.Reserved.Dec(header.RangeSize)
		if header.RangeSize != 0 {
			<-s.snapshotApplySem
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

	// We have a key range [desc.StartKey,desc.EndKey) which we want to apply a
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
		return errors.Errorf("%s: canAcceptSnapshotLocked: cannot add placeholder, have an existing placeholder %s %v", s, exRng, snapHeader.RaftMessageRequest.FromReplica)
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
		return errors.Errorf("%s %v (incoming %v)", msg, exReplica, snapHeader.State.Desc.RSpan()) // exReplica can be nil
	}
	return nil
}

// receiveSnapshot receives an incoming snapshot via a pre-opened GRPC stream.
func (s *Store) receiveSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header, stream incomingSnapshotStream,
) error {
	if fn := s.cfg.TestingKnobs.ReceiveSnapshot; fn != nil {
		if err := fn(header); err != nil {
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

	cleanup, err := s.reserveSnapshot(ctx, header)
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
		return pErr.GoError()
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

	inSnap, err := ss.Receive(ctx, stream, *header)
	if err != nil {
		return err
	}
	inSnap.placeholder = placeholder

	// Use a background context for applying the snapshot, as handleRaftReady is
	// not prepared to deal with arbitrary context cancellation. Also, we've
	// already received the entire snapshot here, so there's no point in
	// abandoning application half-way through if the caller goes away.
	applyCtx := s.AnnotateCtx(context.Background())
	if err := s.processRaftSnapshotRequest(applyCtx, header, inSnap); err != nil {
		return sendSnapshotError(stream, errors.Wrap(err.GoError(), "failed to apply snapshot"))
	}
	return stream.Send(&kvserverpb.SnapshotResponse{Status: kvserverpb.SnapshotResponse_APPLIED})
}

func sendSnapshotError(stream incomingSnapshotStream, err error) error {
	return stream.Send(&kvserverpb.SnapshotResponse{
		Status:  kvserverpb.SnapshotResponse_ERROR,
		Message: err.Error(),
	})
}

// SnapshotStorePool narrows StorePool to make sendSnapshot easier to test.
type SnapshotStorePool interface {
	throttle(reason throttleReason, why string, toStoreID roachpb.StoreID)
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
// Enforcement of this snapshotApplySem-scoped timeout is intended to prevent
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
//  t   | snap1 snap2 snap3 snap4 snap5 ...
//  ----+------------------------------------
//  0   | send
//  15  |       queue queue
//  30  |                   queue
//  45  | ok    send
//  60  |                         queue
//  75  |       fail  fail  send
//  90  |                   fail  send
//  105 |
//  120 |                         fail
//  135 |
//
// If we limit the amount of time we are willing to wait for a reservation to
// something that is small enough to, on success, give us enough time to
// actually stream the data, no starvation can occur. For example, with a 60s
// timeout, 45s needed to stream the data, we can wait at most 15s for a
// reservation and still avoid starvation:
//
//  t   | snap1 snap2 snap3 snap4 snap5 ...
//  ----+------------------------------------
//  0   | send
//  15  |       queue queue
//  30  |       fail  fail  send
//  45  |
//  60  | ok                      queue
//  75  |                   ok    send
//  90  |
//  105 |
//  120 |                         ok
//  135 |
//
// In practice, the snapshot reservation logic (reserveSnapshot) doesn't know
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
//  guaranteed_processing_time = (1 - reservation_queue_timeout_fraction) x timeout
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
	bulkIOWriteBurst,
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
	cc *grpc.ClientConn,
	now hlc.Timestamp,
	desc roachpb.RangeDescriptor,
	to roachpb.ReplicaDescriptor,
) error {
	// Create an engine to use as a buffer for the empty snapshot.
	eng, err := storage.Open(
		context.Background(),
		storage.InMemory(),
		storage.CacheSize(1<<20 /* 1 MiB */),
		storage.MaxSize(512<<20 /* 512 MiB */))
	if err != nil {
		return err
	}
	defer eng.Close()

	var ms enginepb.MVCCStats
	// Seed an empty range into the new engine.
	if err := storage.MVCCPutProto(
		ctx, eng, &ms, keys.RangeDescriptorKey(desc.StartKey), now, nil /* txn */, &desc,
	); err != nil {
		return err
	}

	// SendEmptySnapshot is only used by the cockroach debug reset-quorum tool.
	// It is experimental and unlikely to be used in cluster versions that are
	// older than AddRaftAppliedIndexTermMigration. We do not want the cluster
	// version to fully dictate the value of the writeAppliedIndexTerm
	// parameter, since if this node's view of the version is stale we could
	// regress to a state before the migration. Instead, we return an error if
	// the cluster version is old.
	writeAppliedIndexTerm := st.Version.IsActive(ctx, clusterversion.AddRaftAppliedIndexTermMigration)
	if !writeAppliedIndexTerm {
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
		st.Version.ActiveVersionOrEmpty(ctx).Version,
		writeAppliedIndexTerm,
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
		eng,
		desc.RangeID,
		raftentry.NewCache(1), // cache is not used
		func(func(SideloadStorage) error) error { return nil }, // this is used for sstables, not needed here as there are no logs
		desc.StartKey,
	)
	if err != nil {
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
			Snapshot: outgoingSnap.RaftSnap,
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
		stream,
		noopStorePool{},
		header,
		&outgoingSnap,
		eng.NewBatch,
		func() {},
	)
}

// noopStorePool is a hollowed out StorePool that does not throttle. It's used in recovery scenarios.
type noopStorePool struct{}

func (n noopStorePool) throttle(throttleReason, string, roachpb.StoreID) {}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	st *cluster.Settings,
	stream outgoingSnapshotStream,
	storePool SnapshotStorePool,
	header kvserverpb.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() storage.Batch,
	sent func(),
) error {
	start := timeutil.Now()
	to := header.RaftMessageRequest.ToReplica
	if err := stream.Send(&kvserverpb.SnapshotRequest{Header: &header}); err != nil {
		return err
	}
	// Wait until we get a response from the server. The recipient may queue us
	// (only a limited number of snapshots are allowed concurrently) or flat-out
	// reject the snapshot. After the initial message exchange, we'll go and send
	// the actual snapshot (if not rejected).
	resp, err := stream.Recv()
	if err != nil {
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
		return err
	}
	switch resp.Status {
	case kvserverpb.SnapshotResponse_ERROR:
		storePool.throttle(throttleFailed, resp.Message, to.StoreID)
		return errors.Errorf("%s: remote couldn't accept %s with error: %s",
			to, snap, resp.Message)
	case kvserverpb.SnapshotResponse_ACCEPTED:
	// This is the response we're expecting. Continue with snapshot sending.
	default:
		err := errors.Errorf("%s: server sent an invalid status while negotiating %s: %s",
			to, snap, resp.Status)
		storePool.throttle(throttleFailed, err.Error(), to.StoreID)
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

	numBytesSent, err := ss.Send(ctx, stream, header, snap)
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
	log.Infof(
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
		return errors.Errorf("%s: remote failed to apply snapshot for reason %s", to, resp.Message)
	case kvserverpb.SnapshotResponse_APPLIED:
		return nil
	default:
		return errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status)
	}
}
