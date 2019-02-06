// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/time/rate"
)

const (
	// preemptiveSnapshotRaftGroupID is a bogus ID for which a Raft group is
	// temporarily created during the application of a preemptive snapshot.
	preemptiveSnapshotRaftGroupID = math.MaxUint64

	// Messages that provide detail about why a preemptive snapshot was rejected.
	snapshotStoreTooFullMsg = "store almost out of disk space"
	snapshotApplySemBusyMsg = "store busy applying snapshots"
	storeDrainingMsg        = "store is draining"

	// IntersectingSnapshotMsg is part of the error message returned from
	// canApplySnapshotLocked and is exposed here so testing can rely on it.
	IntersectingSnapshotMsg = "snapshot intersects existing range"
)

// incomingSnapshotStream is the minimal interface on a GRPC stream required
// to receive a snapshot over the network.
type incomingSnapshotStream interface {
	Send(*SnapshotResponse) error
	Recv() (*SnapshotRequest, error)
}

// outgoingSnapshotStream is the minimal interface on a GRPC stream required
// to send a snapshot over the network.
type outgoingSnapshotStream interface {
	Send(*SnapshotRequest) error
	Recv() (*SnapshotResponse, error)
}

// snapshotStrategy is an approach to sending and receiving Range snapshots.
// Each implementation corresponds to a SnapshotRequest_Strategy, and it is
// expected that the implementation that matches the Strategy specified in the
// snapshot header will always be used.
type snapshotStrategy interface {
	// Receive streams SnapshotRequests in from the provided stream and
	// constructs an IncomingSnapshot.
	Receive(
		context.Context, incomingSnapshotStream, SnapshotRequest_Header,
	) (IncomingSnapshot, error)

	// Send streams SnapshotRequests created from the OutgoingSnapshot in to the
	// provided stream.
	Send(
		context.Context, outgoingSnapshotStream, SnapshotRequest_Header, *OutgoingSnapshot,
	) error

	// Status provides a status report on the work performed during the
	// snapshot. Only valid if the strategy succeeded.
	Status() string
}

func assertStrategy(
	ctx context.Context, header SnapshotRequest_Header, expect SnapshotRequest_Strategy,
) {
	if header.Strategy != expect {
		log.Fatalf(ctx, "expected strategy %s, found strategy %s", expect, header.Strategy)
	}
}

// kvBatchSnapshotStrategy is an implementation of snapshotStrategy that streams
// batches of KV pairs in the BatchRepr format.
type kvBatchSnapshotStrategy struct {
	raftCfg *base.RaftConfig
	status  string

	// Fields used when sending snapshots.
	batchSize int64
	limiter   *rate.Limiter
	newBatch  func() engine.Batch
}

// Send implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Receive(
	ctx context.Context, stream incomingSnapshotStream, header SnapshotRequest_Header,
) (IncomingSnapshot, error) {
	assertStrategy(ctx, header, SnapshotRequest_KV_BATCH)

	var batches [][]byte
	var logEntries [][]byte
	for {
		req, err := stream.Recv()
		if err != nil {
			return IncomingSnapshot{}, err
		}
		if req.Header != nil {
			err := errors.New("client error: provided a header mid-stream")
			return IncomingSnapshot{}, sendSnapshotError(stream, err)
		}

		if req.KVBatch != nil {
			batches = append(batches, req.KVBatch)
		}
		if req.LogEntries != nil {
			logEntries = append(logEntries, req.LogEntries...)
		}
		if req.Final {
			snapUUID, err := uuid.FromBytes(header.RaftMessageRequest.Message.Snapshot.Data)
			if err != nil {
				err = errors.Wrap(err, "invalid snapshot")
				return IncomingSnapshot{}, sendSnapshotError(stream, err)
			}

			inSnap := IncomingSnapshot{
				UsesLegacyTruncatedState: !header.UnreplicatedTruncatedState,
				SnapUUID:                 snapUUID,
				Batches:                  batches,
				LogEntries:               logEntries,
				State:                    &header.State,
				snapType:                 snapTypeRaft,
			}
			if header.RaftMessageRequest.ToReplica.ReplicaID == 0 {
				inSnap.snapType = snapTypePreemptive
			}
			kvSS.status = fmt.Sprintf("kv batches: %d, log entries: %d", len(batches), len(logEntries))
			return inSnap, nil
		}
	}
}

// Send implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Send(
	ctx context.Context,
	stream outgoingSnapshotStream,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
) error {
	assertStrategy(ctx, header, SnapshotRequest_KV_BATCH)

	// Iterate over all keys using the provided iterator and stream out batches
	// of key-values.
	n := 0
	var b engine.Batch
	for iter := snap.Iter; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		key := iter.Key()
		value := iter.Value()
		n++
		if b == nil {
			b = kvSS.newBatch()
		}
		if err := b.Put(key, value); err != nil {
			b.Close()
			return err
		}

		if int64(b.Len()) >= kvSS.batchSize {
			if err := kvSS.limiter.WaitN(ctx, 1); err != nil {
				return err
			}
			if err := kvSS.sendBatch(stream, b); err != nil {
				return err
			}
			b = nil
			// We no longer need the keys and values in the batch we just sent,
			// so reset ReplicaDataIterator's allocator and allow its data to
			// be garbage collected.
			iter.ResetAllocator()
		}
	}
	if b != nil {
		if err := kvSS.limiter.WaitN(ctx, 1); err != nil {
			return err
		}
		if err := kvSS.sendBatch(stream, b); err != nil {
			return err
		}
	}

	// Iterate over the specified range of Raft entries and send them all out
	// together.
	firstIndex := header.State.TruncatedState.Index + 1
	endIndex := snap.RaftSnap.Metadata.Index + 1
	preallocSize := endIndex - firstIndex
	const maxPreallocSize = 1000
	if preallocSize > maxPreallocSize {
		// It's possible for the raft log to become enormous in certain
		// sustained failure conditions. We may bail out of the snapshot
		// process early in scanFunc, but in the worst case this
		// preallocation is enough to run the server out of memory. Limit
		// the size of the buffer we will preallocate.
		preallocSize = maxPreallocSize
	}
	logEntries := make([][]byte, 0, preallocSize)

	var raftLogBytes int64
	scanFunc := func(kv roachpb.KeyValue) (bool, error) {
		bytes, err := kv.Value.GetBytes()
		if err == nil {
			logEntries = append(logEntries, bytes)
			raftLogBytes += int64(len(bytes))
			if snap.snapType == snapTypePreemptive &&
				raftLogBytes > 4*kvSS.raftCfg.RaftLogTruncationThreshold {
				// If the raft log is too large, abort the snapshot instead of
				// potentially running out of memory. However, if this is a
				// raft-initiated snapshot (instead of a preemptive one), we
				// have a dilemma. It may be impossible to truncate the raft
				// log until we have caught up a peer with a snapshot. Since
				// we don't know the exact size at which we will run out of
				// memory, we err on the size of allowing the snapshot if it
				// is raft-initiated, while aborting preemptive snapshots at a
				// reasonable threshold. (Empirically, this is good enough:
				// the situations that result in large raft logs have not been
				// observed to result in raft-initiated snapshots).
				//
				// By aborting preemptive snapshots here, we disallow replica
				// changes until the current replicas have caught up and
				// truncated the log (either the range is available, in which
				// case this will eventually happen, or it's not,in which case
				// the preemptive snapshot would be wasted anyway because the
				// change replicas transaction would be unable to commit).
				return false, errors.Errorf(
					"aborting snapshot because raft log is too large "+
						"(%d bytes after processing %d of %d entries)",
					raftLogBytes, len(logEntries), endIndex-firstIndex)
			}
		}
		return false, err
	}

	rangeID := header.State.Desc.RangeID

	if err := iterateEntries(ctx, snap.EngineSnap, rangeID, firstIndex, endIndex, scanFunc); err != nil {
		return err
	}

	// Inline the payloads for all sideloaded proposals.
	//
	// TODO(tschottdorf): could also send slim proposals and attach sideloaded
	// SSTables directly to the snapshot. Probably the better long-term
	// solution, but let's see if it ever becomes relevant. Snapshots with
	// inlined proposals are hopefully the exception.
	{
		var ent raftpb.Entry
		for i := range logEntries {
			if err := protoutil.Unmarshal(logEntries[i], &ent); err != nil {
				return err
			}
			if !sniffSideloadedRaftCommand(ent.Data) {
				continue
			}
			if err := snap.WithSideloaded(func(ss sideloadStorage) error {
				newEnt, err := maybeInlineSideloadedRaftCommand(
					ctx, rangeID, ent, ss, snap.RaftEntryCache,
				)
				if err != nil {
					return err
				}
				if newEnt != nil {
					ent = *newEnt
				}
				return nil
			}); err != nil {
				if errors.Cause(err) == errSideloadedFileNotFound {
					// We're creating the Raft snapshot based on a snapshot of
					// the engine, but the Raft log may since have been
					// truncated and corresponding on-disk sideloaded payloads
					// unlinked. Luckily, we can just abort this snapshot; the
					// caller can retry.
					//
					// TODO(tschottdorf): check how callers handle this. They
					// should simply retry. In some scenarios, perhaps this can
					// happen repeatedly and prevent a snapshot; not sending the
					// log entries wouldn't help, though, and so we'd really
					// need to make sure the entries are always here, for
					// instance by pre-loading them into memory. Or we can make
					// log truncation less aggressive about removing sideloaded
					// files, by delaying trailing file deletion for a bit.
					return &errMustRetrySnapshotDueToTruncation{
						index: ent.Index,
						term:  ent.Term,
					}
				}
				return err
			}
			// TODO(tschottdorf): it should be possible to reuse `logEntries[i]` here.
			var err error
			if logEntries[i], err = protoutil.Marshal(&ent); err != nil {
				return err
			}
		}
	}
	kvSS.status = fmt.Sprintf("kv pairs: %d, log entries: %d", n, len(logEntries))
	return stream.Send(&SnapshotRequest{LogEntries: logEntries})
}

func (kvSS *kvBatchSnapshotStrategy) sendBatch(
	stream outgoingSnapshotStream, batch engine.Batch,
) error {
	repr := batch.Repr()
	batch.Close()
	return stream.Send(&SnapshotRequest{KVBatch: repr})
}

// Status implements the snapshotStrategy interface.
func (kvSS *kvBatchSnapshotStrategy) Status() string { return kvSS.status }

// reserveSnapshot throttles incoming snapshots. The returned closure is used
// to cleanup the reservation and release its resources. A nil cleanup function
// and a non-empty rejectionMessage indicates the reservation was declined.
func (s *Store) reserveSnapshot(
	ctx context.Context, header *SnapshotRequest_Header,
) (_cleanup func(), _rejectionMsg string, _err error) {
	tBegin := timeutil.Now()
	if header.RangeSize == 0 {
		// Empty snapshots are exempt from rate limits because they're so cheap to
		// apply. This vastly speeds up rebalancing any empty ranges created by a
		// RESTORE or manual SPLIT AT, since it prevents these empty snapshots from
		// getting stuck behind large snapshots managed by the replicate queue.
	} else if header.CanDecline {
		storeDesc, ok := s.cfg.StorePool.getStoreDescriptor(s.StoreID())
		if ok && (!maxCapacityCheck(storeDesc) || header.RangeSize > storeDesc.Capacity.Available) {
			return nil, snapshotStoreTooFullMsg, nil
		}
		select {
		case s.snapshotApplySem <- struct{}{}:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-s.stopper.ShouldStop():
			return nil, "", errors.Errorf("stopped")
		default:
			return nil, snapshotApplySemBusyMsg, nil
		}
	} else {
		select {
		case s.snapshotApplySem <- struct{}{}:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-s.stopper.ShouldStop():
			return nil, "", errors.Errorf("stopped")
		}
	}

	// The choice here is essentially arbitrary, but with a default range size of 64mb and the
	// Raft snapshot rate limiting of 8mb/s, we expect to spend less than 8s per snapshot.
	// Preemptive snapshots are limited to 2mb/s (by default), so they can take up to 4x longer,
	// but an average range is closer to 32mb, so we expect ~16s for larger preemptive snapshots,
	// which is what we want to log.
	const snapshotReservationWaitWarnThreshold = 13 * time.Second
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
	}, "", nil
}

// canApplySnapshot returns (_, nil) if the snapshot can be applied to
// this store's replica (i.e. the snapshot is not from an older incarnation of
// the replica) and a placeholder can be added to the replicasByKey map (if
// necessary). If a placeholder is required, it is returned as the first value.
// The authoritative bool determines whether the check is carried out with the
// intention of actually applying the snapshot (in which case an existing replica
// must exist and have its raftMu locked) or as a preliminary check.
func (s *Store) canApplySnapshot(
	ctx context.Context, snapHeader *SnapshotRequest_Header, authoritative bool,
) (*ReplicaPlaceholder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canApplySnapshotLocked(ctx, snapHeader, authoritative)
}

func (s *Store) canApplySnapshotLocked(
	ctx context.Context, snapHeader *SnapshotRequest_Header, authoritative bool,
) (*ReplicaPlaceholder, error) {
	desc := *snapHeader.State.Desc

	// First, check for an existing Replica.
	//
	// We call canApplySnapshotLocked twice for each snapshot application. In
	// the first case, it's an optimization early before having received any
	// data (and we don't use the placeholder if one is returned), and the
	// replica may or may not be present.
	//
	// The second call happens right before we actually plan to apply the
	// snapshot (and a Replica is always in place at that point). This means
	// that without a Replica, we can have false positives, but if we have a
	// replica it needs to take everything into account.
	//
	// TODO(tbg): untangle these two use cases.
	if v, ok := s.mu.replicas.Load(
		int64(desc.RangeID),
	); !ok {
		if authoritative {
			return nil, errors.Errorf("authoritative call requires a replica present")
		}
	} else {
		existingRepl := (*Replica)(v)
		// The raftMu is held which allows us to use the existing replica as a
		// placeholder when we decide that the snapshot can be applied. As long
		// as the caller releases the raftMu only after feeding the snapshot
		// into the replica, this is safe.
		if authoritative {
			existingRepl.raftMu.AssertHeld()
		}

		existingRepl.mu.RLock()
		existingDesc := existingRepl.descRLocked()
		existingIsInitialized := existingRepl.isInitializedRLocked()
		existingIsPreemptive := existingRepl.mu.replicaID == 0
		existingRepl.mu.RUnlock()

		if existingIsInitialized {
			if !snapHeader.IsPreemptive() {
				// Regular Raft snapshots can't be refused at this point,
				// even if they widen the existing replica. See the comments
				// in Replica.maybeAcquireSnapshotMergeLock for how this is
				// made safe.
				//
				// NB: we expect the replica to know its replicaID at this point
				// (i.e. !existingIsPreemptive), though perhaps it's possible
				// that this isn't true if the leader initiates a Raft snapshot
				// (that would provide a range descriptor with this replica in
				// it) but this node reboots (temporarily forgetting its
				// replicaID) before the snapshot arrives.
				return nil, nil
			}

			if existingIsPreemptive {
				// Allow applying a preemptive snapshot on top of another
				// preemptive snapshot. We only need to acquire a placeholder
				// for the part (if any) of the new snapshot that extends past
				// the old one. If there's no such overlap, return early; if
				// there is, "forward" the descriptor's StartKey so that the
				// later code will only check the overlap.
				//
				// NB: morally it would be cleaner to ask for the existing
				// replica to be GC'ed first, but consider that the preemptive
				// snapshot was likely left behind by a failed attempt to
				// up-replicate. This is a relatively common scenario and not
				// worth discarding and resending another snapshot for. Let the
				// snapshot through, which means "pretending that it doesn't
				// intersect the existing replica".
				if !existingDesc.EndKey.Less(desc.EndKey) {
					return nil, nil
				}
				desc.StartKey = existingDesc.EndKey
			}
			// NB: If the existing snapshot is *not* preemptive (i.e. the above
			// branch wasn't taken), the overlap check below will hit an error.
			// This path is hit after a rapid up-down-upreplication to the same
			// store and will resolve as the replicaGCQueue removes the existing
			// replica. We are pretty sure that the existing replica is gc'able,
			// because a preemptive snapshot implies that someone is trying to
			// add this replica to the group at the moment. (We are not however,
			// sure enough that this couldn't happen by accident to GC the
			// replica ourselves - the replica GC queue will perform the proper
			// check).
		} else if snapHeader.IsPreemptive() {
			// Morally, the existing replica now has a nonzero replica ID
			// because we already know that it is not initialized (i.e. has no
			// data). Interestingly, the case in which it has a zero replica ID
			// is also possible and should see the snapshot accepted as it
			// occurs when a preemptive snapshot is handled: we first create a
			// Replica in this state, run this check, and then apply the
			// preemptive snapshot.
			if !existingIsPreemptive {
				// This is similar to the case of a preemptive snapshot hitting
				// a fully initialized replica (i.e. not a preemptive snapshot)
				// at the end of the last branch (which we don't allow), so we
				// want to reject the snapshot. There is a tricky problem to
				// to solve here, though: existingRepl doesn't know anything
				// about its key bounds, and so to check whether it is actually
				// gc'able would require a full scan of the meta2 entries (and
				// we would also need to teach the queues how to deal with un-
				// initialized replicas).
				//
				// So we let the snapshot through (by falling through to the
				// overlap check, where it either picks up placeholders or
				// fails). This is safe (or at least we assume so) because we
				// carry out all snapshot decisions through Raft (though it
				// still is an odd path that we would be wise to avoid if it
				// weren't so difficult).
				//
				// A consequence of letting this snapshot through is opening this
				// replica up to the possibility of erroneous replicaGC. This is
				// because it will retain the replicaID of the current replica,
				// which is going to be initialized after the snapshot (and thus
				// gc'able).
				_ = 0 // avoid staticcheck failure
			}
		}
	}

	// We have a key range [desc.StartKey,desc.EndKey) which we want to apply a
	// snapshot for. Is there a conflicting existing placeholder or an
	// overlapping range?

	// NB: this check seems redundant since placeholders are also represented in
	// replicasByKey (and thus returned in getOverlappingKeyRangeLocked).
	if exRng, ok := s.mu.replicaPlaceholders[desc.RangeID]; ok {
		return nil, errors.Errorf("%s: canApplySnapshotLocked: cannot add placeholder, have an existing placeholder %s", s, exRng)
	}

	// TODO(benesch): consider discovering and GC'ing *all* overlapping ranges,
	// not just the first one that getOverlappingKeyRangeLocked happens to return.
	if exRange := s.getOverlappingKeyRangeLocked(&desc); exRange != nil {
		// We have a conflicting range, so we must block the snapshot.
		// When such a conflict exists, it will be resolved by one range
		// either being split or garbage collected.
		exReplica, err := s.GetReplica(exRange.Desc().RangeID)
		msg := IntersectingSnapshotMsg
		if err != nil {
			log.Warning(ctx, errors.Wrapf(
				err, "unable to look up overlapping replica on %s", exReplica))
		} else {
			inactive := func(r *Replica) bool {
				if r.RaftStatus() == nil {
					return true
				}
				// TODO(benesch): this check does detect inactivity on replicas with
				// epoch-based leases. Since the validity of an epoch-based lease is
				// tied to the owning node's liveness, the lease can be valid well after
				// the leader of the range has cut off communication with this replica.
				// Expiration based leases, by contrast, will expire quickly if the
				// leader of the range stops sending this replica heartbeats.
				lease, pendingLease := r.GetLease()
				now := s.Clock().Now()
				return !r.IsLeaseValid(lease, now) &&
					(pendingLease == (roachpb.Lease{}) || !r.IsLeaseValid(pendingLease, now))
			}
			// We unconditionally send this replica through the GC queue. It's
			// reasonably likely that the GC queue will do nothing because the replica
			// needs to split instead, but better to err on the side of queueing too
			// frequently. Blocking Raft snapshots for too long can wedge a cluster,
			// and if the replica does need to be GC'd, this might be the only code
			// path that notices in a timely fashion.
			//
			// We're careful to avoid starving out other replicas in the GC queue by
			// queueing at a low priority unless we can prove that the range is
			// inactive and thus unlikely to be about to process a split.
			gcPriority := replicaGCPriorityDefault
			if inactive(exReplica) {
				gcPriority = replicaGCPriorityCandidate
			}
			if _, err := s.replicaGCQueue.Add(exReplica, gcPriority); err != nil {
				log.Errorf(ctx, "%s: unable to add replica to GC queue: %s", exReplica, err)
			} else {
				msg += "; initiated GC:"
			}
		}
		return nil, errors.Errorf("%s %v (incoming %v)", msg, exReplica, snapHeader.State.Desc.RSpan()) // exReplica can be nil
	}

	placeholder := &ReplicaPlaceholder{
		rangeDesc: desc,
	}
	return placeholder, nil
}

// receiveSnapshot receives an incoming snapshot via a pre-opened GRPC stream.
func (s *Store) receiveSnapshot(
	ctx context.Context, header *SnapshotRequest_Header, stream incomingSnapshotStream,
) error {
	cleanup, rejectionMsg, err := s.reserveSnapshot(ctx, header)
	if err != nil {
		return err
	}
	if cleanup == nil {
		return stream.Send(&SnapshotResponse{
			Status:  SnapshotResponse_DECLINED,
			Message: rejectionMsg,
		})
	}
	defer cleanup()

	// Check to see if the snapshot can be applied but don't attempt to add
	// a placeholder here, because we're not holding the replica's raftMu.
	// We'll perform this check again later after receiving the rest of the
	// snapshot data - this is purely an optimization to prevent downloading
	// a snapshot that we know we won't be able to apply.
	if _, err := s.canApplySnapshot(ctx, header, false /* authoritative */); err != nil {
		return sendSnapshotError(stream,
			errors.Wrapf(err, "%s,r%d: cannot apply snapshot", s, header.State.Desc.RangeID),
		)
	}

	// Determine which snapshot strategy the sender is using to send this
	// snapshot. If we don't know how to handle the specified strategy, return
	// an error.
	var ss snapshotStrategy
	switch header.Strategy {
	case SnapshotRequest_KV_BATCH:
		ss = &kvBatchSnapshotStrategy{
			raftCfg: &s.cfg.RaftConfig,
		}
	default:
		return sendSnapshotError(stream,
			errors.Errorf("%s,r%d: unknown snapshot strategy: %s",
				s, header.State.Desc.RangeID, header.Strategy),
		)
	}

	if err := stream.Send(&SnapshotResponse{Status: SnapshotResponse_ACCEPTED}); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "accepted snapshot reservation for r%d", header.State.Desc.RangeID)
	}

	inSnap, err := ss.Receive(ctx, stream, *header)
	if err != nil {
		return err
	}
	if err := s.processRaftSnapshotRequest(ctx, header, inSnap); err != nil {
		return sendSnapshotError(stream, errors.Wrap(err.GoError(), "failed to apply snapshot"))
	}

	return stream.Send(&SnapshotResponse{Status: SnapshotResponse_APPLIED})
}

func sendSnapshotError(stream incomingSnapshotStream, err error) error {
	return stream.Send(&SnapshotResponse{
		Status:  SnapshotResponse_ERROR,
		Message: err.Error(),
	})
}

// SnapshotStorePool narrows StorePool to make sendSnapshot easier to test.
type SnapshotStorePool interface {
	throttle(reason throttleReason, toStoreID roachpb.StoreID)
}

var rebalanceSnapshotRate = settings.RegisterByteSizeSetting(
	"kv.snapshot_rebalance.max_rate",
	"the rate limit (bytes/sec) to use for rebalance snapshots",
	envutil.EnvOrDefaultBytes("COCKROACH_PREEMPTIVE_SNAPSHOT_RATE", 2<<20),
)
var recoverySnapshotRate = settings.RegisterByteSizeSetting(
	"kv.snapshot_recovery.max_rate",
	"the rate limit (bytes/sec) to use for recovery snapshots",
	envutil.EnvOrDefaultBytes("COCKROACH_RAFT_SNAPSHOT_RATE", 8<<20),
)

func snapshotRateLimit(
	st *cluster.Settings, priority SnapshotRequest_Priority,
) (rate.Limit, error) {
	switch priority {
	case SnapshotRequest_RECOVERY:
		return rate.Limit(recoverySnapshotRate.Get(&st.SV)), nil
	case SnapshotRequest_REBALANCE:
		return rate.Limit(rebalanceSnapshotRate.Get(&st.SV)), nil
	default:
		return 0, errors.Errorf("unknown snapshot priority: %s", priority)
	}
}

type errMustRetrySnapshotDueToTruncation struct {
	index, term uint64
}

func (e *errMustRetrySnapshotDueToTruncation) Error() string {
	return fmt.Sprintf(
		"log truncation during snapshot removed sideloaded SSTable at index %d, term %d",
		e.index, e.term,
	)
}

// sendSnapshot sends an outgoing snapshot via a pre-opened GRPC stream.
func sendSnapshot(
	ctx context.Context,
	raftCfg *base.RaftConfig,
	st *cluster.Settings,
	stream outgoingSnapshotStream,
	storePool SnapshotStorePool,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() engine.Batch,
	sent func(),
) error {
	start := timeutil.Now()
	to := header.RaftMessageRequest.ToReplica
	if err := stream.Send(&SnapshotRequest{Header: &header}); err != nil {
		return err
	}
	// Wait until we get a response from the server.
	resp, err := stream.Recv()
	if err != nil {
		storePool.throttle(throttleFailed, to.StoreID)
		return err
	}
	switch resp.Status {
	case SnapshotResponse_DECLINED:
		if header.CanDecline {
			storePool.throttle(throttleDeclined, to.StoreID)
			declinedMsg := "reservation rejected"
			if len(resp.Message) > 0 {
				declinedMsg = resp.Message
			}
			return &benignError{errors.Errorf("%s: remote declined %s: %s", to, snap, declinedMsg)}
		}
		storePool.throttle(throttleFailed, to.StoreID)
		return errors.Errorf("%s: programming error: remote declined required %s: %s",
			to, snap, resp.Message)
	case SnapshotResponse_ERROR:
		storePool.throttle(throttleFailed, to.StoreID)
		return errors.Errorf("%s: remote couldn't accept %s with error: %s",
			to, snap, resp.Message)
	case SnapshotResponse_ACCEPTED:
	// This is the response we're expecting. Continue with snapshot sending.
	default:
		storePool.throttle(throttleFailed, to.StoreID)
		return errors.Errorf("%s: server sent an invalid status while negotiating %s: %s",
			to, snap, resp.Status)
	}

	log.Infof(ctx, "sending %s", snap)

	// The size of batches to send. This is the granularity of rate limiting.
	const batchSize = 256 << 10 // 256 KB
	targetRate, err := snapshotRateLimit(st, header.Priority)
	if err != nil {
		return errors.Wrapf(err, "%s", to)
	}

	// Convert the bytes/sec rate limit to batches/sec.
	//
	// TODO(peter): Using bytes/sec for rate limiting seems more natural but has
	// practical difficulties. We either need to use a very large burst size
	// which seems to disable the rate limiting, or call WaitN in smaller than
	// burst size chunks which caused excessive slowness in testing. Would be
	// nice to figure this out, but the batches/sec rate limit works for now.
	limiter := rate.NewLimiter(targetRate/batchSize, 1 /* burst size */)

	// Create a snapshotStrategy based on the desired snapshot strategy.
	var ss snapshotStrategy
	switch header.Strategy {
	case SnapshotRequest_KV_BATCH:
		ss = &kvBatchSnapshotStrategy{
			raftCfg:   raftCfg,
			batchSize: batchSize,
			limiter:   limiter,
			newBatch:  newBatch,
		}
	default:
		log.Fatalf(ctx, "unknown snapshot strategy: %s", header.Strategy)
	}

	if err := ss.Send(ctx, stream, header, snap); err != nil {
		return err
	}

	// Notify the sent callback before the final snapshot request is sent so that
	// the snapshots generated metric gets incremented before the snapshot is
	// applied.
	sent()
	if err := stream.Send(&SnapshotRequest{Final: true}); err != nil {
		return err
	}
	log.Infof(ctx, "streamed snapshot to %s: %s, rate-limit: %s/sec, %.2fs",
		to, ss.Status(), humanizeutil.IBytes(int64(targetRate)),
		timeutil.Since(start).Seconds())

	resp, err = stream.Recv()
	if err != nil {
		return errors.Wrapf(err, "%s: remote failed to apply snapshot", to)
	}
	// NB: wait for EOF which ensures that all processing on the server side has
	// completed (such as defers that might be run after the previous message was
	// received).
	if unexpectedResp, err := stream.Recv(); err != io.EOF {
		return errors.Errorf("%s: expected EOF, got resp=%v err=%v", to, unexpectedResp, err)
	}
	switch resp.Status {
	case SnapshotResponse_ERROR:
		return errors.Errorf("%s: remote failed to apply snapshot for reason %s", to, resp.Message)
	case SnapshotResponse_APPLIED:
		return nil
	default:
		return errors.Errorf("%s: server sent an invalid status during finalization: %s",
			to, resp.Status)
	}
}
