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

	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/time/rate"

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
				SnapUUID:   snapUUID,
				Batches:    batches,
				LogEntries: logEntries,
				State:      &header.State,
				snapType:   snapTypeRaft,
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
func (s *Store) canApplySnapshot(
	ctx context.Context, snapHeader *SnapshotRequest_Header,
) (*ReplicaPlaceholder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canApplySnapshotLocked(ctx, snapHeader)
}

func (s *Store) canApplySnapshotLocked(
	ctx context.Context, snapHeader *SnapshotRequest_Header,
) (*ReplicaPlaceholder, error) {
	desc := *snapHeader.State.Desc
	if v, ok := s.mu.replicas.Load(int64(desc.RangeID)); ok && (*Replica)(v).IsInitialized() {
		// We have an initialized replica. Preemptive snapshots can be applied with
		// no further checks if they do not widen the existing replica. Raft
		// snapshots can be applied with no further checks even if they widen the
		// existing replica—we can't reject them at this point—but see the comments
		// in Replica.maybeAcquireSnapshotMergeLock for how this is made safe.
		existingDesc := (*Replica)(v).Desc()
		if !snapHeader.IsPreemptive() || !existingDesc.EndKey.Less(desc.EndKey) {
			return nil, nil
		}

		// We have a preemptive snapshot that widens an existing replica. Proceed
		// by checking the keyspace covered by the snapshot but not the existing
		// replica.
		desc.StartKey = existingDesc.EndKey
	}

	// We don't have the range, or we have an uninitialized placeholder, or the
	// existing range is less wide. Will we be able to create/initialize it?
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
		return nil, errors.Errorf("%s %v", msg, exReplica) // exReplica can be nil
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
	if _, err := s.canApplySnapshot(ctx, header); err != nil {
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
