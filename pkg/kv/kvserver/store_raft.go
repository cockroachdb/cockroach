// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var (
	logRaftRecvQueueFullEvery = log.Every(1 * time.Second)
	logRaftSendQueueFullEvery = log.Every(1 * time.Second)
)

type raftRequestInfo struct {
	req        *kvserverpb.RaftMessageRequest
	size       int64 // size of req in bytes
	respStream RaftMessageResponseStream
}

type raftReceiveQueue struct {
	mu struct { // not to be locked directly
		destroyed bool
		syncutil.Mutex
		infos         []raftRequestInfo
		enforceMaxLen bool
	}
	maxLen int
	acc    mon.BoundAccount
}

// Len returns the number of requests in the queue.
func (q *raftReceiveQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.mu.infos)
}

// Drain moves the stored requests out of the queue, returning them to
// the caller. Returns true if the returned slice was not empty.
func (q *raftReceiveQueue) Drain() ([]raftRequestInfo, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.drainLocked()
}

func (q *raftReceiveQueue) drainLocked() ([]raftRequestInfo, bool) {
	if len(q.mu.infos) == 0 {
		return nil, false
	}
	infos := q.mu.infos
	q.mu.infos = nil
	q.acc.Clear(context.Background())
	return infos, true
}

func (q *raftReceiveQueue) Delete() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.drainLocked()
	if err := q.acc.ResizeTo(context.Background(), 0); err != nil {
		panic(err) // ResizeTo(., 0) always returns nil
	}
	q.mu.destroyed = true
}

// Recycle makes a slice that the caller knows will no longer be accessed
// available for reuse.
func (q *raftReceiveQueue) Recycle(processed []raftRequestInfo) {
	if cap(processed) > 4 {
		return // cap recycled slice lengths
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.mu.infos == nil {
		for i := range processed {
			processed[i] = raftRequestInfo{}
		}
		q.mu.infos = processed[:0]
	}
}

func (q *raftReceiveQueue) Append(
	req *kvserverpb.RaftMessageRequest, s RaftMessageResponseStream,
) (shouldQueue bool, size int64, appended bool) {
	size = int64(req.Size())
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.mu.destroyed || (q.mu.enforceMaxLen && len(q.mu.infos) >= q.maxLen) {
		return false, size, false
	}
	if q.acc.Grow(context.Background(), size) != nil {
		return false, size, false
	}
	q.mu.infos = append(q.mu.infos, raftRequestInfo{
		req:        req,
		respStream: s,
		size:       size,
	})
	// The operation that enqueues the first message will
	// be put in charge of triggering a drain of the queue.
	return len(q.mu.infos) == 1, size, true
}

func (q *raftReceiveQueue) SetEnforceMaxLen(enforceMaxLen bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.enforceMaxLen = enforceMaxLen
}

func (q *raftReceiveQueue) getEnforceMaxLenForTesting() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.mu.enforceMaxLen
}

type raftReceiveQueues struct {
	mon           *mon.BytesMonitor
	m             syncutil.Map[roachpb.RangeID, raftReceiveQueue]
	enforceMaxLen atomic.Bool
}

func (qs *raftReceiveQueues) Load(rangeID roachpb.RangeID) (*raftReceiveQueue, bool) {
	return qs.m.Load(rangeID)
}

func (qs *raftReceiveQueues) LoadOrCreate(
	rangeID roachpb.RangeID, maxLen int,
) (_ *raftReceiveQueue, loaded bool) {
	if q, ok := qs.Load(rangeID); ok {
		return q, ok // fast path
	}
	q := &raftReceiveQueue{maxLen: maxLen}
	q.acc.Init(context.Background(), qs.mon)
	q, loaded = qs.m.LoadOrStore(rangeID, q)
	if loaded {
		return q, true
	}
	// The sampling of enforceMaxLen can be concurrent with a call to
	// SetEnforceMaxLen. We can sample a stale value, then SetEnforceMaxLen can
	// fully execute, and then set the stale value here. Since
	// qs.SetEnforceMaxLen sets the latest value first, before iterating over
	// the map, it suffices to check after setting the value here that it has
	// not changed. There are two cases:
	//
	// - Has changed: it is possible that our call to q.SetEnforceMaxLen
	//   occurred after the corresponding call in qs.SetEnforceMaxLen, so we
	//   have to loop back and correct it.
	//
	// - Has not changed: there may be a concurrent call to
	//   qs.SetEnforceMaxLen with a different bool parameter, but it has not
	//   yet updated qs.enforceMaxLen. Which is fine -- that call will iterate
	//   over the map and do what is necessary.
	for {
		enforceBefore := qs.enforceMaxLen.Load()
		q.SetEnforceMaxLen(enforceBefore)
		enforceAfter := qs.enforceMaxLen.Load()
		if enforceAfter == enforceBefore {
			break
		}
	}
	return q, false
}

// Delete drains the queue and marks it as deleted. Future Appends
// will result in appended=false.
func (qs *raftReceiveQueues) Delete(rangeID roachpb.RangeID) {
	if q, ok := qs.Load(rangeID); ok {
		q.Delete()
		qs.m.Delete(rangeID)
	}
}

// SetEnforceMaxLen specifies the latest state of whether maxLen needs to be
// enforced or not. Calls to this method are serialized by the caller.
func (qs *raftReceiveQueues) SetEnforceMaxLen(enforceMaxLen bool) {
	// Store the latest value first. A concurrent creation of raftReceiveQueue
	// can race with this method -- see how this is handled in LoadOrCreate.
	qs.enforceMaxLen.Store(enforceMaxLen)
	qs.m.Range(func(_ roachpb.RangeID, q *raftReceiveQueue) bool {
		q.SetEnforceMaxLen(enforceMaxLen)
		return true
	})
}

// HandleDelegatedSnapshot reads the incoming delegated snapshot message and
// throttles sending snapshots before passing the request to the sender replica.
func (s *Store) HandleDelegatedSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest,
) *kvserverpb.DelegateSnapshotResponse {
	ctx = s.AnnotateCtx(ctx)

	if fn := s.cfg.TestingKnobs.SendSnapshot; fn != nil {
		fn(req)
	}

	sp := tracing.SpanFromContext(ctx)

	// This can happen if the delegate doesn't know about the range yet. Return an
	// error immediately.
	sender, err := s.GetReplica(req.RangeID)
	if err != nil {
		return &kvserverpb.DelegateSnapshotResponse{
			Status:         kvserverpb.DelegateSnapshotResponse_ERROR,
			EncodedError:   errors.EncodeError(context.Background(), err),
			CollectedSpans: sp.GetConfiguredRecording(),
		}
	}

	// Pass the request to the sender replica.
	msgAppResp, err := sender.followerSendSnapshot(ctx, req.RecipientReplica, req)
	if err != nil {
		// If an error occurred during snapshot sending, send an error response.
		return &kvserverpb.DelegateSnapshotResponse{
			Status:         kvserverpb.DelegateSnapshotResponse_ERROR,
			EncodedError:   errors.EncodeError(context.Background(), err),
			CollectedSpans: sp.GetConfiguredRecording(),
		}
	}

	return &kvserverpb.DelegateSnapshotResponse{
		Status:         kvserverpb.DelegateSnapshotResponse_APPLIED,
		CollectedSpans: sp.GetConfiguredRecording(),
		MsgAppResp:     msgAppResp,
	}
}

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header, stream SnapshotResponseStream,
) error {
	if fn := s.cfg.TestingKnobs.HandleSnapshotDone; fn != nil {
		defer fn()
	}
	ctx = s.AnnotateCtx(ctx)
	const name = "storage.Store: handle snapshot"
	return s.stopper.RunTaskWithErr(ctx, name, func(ctx context.Context) error {
		s.metrics.RaftRcvdMessages[raftpb.MsgSnap].Inc(1)

		err := s.receiveSnapshot(ctx, header, stream)
		if err != nil && ctx.Err() != nil {
			// Log trace of incoming snapshot on context cancellation (e.g.
			// times out or caller goes away).
			log.Infof(ctx, "incoming snapshot stream failed with error: %v\ntrace:\n%v",
				err, tracing.SpanFromContext(ctx).GetConfiguredRecording())
		}
		return err
	})
}

func (s *Store) uncoalesceBeats(
	ctx context.Context,
	beats []kvserverpb.RaftHeartbeat,
	fromReplica, toReplica roachpb.ReplicaDescriptor,
	msgT raftpb.MessageType,
	respStream RaftMessageResponseStream,
) {
	if len(beats) == 0 {
		return
	}
	if log.V(4) {
		log.Infof(ctx, "uncoalescing %d beats of type %v: %+v", len(beats), msgT, beats)
	}
	beatReqs := make([]kvserverpb.RaftMessageRequest, len(beats))
	batch := s.scheduler.NewEnqueueBatch()
	defer batch.Close()
	for i, beat := range beats {
		msg := raftpb.Message{
			Type:   msgT,
			From:   raftpb.PeerID(beat.FromReplicaID),
			To:     raftpb.PeerID(beat.ToReplicaID),
			Term:   uint64(beat.Term),
			Commit: uint64(beat.Commit),
		}
		beatReqs[i] = kvserverpb.RaftMessageRequest{
			RangeID: beat.RangeID,
			FromReplica: roachpb.ReplicaDescriptor{
				NodeID:    fromReplica.NodeID,
				StoreID:   fromReplica.StoreID,
				ReplicaID: beat.FromReplicaID,
			},
			ToReplica: roachpb.ReplicaDescriptor{
				NodeID:    toReplica.NodeID,
				StoreID:   toReplica.StoreID,
				ReplicaID: beat.ToReplicaID,
			},
			Message:                   msg,
			Quiesce:                   beat.Quiesce,
			LaggingFollowersOnQuiesce: beat.LaggingFollowersOnQuiesce,
		}
		if log.V(4) {
			log.Infof(ctx, "uncoalesced beat: %+v", beatReqs[i])
		}

		enqueue := s.HandleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream)
		if enqueue {
			batch.Add(beat.RangeID)
		}
	}
	s.scheduler.EnqueueRaftRequests(batch)
}

// HandleRaftRequest dispatches a raft message to the appropriate Replica. It
// requires that s.mu is not held.
func (s *Store) HandleRaftRequest(
	ctx context.Context, req *kvserverpb.RaftMessageRequest, respStream RaftMessageResponseStream,
) *kvpb.Error {
	comparisonResult := s.getLocalityComparison(ctx, req.FromReplica.NodeID, req.ToReplica.NodeID)
	s.metrics.updateCrossLocalityMetricsOnIncomingRaftMsg(comparisonResult, int64(req.Size()))
	// NB: unlike the other two IncomingRaftMessageHandler methods implemented by
	// Store, this one doesn't need to directly run through a Stopper task because
	// it delegates all work through a raftScheduler, whose workers' lifetimes are
	// already tied to the Store's Stopper.
	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		if req.RangeID != 0 {
			log.Fatalf(ctx, "coalesced heartbeats must have rangeID == 0")
		}
		s.uncoalesceBeats(ctx, req.Heartbeats, req.FromReplica, req.ToReplica, raftpb.MsgHeartbeat, respStream)
		s.uncoalesceBeats(ctx, req.HeartbeatResps, req.FromReplica, req.ToReplica, raftpb.MsgHeartbeatResp, respStream)
		return nil
	}
	enqueue := s.HandleRaftUncoalescedRequest(ctx, req, respStream)
	if enqueue {
		s.scheduler.EnqueueRaftRequest(req.RangeID)
	}
	return nil
}

// HandleRaftUncoalescedRequest dispatches a raft message to the appropriate
// Replica. The method returns whether the Range needs to be enqueued in the
// Raft scheduler. It requires that s.mu is not held.
func (s *Store) HandleRaftUncoalescedRequest(
	ctx context.Context, req *kvserverpb.RaftMessageRequest, respStream RaftMessageResponseStream,
) (enqueue bool) {
	if len(req.Heartbeats)+len(req.HeartbeatResps) > 0 {
		log.Fatalf(ctx, "HandleRaftUncoalescedRequest cannot be given coalesced heartbeats or heartbeat responses, received %s", req)
	}
	// HandleRaftRequest is called on locally uncoalesced heartbeats (which are
	// not sent over the network if the environment variable is set) so do not
	// count them.
	s.metrics.RaftRcvdMessages[req.Message.Type].Inc(1)

	// NB: add a buffer for extra messages, to allow heartbeats getting through
	// even if MsgApp quota is maxed out by the sender.
	q, _ := s.raftRecvQueues.LoadOrCreate(req.RangeID,
		s.cfg.RaftMaxInflightMsgs+replicaQueueExtraSize)
	enqueue, size, appended := q.Append(req, respStream)
	if !appended {
		// TODO(peter): Return an error indicating the request was dropped. Note
		// that dropping the request is safe. Raft will retry.
		s.metrics.RaftRcvdDropped.Inc(1)
		s.metrics.RaftRcvdDroppedBytes.Inc(size)
		if logRaftRecvQueueFullEvery.ShouldLog() {
			log.Warningf(ctx, "raft receive queue for r%d is full", req.RangeID)
		}
		return false
	}
	return enqueue
}

// HandleRaftRequestSent is called to capture outgoing Raft messages just prior
// to their transmission to the raftSendQueue. Note that the message might not
// be successfully queued if it gets dropped by SendAsync due to a full outgoing
// queue. Currently, this is only used for metrics update which is why it only
// takes specific properties of the request as arguments.
func (s *Store) HandleRaftRequestSent(
	ctx context.Context, fromNodeID roachpb.NodeID, toNodeID roachpb.NodeID, msgSize int64,
) {
	comparisonResult := s.getLocalityComparison(ctx, fromNodeID, toNodeID)
	s.metrics.updateCrossLocalityMetricsOnOutgoingRaftMsg(comparisonResult, msgSize)
}

// withReplicaForRequest calls the supplied function with the (lazily
// initialized) Replica specified in the request. The replica passed to
// the function will have its Replica.raftMu locked.
func (s *Store) withReplicaForRequest(
	ctx context.Context,
	req *kvserverpb.RaftMessageRequest,
	f func(context.Context, *Replica) *kvpb.Error,
) *kvpb.Error {
	// Lazily create the replica.
	r, _, err := s.getOrCreateReplica(
		ctx,
		req.RangeID,
		req.ToReplica.ReplicaID,
		&req.FromReplica,
	)
	if err != nil {
		return kvpb.NewError(err)
	}
	defer r.raftMu.Unlock()
	r.setLastReplicaDescriptors(req)
	return f(ctx, r)
}

// processRaftRequestWithReplica processes the (non-snapshot) Raft request on
// the specified replica. Notably, it does not handle updates to the Raft Ready
// state; callers will probably want to handle this themselves at some point.
func (s *Store) processRaftRequestWithReplica(
	ctx context.Context, r *Replica, req *kvserverpb.RaftMessageRequest,
) *kvpb.Error {
	// Record the CPU time processing the request for this replica. This is
	// recorded regardless of errors that are encountered.
	defer r.MeasureRaftCPUNanos(grunning.Time())

	if verboseRaftLoggingEnabled() {
		log.Infof(ctx, "incoming raft message:\n%s", raft.DescribeMessage(req.Message, raftEntryFormatter))
	}

	if req.Message.Type == raftpb.MsgSnap {
		log.Fatalf(ctx, "unexpected snapshot: %+v", req)
	}

	if req.Quiesce {
		if req.Message.Type != raftpb.MsgHeartbeat {
			log.Fatalf(ctx, "unexpected quiesce: %+v", req)
		}
		if r.maybeQuiesceOnNotify(
			ctx,
			req.Message,
			laggingReplicaSet(req.LaggingFollowersOnQuiesce),
		) {
			return nil
		}
	}

	if req.ToReplica.ReplicaID == 0 {
		log.VEventf(ctx, 1, "refusing incoming Raft message %s from %+v to %+v",
			req.Message.Type, req.FromReplica, req.ToReplica)
		return kvpb.NewErrorf(
			"cannot recreate replica that is not a member of its range (StoreID %s not found in r%d)",
			r.store.StoreID(), req.RangeID,
		)
	}

	drop := maybeDropMsgApp(ctx, (*replicaMsgAppDropper)(r), &req.Message, req.RangeStartKey)
	if !drop {
		if err := r.stepRaftGroupRaftMuLocked(req); err != nil {
			return kvpb.NewError(err)
		}
	}
	return nil
}

// processRaftSnapshotRequest processes the incoming snapshot Raft request on
// the request's specified replica. The function makes sure to handle any
// updated Raft Ready state. It also adds and later removes the (potentially)
// necessary placeholder to protect against concurrent access to the keyspace
// encompassed by the snapshot but not yet guarded by the replica.
//
// If (and only if) no error is returned, the placeholder (if any) in inSnap
// will have been removed.
func (s *Store) processRaftSnapshotRequest(
	ctx context.Context, snapHeader *kvserverpb.SnapshotRequest_Header, inSnap IncomingSnapshot,
) (*raftpb.Message, *kvpb.Error) {
	var msgAppResp *raftpb.Message
	pErr := s.withReplicaForRequest(ctx, &snapHeader.RaftMessageRequest, func(
		ctx context.Context, r *Replica,
	) (pErr *kvpb.Error) {
		ctx = r.AnnotateCtx(ctx)
		if snapHeader.RaftMessageRequest.Message.Type != raftpb.MsgSnap {
			log.Fatalf(ctx, "expected snapshot: %+v", snapHeader.RaftMessageRequest)
		}

		typ := removePlaceholderFailed
		defer func() {
			// In the typical case, handleRaftReadyRaftMuLocked calls through to
			// applySnapshot which will apply the snapshot and also converts the
			// placeholder entry (if any) to the now-initialized replica. However we
			// may also error out below, or raft may also ignore the snapshot, and so
			// the placeholder would remain.
			//
			// NB: it's unclear in which case we could actually get raft to ignore a
			// snapshot attached to a placeholder. A placeholder existing implies that
			// the snapshot is targeting an uninitialized replica. The only known reason
			// for raft to ignore a snapshot is if it doesn't move the applied index
			// forward, but an uninitialized replica's applied index is zero (and a
			// snapshot's is at least RaftInitialLogIndex).
			if inSnap.placeholder != nil {
				if _, err := s.removePlaceholder(ctx, inSnap.placeholder, typ); err != nil {
					log.Fatalf(ctx, "unable to remove placeholder: %s", err)
				}
			}
		}()

		if snapHeader.RaftMessageRequest.Message.From == snapHeader.RaftMessageRequest.Message.To {
			// This is a special case exercised during recovery from loss of quorum.
			// In this case, a forged snapshot will be sent to the replica and will
			// hit this code path (if we make up a non-existent follower, Raft will
			// drop the message, hence we are forced to make the receiver the sender).
			//
			// Unfortunately, at the time of writing, Raft assumes that a snapshot
			// is always received from the leader (of the given term), which plays
			// poorly with these forged snapshots. However, a zero sender works just
			// fine as the value zero represents "no known leader".
			//
			// We prefer not to introduce a zero origin of the message as throughout
			// our code we rely on it being present. Instead, we reset the origin
			// that raft looks at just before handing the message off.
			snapHeader.RaftMessageRequest.Message.From = 0
		}
		// NB: we cannot get errRemoved here because we're promised by
		// withReplicaForRequest that this replica is not currently being removed
		// and we've been holding the raftMu the entire time.
		if err := r.stepRaftGroupRaftMuLocked(&snapHeader.RaftMessageRequest); err != nil {
			return kvpb.NewError(err)
		}

		// We've handed the snapshot to Raft, which will typically apply it (in
		// which case the placeholder, if any, is removed by the time
		// handleRaftReadyRaftMuLocked returns. We handle the other case in a
		// defer() above. Note that we could infer when the placeholder should still
		// be there based on `stats.snap.applied`	but it is a questionable use of
		// stats and more susceptible to bugs.
		typ = removePlaceholderDropped
		stats, err := r.handleRaftReadyRaftMuLocked(ctx, inSnap)
		maybeFatalOnRaftReadyErr(ctx, err)
		if !stats.snap.applied {
			// This line would be hit if a snapshot was sent when it isn't necessary
			// (i.e. follower was able to catch up via the log in the interim) or when
			// multiple snapshots raced (as is possible when raft leadership changes
			// and both the old and new leaders send snapshots).
			log.Infof(ctx, "ignored stale snapshot at index %d", snapHeader.RaftMessageRequest.Message.Snapshot.Metadata.Index)
			s.metrics.RangeSnapshotRecvUnusable.Inc(1)
		}
		// If the snapshot was applied and acked with an MsgAppResp, return that
		// message up the stack. We're using msgAppRespCh as a shortcut to avoid
		// plumbing return parameters through an additional few layers of raft
		// handling.
		//
		// NB: in practice there's always an MsgAppResp here, but it is better not
		// to rely on what is essentially discretionary raft behavior.
		select {
		case msg := <-inSnap.msgAppRespCh:
			msgAppResp = &msg
		default:
		}
		return nil
	})
	if pErr != nil {
		return nil, pErr
	}
	return msgAppResp, nil
}

// HandleRaftResponse implements the IncomingRaftMessageHandler interface. Per
// the interface specification, an error is returned if and only if the
// underlying Raft connection should be closed. It requires that s.mu is not
// held.
func (s *Store) HandleRaftResponse(
	ctx context.Context, resp *kvserverpb.RaftMessageResponse,
) error {
	ctx = s.AnnotateCtx(ctx)
	const name = "storage.Store: handle raft response"
	return s.stopper.RunTaskWithErr(ctx, name, func(ctx context.Context) error {
		repl, replErr := s.GetReplica(resp.RangeID)
		if replErr == nil {
			// Best-effort context annotation of replica.
			ctx = repl.AnnotateCtx(ctx)
		}
		switch val := resp.Union.GetValue().(type) {
		case *kvpb.Error:
			switch tErr := val.GetDetail().(type) {
			case *kvpb.ReplicaTooOldError:
				if replErr != nil {
					// RangeNotFoundErrors are expected here; nothing else is.
					if !errors.HasType(replErr, (*kvpb.RangeNotFoundError)(nil)) {
						log.Errorf(ctx, "%v", replErr)
					}
					return nil
				}

				// Grab the raftMu in addition to the replica mu because
				// cancelFailedProposalsLocked below requires it.
				repl.raftMu.Lock()
				defer repl.raftMu.Unlock()
				repl.mu.Lock()

				// If the replica ID in the error does not match then we know
				// that the replica has been removed and re-added quickly. In
				// that case, we don't want to add it to the replicaGCQueue.
				// If the replica is not alive then we also should ignore this error.
				if tErr.ReplicaID != repl.replicaID ||
					!repl.mu.destroyStatus.IsAlive() ||
					// Ignore if we want to test the replicaGC queue.
					s.TestingKnobs().DisableEagerReplicaRemoval {
					repl.mu.Unlock()
					return nil
				}

				// The replica will be garbage collected soon (we are sure
				// since our replicaID is definitely too old), but in the meantime we
				// already want to bounce all traffic from it. Note that the replica
				// could be re-added with a higher replicaID, but we want to clear the
				// replica's data before that happens.
				if log.V(1) {
					log.Infof(ctx, "setting local replica to destroyed due to ReplicaTooOld error")
				}

				repl.mu.Unlock()
				nextReplicaID := tErr.ReplicaID + 1
				return s.removeReplicaRaftMuLocked(ctx, repl, nextReplicaID, RemoveOptions{
					DestroyData: true,
				})
			case *kvpb.RaftGroupDeletedError:
				if replErr != nil {
					// RangeNotFoundErrors are expected here; nothing else is.
					if !errors.HasType(replErr, (*kvpb.RangeNotFoundError)(nil)) {
						log.Errorf(ctx, "%v", replErr)
					}
					return nil
				}

				// If the replica is talking to a replica that's been deleted, it must be
				// out of date. While this may just mean it's slightly behind, it can
				// also mean that it is so far behind it no longer knows where any of the
				// other replicas are (#23994). Add it to the replica GC queue to do a
				// proper check.
				s.replicaGCQueue.AddAsync(ctx, repl, replicaGCPriorityDefault)
			case *kvpb.StoreNotFoundError:
				log.Warningf(ctx, "raft error: node %d claims to not contain store %d for replica %s: %s",
					resp.FromReplica.NodeID, resp.FromReplica.StoreID, resp.FromReplica, val)
				return val.GetDetail() // close Raft connection
			default:
				log.Warningf(ctx, "got error from r%d, replica %s: %s",
					resp.RangeID, resp.FromReplica, val)
			}
		default:
			log.Warningf(ctx, "got unknown raft response type %T from replica %s: %s", val, resp.FromReplica, val)
		}
		return nil
	})
}

// enqueueRaftUpdateCheck asynchronously registers the given range ID to be
// checked for raft updates when the processRaft goroutine is idle.
func (s *Store) enqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.scheduler.EnqueueRaftReady(rangeID)
}

// TODO(tbg): rename this to processRecvQueue.
func (s *Store) processRequestQueue(ctx context.Context, rangeID roachpb.RangeID) bool {
	q, ok := s.raftRecvQueues.Load(rangeID)
	if !ok {
		return false
	}
	infos, ok := q.Drain()
	if !ok {
		return false
	}
	defer q.Recycle(infos)

	var hadError bool
	for i := range infos {
		info := &infos[i]
		if pErr := s.withReplicaForRequest(
			ctx, info.req, func(_ context.Context, r *Replica) *kvpb.Error {
				return s.processRaftRequestWithReplica(r.raftCtx, r, info.req)
			},
		); pErr != nil {
			hadError = true
			if err := info.respStream.Send(newRaftMessageResponse(info.req, pErr)); err != nil {
				// Seems excessive to log this on every occurrence as the other side
				// might have closed.
				log.VEventf(ctx, 1, "error sending error: %s", err)
			}
		}
		s.metrics.RaftRcvdSteppedBytes.Inc(info.size)
		infos[i] = raftRequestInfo{}
	}

	if hadError {
		// If we're unable to process a request, consider dropping the request queue
		// to free up space in the map.
		// This is relevant if requests failed because the target replica could not
		// be created (for example due to the Raft tombstone). The particular code
		// here takes into account that we don't want to drop the queue if there
		// are other messages waiting on it, or if the target replica exists. Raft
		// tolerates the occasional dropped message, but our unit tests are less
		// forgiving.
		//
		// See https://github.com/cockroachdb/cockroach/issues/30951#issuecomment-428010411.
		//
		// TODO(tbg): for adding actual memory accounting, we need more clarity about
		// the contract. For example, it would be a problem if the queue got deleted
		// (as a result of the replica getting deleted) but then getting recreated errantly.
		// In that case, we would "permanently" leak an allocation, which over time could
		// eat up the budget. We must ensure, essentially, that we create a queue only
		// when the replica is alive (according to its destroyStatus) and ensure it is
		// destroyed once that changes.
		if _, exists := s.mu.replicasByRangeID.Load(rangeID); !exists && q.Len() == 0 {
			s.raftRecvQueues.Delete(rangeID)
		}
	}

	// NB: Even if we had errors and the corresponding replica no longer
	// exists, returning true here won't cause a new, uninitialized replica
	// to be created in processReady().
	return true // ready
}

func (s *Store) processReady(rangeID roachpb.RangeID) {
	r, ok := s.mu.replicasByRangeID.Load(rangeID)
	if !ok {
		return
	}

	// Record the CPU time processing the request for this replica. This is
	// recorded regardless of errors that are encountered.
	defer r.MeasureRaftCPUNanos(grunning.Time())

	ctx := r.raftCtx
	stats, err := r.handleRaftReady(ctx, noSnap)
	maybeFatalOnRaftReadyErr(ctx, err)
	elapsed := stats.tEnd.Sub(stats.tBegin)
	s.metrics.RaftWorkingDurationNanos.Inc(elapsed.Nanoseconds())
	s.metrics.RaftHandleReadyLatency.RecordValue(elapsed.Nanoseconds())
	// Warn if Raft processing took too long. We use the same duration as we
	// use for warning about excessive raft mutex lock hold times. Long
	// processing time means we'll have starved local replicas of ticks and
	// remote replicas will likely start campaigning.
	if elapsed >= defaultReplicaRaftMuWarnThreshold {
		log.Infof(ctx, "%s; node might be overloaded", stats)
	}
}

func (s *Store) processTick(_ context.Context, rangeID roachpb.RangeID) bool {
	r, ok := s.mu.replicasByRangeID.Load(rangeID)
	if !ok {
		return false
	}

	livenessMap, _ := s.livenessMap.Load().(livenesspb.IsLiveMap)
	ioThresholds := s.ioThresholds.Current()

	// Record the CPU time processing the request for this replica. This is
	// recorded regardless of errors that are encountered.
	defer r.MeasureRaftCPUNanos(grunning.Time())
	start := timeutil.Now()
	ctx := r.raftCtx

	exists, err := r.tick(ctx, livenessMap, ioThresholds)
	if err != nil {
		log.Errorf(ctx, "%v", err)
	}
	s.metrics.RaftTickingDurationNanos.Inc(timeutil.Since(start).Nanoseconds())
	return exists // ready
}

func (s *Store) processRACv2PiggybackedAdmitted(ctx context.Context, rangeID roachpb.RangeID) {
	if r, ok := s.mu.replicasByRangeID.Load(rangeID); ok {
		r.processRACv2PiggybackedAdmitted(ctx)
	}
}

func (s *Store) processRACv2RangeController(ctx context.Context, rangeID roachpb.RangeID) {
	if r, ok := s.mu.replicasByRangeID.Load(rangeID); ok {
		r.processRACv2RangeController(ctx)
	}
}

// nodeIsLiveCallback is invoked when a node transitions from non-live to live.
// Iterate through all replicas and find any which belong to ranges containing
// the implicated node. Unquiesce if currently quiesced and the node's replica
// is not up-to-date.
//
// See the comment in shouldFollowerQuiesceOnNotify for details on how these two
// functions combine to provide the guarantee that:
//
//	If a quorum of replica in a Raft group is alive and at least
//	one of these replicas is up-to-date, the Raft group will catch
//	up any of the live, lagging replicas.
//
// Note that this mechanism can race with concurrent invocations of processTick,
// which may have a copy of the previous livenessMap where the now-live node is
// down. Those instances should be rare, however, and we expect the newly live
// node to eventually unquiesce the range.
func (s *Store) nodeIsLiveCallback(l livenesspb.Liveness) {
	ctx := context.TODO()
	s.updateLivenessMap()

	s.mu.replicasByRangeID.Range(func(_ roachpb.RangeID, r *Replica) bool {
		r.mu.RLock()
		quiescent := r.mu.quiescent
		lagging := r.mu.laggingFollowersOnQuiesce
		r.mu.RUnlock()
		if quiescent && lagging.MemberStale(l) {
			r.maybeUnquiesce(ctx, false /* wakeLeader */, false /* mayCampaign */) // already leader
		}
		return true
	})
}

// supportWithdrawnCallback is called every time the local store withdraws
// support form other stores in store liveness. The goal of this callback is to
// unquiesce any replicas on the local store that have leaders on any of the
// remote stores.
func (s *Store) supportWithdrawnCallback(supportWithdrawnForStoreIDs map[roachpb.StoreID]struct{}) {
	// TODO(mira): It might be tempting to check the cluster setting
	// RaftStoreLivenessQuiescenceEnabled here and not proceed if it's disabled.
	// But that seems risky; if the setting transitions from enabled to disabled,
	// replicas could get stuck in an asleep state indefinitely.
	//
	// TODO(mira): Is it expensive to iterate over all replicas? This callback
	// will be called at most every SupportExpiryInterval (100ms). The
	// nodeIsLiveCallback above uses the same pattern. Alternatively, we'd have to
	// maintain a map from remote store to a list of replicas that have leaders on
	// that remote store. And we'd have to serialize access to it and keep it in
	// sync with store.unquiescedOrAwakeReplicas.
	s.mu.replicasByRangeID.Range(func(_ roachpb.RangeID, r *Replica) bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		leader, err := r.getReplicaDescriptorByIDRLocked(r.mu.leaderID, roachpb.ReplicaDescriptor{})
		// If we couldn't locate the leader, wake up the replica.
		if err != nil || leader.StoreID == 0 {
			r.maybeWakeUpRMuLocked()
			return true
		}
		// If a replica is asleep, and we just withdrew support for its leader,
		// wake it up.
		if _, ok := supportWithdrawnForStoreIDs[leader.StoreID]; ok {
			r.maybeWakeUpRMuLocked()
		}
		return true
	})
}

func (s *Store) processRaft(ctx context.Context) {
	s.scheduler.Start(s.stopper)
	// Wait for the scheduler worker goroutines to finish.
	if err := s.stopper.RunAsyncTask(ctx, "sched-wait", s.scheduler.Wait); err != nil {
		s.scheduler.Wait(ctx)
	}

	_ = s.stopper.RunAsyncTask(ctx, "sched-tick-loop", s.raftTickLoop)
	_ = s.stopper.RunAsyncTask(ctx, "coalesced-hb-loop", s.coalescedHeartbeatsLoop)
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.cfg.Transport.StopIncomingRaftMessages(s.StoreID())
		s.cfg.Transport.StopOutgoingMessage(s.StoreID())
	}))

	for _, w := range s.syncWaiters {
		w.Start(ctx, s.stopper)
	}

	// We'll want to cancel all in-flight proposals. Proposals embed tracing
	// spans in them, and we don't want to be leaking any.
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.VisitReplicas(func(r *Replica) (more bool) {
			r.mu.Lock()
			r.mu.proposalBuf.FlushLockedWithoutProposing(ctx)
			for k, prop := range r.mu.proposals {
				delete(r.mu.proposals, k)
				prop.finishApplication(
					context.Background(),
					makeProposalResultErr(
						kvpb.NewAmbiguousResultErrorf("store is stopping")))
			}
			r.mu.Unlock()
			return true
		})
	}))
}

func (s *Store) raftTickLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RaftTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Update the liveness map.
			if s.cfg.NodeLiveness != nil {
				s.updateLivenessMap()
			}
			s.updateIOThresholdMap()

			s.unquiescedOrAwakeReplicas.Lock()
			// Why do we bother to ever queue a Replica on the Raft scheduler for
			// tick processing? Couldn't we just call Replica.tick() here? Yes, but
			// then a single bad/slow Replica can disrupt tick processing for every
			// Replica on the store which cascades into Raft elections and more
			// disruption.
			batch := s.scheduler.NewEnqueueBatch()
			for rangeID := range s.unquiescedOrAwakeReplicas.m {
				batch.Add(rangeID)
			}
			s.unquiescedOrAwakeReplicas.Unlock()

			s.scheduler.EnqueueRaftTicks(batch)
			batch.Close()
			s.metrics.RaftTicks.Inc(1)

		case <-s.stopper.ShouldQuiesce():
			return
		}
	}
}

func (s *Store) updateIOThresholdMap() {
	ioThresholdMap := map[roachpb.StoreID]*admissionpb.IOThreshold{}
	for _, sd := range s.cfg.StorePool.GetStores() {
		ioThreshold := sd.Capacity.IOThreshold // need a copy
		ioThresholdMap[sd.StoreID] = &ioThreshold
	}
	threshold := pauseReplicationIOThreshold.Get(&s.cfg.Settings.SV)
	if threshold <= 0 {
		threshold = math.MaxFloat64
	}
	old, cur := s.ioThresholds.Replace(ioThresholdMap, threshold)
	// Log whenever the set of overloaded stores changes.
	shouldLog := log.V(1) || old.seq != cur.seq
	if shouldLog {
		log.Infof(
			s.AnnotateCtx(context.Background()), "pausable stores: %+v", cur)
	}
}

func (s *Store) updateLivenessMap() {
	nextMap := s.cfg.NodeLiveness.GetIsLiveMap()
	for nodeID, entry := range nextMap {
		if entry.IsLive {
			continue
		}
		// Liveness claims that this node is down, but ConnHealth gets the last say
		// because we'd rather quiesce a range too little than one too often. Note
		// that this policy is different from the one governing the releasing of
		// proposal quota; see comments over there.
		//
		// NB: This has false negatives when we haven't attempted to connect to the
		// node yet, where it will return rpc.ErrNotHeartbeated regardless of
		// whether the node is up or not. Once connected, the RPC circuit breakers
		// will continually probe the connection. The check can also have false
		// positives if the node goes down after populating the map, but that
		// matters even less.
		entry.IsLive = s.cfg.NodeDialer.ConnHealth(nodeID, rpc.SystemClass) == nil
		nextMap[nodeID] = entry
	}
	s.livenessMap.Store(nextMap)
}

// Since coalesced heartbeats adds latency to heartbeat messages, it is
// beneficial to have it run on a faster cycle than once per tick, so that
// the delay does not impact latency-sensitive features such as quiescence.
func (s *Store) coalescedHeartbeatsLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CoalescedHeartbeatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendQueuedHeartbeats(ctx)
		case <-s.stopper.ShouldQuiesce():
			return
		}
	}
}

// sendQueuedHeartbeatsToNode requires that the s.coalescedMu lock is held. It
// returns the number of heartbeats that were sent.
func (s *Store) sendQueuedHeartbeatsToNode(
	ctx context.Context, beats, resps []kvserverpb.RaftHeartbeat, to roachpb.StoreIdent,
) int {
	var msgType raftpb.MessageType

	if len(beats) == 0 && len(resps) == 0 {
		return 0
	} else if len(resps) == 0 {
		msgType = raftpb.MsgHeartbeat
	} else if len(beats) == 0 {
		msgType = raftpb.MsgHeartbeatResp
	} else {
		log.Fatal(ctx, "cannot coalesce both heartbeats and responses")
	}

	chReq := newRaftMessageRequest()
	*chReq = kvserverpb.RaftMessageRequest{
		RangeID: 0,
		ToReplica: roachpb.ReplicaDescriptor{
			NodeID:    to.NodeID,
			StoreID:   to.StoreID,
			ReplicaID: 0,
		},
		FromReplica: roachpb.ReplicaDescriptor{
			NodeID:  s.Ident.NodeID,
			StoreID: s.Ident.StoreID,
		},
		Message: raftpb.Message{
			Type: msgType,
		},
		Heartbeats:     beats,
		HeartbeatResps: resps,
	}

	if log.V(4) {
		log.Infof(ctx, "sending raft request (coalesced) %+v", chReq)
	}

	if !s.cfg.Transport.SendAsync(chReq, rpc.SystemClass) {
		for _, beat := range beats {
			if repl, ok := s.mu.replicasByRangeID.Load(beat.RangeID); ok {
				repl.addUnreachableRemoteReplica(beat.ToReplicaID)
			}
		}
		for _, resp := range resps {
			if repl, ok := s.mu.replicasByRangeID.Load(resp.RangeID); ok {
				repl.addUnreachableRemoteReplica(resp.ToReplicaID)
			}
		}
		return 0
	}
	return len(beats) + len(resps)
}

func (s *Store) sendQueuedHeartbeats(ctx context.Context) {
	s.coalescedMu.Lock()
	heartbeats := s.coalescedMu.heartbeats
	heartbeatResponses := s.coalescedMu.heartbeatResponses
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
	s.coalescedMu.Unlock()

	var beatsSent int

	for to, beats := range heartbeats {
		beatsSent += s.sendQueuedHeartbeatsToNode(ctx, beats, nil, to)
	}
	for to, resps := range heartbeatResponses {
		beatsSent += s.sendQueuedHeartbeatsToNode(ctx, nil, resps, to)
	}
	s.metrics.RaftCoalescedHeartbeatsPending.Update(int64(beatsSent))
}

func (s *Store) updateCapacityGauges(ctx context.Context) error {
	desc, err := s.Descriptor(ctx, false /* useCached */)
	if err != nil {
		return err
	}
	s.metrics.Capacity.Update(desc.Capacity.Capacity)
	s.metrics.Available.Update(desc.Capacity.Available)
	s.metrics.Used.Update(desc.Capacity.Used)

	return nil
}
