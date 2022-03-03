// Copyright 2019 The Cockroach Authors.
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
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type raftRequestInfo struct {
	req        *kvserverpb.RaftMessageRequest
	respStream RaftMessageResponseStream
}

type raftRequestQueue struct {
	syncutil.Mutex
	infos []raftRequestInfo
}

func (q *raftRequestQueue) drain() ([]raftRequestInfo, bool) {
	q.Lock()
	defer q.Unlock()
	if len(q.infos) == 0 {
		return nil, false
	}
	infos := q.infos
	q.infos = nil
	return infos, true
}

func (q *raftRequestQueue) recycle(processed []raftRequestInfo) {
	if cap(processed) > 4 {
		return // cap recycled slice lengths
	}
	q.Lock()
	defer q.Unlock()
	if q.infos == nil {
		for i := range processed {
			processed[i] = raftRequestInfo{}
		}
		q.infos = processed[:0]
	}
}

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	ctx context.Context, header *kvserverpb.SnapshotRequest_Header, stream SnapshotResponseStream,
) error {
	ctx = s.AnnotateCtx(ctx)
	const name = "storage.Store: handle snapshot"
	return s.stopper.RunTaskWithErr(ctx, name, func(ctx context.Context) error {
		s.metrics.RaftRcvdMessages[raftpb.MsgSnap].Inc(1)

		if s.IsDraining() {
			return stream.Send(&kvserverpb.SnapshotResponse{
				Status:  kvserverpb.SnapshotResponse_ERROR,
				Message: storeDrainingMsg,
			})
		}

		return s.receiveSnapshot(ctx, header, stream)
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
	var toEnqueue []roachpb.RangeID
	for i, beat := range beats {
		msg := raftpb.Message{
			Type:   msgT,
			From:   uint64(beat.FromReplicaID),
			To:     uint64(beat.ToReplicaID),
			Term:   beat.Term,
			Commit: beat.Commit,
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
			toEnqueue = append(toEnqueue, beat.RangeID)
		}
	}
	s.scheduler.EnqueueRaftRequests(toEnqueue...)
}

// HandleRaftRequest dispatches a raft message to the appropriate Replica. It
// requires that s.mu is not held.
func (s *Store) HandleRaftRequest(
	ctx context.Context, req *kvserverpb.RaftMessageRequest, respStream RaftMessageResponseStream,
) *roachpb.Error {
	// NB: unlike the other two RaftMessageHandler methods implemented by Store,
	// this one doesn't need to directly run through a Stopper task because it
	// delegates all work through a raftScheduler, whose workers' lifetimes are
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

	value, ok := s.replicaQueues.Load(int64(req.RangeID))
	if !ok {
		value, _ = s.replicaQueues.LoadOrStore(int64(req.RangeID), unsafe.Pointer(&raftRequestQueue{}))
	}
	q := (*raftRequestQueue)(value)
	q.Lock()
	defer q.Unlock()
	if len(q.infos) >= replicaRequestQueueSize {
		// TODO(peter): Return an error indicating the request was dropped. Note
		// that dropping the request is safe. Raft will retry.
		s.metrics.RaftRcvdMsgDropped.Inc(1)
		return false
	}
	q.infos = append(q.infos, raftRequestInfo{
		req:        req,
		respStream: respStream,
	})
	// processRequestQueue will process all infos in the slice each time it
	// runs, so we only need to schedule a Raft request event if we added the
	// first info in the slice. Everyone else can rely on the request that added
	// the first info already having scheduled a Raft request event.
	return len(q.infos) == 1 /* enqueue */
}

// withReplicaForRequest calls the supplied function with the (lazily
// initialized) Replica specified in the request. The replica passed to
// the function will have its Replica.raftMu locked.
func (s *Store) withReplicaForRequest(
	ctx context.Context,
	req *kvserverpb.RaftMessageRequest,
	f func(context.Context, *Replica) *roachpb.Error,
) *roachpb.Error {
	// Lazily create the replica.
	r, _, err := s.getOrCreateReplica(
		ctx,
		req.RangeID,
		req.ToReplica.ReplicaID,
		&req.FromReplica,
	)
	if err != nil {
		return roachpb.NewError(err)
	}
	defer r.raftMu.Unlock()
	r.setLastReplicaDescriptorsRaftMuLocked(req)
	return f(ctx, r)
}

// processRaftRequestWithReplica processes the (non-snapshot) Raft request on
// the specified replica. Notably, it does not handle updates to the Raft Ready
// state; callers will probably want to handle this themselves at some point.
func (s *Store) processRaftRequestWithReplica(
	ctx context.Context, r *Replica, req *kvserverpb.RaftMessageRequest,
) *roachpb.Error {
	if verboseRaftLoggingEnabled() {
		log.Infof(ctx, "incoming raft message:\n%s", raftDescribeMessage(req.Message, raftEntryFormatter))
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
		return roachpb.NewErrorf(
			"cannot recreate replica that is not a member of its range (StoreID %s not found in r%d)",
			r.store.StoreID(), req.RangeID,
		)
	}

	drop := maybeDropMsgApp(ctx, (*replicaMsgAppDropper)(r), &req.Message, req.RangeStartKey)
	if !drop {
		if err := r.stepRaftGroup(req); err != nil {
			return roachpb.NewError(err)
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
) *roachpb.Error {
	return s.withReplicaForRequest(ctx, &snapHeader.RaftMessageRequest, func(
		ctx context.Context, r *Replica,
	) (pErr *roachpb.Error) {
		ctx = r.AnnotateCtx(ctx)
		if snapHeader.RaftMessageRequest.Message.Type != raftpb.MsgSnap {
			log.Fatalf(ctx, "expected snapshot: %+v", snapHeader.RaftMessageRequest)
		}

		var stats handleRaftReadyStats
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
			// snapshot's is at least raftInitialLogIndex).
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
		if err := r.stepRaftGroup(&snapHeader.RaftMessageRequest); err != nil {
			return roachpb.NewError(err)
		}

		// We've handed the snapshot to Raft, which will typically apply it (in
		// which case the placeholder, if any, is removed by the time
		// handleRaftReadyRaftMuLocked returns. We handle the other case in a
		// defer() above. Note that we could infer when the placeholder should still
		// be there based on `stats.snap.applied`	but it is a questionable use of
		// stats and more susceptible to bugs.
		typ = removePlaceholderDropped
		var expl string
		var err error
		stats, expl, err = r.handleRaftReadyRaftMuLocked(ctx, inSnap)
		maybeFatalOnRaftReadyErr(ctx, expl, err)
		if !stats.snap.applied {
			// This line would be hit if a snapshot was sent when it isn't necessary
			// (i.e. follower was able to catch up via the log in the interim) or when
			// multiple snapshots raced (as is possible when raft leadership changes
			// and both the old and new leaders send snapshots).
			log.Infof(ctx, "ignored stale snapshot at index %d", snapHeader.RaftMessageRequest.Message.Snapshot.Metadata.Index)
		}
		return nil
	})
}

// HandleRaftResponse implements the RaftMessageHandler interface. Per the
// interface specification, an error is returned if and only if the underlying
// Raft connection should be closed.
// It requires that s.mu is not held.
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
		case *roachpb.Error:
			switch tErr := val.GetDetail().(type) {
			case *roachpb.ReplicaTooOldError:
				if replErr != nil {
					// RangeNotFoundErrors are expected here; nothing else is.
					if !errors.HasType(replErr, (*roachpb.RangeNotFoundError)(nil)) {
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
			case *roachpb.RaftGroupDeletedError:
				if replErr != nil {
					// RangeNotFoundErrors are expected here; nothing else is.
					if !errors.HasType(replErr, (*roachpb.RangeNotFoundError)(nil)) {
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
			case *roachpb.StoreNotFoundError:
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

func (s *Store) processRequestQueue(ctx context.Context, rangeID roachpb.RangeID) bool {
	value, ok := s.replicaQueues.Load(int64(rangeID))
	if !ok {
		return false
	}
	q := (*raftRequestQueue)(value)
	infos, ok := q.drain()
	if !ok {
		return false
	}
	defer q.recycle(infos)

	var hadError bool
	for i := range infos {
		info := &infos[i]
		if pErr := s.withReplicaForRequest(
			ctx, info.req, func(_ context.Context, r *Replica) *roachpb.Error {
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
		if _, exists := s.mu.replicasByRangeID.Load(rangeID); !exists {
			q.Lock()
			if len(q.infos) == 0 {
				s.replicaQueues.Delete(int64(rangeID))
			}
			q.Unlock()
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

	ctx := r.raftCtx
	start := timeutil.Now()
	stats, expl, err := r.handleRaftReady(ctx, noSnap)
	maybeFatalOnRaftReadyErr(ctx, expl, err)
	elapsed := timeutil.Since(start)
	s.metrics.RaftWorkingDurationNanos.Inc(elapsed.Nanoseconds())
	s.metrics.RaftHandleReadyLatency.RecordValue(elapsed.Nanoseconds())
	// Warn if Raft processing took too long. We use the same duration as we
	// use for warning about excessive raft mutex lock hold times. Long
	// processing time means we'll have starved local replicas of ticks and
	// remote replicas will likely start campaigning.
	if elapsed >= defaultReplicaRaftMuWarnThreshold {
		log.Infof(ctx, "handle raft ready: %.1fs [applied=%d, batches=%d, state_assertions=%d]; node might be overloaded",
			elapsed.Seconds(), stats.entriesProcessed, stats.batchesProcessed, stats.stateAssertions)
	}
}

func (s *Store) processTick(_ context.Context, rangeID roachpb.RangeID) bool {
	r, ok := s.mu.replicasByRangeID.Load(rangeID)
	if !ok {
		return false
	}
	livenessMap, _ := s.livenessMap.Load().(liveness.IsLiveMap)

	start := timeutil.Now()
	ctx := r.raftCtx
	exists, err := r.tick(ctx, livenessMap)
	if err != nil {
		log.Errorf(ctx, "%v", err)
	}
	s.metrics.RaftTickingDurationNanos.Inc(timeutil.Since(start).Nanoseconds())
	return exists // ready
}

// nodeIsLiveCallback is invoked when a node transitions from non-live to live.
// Iterate through all replicas and find any which belong to ranges containing
// the implicated node. Unquiesce if currently quiesced and the node's replica
// is not up-to-date.
//
// See the comment in shouldFollowerQuiesceOnNotify for details on how these two
// functions combine to provide the guarantee that:
//
//   If a quorum of replica in a Raft group is alive and at least
//   one of these replicas is up-to-date, the Raft group will catch
//   up any of the live, lagging replicas.
//
// Note that this mechanism can race with concurrent invocations of processTick,
// which may have a copy of the previous livenessMap where the now-live node is
// down. Those instances should be rare, however, and we expect the newly live
// node to eventually unquiesce the range.
func (s *Store) nodeIsLiveCallback(l livenesspb.Liveness) {
	s.updateLivenessMap()

	s.mu.replicasByRangeID.Range(func(r *Replica) {
		r.mu.RLock()
		quiescent := r.mu.quiescent
		lagging := r.mu.laggingFollowersOnQuiesce
		r.mu.RUnlock()
		if quiescent && lagging.MemberStale(l) {
			r.maybeUnquiesce()
		}
	})
}

func (s *Store) processRaft(ctx context.Context) {
	if s.cfg.TestingKnobs.DisableProcessRaft {
		return
	}

	s.scheduler.Start(s.stopper)
	// Wait for the scheduler worker goroutines to finish.
	if err := s.stopper.RunAsyncTask(ctx, "sched-wait", s.scheduler.Wait); err != nil {
		s.scheduler.Wait(ctx)
	}

	_ = s.stopper.RunAsyncTask(ctx, "sched-tick-loop", s.raftTickLoop)
	_ = s.stopper.RunAsyncTask(ctx, "coalesced-hb-loop", s.coalescedHeartbeatsLoop)
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.cfg.Transport.Stop(s.StoreID())
	}))

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
					proposalResult{
						Err: roachpb.NewError(roachpb.NewAmbiguousResultError("store is stopping")),
					},
				)
			}
			r.mu.Unlock()
			return true
		})
	}))
}

func (s *Store) raftTickLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RaftTickInterval)
	defer ticker.Stop()

	var rangeIDs []roachpb.RangeID

	for {
		select {
		case <-ticker.C:
			rangeIDs = rangeIDs[:0]
			// Update the liveness map.
			if s.cfg.NodeLiveness != nil {
				s.updateLivenessMap()
			}

			s.unquiescedReplicas.Lock()
			// Why do we bother to ever queue a Replica on the Raft scheduler for
			// tick processing? Couldn't we just call Replica.tick() here? Yes, but
			// then a single bad/slow Replica can disrupt tick processing for every
			// Replica on the store which cascades into Raft elections and more
			// disruption.
			for rangeID := range s.unquiescedReplicas.m {
				rangeIDs = append(rangeIDs, rangeID)
			}
			s.unquiescedReplicas.Unlock()

			s.scheduler.EnqueueRaftTicks(rangeIDs...)
			s.metrics.RaftTicks.Inc(1)

		case <-s.stopper.ShouldQuiesce():
			return
		}
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
		// NB: This has false negatives. If a node doesn't have a conn open to it
		// when ConnHealth is called, then ConnHealth will return
		// rpc.ErrNotHeartbeated regardless of whether the node is up or not. That
		// said, for the nodes that matter, we're likely talking to them via the
		// Raft transport, so ConnHealth should usually indicate a real problem if
		// it gives us an error back. The check can also have false positives if the
		// node goes down after populating the map, but that matters even less.
		entry.IsLive = (s.cfg.NodeDialer.ConnHealth(nodeID, rpc.SystemClass) == nil)
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
