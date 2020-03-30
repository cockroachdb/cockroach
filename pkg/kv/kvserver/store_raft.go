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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	crdberrors "github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

type raftRequestInfo struct {
	req        *RaftMessageRequest
	respStream RaftMessageResponseStream
}

type raftRequestQueue struct {
	syncutil.Mutex
	infos []raftRequestInfo
	// TODO(nvanbenschoten): consider recycling []raftRequestInfo slices. This
	// could be done without any new mutex locking by storing two slices here
	// and swapping them under lock in processRequestQueue.
}

// HandleSnapshot reads an incoming streaming snapshot and applies it if
// possible.
func (s *Store) HandleSnapshot(
	header *SnapshotRequest_Header, stream SnapshotResponseStream,
) error {
	ctx := s.AnnotateCtx(stream.Context())
	const name = "storage.Store: handle snapshot"
	return s.stopper.RunTaskWithErr(ctx, name, func(ctx context.Context) error {
		s.metrics.RaftRcvdMessages[raftpb.MsgSnap].Inc(1)

		if s.IsDraining() {
			return stream.Send(&SnapshotResponse{
				Status:  SnapshotResponse_DECLINED,
				Message: storeDrainingMsg,
			})
		}

		return s.receiveSnapshot(ctx, header, stream)
	})
}

// learnerType exists to avoid allocating on every coalesced beat to a learner.
var learnerType = roachpb.LEARNER

func (s *Store) uncoalesceBeats(
	ctx context.Context,
	beats []RaftHeartbeat,
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
	beatReqs := make([]RaftMessageRequest, len(beats))
	for i, beat := range beats {
		msg := raftpb.Message{
			Type:   msgT,
			From:   uint64(beat.FromReplicaID),
			To:     uint64(beat.ToReplicaID),
			Term:   beat.Term,
			Commit: beat.Commit,
		}
		beatReqs[i] = RaftMessageRequest{
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
			Message: msg,
			Quiesce: beat.Quiesce,
		}
		if beat.ToIsLearner {
			beatReqs[i].ToReplica.Type = &learnerType
		}
		if log.V(4) {
			log.Infof(ctx, "uncoalesced beat: %+v", beatReqs[i])
		}

		if err := s.HandleRaftUncoalescedRequest(ctx, &beatReqs[i], respStream); err != nil {
			log.Errorf(ctx, "could not handle uncoalesced heartbeat %s", err)
		}
	}
}

// HandleRaftRequest dispatches a raft message to the appropriate Replica. It
// requires that s.mu is not held.
func (s *Store) HandleRaftRequest(
	ctx context.Context, req *RaftMessageRequest, respStream RaftMessageResponseStream,
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
	return s.HandleRaftUncoalescedRequest(ctx, req, respStream)
}

// HandleRaftUncoalescedRequest dispatches a raft message to the appropriate
// Replica. It requires that s.mu is not held.
func (s *Store) HandleRaftUncoalescedRequest(
	ctx context.Context, req *RaftMessageRequest, respStream RaftMessageResponseStream,
) *roachpb.Error {

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
	if len(q.infos) >= replicaRequestQueueSize {
		q.Unlock()
		// TODO(peter): Return an error indicating the request was dropped. Note
		// that dropping the request is safe. Raft will retry.
		s.metrics.RaftRcvdMsgDropped.Inc(1)
		return nil
	}
	q.infos = append(q.infos, raftRequestInfo{
		req:        req,
		respStream: respStream,
	})
	first := len(q.infos) == 1
	q.Unlock()

	// processRequestQueue will process all infos in the slice each time it
	// runs, so we only need to schedule a Raft request event if we added the
	// first info in the slice. Everyone else can rely on the request that added
	// the first info already having scheduled a Raft request event.
	if first {
		s.scheduler.EnqueueRaftRequest(req.RangeID)
	}
	return nil
}

// withReplicaForRequest calls the supplied function with the (lazily
// initialized) Replica specified in the request. The replica passed to
// the function will have its Replica.raftMu locked.
func (s *Store) withReplicaForRequest(
	ctx context.Context, req *RaftMessageRequest, f func(context.Context, *Replica) *roachpb.Error,
) *roachpb.Error {
	// Lazily create the replica.
	r, _, err := s.getOrCreateReplica(
		ctx,
		req.RangeID,
		req.ToReplica.ReplicaID,
		&req.FromReplica,
		req.ToReplica.GetType() == roachpb.LEARNER,
	)
	if err != nil {
		return roachpb.NewError(err)
	}
	defer r.raftMu.Unlock()
	ctx = r.AnnotateCtx(ctx)
	r.setLastReplicaDescriptors(req)
	return f(ctx, r)
}

// processRaftRequestWithReplica processes the (non-snapshot) Raft request on
// the specified replica. Notably, it does not handle updates to the Raft Ready
// state; callers will probably want to handle this themselves at some point.
func (s *Store) processRaftRequestWithReplica(
	ctx context.Context, r *Replica, req *RaftMessageRequest,
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
		// If another replica tells us to quiesce, we verify that according to
		// it, we are fully caught up, and that we believe it to be the leader.
		// If we didn't do this, this replica could only unquiesce by means of
		// an election, which means that the request prompting the unquiesce
		// would end up with latency on the order of an election timeout.
		//
		// There are additional checks in quiesceLocked() that prevent us from
		// quiescing if there's outstanding work.
		r.mu.Lock()
		status := r.raftBasicStatusRLocked()
		ok := status.Term == req.Message.Term &&
			status.Commit == req.Message.Commit &&
			status.Lead == req.Message.From &&
			r.quiesceLocked()
		r.mu.Unlock()
		if ok {
			return nil
		}
		if log.V(4) {
			log.Infof(ctx, "not quiescing: local raft status is %+v, incoming quiesce message is %+v", status, req.Message)
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

// processRaftSnapshotRequest processes the incoming non-preemptive snapshot
// Raft request on the request's specified replica. The function makes sure to
// handle any updated Raft Ready state. It also adds and later removes the
// (potentially) necessary placeholder to protect against concurrent access to
// the keyspace encompassed by the snapshot but not yet guarded by the replica.
func (s *Store) processRaftSnapshotRequest(
	ctx context.Context, snapHeader *SnapshotRequest_Header, inSnap IncomingSnapshot,
) *roachpb.Error {
	if snapHeader.IsPreemptive() {
		return roachpb.NewError(crdberrors.AssertionFailedf(`expected a raft or learner snapshot`))
	}

	return s.withReplicaForRequest(ctx, &snapHeader.RaftMessageRequest, func(
		ctx context.Context, r *Replica,
	) (pErr *roachpb.Error) {
		if snapHeader.RaftMessageRequest.Message.Type != raftpb.MsgSnap {
			log.Fatalf(ctx, "expected snapshot: %+v", snapHeader.RaftMessageRequest)
		}

		// Check to see if a snapshot can be applied. Snapshots can always be applied
		// to initialized replicas. Note that if we add a placeholder we need to
		// already be holding Replica.raftMu in order to prevent concurrent
		// raft-ready processing of uninitialized replicas.
		var addedPlaceholder bool
		var removePlaceholder bool
		if err := func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			placeholder, err := s.canApplySnapshotLocked(ctx, snapHeader)
			if err != nil {
				// If the storage cannot accept the snapshot, return an
				// error before passing it to RawNode.Step, since our
				// error handling options past that point are limited.
				log.Infof(ctx, "cannot apply snapshot: %s", err)
				return err
			}

			if placeholder != nil {
				// NB: The placeholder added here is either removed below after a
				// preemptive snapshot is applied or after the next call to
				// Replica.handleRaftReady. Note that we can only get here if the
				// replica doesn't exist or is uninitialized.
				if err := s.addPlaceholderLocked(placeholder); err != nil {
					log.Fatalf(ctx, "could not add vetted placeholder %s: %+v", placeholder, err)
				}
				addedPlaceholder = true
			}
			return nil
		}(); err != nil {
			return roachpb.NewError(err)
		}

		if addedPlaceholder {
			// If we added a placeholder remove it before we return unless some other
			// part of the code takes ownership of the removal (indicated by setting
			// removePlaceholder to false).
			removePlaceholder = true
			defer func() {
				if removePlaceholder {
					if s.removePlaceholder(ctx, snapHeader.RaftMessageRequest.RangeID) {
						atomic.AddInt32(&s.counts.removedPlaceholders, 1)
					}
				}
			}()
		}
		// NB: we cannot get errRemoved here because we're promised by
		// withReplicaForRequest that this replica is not currently being removed
		// and we've been holding the raftMu the entire time.
		if err := r.stepRaftGroup(&snapHeader.RaftMessageRequest); err != nil {
			return roachpb.NewError(err)
		}
		_, expl, err := r.handleRaftReadyRaftMuLocked(ctx, inSnap)
		maybeFatalOnRaftReadyErr(ctx, expl, err)
		removePlaceholder = false
		return nil
	})
}

// HandleRaftResponse implements the RaftMessageHandler interface. Per the
// interface specification, an error is returned if and only if the underlying
// Raft connection should be closed.
// It requires that s.mu is not held.
func (s *Store) HandleRaftResponse(ctx context.Context, resp *RaftMessageResponse) error {
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
				if tErr.ReplicaID != repl.mu.replicaID ||
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
	q.Lock()
	infos := q.infos
	q.infos = nil
	q.Unlock()
	if len(infos) == 0 {
		return false
	}

	var hadError bool
	for i := range infos {
		info := &infos[i]
		if pErr := s.withReplicaForRequest(
			ctx, info.req, func(ctx context.Context, r *Replica) *roachpb.Error {
				return s.processRaftRequestWithReplica(ctx, r, info.req)
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
		if _, exists := s.mu.replicas.Load(int64(rangeID)); !exists {
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

func (s *Store) processReady(ctx context.Context, rangeID roachpb.RangeID) {
	value, ok := s.mu.replicas.Load(int64(rangeID))
	if !ok {
		return
	}

	r := (*Replica)(value)
	ctx = r.AnnotateCtx(ctx)
	start := timeutil.Now()
	stats, expl, err := r.handleRaftReady(ctx, noSnap)
	removed := maybeFatalOnRaftReadyErr(ctx, expl, err)
	elapsed := timeutil.Since(start)
	s.metrics.RaftWorkingDurationNanos.Inc(elapsed.Nanoseconds())
	// Warn if Raft processing took too long. We use the same duration as we
	// use for warning about excessive raft mutex lock hold times. Long
	// processing time means we'll have starved local replicas of ticks and
	// remote replicas will likely start campaigning.
	if elapsed >= defaultReplicaRaftMuWarnThreshold {
		log.Warningf(ctx, "handle raft ready: %.1fs [applied=%d, batches=%d, state_assertions=%d]",
			elapsed.Seconds(), stats.entriesProcessed, stats.batchesProcessed, stats.stateAssertions)
	}
	if !removed && !r.IsInitialized() {
		// Only an uninitialized replica can have a placeholder since, by
		// definition, an initialized replica will be present in the
		// replicasByKey map. While the replica will usually consume the
		// placeholder itself, that isn't guaranteed and so this invocation
		// here is crucial (i.e. don't remove it).
		//
		// We need to hold raftMu here to prevent removing a placeholder that is
		// actively being used by Store.processRaftRequest.
		r.raftMu.Lock()
		if s.removePlaceholder(ctx, r.RangeID) {
			atomic.AddInt32(&s.counts.droppedPlaceholders, 1)
		}
		r.raftMu.Unlock()
	}
}

func (s *Store) processTick(ctx context.Context, rangeID roachpb.RangeID) bool {
	value, ok := s.mu.replicas.Load(int64(rangeID))
	if !ok {
		return false
	}
	livenessMap, _ := s.livenessMap.Load().(IsLiveMap)

	start := timeutil.Now()
	r := (*Replica)(value)
	exists, err := r.tick(livenessMap)
	if err != nil {
		log.Errorf(ctx, "%v", err)
	}
	s.metrics.RaftTickingDurationNanos.Inc(timeutil.Since(start).Nanoseconds())
	return exists // ready
}

// nodeIsLiveCallback is invoked when a node transitions from non-live
// to live. Iterate through all replicas and find any which belong to
// ranges containing the implicated node. Unquiesce if currently
// quiesced. Note that this mechanism can race with concurrent
// invocations of processTick, which may have a copy of the previous
// livenessMap where the now-live node is down. Those instances should
// be rare, however, and we expect the newly live node to eventually
// unquiesce the range.
func (s *Store) nodeIsLiveCallback(nodeID roachpb.NodeID) {
	s.updateLivenessMap()

	s.mu.replicas.Range(func(k int64, v unsafe.Pointer) bool {
		r := (*Replica)(v)
		for _, rep := range r.Desc().Replicas().All() {
			if rep.NodeID == nodeID {
				r.unquiesce()
			}
		}
		return true
	})
}

func (s *Store) processRaft(ctx context.Context) {
	if s.cfg.TestingKnobs.DisableProcessRaft {
		return
	}

	s.scheduler.Start(ctx, s.stopper)
	// Wait for the scheduler worker goroutines to finish.
	s.stopper.RunWorker(ctx, s.scheduler.Wait)

	s.stopper.RunWorker(ctx, s.raftTickLoop)
	s.stopper.RunWorker(ctx, s.coalescedHeartbeatsLoop)
	s.stopper.AddCloser(stop.CloserFn(func() {
		s.cfg.Transport.Stop(s.StoreID())
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

			s.scheduler.EnqueueRaftTick(rangeIDs...)
			s.metrics.RaftTicks.Inc(1)

		case <-s.stopper.ShouldStop():
			return
		}
	}
}

func (s *Store) updateLivenessMap() {
	nextMap := s.cfg.NodeLiveness.GetIsLiveMap()
	for nodeID, entry := range nextMap {
		if entry.IsLive {
			// Make sure we ask all live nodes for closed timestamp updates.
			s.cfg.ClosedTimestamp.Clients.EnsureClient(nodeID)
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
		case <-s.stopper.ShouldStop():
			return
		}
	}
}

// sendQueuedHeartbeatsToNode requires that the s.coalescedMu lock is held. It
// returns the number of heartbeats that were sent.
func (s *Store) sendQueuedHeartbeatsToNode(
	ctx context.Context, beats, resps []RaftHeartbeat, to roachpb.StoreIdent,
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
	*chReq = RaftMessageRequest{
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
			if value, ok := s.mu.replicas.Load(int64(beat.RangeID)); ok {
				(*Replica)(value).addUnreachableRemoteReplica(beat.ToReplicaID)
			}
		}
		for _, resp := range resps {
			if value, ok := s.mu.replicas.Load(int64(resp.RangeID)); ok {
				(*Replica)(value).addUnreachableRemoteReplica(resp.ToReplicaID)
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
	s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]RaftHeartbeat{}
	s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]RaftHeartbeat{}
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

func (s *Store) updateCapacityGauges() error {
	desc, err := s.Descriptor(false /* useCached */)
	if err != nil {
		return err
	}
	s.metrics.Capacity.Update(desc.Capacity.Capacity)
	s.metrics.Available.Update(desc.Capacity.Available)
	s.metrics.Used.Update(desc.Capacity.Used)

	return nil
}
