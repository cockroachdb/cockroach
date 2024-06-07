// Copyright 2023 The Cockroach Authors.
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
	"cmp"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowconnectedstream"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// replicaFlowControlIntegrationImpl is the canonical implementation of the
// replicaFlowControlIntegration interface.
type replicaFlowControlIntegrationImpl struct {
	replicaForFlowControl replicaForFlowControl
	handleFactory         kvflowcontrol.HandleFactory
	knobs                 *kvflowcontrol.TestingKnobs

	// The fields below are non-nil iff the replica is a raft leader and part of
	// the range.

	// innerHandle is the underlying kvflowcontrol.Handle, which we
	// deduct/return flow tokens to, and inform of connected/disconnected
	// replication streams.
	innerHandle kvflowcontrol.Handle
	// lastKnownReplicas tracks the set of last know replicas in the range. This
	// is updated whenever the range descriptor is changed, and we react to any
	// deltas by disconnecting streams for replicas no longer part of the range,
	// connecting streams for new members of the range, or closing innerHandle
	// if we ourselves are no longer part of the range.
	lastKnownReplicas roachpb.ReplicaSet
	// disconnectedStreams tracks the set of replication streams we're not
	// currently connected to, but want to in the near future should things
	// change. This includes paused followers (who could be unpaused),
	// inactive/dead followers (who could become active if the node they're on
	// is restarted), followers we're not connected to via the raft transport
	// (the transport streams could re-establish), replicas that are being
	// caught up via snapshots or are being probed for the last committed index.
	// This does not include replicas that are no longer part of the range,
	// since we're not looking to reconnect to them in the future.
	disconnectedStreams map[roachpb.ReplicaID]kvflowcontrol.Stream
}

var _ replicaFlowControlIntegration = &replicaFlowControlIntegrationImpl{}

func newReplicaFlowControlIntegration(
	replicaForFlowControl replicaForFlowControl,
	handleFactory kvflowcontrol.HandleFactory,
	knobs *kvflowcontrol.TestingKnobs,
) *replicaFlowControlIntegrationImpl {
	if knobs == nil {
		knobs = &kvflowcontrol.TestingKnobs{}
	}
	return &replicaFlowControlIntegrationImpl{
		replicaForFlowControl: replicaForFlowControl,
		handleFactory:         handleFactory,
		knobs:                 knobs,
	}
}

// onBecameLeader is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) onBecameLeader(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle != nil {
		log.Fatal(ctx, "flow control handle was not nil before becoming the leader")
	}
	if !f.replicaForFlowControl.getTenantID().IsSet() {
		log.Fatal(ctx, "unset tenant ID")
	}
	if f.knobs.UseOnlyForScratchRanges && !f.replicaForFlowControl.isScratchRange() {
		return // nothing to do
	}

	// See I5 from kvflowcontrol/doc.go. The per-replica kvflowcontrol.Handle is
	// tied to the lifetime of a leaseholder replica having raft leadership. We
	// don't intercept lease acquisitions/transfers -- simply raft leadership.
	// When leadership is lost we release all held flow tokens. Tokens are only
	// deducted at proposal time when the proposing replica is both the raft
	// leader and leaseholder (the latter is tautological since only
	// leaseholders propose). We're relying on timely acquisition of raft
	// leadership by the leaseholder to not be persistently over admitting.
	f.innerHandle = f.handleFactory.NewHandle(
		f.replicaForFlowControl.getRangeID(),
		f.replicaForFlowControl.getTenantID(),
	)
	f.lastKnownReplicas = f.replicaForFlowControl.getDescriptor().Replicas()
	f.disconnectedStreams = make(map[roachpb.ReplicaID]kvflowcontrol.Stream)

	// Connect to the local stream.
	localRepl, found := f.lastKnownReplicas.GetReplicaDescriptorByID(f.replicaForFlowControl.getReplicaID())
	if !found {
		// This assertion relies on replicaForFlowControl being locked, so the
		// descriptor could not have changed state while this callback is
		// ongoing. We never disconnect the local stream until we lost raft
		// leadership or are no longer a raft member.
		log.Fatalf(ctx, "leader (replid=%d) didn't find self in last known replicas (%s)",
			f.replicaForFlowControl.getReplicaID(), f.lastKnownReplicas)
	}
	f.innerHandle.ConnectStream(ctx,
		f.replicaForFlowControl.getAppliedLogPosition(),
		kvflowcontrol.Stream{
			TenantID: f.replicaForFlowControl.getTenantID(),
			StoreID:  localRepl.StoreID,
		},
	)

	// Start off every remote stream as disconnected. Later we'll try to
	// reconnect them.
	var toDisconnect []roachpb.ReplicaDescriptor
	for _, desc := range f.replicaForFlowControl.getDescriptor().Replicas().Descriptors() {
		if desc.ReplicaID != localRepl.ReplicaID {
			toDisconnect = append(toDisconnect, desc)
		}
	}
	f.disconnectStreams(ctx, toDisconnect, "unknown followers on new leader")
	f.tryReconnect(ctx)

	if log.V(1) {
		var disconnected []kvflowcontrol.Stream
		for _, stream := range f.disconnectedStreams {
			disconnected = append(disconnected, stream)
		}
		slices.SortFunc(disconnected, func(a, b kvflowcontrol.Stream) int {
			return cmp.Compare(a.StoreID, b.StoreID)
		})
		log.VInfof(ctx, 1, "assumed raft leadership: initializing flow handle for %s starting at %s (disconnected streams: %s)",
			f.replicaForFlowControl.getDescriptor(),
			f.replicaForFlowControl.getAppliedLogPosition(),
			disconnected,
		)
	}
}

// onBecameFollower is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) onBecameFollower(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return
	}

	// See I5 from kvflowcontrol/doc.go. The per-replica kvflowcontrol.Handle is
	// tied to the lifetime of a leaseholder replica having raft leadership.
	// When leadership is lost, or the lease changes hands, we release all held
	// flow tokens. Here we're not dealing with prolonged leaseholder != leader
	// scenarios.
	log.VInfof(ctx, 1, "lost raft leadership: releasing flow tokens and closing handle for %s",
		f.replicaForFlowControl.getDescriptor())
	f.clearState(ctx)
}

// onDescChanged is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) onDescChanged(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	addedReplicas, removedReplicas := f.lastKnownReplicas.Difference(
		f.replicaForFlowControl.getDescriptor().Replicas(),
	)

	ourReplicaID := f.replicaForFlowControl.getReplicaID()
	for _, repl := range removedReplicas {
		if repl.ReplicaID == ourReplicaID {
			// We're observing ourselves get removed from the raft group, but
			// are still retaining raft leadership. Close the underlying handle
			// and bail. See TestFlowControlRaftMembershipRemoveSelf.
			f.clearState(ctx)
			return
		}
	}

	// See I10 from kvflowcontrol/doc.go. We stop deducting flow tokens for
	// replicas that are no longer part of the raft group, free-ing up all
	// held tokens.
	f.disconnectStreams(ctx, removedReplicas, "removed replicas")
	for _, repl := range removedReplicas {
		// We'll not reconnect to these replicas either, so untrack them.
		delete(f.disconnectedStreams, repl.ReplicaID)
	}

	// Start off new replicas as disconnected. We'll subsequently try to
	// re-add them, once we know their log positions and consider them
	// sufficiently caught up. See I3a from kvflowcontrol/doc.go.
	f.disconnectStreams(ctx, addedReplicas, "newly added replicas")

	if len(addedReplicas) > 0 || len(removedReplicas) > 0 {
		log.VInfof(ctx, 1, "desc changed from %s to %s: added=%s removed=%s",
			f.lastKnownReplicas, f.replicaForFlowControl.getDescriptor(), addedReplicas, removedReplicas,
		)
	}
	f.lastKnownReplicas = f.replicaForFlowControl.getDescriptor().Replicas()
}

// onFollowersPaused is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) onFollowersPaused(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	// See I3 from kvflowcontrol/doc.go. We don't deduct flow tokens for
	// replication traffic that's not headed to paused replicas.
	f.refreshStreams(ctx, "paused followers")
}

// onRaftTransportDisconnected is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) onRaftTransportDisconnected(
	ctx context.Context, storeIDs ...roachpb.StoreID,
) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	if fn := f.knobs.MaintainStreamsForBrokenRaftTransport; fn != nil && fn() {
		return // nothing to do
	}

	disconnectedStores := make(map[roachpb.StoreID]struct{})
	for _, storeID := range storeIDs {
		disconnectedStores[storeID] = struct{}{}
	}

	ourReplicaID := f.replicaForFlowControl.getReplicaID()
	var toDisconnect []roachpb.ReplicaDescriptor
	for _, repl := range f.lastKnownReplicas.Descriptors() {
		if repl.ReplicaID == ourReplicaID {
			continue
		}
		if _, found := disconnectedStores[repl.StoreID]; found {
			toDisconnect = append(toDisconnect, repl)
		}
	}
	f.disconnectStreams(ctx, toDisconnect, "raft transport disconnected")
	f.tryReconnect(ctx)
}

// onProposalQuotaUpdated is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) onRaftTicked(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	f.refreshStreams(ctx, "refreshing streams")
}

// onDestroyed is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) onDestroyed(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	// During merges, the context might have the subsuming range, so we
	// explicitly (re-)annotate it here.
	ctx = f.replicaForFlowControl.annotateCtx(ctx)

	// See I6, I9 from kvflowcontrol/doc.go. We want to free up all held flow
	// tokens when a replica is being removed, for example when it's being
	// rebalanced away, is no longer part of the raft group, is being GC-ed,
	// destroyed as part of the EndTxn merge trigger, or subsumed if applying
	// the merge as part of an incoming snapshot.
	f.clearState(ctx)
}

// handle is part of the replicaFlowControlIntegration interface.
func (f *replicaFlowControlIntegrationImpl) handle() (kvflowcontrol.Handle, bool) {
	f.replicaForFlowControl.assertLocked()
	return f.innerHandle, f.innerHandle != nil
}

// refreshStreams disconnects any streams we're not actively replicating to, and
// reconnect previously disconnected streams if we're able.
func (f *replicaFlowControlIntegrationImpl) refreshStreams(ctx context.Context, reason string) {
	f.disconnectStreams(ctx, f.notActivelyReplicatingTo(), reason)
	// TODO(sumeer): we call notActivelyReplicatingTo() again in tryReconnect(),
	// which is wasteful, since refreshStreams is called on every raft tick.
	// Simply pass the return value from the call above to the following method.
	f.tryReconnect(ctx)
}

// notActivelyReplicatingTo lists the replicas that aren't actively receiving
// log entries to append to its log. This encompasses newly added replicas that
// we're still probing to figure out its last index (I4), replicas that are
// pending raft snapshots because the leader has truncated away entries higher
// than its last position (I4), replicas on dead nodes (I2), replicas we're not
// connected to via the raft transport (I1), and paused followers (I3).
func (f *replicaFlowControlIntegrationImpl) notActivelyReplicatingTo() []roachpb.ReplicaDescriptor {
	// These methods return maps, which are mostly lazily allocated, since they
	// are expected to be empty. If we need to avoid even the lazy allocation,
	// we could use the fact that the contents of these maps are used while
	// holding replicaFlowControl.mu, so the allocations could be done once, and
	// kept as members of replicaFlowControl.
	pausedFollowers := f.replicaForFlowControl.getPausedFollowers()
	behindFollowers := f.replicaForFlowControl.getBehindFollowers()
	inactiveFollowers := f.replicaForFlowControl.getInactiveFollowers()
	disconnectedFollowers := f.replicaForFlowControl.getDisconnectedFollowers()

	maintainStreamsForBrokenRaftTransport := f.knobs.MaintainStreamsForBrokenRaftTransport != nil &&
		f.knobs.MaintainStreamsForBrokenRaftTransport()
	maintainStreamsForInactiveFollowers := f.knobs.MaintainStreamsForInactiveFollowers != nil &&
		f.knobs.MaintainStreamsForInactiveFollowers()
	maintainStreamsForBehindFollowers := f.knobs.MaintainStreamsForBehindFollowers != nil &&
		f.knobs.MaintainStreamsForBehindFollowers()

	notActivelyReplicatingTo := make(map[roachpb.ReplicaDescriptor]struct{})
	ourReplicaID := f.replicaForFlowControl.getReplicaID()
	for _, repl := range f.lastKnownReplicas.Descriptors() {
		if repl.ReplicaID == ourReplicaID {
			// NB: We ignore ourselves from the {paused,behind}-followers
			// blocklist (we're the leader), the raft transport check (we're not
			// connected to ourselves through the transport), and the
			// last-updated map. The latter is a bit odd - for followers we
			// update the timestamps when we step a message from them into the
			// local raft group, but for the leader we only update it whenever
			// it ticks. So in workloads where the leader only sees occasional
			// writes, it could see itself as non-live. This is likely
			// unintentional, but we paper over it here anyway.
			continue
		}

		if _, found := pausedFollowers[repl.ReplicaID]; found {
			// As of 7/23, there are no strong guarantees around the set of
			// paused followers we're tracking, nothing that ensures that what's
			// tracked is guaranteed to be a member of the range descriptor.
			// This is why we treat the range descriptor derived state as
			// authoritative (we're using it in the loop iteration and only
			// tracking replicas here that are both paused AND part of the
			// descriptor).
			notActivelyReplicatingTo[repl] = struct{}{}
		}

		if _, found := behindFollowers[repl.ReplicaID]; found &&
			!maintainStreamsForBehindFollowers {
			notActivelyReplicatingTo[repl] = struct{}{}
		}

		if _, found := inactiveFollowers[repl.ReplicaID]; found &&
			!maintainStreamsForInactiveFollowers {
			notActivelyReplicatingTo[repl] = struct{}{}

			// TODO(irfansharif): Experimentally this gets triggered quite often. It
			// might be too sensitive and may result in ineffective flow control as
			// a result. Fix as part of #95563.
		}

		if _, found := disconnectedFollowers[repl.ReplicaID]; found &&
			!maintainStreamsForBrokenRaftTransport {
			notActivelyReplicatingTo[repl] = struct{}{}
		}
	}

	var repls []roachpb.ReplicaDescriptor
	for repl := range notActivelyReplicatingTo {
		repls = append(repls, repl)
	}
	return repls
}

// disconnectStreams disconnects replication streams for the given replicas.
func (f *replicaFlowControlIntegrationImpl) disconnectStreams(
	ctx context.Context, toDisconnect []roachpb.ReplicaDescriptor, reason string,
) {
	ourReplicaID := f.replicaForFlowControl.getReplicaID()
	for _, repl := range toDisconnect {
		if repl.ReplicaID == ourReplicaID {
			log.Fatal(ctx, "replica attempting to disconnect from itself")
		}
		if _, found := f.disconnectedStreams[repl.ReplicaID]; found {
			continue // already disconnected, nothing to do
		}
		stream := kvflowcontrol.Stream{
			TenantID: f.replicaForFlowControl.getTenantID(),
			StoreID:  repl.StoreID,
		}
		f.innerHandle.DisconnectStream(ctx, stream)
		f.disconnectedStreams[repl.ReplicaID] = stream
		log.VInfof(ctx, 1, "tracked disconnected stream: %s (reason: %s)", stream, reason)
	}
}

// tryReconnect tries to reconnect to previously disconnected streams.
func (f *replicaFlowControlIntegrationImpl) tryReconnect(ctx context.Context) {
	var disconnectedRepls []roachpb.ReplicaID
	for replID := range f.disconnectedStreams {
		disconnectedRepls = append(disconnectedRepls, replID)
	}
	if buildutil.CrdbTestBuild {
		slices.Sort(disconnectedRepls) // for determinism in tests
	}

	notActivelyReplicatingTo := f.notActivelyReplicatingTo()
	appliedLogPosition := f.replicaForFlowControl.getAppliedLogPosition()
	for _, replID := range disconnectedRepls {
		if _, ok := f.lastKnownReplicas.GetReplicaDescriptorByID(replID); !ok {
			log.Fatalf(ctx, "%s: tracking %s in disconnected streams despite it not being in descriptor: %s",
				f.replicaForFlowControl.getReplicaID(), replID, f.lastKnownReplicas)
		}

		notReplicatedTo := false
		for _, notReplicatedToRepl := range notActivelyReplicatingTo {
			if replID == notReplicatedToRepl.ReplicaID {
				notReplicatedTo = true
				break
			}
		}
		if notReplicatedTo {
			continue // not being actively replicated to, yet; nothing to reconnect
		}

		// See I1, I2, I3, I3a, I4 from kvflowcontrol/doc.go. Replica is
		// connected to via the RaftTransport (I1), on a live node (I2), not
		// paused (I3), and is being actively replicated to through log entries
		// (I3a, I4). Re-connect so we can start deducting tokens for it.
		stream := f.disconnectedStreams[replID]
		f.innerHandle.ConnectStream(ctx, appliedLogPosition, stream)
		delete(f.disconnectedStreams, replID)
	}
}

// clearState closes the underlying kvflowcontrol.Handle and clears internal
// tracking state.
func (f *replicaFlowControlIntegrationImpl) clearState(ctx context.Context) {
	f.innerHandle.Close(ctx)
	f.innerHandle = nil
	f.lastKnownReplicas = roachpb.MakeReplicaSet(nil)
	f.disconnectedStreams = nil
}

// ===================== RACv2 =====================

type replicaRACv2Integration struct {
	replica *Replica
	// This can be 0, if the leaderID is not known.
	leaderID              roachpb.ReplicaID
	leaderNodeID          roachpb.NodeID
	replicas              kvflowconnectedstream.ReplicaSet
	rcAtLeader            kvflowconnectedstream.RangeController
	raftAdmittedInterface kvflowconnectedstream.RaftAdmittedInterface

	destroyed bool
}

// Should we make all the state transitions in handleRaftReadyRaftMuLocked?
//
//   - Transitioning from follower => leader: r.mu.leaderID = leaderID is being set
//     in handleRaftReadyRaftMuLocked. But Raft knows it is the leader after a Step.
//     The small lag is ok in that we won't be delaying eval.
//
//   - Transitioning from leader => follower: Or the replica getting destroyed.
//     We need to return the flow tokens immediaely, since we won't have them returned
//     via Raft. And we need to stop WaitForEval.
//
// Due to the latter, we call tryUpdateLeader after calling Step.
//
// Replica.raftMu is held. Replica.mu is not held.
func (rr2 *replicaRACv2Integration) tryUpdateLeader(leaderID roachpb.ReplicaID) {
	if rr2.destroyed || leaderID == rr2.leaderID {
		return
	}
	rd, ok := rr2.replicas[leaderID]
	if !ok {
		panic("")
	}
	rr2.leaderNodeID = rd.NodeID
	// INVARIANT: leaderID != rr2.leaderID
	if rr2.leaderID == rr2.replica.replicaID && leaderID != rr2.replica.replicaID {
		// Transition from leader to follower.
		rr2.rcAtLeader.Close()
		rr2.rcAtLeader = nil
	} else if leaderID == rr2.replica.replicaID {
		// Transition from follower to leader.
		var tenantID roachpb.TenantID
		var rn *raft.RawNode
		var leaseholderID roachpb.ReplicaID
		var desc *roachpb.RangeDescriptor
		func() {
			rr2.replica.mu.RLock()
			defer rr2.replica.mu.RUnlock()
			tenantID = rr2.replica.mu.tenantID
			rn = rr2.replica.mu.internalRaftGroup
			leaseholderID = rr2.replica.mu.state.Lease.Replica.ReplicaID
			desc = rr2.replica.mu.state.Desc
		}()
		opts := kvflowconnectedstream.RangeControllerOptions{
			RangeID:           rr2.replica.RangeID,
			TenantID:          tenantID,
			LocalReplicaID:    rr2.replica.replicaID,
			SSTokenCounter:    rr2.replica.store.cfg.RACv2StreamsTokenCounter,
			SendTokensWatcher: rr2.replica.store.cfg.RACv2SendTokensWatcher,
			RaftInterface:     kvflowconnectedstream.NewRaftInterface(rn),
			MessageSender:     rr2.replica,
			Scheduler:         (*racV2Scheduler)(rr2.replica.store.scheduler),
		}
		state := kvflowconnectedstream.RangeControllerInitState{
			ReplicaSet:  descToReplicaSet(desc),
			Leaseholder: leaseholderID,
		}
		rr2.rcAtLeader = kvflowconnectedstream.NewRangeControllerImpl(opts, state)
	}
	rr2.leaderID = leaderID
}

func descToReplicaSet(desc *roachpb.RangeDescriptor) kvflowconnectedstream.ReplicaSet {
	rs := kvflowconnectedstream.ReplicaSet{}
	for _, r := range desc.InternalReplicas {
		rs[r.ReplicaID] = r
	}
	return rs
}

// We need to know when r.mu.destroyStatus is updated, so that we can close,
// and return tokens. RACv1 is handling this state change in
// disconnectReplicationRaftMuLocked. Make sure this is not too late in that
// these flow tokens may be needed by others.
//
// Replica.raftMu is held. Replica.mu is not held.
func (rr2 *replicaRACv2Integration) onDestroy() {
	if rr2.rcAtLeader != nil {
		rr2.rcAtLeader.Close()
		rr2.rcAtLeader = nil
	}
	rr2.destroyed = true
}

// Harmless for this to be eventually consistent, so we do this
// handleRaftReadyRaftMuLocked.
//
// Replica.raftMu is held. Replica.mu is not held.
func (rr2 *replicaRACv2Integration) tryUpdateLeaseholder(replicaID roachpb.ReplicaID) {
	if rr2.rcAtLeader != nil {
		rr2.rcAtLeader.SetLeaseholder(replicaID)
	}
}

// RangeController implements kvadmission.RangeControllerProvider.
//
// TODO(racV2-integration): synchronization.
func (rr2 *replicaRACv2Integration) RangeController() kvflowconnectedstream.RangeController {
	return rr2.rcAtLeader
}

// Replica.raftMu and Replica.mu are held.
func (rr2 *replicaRACv2Integration) onDescChanged(desc *roachpb.RangeDescriptor) {
	rr2.replicas = descToReplicaSet(desc)
	if rr2.rcAtLeader == nil {
		return
	}
	rr2.rcAtLeader.SetReplicas(rr2.replicas)
}

func (rr2 *replicaRACv2Integration) processRangeControllerSchedulerEvent() {
	if rr2.rcAtLeader != nil {
		rr2.rcAtLeader.HandleControllerSchedulerEvent()
	}
}

// entries can be empty, and there may not have been a Ready.
func (rr2 *replicaRACv2Integration) handleRaftEvent(entries []raftpb.Entry) {
	// TODO(racV2-integration):

	/*
			- MsgStoreAppendResp has already been stepped.

			  - [AH2] In handleRaftReadyRaftMuLocked, this queue is used to remove things from
			    notAdmitted. Additionally RaftInterface is queried for stableIndex. If a
			    notAdmitted[i] is empty, admitted[i] = stableIndex, else admitted[i] =
			    notAdmitted[i][0]-1. RaftInterface.AdvanceAdmitted is called, which may
			    return a MsgAppResp.
			  - MsgAppResp is sent.

			We will do a similar piggy-backing:

			- Say m is the raftpb.Message corresponding to the MsgAppResp that was
			  returned from RaftInterface.AdvanceAdmitted
			- Wrap it with the RangeID, and enqueue it for the leader's nodeid in the
			  RaftTransport (this will actually be much simpler than the
			  current RaftTransport.kvflowControl plumbing).
			- Every RaftMessageRequest for the node will piggy-back these.

		add to AdmittedPiggybackStateManager.
	*/

	if rr2.rcAtLeader == nil {
		return
	}
	// TODO(racV2-integration): unnecessary allocation?
	raftEvent := kvflowconnectedstream.MakeRaftEvent(&raft.Ready{
		Entries: entries,
	})
	rr2.rcAtLeader.HandleRaftEvent(raftEvent)
}

// ====== For all replicas, leader or follower ======

// Corresponding to raft indices [first,last].
//
// Only called when RaftMessageRequest is received with a MsgApp. So never
// called for leader.
func (rr2 *replicaRACv2Integration) sideChannelForInheritedPriority(
	first, last uint64, inheritedPri kvflowconnectedstream.RaftPriority,
) {
	// TODO(racV2-integration):
	switch inheritedPri {
	case kvflowconnectedstream.RaftUnusedZeroValuePriority:
	case kvflowconnectedstream.NotSubjectToACForFlowControl:
	case kvflowconnectedstream.PriorityNotInheritedForFlowControl:
	default:
	}
}

func (rr2 *replicaRACv2Integration) admittedLogEntry(
	ctx context.Context,
	origin roachpb.NodeID,
	pri admissionpb.WorkPriority,
	storeID roachpb.StoreID,
	pos admission.LogPosition,
) {
	// TODO(racV2-integration):
	//
	// Can be synchronous within handleRaftReadyRaftMuLocked.

	/*
	   - [AH1] The structs corresponding to the admitted entries are queued in the
	      Replica akin to how we queue Replica.localMsgs and the raftScheduler will be
	      told to do ready processing (there may not be a Raft ready, but this is akin
	      to how handleRaftReadyRaftMuLocked does
	      deliverLocalRaftMsgsRaftMuLockedReplicaMuLocked).

	*/
}

// Subset of kvadmission.Controller needed here.
type acWorkQueue interface {
	AdmitRaftEntryV1OrV2(
		ctx context.Context, tenantID roachpb.TenantID, storeID roachpb.StoreID, rangeID roachpb.RangeID,
		entry raftpb.Entry, meta kvflowcontrolpb.RaftAdmissionMeta, isSideloaded bool)
}

// TODO(racV2-integration): add a ReplicaID too the struct we hand to AC.
// (rangeID, storeID) does not uniquely identify a replica. If a store is
// removed from a range, and then added back in, it will have the same
// (rangeID, storeID), but notifications for old admitted entries, and the
// corresponding messages should be ignored.
//
// TODO(racV2-integration): we will need to ensure that such piggy-backed
// MsgAppResp are matched with the ReplicaID on the leader before stepping
// them into Raft.

// admitRaftEntry is called to admit a raft entry, on any kind of replica. It
// should use the state in the entry, and the information provided via
// sideChannelForInheritedPriority, to enqueue onto queue. It is possible that
// admittedLogEntry will be called while AdmitRaftEntryV1OrV2 is ongoing.
func (rr2 *replicaRACv2Integration) admitRaftEntry(
	ctx context.Context,
	queue acWorkQueue,
	tenantID roachpb.TenantID,
	storeID roachpb.StoreID,
	rangeID roachpb.RangeID,
	entry raftpb.Entry,
) {
	// TODO(racV2-integration):

	/*
		- calls AdmitRaftEntry for the entries in raftpb.MsgStorageAppend. In
		addition to queueing in the StoreWorkQueue, this will track (on the
		Replica): notAdmitted [numFlowPri][]uint64, where the []uint64 is in
		increasing entry index order. We don't bother with the term in the
		tracking (unlike the RaftLogPosition struct in the current code). These
			slices are appended to here, based on the entries in MsgStorageAppend.
	*/

}

// TODO: write the code here and then move it to a file in kvflowconnectedstream.

// AdmittedPiggybackStateManager ...
//
// The following is not a replica level object. There is one per node. Move
// this elsehwere.
type AdmittedPiggybackStateManager interface {
	// AddMsgForRange ...
	// m must be a MsgAppResp.
	//
	// Keeps for each rangeID the latest m. There shouldn't be multiple replicas
	// for a range at this node, so that is sufficient. Also, keys these by leaderNodeID,
	// so can be grabbed to piggyback to that range.
	AddMsgForRange(rangeID roachpb.RangeID, leaderNodeID roachpb.NodeID, m raftpb.Message)
	// PopMsgsForNode ...
	// TODO: see how kvflowdispatch handles this.
	PopMsgsForNode(nodeID roachpb.NodeID) []kvflowcontrolpb.AdmittedForRangeRACv2
	// NodesWithMsgs is used to periodically drop msgs from disconnected nodes.
	// See RaftTransport.dropFlowTokensForDisconnectedNodes.
	NodesWithMsgs() []roachpb.NodeID
}

// TODO: implement.

func NewAdmittedPiggybackStateManager() AdmittedPiggybackStateManager {
	return nil
}
