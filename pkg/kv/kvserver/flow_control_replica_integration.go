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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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

// replicaRACv2Integration encapsulated the per-Replica state for integrating
// with RACv2. It exists on both the leader and followers, and is a field in
// Replica (Replica.raftMu.racV2Integration).
//
// It is partly initialized with only the *Replica. The remaining is lazily
// initialized, as described below.
type replicaRACv2Integration struct {
	// The Replica it is a part of.
	replica             *Replica
	admittedPiggybacker AdmittedPiggybackStateManager

	// raftAdmittedInterface is exercised on all replicas.
	// NOTE: raftAdmittedInterface should be called when the replica.raftMu is
	// held.
	raftAdmittedInterface kvflowconnectedstream.RaftAdmittedInterface
	rawNode               *raft.RawNode

	// The fields below are accessed while holding the mutex. Note that the lock
	// ordering between this mutex and replica.RaftMu: replica.RaftMu < this.mu.
	mu struct {
		syncutil.Mutex

		// rcAtLeader is non-nil iff leaderID is equal to Replica.replicaID.
		rcAtLeader kvflowconnectedstream.RangeController
		// Transitions once from false => true when the Replica is destroyed.
		destroyed bool
		// Lazily initialized in onDescChanged, which must happen when the Replica
		// becomes initialized. Until replicas is non-nil, most events are ignored.
		replicas kvflowconnectedstream.ReplicaSet
		// leaderID is set in tryUpdateLeader.
		leaderID roachpb.ReplicaID
		// leaderNodeID is a function of leaderID and replicas. It is set when
		// leaderID is non-zero and replicas is initialized.
		leaderNodeID                roachpb.NodeID
		leaderStoreID               roachpb.StoreID
		scheduledAdmittedProcessing bool
		waitingForAdmissionState    waitingForAdmissionState
		// NOTE: priorityInheritanceState should only be accessed while holding
		// replica.raftMu.
		priorityInheritanceState     priorityInheritanceState
		enqueuedPiggybackedResponses map[roachpb.ReplicaID]raftpb.Message
	}
}

// We considered making this state transition in handleRaftReadyRaftMuLocked:
//
//   - Transitioning from follower => leader: r.mu.leaderID = leaderID is being set
//     in handleRaftReadyRaftMuLocked. But Raft knows it is the leader after a Step.
//     The small lag is ok in that we won't be delaying eval.
//
//   - Transitioning from leader => follower: Or the replica getting destroyed.
//     We need to return the flow tokens immediately, since we won't have them returned
//     via Raft. And we need to stop WaitForEval.
//
// Due to the latter, we call tryUpdateLeader after calling Step, instead of
// in handleRaftReadyRaftMuLocked.
//
// Must not be called during Ready processing, for it to compute the correct
// value of nextRaftIndex.
//
// Replica.raftMu is held. Replica.mu is not held.
func (rr2 *replicaRACv2Integration) tryUpdateLeader(
	ctx context.Context, leaderID roachpb.ReplicaID, force bool,
) {
	rr2.replica.raftMu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	var nextRaftIndex uint64
	if leaderID == rr2.replica.replicaID {
		nextRaftIndex = rr2.rawNode.NextUnstableIndex()
	}
	rr2.tryUpdateLeaderLocked(ctx, leaderID, force, nextRaftIndex)
}

// nextRaftIndex is used only if this replica has become the leader.
func (rr2 *replicaRACv2Integration) tryUpdateLeaderLocked(
	ctx context.Context, leaderID roachpb.ReplicaID, force bool, nextRaftIndex uint64,
) {
	if rr2.mu.destroyed || (leaderID == rr2.mu.leaderID && !force) {
		return
	}
	rr2.mu.leaderID = leaderID
	if rr2.mu.replicas == nil {
		// The state machine has not been initialized yet, so nothing more to do.
		return
	}
	if rr2.mu.leaderID == 0 {
		rr2.mu.leaderNodeID = 0
		rr2.mu.leaderStoreID = 0
	} else {
		rd, ok := rr2.mu.replicas[leaderID]
		if !ok {
			// TODO(racV2-integration): This is a bug. We should not be in this
			// state. Rarely hitting this in TestRACV2Basic/relocate_range --stress.
			//
			//  leader=4 is not in the set of replicas=[(n1,s1):1,(n2,s2):2LEARNER,(n3,s3):3LEARNER]
			//  desc=r69:/{Table/Max-Max} [(n1,s1):1, (n2,s2):2LEARNER, (n3,s3):3LEARNER, next=4, gen=3]
			//
			log.Errorf(ctx,
				"leader=%d is not in the set of replicas=%v desc=%v",
				leaderID, rr2.mu.replicas, rr2.replica.Desc())
			return
		}
		rr2.mu.leaderNodeID = rd.NodeID
		rr2.mu.leaderStoreID = rd.StoreID
	}
	if rr2.mu.leaderID != rr2.replica.replicaID {
		if rr2.mu.rcAtLeader != nil {
			// Transition from leader to follower.
			rr2.mu.rcAtLeader.Close(ctx)
			rr2.mu.rcAtLeader = nil
		}
	} else {
		if rr2.mu.rcAtLeader == nil {
			// Transition from follower to leader.
			var tenantID roachpb.TenantID
			var rn *raft.RawNode
			var leaseholderID roachpb.ReplicaID
			func() {
				rr2.replica.mu.RLock()
				defer rr2.replica.mu.RUnlock()
				tenantID = rr2.replica.mu.tenantID
				rn = rr2.replica.mu.internalRaftGroup
				leaseholderID = rr2.replica.mu.state.Lease.Replica.ReplicaID
			}()
			opts := kvflowconnectedstream.RangeControllerOptions{
				RangeID:           rr2.replica.RangeID,
				TenantID:          tenantID,
				LocalReplicaID:    rr2.replica.replicaID,
				SSTokenCounter:    rr2.replica.store.cfg.RACv2StreamsTokenCounter,
				SendTokensWatcher: rr2.replica.store.cfg.RACv2SendTokensWatcher,
				RaftInterface:     kvflowconnectedstream.NewRaftNode(rn),
				MessageSender:     rr2.replica,
				Scheduler:         (*racV2Scheduler)(rr2.replica.store.scheduler),
			}
			state := kvflowconnectedstream.RangeControllerInitState{
				ReplicaSet:  rr2.mu.replicas,
				Leaseholder: leaseholderID,
			}
			rr2.mu.rcAtLeader = kvflowconnectedstream.NewRangeControllerImpl(ctx, opts, state, nextRaftIndex)
		}
	}
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
// disconnectReplicationRaftMuLocked, and so is RACv2. Make sure this is not
// too late in that these flow tokens may be needed by others.
//
// Replica.raftMu is held. Replica.mu is not held.
func (rr2 *replicaRACv2Integration) onDestroy(ctx context.Context) {
	rr2.replica.raftMu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	if rr2.mu.rcAtLeader != nil {
		rr2.mu.rcAtLeader.Close(ctx)
	}

	// We want to retain the mutex throughout this method scope. If we were to
	// swap rr2 with replicaRACv2Integration{}, we would lose the mutex and any
	// caller waiting on the mutex would deadlock.
	rr2.mu.rcAtLeader = nil
	rr2.mu.replicas = nil
	rr2.mu.leaderID = 0
	rr2.mu.leaderNodeID = 0
	rr2.mu.leaderStoreID = 0
	rr2.mu.scheduledAdmittedProcessing = false
	rr2.mu.waitingForAdmissionState = waitingForAdmissionState{}
	rr2.mu.priorityInheritanceState = priorityInheritanceState{}
	rr2.mu.enqueuedPiggybackedResponses = nil
	rr2.mu.destroyed = true
}

// Harmless for this to be eventually consistent, so we do this in
// handleRaftReadyRaftMuLocked.
//
// Replica.raftMu is held. Replica.mu is not held.
//
// TODO(racV2-integration): Update naming throughout to indicate whether raft /
// replica mu are held.
func (rr2 *replicaRACv2Integration) tryUpdateLeaseholder(
	ctx context.Context, replicaID roachpb.ReplicaID,
) {
	rr2.replica.raftMu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	if rr2.mu.rcAtLeader != nil {
		rr2.mu.rcAtLeader.SetLeaseholder(ctx, replicaID)
	}
}

// RangeController implements kvadmission.RangeControllerProvider. This is
// needed for eval.
func (rr2 *replicaRACv2Integration) RangeController() kvflowconnectedstream.RangeController {
	if rr2 == nil {
		// TODO(racV2-integration): This is a bug. Hit this running kv0 @ 1k rate
		// on a 3 node cluster.
		return nil
	}
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	return rr2.mu.rcAtLeader
}

// Replica.raftMu and Replica.mu are held.
//
// onDescChanged must not be called during Ready processing, since it will
// mess up the computation of the nextRaftIndex.
func (rr2 *replicaRACv2Integration) onDescChanged(
	ctx context.Context, desc *roachpb.RangeDescriptor,
) {
	rr2.replica.raftMu.AssertHeld()
	rr2.replica.mu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	wasUninitialized := false
	if rr2.mu.replicas == nil {
		wasUninitialized = true
		var rn *raft.RawNode
		func() {
			// TODO(racV2-integration): On init, the raft and replica mu is already
			// held so we don't lock. In other cases, they may not be.
			//
			// TODO(sumeer): what does this "so we don't lock" mean. Is this a stale
			// comment, given replicaRACv2Integration never locks raftMu.
			rn = rr2.replica.mu.internalRaftGroup
		}()
		rr2.raftAdmittedInterface = kvflowconnectedstream.NewRaftNode(rn)
		rr2.rawNode = rn
	}
	rr2.mu.replicas = descToReplicaSet(desc)
	if wasUninitialized {
		if rr2.mu.leaderID != 0 {
			rr2.tryUpdateLeaderLocked(ctx, rr2.mu.leaderID, true, rr2.rawNode.NextUnstableIndex())
		}
	} else {
		if rr2.mu.rcAtLeader == nil {
			return
		}
		rr2.mu.rcAtLeader.SetReplicas(ctx, rr2.mu.replicas)
	}
}

// raftMu is held.
func (rr2 *replicaRACv2Integration) processRangeControllerSchedulerEvent(ctx context.Context) {
	rr2.replica.raftMu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	if rr2.mu.rcAtLeader != nil {
		rr2.mu.rcAtLeader.HandleControllerSchedulerEvent(ctx)
	}
}

// raftMu is not held.
func (rr2 *replicaRACv2Integration) enqueuePiggybackedAdmitted(msg raftpb.Message) {
	rr2.mu.Lock()
	defer rr2.mu.Unlock()
	// Only need to keep the latest message from a replica.
	rr2.mu.enqueuedPiggybackedResponses[roachpb.ReplicaID(msg.From)] = msg
}

// raftMu is held.
func (rr2 *replicaRACv2Integration) processPiggybackedAdmitted(ctx context.Context) bool {
	rr2.replica.raftMu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	if len(rr2.mu.enqueuedPiggybackedResponses) == 0 {
		return false
	}
	_ = rr2.replica.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		for k, m := range rr2.mu.enqueuedPiggybackedResponses {
			err := raftGroup.Step(m)
			if err != nil {
				log.Errorf(ctx, "%s", err)
			}
			delete(rr2.mu.enqueuedPiggybackedResponses, k)
		}
		return true, nil
	})
	return true
}

// raftMu is held.
// entries can be empty, and there may not have been a Ready.
func (rr2 *replicaRACv2Integration) handleRaftEvent(ctx context.Context, entries []raftpb.Entry) {
	rr2.replica.raftMu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	if rr2.rawNode != nil {
		// When there is a single replica in the raft group, it decides it is the
		// leader without any external communication. We discover the leaderID
		// here.
		st := rr2.rawNode.BasicStatus()
		if roachpb.ReplicaID(st.Lead) != rr2.mu.leaderID {
			var nextRaftIndex uint64
			if roachpb.ReplicaID(st.Lead) == rr2.replica.replicaID {
				nextRaftIndex = rr2.rawNode.NextUnstableIndex()
				if len(entries) > 0 {
					nextRaftIndex = entries[0].Index
				}
			}
			rr2.tryUpdateLeaderLocked(ctx, roachpb.ReplicaID(st.Lead), false, nextRaftIndex)
		}
	}
	if rr2.mu.replicas == nil {
		return
	}
	// Admitted processing, on all replicas.
	func() {
		defer func() {
			rr2.mu.scheduledAdmittedProcessing = false
		}()
		// If there was a recent MsgStoreAppendResp that triggered this Ready
		// processing, it has already been stepped, so the stable index would have
		// advanced.
		rr2.replica.raftMu.AssertHeld()
		sindex := rr2.raftAdmittedInterface.StableIndex()
		nextAdmitted := rr2.mu.waitingForAdmissionState.computeAdmitted(sindex)
		if admittedIncreased(rr2.raftAdmittedInterface.GetAdmitted(), nextAdmitted) {
			msgAppResp := rr2.raftAdmittedInterface.SetAdmitted(nextAdmitted)
			if rr2.mu.leaderID != 0 && rr2.mu.leaderID != rr2.replica.replicaID {
				rr2.admittedPiggybacker.AddMsgForRange(rr2.replica.RangeID, rr2.mu.leaderNodeID, rr2.mu.leaderStoreID, msgAppResp)
			}
			// rr2.mu.leaderID can be 0 if there is no known leader.
			// If the local replica is the leader we have already updated admitted.
		}
	}()

	if rr2.mu.rcAtLeader == nil {
		return
	}
	// TODO(racV2-integration): unnecessary allocation?
	raftEvent := kvflowconnectedstream.MakeRaftEvent(&raft.Ready{
		Entries: entries,
	})
	rr2.mu.rcAtLeader.HandleRaftEvent(ctx, raftEvent)
}

// Corresponding to raft indices [first, last].
//
// Only called when RaftMessageRequest is received with a MsgApp. So never
// called for leader.
//
// raftMu is held.
func (rr2 *replicaRACv2Integration) sideChannelForInheritedPriority(
	first, last uint64, inheritedPri uint8,
) {
	rr2.replica.raftMu.AssertHeld()
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	if inheritedPri == 0 {
		return
	}
	pri := kvflowcontrolpb.UndoRaftPriorityConversionForUnusedZero(inheritedPri)
	rr2.mu.priorityInheritanceState.sideChannelForInheritedPriority(first, last, pri)
}

// raftMu is not held.
func (rr2 *replicaRACv2Integration) admittedLogEntry(
	ctx context.Context, pri kvflowcontrolpb.RaftPriority, index uint64,
) {
	rr2.mu.Lock()
	defer rr2.mu.Unlock()

	removeOk := rr2.mu.waitingForAdmissionState.remove(index, pri)
	scheduledAdmittedProcessing := rr2.mu.scheduledAdmittedProcessing
	if removeOk && !scheduledAdmittedProcessing {
		// TODO(racV2-integration): performance optimization: this ready
		// enqueueing is wasteful if this is synchronous within
		// handeRaftReadyRaftMuLocked since we can't advance admitted until the
		// persisted log advances.
		//
		// TODO(racV2-integration): We may not want to hold the mu while calling
		// EnqueueRaftReady.
		rr2.replica.store.scheduler.EnqueueRaftReady(rr2.replica.RangeID)
		rr2.mu.scheduledAdmittedProcessing = true
	}
}

// Subset of kvadmission.Controller needed here.
// We don't actually use anything other than the RaftPriority and index
type acWorkQueue interface {
	AdmitRaftEntryV1OrV2(
		ctx context.Context, tenantID roachpb.TenantID, storeID roachpb.StoreID, rangeID roachpb.RangeID,
		entry raftpb.Entry, meta kvflowcontrolpb.RaftAdmissionMeta, isSideloaded bool)
}

// TODO(racV2-integration): add a ReplicaID to the struct we hand to AC.
// (rangeID, storeID) does not uniquely identify a replica. If a store is
// removed from a range, and then added back in, it will have the same
// (rangeID, storeID), but notifications for old admitted entries, and the
// corresponding messages should be ignored.
//
// TODO(racV2-integration): we will need to ensure that such piggy-backed
// MsgAppResp are matched with the ReplicaID on the leader before stepping
// them into Raft.

// admitRaftEntry is called to admit a raft entry, on any kind of replica. It
// uses the state in the entry, and the information provided via
// sideChannelForInheritedPriority, to enqueue onto queue. It is possible that
// admittedLogEntry will be called while AdmitRaftEntryV1OrV2 is ongoing.
//
// This should be called while holding Replica.raftMu.
func (rr2 *replicaRACv2Integration) admitRaftEntry(
	ctx context.Context,
	queue acWorkQueue,
	tenantID roachpb.TenantID,
	storeID roachpb.StoreID,
	rangeID roachpb.RangeID,
	entry raftpb.Entry,
) {
	rr2.replica.raftMu.AssertHeld()
	typ, priBits, err := raftlog.EncodingOf(entry)
	if err != nil {
		panic(errors.AssertionFailedf("unable to determine raft command encoding: %v", err))
	}
	if !typ.UsesAdmissionControl() {
		return // nothing to do
	}
	if typ != raftlog.EntryEncodingStandardWithRaftPriority &&
		typ != raftlog.EntryEncodingSideloadedWithRaftPriority {
		panic("expected a RACv2 encoding")
	}
	meta, err := raftlog.DecodeRaftAdmissionMeta(entry.Data)
	if err != nil {
		panic(errors.AssertionFailedf("unable to decode raft command admission data: %v", err))
	}
	if kvflowcontrolpb.RaftPriority(meta.AdmissionPriority) != priBits {
		panic("inconsistent priorities")
	}
	func() {
		rr2.mu.Lock()
		defer rr2.mu.Unlock()

		pri, doAC := rr2.mu.priorityInheritanceState.getEffectivePriority(entry.Index, priBits)
		if !doAC {
			return
		}
		meta.AdmissionPriority = int32(kvflowcontrolpb.RaftPriorityToAdmissionPriority(pri))
		rr2.mu.waitingForAdmissionState.add(entry.Index, pri)
	}()

	queue.AdmitRaftEntryV1OrV2(ctx, tenantID, storeID, rangeID, entry, meta, typ.IsSideloaded())
}

// priorityInheritanceState records the mapping from raft log indices to their
// inherited priorities.
//
// The lifetime of a particular log index begins when this entry is appended to
// the in-memory log, and ends when the entry is sent to storage with the
// priority that is extracted from this struct. The same log index can reappear
// in this struct multiple times, typically when a new raft leader overwrites a
// suffix of the log proposed by the previous leader.
type priorityInheritanceState struct {
	// intervals keeps the index-to-priority mapping in the increasing order of
	// log indices. Contiguous index runs with the same priority are compressed.
	//
	// A new [first, last] interval added here will throw away existing state for
	// indices >= first. A read at index i will cause a prefix of indices <= i to
	// be discarded.
	intervals []indexInterval
}

type indexInterval struct {
	first uint64
	last  uint64
	pri   kvflowcontrolpb.RaftPriority
}

// sideChannelForInheritedPriority is called on follower replicas, and is used
// to provide information about priority inheritance for the [first, last] range
// of log indices.
//
// NB: Should only be called while holding replicaFlowControlIntegrationImpl.mu.
func (p *priorityInheritanceState) sideChannelForInheritedPriority(
	first, last uint64, inheritedPri kvflowcontrolpb.RaftPriority,
) {
	// Drop all intervals starting at or after the first index. Do it from the
	// right end, so that the append-only case is the fast path. When a suffix of
	// entries is overwritten, the cost of this loop is an amortized O(1).
	keep := len(p.intervals)
	for ; keep != 0 && p.intervals[keep-1].first >= first; keep-- {
	}
	p.intervals = p.intervals[:keep]
	// Partially subsume the last interval if it overlaps with the added one.
	if keep != 0 && p.intervals[keep-1].last+1 >= first {
		// Extend the last interval if it has the same priority, instead of
		// appending a new one that immediately follows it.
		if p.intervals[keep-1].pri == inheritedPri {
			p.intervals[keep-1].last = last
			return
		}
		p.intervals[keep-1].last = first - 1
	}
	// Append the new interval.
	p.intervals = append(p.intervals, indexInterval{
		first: first, last: last, pri: inheritedPri,
	})
}

func (p *priorityInheritanceState) getEffectivePriority(
	index uint64, pri kvflowcontrolpb.RaftPriority,
) (effectivePri kvflowcontrolpb.RaftPriority, doAC bool) {
	// Garbage collect intervals ending before the given index.
	drop := 0
	for ln := len(p.intervals); drop < ln && p.intervals[drop].last < index; drop++ {
	}
	p.intervals = p.intervals[drop:]
	// If there is no interval containing the index, return the default priority.
	if len(p.intervals) == 0 || p.intervals[0].first > index {
		return pri, true
	}
	inherited := p.intervals[0].pri
	// Remove the prefix of indices <= index.
	if p.intervals[0].last > index {
		p.intervals[0].first = index + 1
	} else {
		p.intervals = p.intervals[1:]
	}

	switch inherited {
	case kvflowcontrolpb.NotSubjectToACForFlowControl:
		return 0, false
	case kvflowcontrolpb.PriorityNotInheritedForFlowControl:
		return pri, true
	default:
		return inherited, true
	}
}

// waitingForAdmissionState records the indices of individual entries that are
// waiting for admission in the AC queues.
type waitingForAdmissionState struct {
	// The indices for each priority are in increasing index order.
	//
	// Say the indices for a priority are 3, 6, 10. We track the individual
	// indices since when 3 is popped, we can advance admitted for that priority
	// to 5. When 6 is popped, admitted can advance to 9. We should never have a
	// situation where indices are popped out of order, but we tolerate that by
	// popping the prefix upto the index being popped.
	waiting [kvflowcontrolpb.NumRaftPriorities][]uint64
}

func (w *waitingForAdmissionState) remove(
	index uint64, pri kvflowcontrolpb.RaftPriority,
) (admittedMayAdvance bool) {
	pos, found := slices.BinarySearch(w.waiting[pri], index)
	if !found {
		return false
	}
	w.waiting[pri] = w.waiting[pri][pos+1:]
	return true
}

func (w *waitingForAdmissionState) add(index uint64, pri kvflowcontrolpb.RaftPriority) {
	pos, found := slices.BinarySearch(w.waiting[pri], index)
	if found {
		return
	}
	slices.Insert(w.waiting[pri], pos, index)
}

func (w *waitingForAdmissionState) computeAdmitted(
	stableIndex uint64,
) [kvflowcontrolpb.NumRaftPriorities]uint64 {
	var admitted [kvflowcontrolpb.NumRaftPriorities]uint64
	for i := range w.waiting {
		admitted[i] = stableIndex
		if len(w.waiting[i]) > 0 {
			upperBoundAdmitted := w.waiting[i][0] - 1
			if upperBoundAdmitted < admitted[i] {
				admitted[i] = upperBoundAdmitted
			}
		}
	}
	return admitted
}

func admittedIncreased(prev, next [kvflowcontrolpb.NumRaftPriorities]uint64) bool {
	for i := range prev {
		if prev[i] < next[i] {
			return true
		}
	}
	return false
}
