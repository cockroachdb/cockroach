// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"cmp"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	if fn := f.knobs.V1.MaintainStreamsForBrokenRaftTransport; fn != nil && fn() {
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

	maintainStreamsForBrokenRaftTransport := f.knobs.V1.MaintainStreamsForBrokenRaftTransport != nil &&
		f.knobs.V1.MaintainStreamsForBrokenRaftTransport()
	maintainStreamsForInactiveFollowers := f.knobs.V1.MaintainStreamsForInactiveFollowers != nil &&
		f.knobs.V1.MaintainStreamsForInactiveFollowers()
	maintainStreamsForBehindFollowers := f.knobs.V1.MaintainStreamsForBehindFollowers != nil &&
		f.knobs.V1.MaintainStreamsForBehindFollowers()

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

type noopReplicaFlowControlIntegration struct{}

func (n noopReplicaFlowControlIntegration) onBecameLeader(context.Context)    {}
func (n noopReplicaFlowControlIntegration) onBecameFollower(context.Context)  {}
func (n noopReplicaFlowControlIntegration) onDescChanged(context.Context)     {}
func (n noopReplicaFlowControlIntegration) onFollowersPaused(context.Context) {}
func (n noopReplicaFlowControlIntegration) onRaftTransportDisconnected(
	context.Context, ...roachpb.StoreID,
) {
}
func (n noopReplicaFlowControlIntegration) onRaftTicked(context.Context) {}
func (n noopReplicaFlowControlIntegration) onDestroyed(context.Context)  {}
func (n noopReplicaFlowControlIntegration) handle() (kvflowcontrol.Handle, bool) {
	return nil, false
}

type replicaForRACv2 Replica

var _ replica_rac2.ReplicaForTesting = &replicaForRACv2{}

// IsScratchRange implements replica_rac2.ReplicaForTesting.
func (r *replicaForRACv2) IsScratchRange() bool {
	return (*Replica)(r).IsScratchRange()
}

// MuLock implements replica_rac2.ReplicaForRaftNode.
func (r *replicaForRACv2) MuLock() {
	r.mu.Lock()
}

// MuUnlock implements replica_rac2.ReplicaForRaftNode.
func (r *replicaForRACv2) MuUnlock() {
	r.mu.Unlock()
}
