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
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	rafttracker "go.etcd.io/raft/v3/tracker"
)

// TODO(irfansharif): Write integration tests, walking through
// kvflowcontrol/doc.go. Do this as part of #95563. Randomize an in-memory
// workload with various chaos events, like nodes dying, streams breaking,
// splits, merges, etc. and assert stable flow tokens. Leader replica removing
// itself from descriptor. Uninitialized replica below raft for which we've
// deducted flow tokens for (dealt with by looking at StateReplicate). Dropped
// proposals -- we should only be deducting tokens once submitting to raft. But
// if leader's raft messages to follower get dropped (and not vice versa),
// leader will still see follower as active and not disconnect streams. Has this
// changed with us upgrading asymmetric partitions to bidirectional ones?

// replicaFlowControlIntegration is used to integrate with replication flow
// control. It's intercepts various points in a replica's lifecycle, like it
// acquiring raft leadership or losing it, or its raft membership changing, etc.
//
// Accessing it requires Replica.mu to be held, exclusively (this is asserted on
// in the canonical implementation).
type replicaFlowControlIntegration interface {
	handle() (kvflowcontrol.Handle, bool)
	onBecameLeader(context.Context)
	onBecameFollower(context.Context)
	onDescChanged(context.Context)
	onFollowersPaused(context.Context)
	onReplicaDestroyed(context.Context)
	onProposalQuotaUpdated(context.Context)
	onRaftTransportDisconnected(context.Context, ...roachpb.StoreID)
}

var _ replicaFlowControlIntegration = &replicaFlowControlIntegrationImpl{}

type replicaFlowControlIntegrationImpl struct {
	replicaForFlowControl replicaForFlowControl
	handleFactory         kvflowcontrol.HandleFactory

	innerHandle         kvflowcontrol.Handle
	lastKnownReplicas   roachpb.ReplicaSet
	disconnectedStreams map[roachpb.ReplicaID]kvflowcontrol.Stream
}

func newReplicaFlowControlIntegration(
	replicaForFlowControl replicaForFlowControl, handleFactory kvflowcontrol.HandleFactory,
) *replicaFlowControlIntegrationImpl {
	return &replicaFlowControlIntegrationImpl{
		replicaForFlowControl: replicaForFlowControl,
		handleFactory:         handleFactory,
	}
}

func (f *replicaFlowControlIntegrationImpl) handle() (kvflowcontrol.Handle, bool) {
	f.replicaForFlowControl.assertLocked()
	return f.innerHandle, f.innerHandle != nil
}

func (f *replicaFlowControlIntegrationImpl) onBecameLeader(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle != nil {
		log.Fatal(ctx, "flow control handle was not nil before becoming the leader")
	}
	if !f.replicaForFlowControl.getTenantID().IsSet() {
		log.Fatal(ctx, "unset tenant ID")
	}

	// See I5 from kvflowcontrol/doc.go. The per-replica kvflowcontrol.Handle is
	// tied to the lifetime of a leaseholder replica having raft leadership.
	f.innerHandle = f.handleFactory.NewHandle(
		f.replicaForFlowControl.getRangeID(),
		f.replicaForFlowControl.getTenantID(),
	)
	f.lastKnownReplicas = f.replicaForFlowControl.getDescriptor().Replicas()
	f.disconnectedStreams = make(map[roachpb.ReplicaID]kvflowcontrol.Stream)

	appliedLogPosition := f.replicaForFlowControl.getAppliedLogPosition()
	for _, desc := range f.replicaForFlowControl.getDescriptor().Replicas().Descriptors() {
		// Start off every remote stream as disconnected. Later we'll try to
		// reconnect them.
		stream := kvflowcontrol.Stream{
			TenantID: f.replicaForFlowControl.getTenantID(),
			StoreID:  desc.StoreID,
		}
		if f.replicaForFlowControl.getReplicaID() != desc.ReplicaID {
			f.disconnectedStreams[desc.ReplicaID] = stream
			continue
		}
		// Connect to the local stream.
		f.innerHandle.ConnectStream(ctx, appliedLogPosition, stream)
	}
	f.tryReconnect(ctx)

	if log.V(1) {
		var disconnected []kvflowcontrol.Stream
		for _, stream := range f.disconnectedStreams {
			disconnected = append(disconnected, stream)
		}
		sort.Slice(disconnected, func(i, j int) bool {
			return disconnected[i].StoreID < disconnected[j].StoreID
		})
		log.Infof(ctx, "assumed raft leadership: initializing flow handle for %s starting at %s (disconnected streams: %s)",
			f.replicaForFlowControl.getDescriptor(), appliedLogPosition, disconnected)
	}
}

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
	f.innerHandle.Close(ctx)
	f.innerHandle = nil
	f.lastKnownReplicas = roachpb.MakeReplicaSet(nil)
	f.disconnectedStreams = nil
}

func (f *replicaFlowControlIntegrationImpl) onDescChanged(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	addedReplicas, removedReplicas := f.lastKnownReplicas.Difference(
		f.replicaForFlowControl.getDescriptor().Replicas(),
	)
	for _, repl := range removedReplicas {
		if repl.ReplicaID == f.replicaForFlowControl.getReplicaID() {
			// We're observing ourselves get removed from the raft group, but
			// are still retaining raft leadership. Close the underlying handle
			// and bail.
			//
			// TODO(irfansharif): Is this even possible?
			f.innerHandle.Close(ctx)
			f.innerHandle = nil
			f.lastKnownReplicas = roachpb.MakeReplicaSet(nil)
			f.disconnectedStreams = nil
			return
		}
		// See I10 from kvflowcontrol/doc.go. We stop deducting flow tokens for
		// replicas that are no longer part of the raft group, free-ing up all
		// held tokens.
		f.innerHandle.DisconnectStream(ctx, kvflowcontrol.Stream{
			TenantID: f.replicaForFlowControl.getTenantID(),
			StoreID:  repl.StoreID,
		})
		delete(f.disconnectedStreams, repl.ReplicaID)
	}

	for _, repl := range addedReplicas {
		// Start off new replicas as disconnected. We'll subsequently try to
		// re-add them, once we know their log positions and consider them
		// sufficiently caught up. See I3a from kvflowcontrol/doc.go.
		if repl.ReplicaID == f.replicaForFlowControl.getReplicaID() {
			log.Fatalf(ctx, "observed replica adding itself to the range descriptor")
		}
		if _, found := f.disconnectedStreams[repl.ReplicaID]; found {
			continue // already disconnected, nothing to do
		}
		stream := kvflowcontrol.Stream{
			TenantID: f.replicaForFlowControl.getTenantID(),
			StoreID:  repl.StoreID,
		}
		f.disconnectedStreams[repl.ReplicaID] = stream
	}
	if len(addedReplicas) > 0 || len(removedReplicas) > 0 {
		log.VInfof(ctx, 1, "desc changed from %s to %s: added=%s removed=%s",
			f.lastKnownReplicas, f.replicaForFlowControl.getDescriptor(), addedReplicas, removedReplicas,
		)
	}
	f.lastKnownReplicas = f.replicaForFlowControl.getDescriptor().Replicas()
}

func (f *replicaFlowControlIntegrationImpl) onFollowersPaused(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	var toDisconnect []roachpb.ReplicaDescriptor
	// See I3 from kvflowcontrol/doc.go. We don't deduct flow tokens for
	// replication traffic that's not headed to paused replicas.
	for replID := range f.replicaForFlowControl.getPausedFollowers() {
		repl, ok := f.lastKnownReplicas.GetReplicaDescriptorByID(replID)
		if !ok {
			// As of 4/23, we don't make any strong guarantees around the set of
			// paused followers we're tracking, nothing that ensures that what's
			// tracked is guaranteed to be a member of the range descriptor. We
			// treat the range descriptor derived state as authoritative.
			continue
		}
		if repl.ReplicaID == f.replicaForFlowControl.getReplicaID() {
			log.Fatalf(ctx, "observed replica pausing replication traffic to itself")
		}
		toDisconnect = append(toDisconnect, repl)
	}

	f.disconnectStreams(ctx, toDisconnect, "paused followers")
	f.tryReconnect(ctx)
}

func (f *replicaFlowControlIntegrationImpl) onReplicaDestroyed(ctx context.Context) {
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
	f.innerHandle.Close(ctx)
	f.innerHandle = nil
	f.lastKnownReplicas = roachpb.MakeReplicaSet(nil)
	f.disconnectedStreams = nil
}

func (f *replicaFlowControlIntegrationImpl) onProposalQuotaUpdated(ctx context.Context) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	var toDisconnect []roachpb.ReplicaDescriptor

	// Disconnect any recently inactive followers.
	//
	// TODO(irfansharif): Experimentally this gets triggered quite often. It
	// might be too sensitive and may result in ineffective flow control as
	// a result. Fix as part of #95563.
	for _, repl := range f.lastKnownReplicas.Descriptors() {
		if f.replicaForFlowControl.isFollowerLive(ctx, repl.ReplicaID) {
			continue // nothing to do
		}
		if repl.ReplicaID == f.replicaForFlowControl.getReplicaID() {
			// NB: We ignore ourselves from this last-updated map. For followers
			// we update the timestamps when we step a message from them into
			// the local raft group, but for the leader we only update it
			// whenever it ticks. So in workloads where the leader only sees
			// occasional writes, it could see itself as non-live. This is
			// likely unintentional, but we paper over it here.
			continue // nothing to do
		}
		toDisconnect = append(toDisconnect, repl)
	}
	f.disconnectStreams(ctx, toDisconnect, "inactive followers")

	// Disconnect any streams we're not actively replicating to.
	toDisconnect = nil
	for _, replID := range f.notActivelyReplicatingTo() {
		repl, ok := f.lastKnownReplicas.GetReplicaDescriptorByID(replID)
		if !ok {
			continue
		}
		if repl.ReplicaID == f.replicaForFlowControl.getReplicaID() {
			log.Fatalf(ctx, "leader replica observed that it was not being actively replicated to")
		}
		toDisconnect = append(toDisconnect, repl)
	}
	f.disconnectStreams(ctx, toDisconnect, "not actively replicating")

	f.tryReconnect(ctx)
}

// notActivelyReplicatingTo lists the replicas that aren't actively receiving
// log entries to append to its log, from raft's perspective (i.e. this is
// unrelated to CRDB-level follower pausing). This encompasses newly added
// replicas that we're still probing to figure out its last index, replicas
// that are pending raft snapshots because the leader has truncated away entries
// higher than its last position, and replicas we're not currently connected to
// via the raft transport.
func (f *replicaFlowControlIntegrationImpl) notActivelyReplicatingTo() []roachpb.ReplicaID {
	var res []roachpb.ReplicaID
	f.replicaForFlowControl.withReplicaProgress(func(replID roachpb.ReplicaID, progress rafttracker.Progress) {
		if replID == f.replicaForFlowControl.getReplicaID() {
			return
		}
		repl, ok := f.lastKnownReplicas.GetReplicaDescriptorByID(replID)
		if !ok {
			return
		}

		if progress.State != rafttracker.StateReplicate {
			res = append(res, replID)
			// TODO(irfansharif): Integrating with these other progress fields
			// from raft. For replicas exiting rafttracker.StateProbe, perhaps
			// compare progress.Match against status.Commit to make sure it's
			// sufficiently caught up with respect to its raft log before we
			// start deducting tokens for it (lest we run into I3a from
			// kvflowcontrol/doc.go). To play well with the replica-level
			// proposal quota pool, maybe we also factor its base index?
			// Replicas that crashed and came back could come back in
			// StateReplicate but be behind on their logs. If we're deducting
			// tokens right away for subsequent proposals, it would take some
			// time for it to catch up and then later return those tokens to us.
			// This is I3a again; do it as part of #95563.
			_ = progress.RecentActive
			_ = progress.MsgAppFlowPaused
			_ = progress.Match
			return
		}

		if !f.replicaForFlowControl.isRaftTransportConnectedTo(repl.StoreID) {
			res = append(res, replID)
		}
	})
	return res
}

func (f *replicaFlowControlIntegrationImpl) disconnectStreams(
	ctx context.Context, toDisconnect []roachpb.ReplicaDescriptor, reason string,
) {
	for _, repl := range toDisconnect {
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

func (f *replicaFlowControlIntegrationImpl) onRaftTransportDisconnected(
	ctx context.Context, storeIDs ...roachpb.StoreID,
) {
	f.replicaForFlowControl.assertLocked()
	if f.innerHandle == nil {
		return // nothing to do
	}

	disconnectedStores := make(map[roachpb.StoreID]struct{})
	for _, storeID := range storeIDs {
		disconnectedStores[storeID] = struct{}{}
	}

	var toDisconnect []roachpb.ReplicaDescriptor
	for _, repl := range f.lastKnownReplicas.Descriptors() {
		if _, found := disconnectedStores[repl.StoreID]; found {
			toDisconnect = append(toDisconnect, repl)
		}
	}
	f.disconnectStreams(ctx, toDisconnect, "raft transport disconnected")
	f.tryReconnect(ctx)
}

func (f *replicaFlowControlIntegrationImpl) tryReconnect(ctx context.Context) {
	// Try reconnecting streams we disconnected.
	pausedFollowers := f.replicaForFlowControl.getPausedFollowers()
	notActivelyReplicatingTo := f.notActivelyReplicatingTo()
	appliedLogPosition := f.replicaForFlowControl.getAppliedLogPosition()

	var disconnectedRepls []roachpb.ReplicaID
	for replID := range f.disconnectedStreams {
		disconnectedRepls = append(disconnectedRepls, replID)
	}
	sort.Slice(disconnectedRepls, func(i, j int) bool { // for determinism in tests
		return disconnectedRepls[i] < disconnectedRepls[j]
	})
	for _, replID := range disconnectedRepls {
		stream := f.disconnectedStreams[replID]
		if _, ok := pausedFollowers[replID]; ok {
			continue // still paused, nothing to reconnect
		}

		repl, ok := f.lastKnownReplicas.GetReplicaDescriptorByID(replID)
		if !ok {
			log.Fatalf(ctx, "%s: tracking %s in disconnected streams despite it not being in descriptor: %s",
				f.replicaForFlowControl.getReplicaID(), replID, f.lastKnownReplicas)
		}
		if !f.replicaForFlowControl.isFollowerLive(ctx, replID) {
			continue // still inactive, nothing to reconnect
		}

		notReplicatedTo := false
		for _, notReplicatedToRepl := range notActivelyReplicatingTo {
			if replID == notReplicatedToRepl {
				notReplicatedTo = true
				break
			}
		}
		if notReplicatedTo {
			continue // not actively replicated to, yet; nothing to reconnect
		}

		if !f.replicaForFlowControl.isRaftTransportConnectedTo(repl.StoreID) {
			continue // not connected to via raft transport
		}

		// See I1, I2, I3, I3a, I4 from kvflowcontrol/doc.go. Replica is
		// connected to via the RaftTransport (I1), on a live node (I2), not
		// paused (I3), and is being actively replicated to through log entries
		// (I3a, I4). Re-connect so we can start deducting tokens for it.
		f.innerHandle.ConnectStream(ctx, appliedLogPosition, stream)
		delete(f.disconnectedStreams, replID)
	}
}
