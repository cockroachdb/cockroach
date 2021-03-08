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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// EmitMLAI registers the replica's last assigned max lease index with the
// closed timestamp tracker. This is called to emit an update about this
// replica in the absence of write activity.
func (r *Replica) EmitMLAI() {
	r.mu.RLock()
	lai := r.mu.proposalBuf.LastAssignedLeaseIndexRLocked()
	if r.mu.state.LeaseAppliedIndex > lai {
		lai = r.mu.state.LeaseAppliedIndex
	}
	epoch := r.mu.state.Lease.Epoch
	isLeaseholder := r.mu.state.Lease.Replica.ReplicaID == r.mu.replicaID
	isMergeInProgress := r.mergeInProgressRLocked()
	r.mu.RUnlock()

	// If we're the leaseholder of an epoch-based lease, notify the minPropTracker
	// of the current LAI to trigger a re-broadcast of this range's LAI.
	if isLeaseholder && epoch > 0 {
		ctx := r.AnnotateCtx(context.Background())
		_, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
		if isMergeInProgress {
			// A critical requirement for the correctness of range merges is that we
			// don't allow follower reads on closed timestamps that are greater than
			// the subsumption time of the RHS range. Thus, while a range is subsumed,
			// we ensure that any intervening closed timestamp updates (until the
			// merge either commits or aborts) can only be activated *after* the merge
			// has completed (successfully or otherwise), by requiring that follower
			// replicas must catch up to an MLAI that succeeds the range's current
			// lease applied index. See comment block at the end of Subsume() in
			// cmd_subsume.go for more details.
			//
			// Omitting the closed timestamp update here would be legal, but
			// undesirable because if the range were to go on to quiesce, the follower
			// replicas would not be able to implicitly tick their closed timestamps
			// without `Request`ing it from the new leaseholder. Emitting it here
			// avoids that little bit of latency.
			untrack(ctx, ctpb.Epoch(epoch), r.RangeID, ctpb.LAI(lai+1))
		} else {
			untrack(ctx, ctpb.Epoch(epoch), r.RangeID, ctpb.LAI(lai))
		}
	}
}

// BumpSideTransportClosed advances the range's closed timestamp if it can. If
// the closed timestamp is advanced, the function synchronizes with incoming
// requests, making sure that future requests are not allowed to write below the
// new closed timestamp.
//
// Returns false is the desired timestamp could not be closed. This can happen if the
// lease is no longer valid, if the range has proposals in-flight, if there are
// requests evaluating above the desired closed timestamp, or if the range has already
// closed a higher timestamp.
//
// If the closed timestamp was advanced, the function returns a LAI to be
// attached to the newly closed timestamp.
//
// This is called by the closed timestamp side-transport. The desired closed timestamp
// is passed as a map from range policy to timestamp; this function looks up the entry
// for this range.
func (r *Replica) BumpSideTransportClosed(
	ctx context.Context,
	now hlc.ClockTimestamp,
	targetByPolicy [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp,
) (ok bool, _ ctpb.LAI, _ roachpb.RangeClosedTimestampPolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// This method can be called even after a Replica is destroyed and removed
	// from the Store's replicas map, because unlinkReplicaByRangeIDLocked does
	// not synchronize with sidetransport.Sender.publish, which maintains a
	// local copy of its leaseholder map. To avoid issues resulting from this,
	// we first check if the replica is destroyed.
	if _, err := r.isDestroyedRLocked(); err != nil {
		return false, 0, 0
	}

	lai := ctpb.LAI(r.mu.state.LeaseAppliedIndex)
	policy := r.closedTimestampPolicyRLocked()
	target := targetByPolicy[policy]
	st := r.leaseStatusForRequestRLocked(ctx, now, hlc.Timestamp{} /* reqTS */)
	// We need to own the lease but note that stasis (LeaseState_UNUSABLE) doesn't
	// matter.
	valid := st.IsValid() || st.State == kvserverpb.LeaseState_UNUSABLE
	if !valid || !st.OwnedBy(r.StoreID()) {
		return false, 0, 0
	}
	if st.ClosedTimestampUpperBound().Less(target) {
		return false, 0, 0
	}

	// If the range is merging into its left-hand neighbor, we can't close
	// timestamps any more because the joint-range would not be aware of reads
	// performed based on this advanced closed timestamp.
	if r.mergeInProgressRLocked() {
		return false, 0, 0
	}

	// If there are pending Raft proposals in-flight or committed entries that
	// have yet to be applied, the side-transport doesn't advance the closed
	// timestamp. The side-transport can't publish a closed timestamp with an
	// LAI that takes the in-flight LAIs into consideration, because the
	// in-flight proposals might not actually end up applying. In order to
	// publish a closed timestamp with an LAI that doesn't consider these
	// in-flight proposals we'd have to check that they're all trying to write
	// above `target`; that's too expensive.
	//
	// Note that the proposals in the proposalBuf don't matter here; these
	// proposals and their timestamps are still tracked in proposal buffer's
	// tracker, and they'll be considered below.
	if len(r.mu.proposals) > 0 || r.mu.applyingEntries {
		return false, 0, 0
	}

	// MaybeForwardClosedLocked checks that there are no evaluating requests
	// writing under target.
	if !r.mu.proposalBuf.MaybeForwardClosedLocked(ctx, target) {
		return false, 0, 0
	}

	// Update the replica directly since there's no side-transport connection to
	// the local node.
	r.mu.sideTransportClosedTimestamp = target
	r.mu.sideTransportCloseTimestampLAI = lai
	return true, lai, policy
}

// closedTimestampTargetRLocked computes the timestamp we'd like to close for
// this range. Note that we might not be able to ultimately close this timestamp
// if there are requests in flight.
func (r *Replica) closedTimestampTargetRLocked() hlc.Timestamp {
	return closedts.TargetForPolicy(
		r.Clock().NowAsClockTimestamp(),
		r.Clock().MaxOffset(),
		closedts.TargetDuration.Get(&r.ClusterSettings().SV),
		closedts.LeadForGlobalReadsOverride.Get(&r.ClusterSettings().SV),
		closedts.SideTransportCloseInterval.Get(&r.ClusterSettings().SV),
		r.closedTimestampPolicyRLocked(),
	)
}

// ForwardSideTransportClosedTimestamp forwards
// r.mu.sideTransportClosedTimestamp. It is called by the closed timestamp
// side-transport receiver.
func (r *Replica) ForwardSideTransportClosedTimestamp(
	ctx context.Context, closedTS hlc.Timestamp, lai ctpb.LAI,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.sideTransportClosedTimestamp.Forward(closedTS) {
		if r.mu.sideTransportCloseTimestampLAI > lai {
			log.Fatalf(ctx, "received side-transport notification with higher closed timestamp "+
				"but lower LAI: r%d current LAI: %d received LAI: %d",
				r.RangeID, r.mu.sideTransportCloseTimestampLAI, lai)
		}
		r.mu.sideTransportCloseTimestampLAI = lai
	}
}

// getSideTransportClosedTimestamp returns the replica's information about the
// timestamp that was closed by the side-transport. Note that this not include
// r.mu.state.RaftClosedTimestamp. Also note that this might not be the highest
// closed timestamp communicated by the side-transport - the
// ClosedTimestampReceiver should be checked too if an up-to-date value is
// required.
//
// It's the responsibility of the caller to check the returned LAI against the
// replica's applied LAI. If the returned LAI hasn't applied, the closed
// timestamp cannot be used.
func (r *Replica) getSideTransportClosedTimestampRLocked() (closedTS hlc.Timestamp, lai ctpb.LAI) {
	return r.mu.sideTransportClosedTimestamp, r.mu.sideTransportCloseTimestampLAI
}
