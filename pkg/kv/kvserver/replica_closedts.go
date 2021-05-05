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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
) sidetransport.BumpSideTransportClosedResult {
	var res sidetransport.BumpSideTransportClosedResult
	r.mu.Lock()
	defer r.mu.Unlock()
	res.Desc = r.descRLocked()

	// This method can be called even after a Replica is destroyed and removed
	// from the Store's replicas map, because unlinkReplicaByRangeIDLocked does
	// not synchronize with sidetransport.Sender.publish, which maintains a
	// local copy of its leaseholder map. To avoid issues resulting from this,
	// we first check if the replica is destroyed.
	if _, err := r.isDestroyedRLocked(); err != nil {
		res.FailReason = sidetransport.ReplicaDestroyed
		return res
	}

	lai := ctpb.LAI(r.mu.state.LeaseAppliedIndex)
	policy := r.closedTimestampPolicyRLocked()
	target := targetByPolicy[policy]
	st := r.leaseStatusForRequestRLocked(ctx, now, hlc.Timestamp{} /* reqTS */)
	// We need to own the lease but note that stasis (LeaseState_UNUSABLE) doesn't
	// matter.
	valid := st.IsValid() || st.State == kvserverpb.LeaseState_UNUSABLE
	if !valid || !st.OwnedBy(r.StoreID()) {
		res.FailReason = sidetransport.InvalidLease
		return res
	}
	if st.ClosedTimestampUpperBound().Less(target) {
		res.FailReason = sidetransport.TargetOverLeaseExpiration
		return res
	}

	// If the range is merging into its left-hand neighbor, we can't close
	// timestamps any more because the joint-range would not be aware of reads
	// performed based on this advanced closed timestamp.
	if r.mergeInProgressRLocked() {
		res.FailReason = sidetransport.MergeInProgress
		return res
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
		res.FailReason = sidetransport.ProposalsInFlight
		return res
	}

	// MaybeForwardClosedLocked checks that there are no evaluating requests
	// writing under target.
	if !r.mu.proposalBuf.MaybeForwardClosedLocked(ctx, target) {
		res.FailReason = sidetransport.RequestsEvaluatingBelowTarget
		return res
	}

	// Update the replica directly since there's no side-transport connection to
	// the local node.
	r.sideTransportClosedTimestamp.forward(ctx, target, lai)
	res.OK = true
	res.LAI = lai
	res.Policy = policy
	return res
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

// ForwardSideTransportClosedTimestamp forwards the side-transport closed
// timestamp. It is called by the closed timestamp side-transport receiver.
func (r *Replica) ForwardSideTransportClosedTimestamp(
	ctx context.Context, closed hlc.Timestamp, lai ctpb.LAI,
) {
	r.sideTransportClosedTimestamp.forward(ctx, closed, lai)
}

// sidetransportAccess encapsulates state related to the closed timestamp's
// information about the range. It stores a potentially stale closed timestamp
// directly and, when that's not sufficient for a caller, it delegates to the
// sidetransport.Receiver for more up to date information and updates the local
// state. The idea is that the local state is cheap to access and acts as a
// cache.
//
// Note that the sidetransportAccess does not incorporate the closed timestamp
// information carried by Raft commands. That can be found in
// r.mu.state.RaftClosedTimestamp. Generally, the Raft state should be queried
// in parallel with the side transport state to determine an up to date closed
// timestamp (i.e. the maximum across the two). For a given LAI, the side
// transport closed timestamp will always lead the Raft closed timestamp. Across
// LAIs, the larger LAI will always include the larger closed timestamp,
// independent of the source.
type sidetransportAccess struct {
	rangeID  roachpb.RangeID
	receiver sidetransportReceiver
	mu       struct {
		syncutil.RWMutex
		// closedTimestamp is the cached info about the closed timestamp that was
		// communicated by the side transport. The replica can use it if it has
		// applied commands up to (and including) lai.
		closedTimestamp hlc.Timestamp
		lai             ctpb.LAI
	}
}

// sidetransportReceiver abstracts *sidetransport.Receiver.
type sidetransportReceiver interface {
	GetClosedTimestamp(
		ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
	) (hlc.Timestamp, ctpb.LAI)
	HTML() string
}

func (st *sidetransportAccess) init(receiver sidetransportReceiver, rangeID roachpb.RangeID) {
	if receiver != nil {
		// Avoid st.receiver becoming a typed nil.
		st.receiver = receiver
	}
	st.rangeID = rangeID
}

// forward bumps the local closed timestamp info.
func (st *sidetransportAccess) forward(ctx context.Context, closed hlc.Timestamp, lai ctpb.LAI) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.mu.closedTimestamp.Forward(closed) {
		if st.mu.lai > lai {
			log.Fatalf(ctx, "received side-transport notification with higher closed timestamp "+
				"but lower LAI: r%d current LAI: %d received LAI: %d",
				st.rangeID, st.mu.lai, lai)
		}
		st.mu.lai = lai
	}
}

// get returns the closed timestamp that the side transport knows for the range.
// leaseholder is the known leaseholder for the range. appliedLAI is the LAI
// that the replica has caught up to. sufficient, if not empty, is a hint
// indicating that any lower or equal closed timestamp suffices; the caller
// doesn't need the highest closed timestamp necessarily.
//
// Returns an empty timestamp if no closed timestamp is known.
//
// get can be called without holding replica.mu. This means that a caller can
// pass an appliedLAI that's lower than what a previous caller passed in. That's
// fine, except the second caller might get an empty result.
func (st *sidetransportAccess) get(
	ctx context.Context, leaseholder roachpb.NodeID, appliedLAI ctpb.LAI, sufficient hlc.Timestamp,
) hlc.Timestamp {
	st.mu.RLock()
	closed := st.mu.closedTimestamp
	lai := st.mu.lai
	st.mu.RUnlock()

	// The local replica hasn't caught up to the closed timestamp we have stored,
	// so what we have stored is not usable. There's no point in going to the
	// receiver, as that one can only have an even higher LAI.
	if appliedLAI < lai {
		return hlc.Timestamp{}
	}

	// If the local info is enough to satisfy sufficient, we're done.
	if !sufficient.IsEmpty() && sufficient.LessEq(closed) {
		return closed
	}

	// Check with the receiver.

	// Some tests don't have the receiver set.
	if st.receiver == nil {
		return closed
	}

	receiverClosed, receiverLAI := st.receiver.GetClosedTimestamp(ctx, st.rangeID, leaseholder)
	if receiverClosed.IsEmpty() || appliedLAI < receiverLAI {
		return closed
	}

	// Update the local closed timestamp info.
	if closed.Forward(receiverClosed) {
		st.forward(ctx, closed, receiverLAI)
	}
	return closed
}
