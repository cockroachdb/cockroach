// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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

	lai := r.shMu.state.LeaseAppliedIndex
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
	// the local node. We pass knownApplied = true because we pulled this lease
	// applied index directly from the applied replica state.
	const knownApplied = true
	r.sideTransportClosedTimestamp.forward(ctx, target, lai, knownApplied)
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
	ctx context.Context, closed hlc.Timestamp, lai kvpb.LeaseAppliedIndex,
) {
	// We pass knownApplied = false because we don't know whether this lease
	// applied index has been applied locally yet.
	const knownApplied = false
	r.sideTransportClosedTimestamp.forward(ctx, closed, lai, knownApplied)
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

		// cur is the largest closed timestamp communicated by the side transport
		// whose corresponding lai has definitely been applied by the local replica.
		// It is used to ensure that even as next increases, the timestamp returned
		// from get never regresses across sequential calls.
		cur closedTimestamp

		// next is the largest closed timestamp communicated by the side transport,
		// along with its corresponding lai. This lai may or may not have already
		// been applied by the range. When a call to get includes an lai equal to or
		// greater than lai, next is moved to cur and is cleared.
		next closedTimestamp
	}
}

// sidetransportReceiver abstracts *sidetransport.Receiver.
type sidetransportReceiver interface {
	GetClosedTimestamp(
		ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
	) (hlc.Timestamp, kvpb.LeaseAppliedIndex)
	HTML() string
}

// closedTimestamp is a combination of a timestamp and a lease applied index.
// The timestamp is considered to be locally "closed" when the corresponding
// lease applied index has been locally applied.
type closedTimestamp struct {
	ts  hlc.Timestamp
	lai kvpb.LeaseAppliedIndex
}

// regression returns whether the combination of the two closed timestamps
// indicate an illegal regression. The closed timestamp is said to regress when
// its timestamp decreases as its lease applied index increases.
func (a closedTimestamp) regression(b closedTimestamp) bool {
	if a.lai == b.lai {
		return false
	}
	if a.lai < b.lai {
		return b.ts.Less(a.ts)
	}
	return a.ts.Less(b.ts)
}

// merge combines the two closed timestamp sources into the receiver.
func (a *closedTimestamp) merge(b closedTimestamp) {
	if a.lai == b.lai {
		a.ts.Forward(b.ts)
	} else if a.lai < b.lai {
		a.ts = b.ts
		a.lai = b.lai
	}
}

func (st *sidetransportAccess) init(receiver sidetransportReceiver, rangeID roachpb.RangeID) {
	if receiver != nil {
		// Avoid st.receiver becoming a typed nil.
		st.receiver = receiver
	}
	st.rangeID = rangeID
}

// forward bumps the local closed timestamp info using the provided updated.
// Callers should specify using the knownApplied flag whether they know the
// lease applied index in the update to be locally applied or not.
//
// The method returns the current applied closed timestamp.
func (st *sidetransportAccess) forward(
	ctx context.Context, closed hlc.Timestamp, lai kvpb.LeaseAppliedIndex, knownApplied bool,
) closedTimestamp {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Sanity checks.
	up := closedTimestamp{ts: closed, lai: lai}
	if st.mu.cur.lai != 0 {
		st.assertNoRegression(ctx, st.mu.cur, up)
	}
	if st.mu.next.lai != 0 {
		st.assertNoRegression(ctx, st.mu.next, up)
	}

	if up.lai <= st.mu.cur.lai {
		// The caller doesn't know that this lai was applied, but we do.
		knownApplied = true
	}
	if knownApplied {
		// Known applied, so merge into cur and merge + clear next if necessary.
		if up.lai >= st.mu.next.lai {
			up.merge(st.mu.next)
			st.mu.next = closedTimestamp{}
		}
		st.mu.cur.merge(up)
	} else {
		// Not known applied, so merge into next.
		st.mu.next.merge(up)
	}
	return st.mu.cur
}

func (st *sidetransportAccess) assertNoRegression(ctx context.Context, cur, up closedTimestamp) {
	if cur.regression(up) {
		log.Fatalf(ctx, "side-transport update saw closed timestamp regression on r%d: "+
			"(lai=%d, ts=%s) -> (lai=%d, ts=%s)", st.rangeID, cur.lai, cur.ts, up.lai, up.ts)
	}
}

// get returns the closed timestamp that the side transport knows for the range.
// leaseholder is the known leaseholder for the range. appliedLAI is the LAI
// that the replica has caught up to. sufficient, if not empty, is a hint
// indicating that any lower or equal closed timestamp suffices; the caller
// doesn't need the highest closed timestamp necessarily.
//
// Returns an empty timestamp if no closed timestamp is known. However, once a
// closed timestamp is known, an empty timestamp will never again be returned.
// Furthermore, the returned timestamp will never regress across sequential
// calls to get.
//
// It is safe for a caller to pass an appliedLAI that's lower than what a
// previous caller passed in. This means that get can be called without holding
// the replica.mu.
func (st *sidetransportAccess) get(
	ctx context.Context,
	leaseholder roachpb.NodeID,
	appliedLAI kvpb.LeaseAppliedIndex,
	sufficient hlc.Timestamp,
) hlc.Timestamp {
	st.mu.RLock()
	cur, next := st.mu.cur, st.mu.next
	st.mu.RUnlock()

	// If the current info is enough to satisfy sufficient, we're done.
	if !sufficient.IsEmpty() && sufficient.LessEq(cur.ts) {
		return cur.ts
	}

	// If we know about a larger closed timestamp at a higher lease applied index,
	// check whether our applied lai is sufficient to promote it to cur.
	if next.lai != 0 {
		if next.lai > appliedLAI {
			// The local replica hasn't caught up to the closed timestamp we have
			// stored, so what we have stored is not usable. There's no point in going
			// to the receiver, as that one can only have an even higher LAI.
			return cur.ts
		}

		// The local replica has caught up to the closed timestamp we have stored.
		cur = st.forward(ctx, next.ts, next.lai, true /* knownApplied */)
		next = closedTimestamp{} // prevent use below

		// Check again if the local, current info is enough to satisfy sufficient.
		if !sufficient.IsEmpty() && sufficient.LessEq(cur.ts) {
			return cur.ts
		}
	}

	// Check with the receiver.

	// Some tests don't have the receiver set.
	if st.receiver == nil {
		return cur.ts
	}

	recTS, recLAI := st.receiver.GetClosedTimestamp(ctx, st.rangeID, leaseholder)
	if recTS.LessEq(cur.ts) {
		// Short-circuit if the receiver doesn't know anything new.
		return cur.ts
	}

	// Otherwise, update the access's local state with the additional information
	// from the side transport receiver and return the largest closed timestamp
	// that we know to be applied.
	knownApplied := recLAI <= appliedLAI
	cur = st.forward(ctx, recTS, recLAI, knownApplied)
	return cur.ts
}
