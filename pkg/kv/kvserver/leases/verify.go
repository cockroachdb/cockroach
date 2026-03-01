// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package leases

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// VerifyInput is the set of input parameters for lease acquisition or transfer
// safety verification.
type VerifyInput struct {
	// See comments about these fields in the Input struct.

	// Information about the local replica.
	LocalStoreID   roachpb.StoreID
	LocalReplicaID roachpb.ReplicaID
	Desc           *roachpb.RangeDescriptor

	// Information about raft.
	RaftStatus    *raft.Status
	RaftCompacted kvpb.RaftIndex

	// Information about the previous lease.
	PrevLease        roachpb.Lease
	PrevLeaseExpired bool

	// Information about the (requested) next lease.
	NextLeaseHolder roachpb.ReplicaDescriptor

	// BypassSafetyChecks configures lease transfers to skip safety checks.
	BypassSafetyChecks bool

	// TargetHasSendQueue indicates that the lease transfer target has a send
	// queue, meaning it is behind on its raft log. When true (and
	// BypassSafetyChecks is false), the lease transfer will be rejected because
	// the target may cause proportional unavailability until it catches up.
	// When send stream stats are unavailable (e.g. not the leader, RACv2 not
	// active), callers should leave this false to conservatively allow the
	// transfer.
	TargetHasSendQueue bool

	// DesiredLeaseType is the desired lease type for the replica.
	DesiredLeaseType roachpb.LeaseType
}

// toBuildInput converts a VerifyInput to an BuildInput. This is a lossy
// conversion and should only be used to call methods on BuildInput.
func (i VerifyInput) toBuildInput() BuildInput {
	return BuildInput{
		LocalStoreID:    i.LocalStoreID,
		PrevLease:       i.PrevLease,
		NextLeaseHolder: i.NextLeaseHolder,
	}
}

// Acquisition returns whether the lease request is an acquisition.
func (i VerifyInput) Acquisition() bool { return i.toBuildInput().Acquisition() }

// Extension returns whether the lease request is an extension.
func (i VerifyInput) Extension() bool { return i.toBuildInput().Extension() }

// Transfer returns whether the lease request is a transfer.
func (i VerifyInput) Transfer() bool { return i.toBuildInput().Transfer() }

// Verify performs safety checks on lease acquisition / transfer requests. It
// returns an error if the request is not safe to proceed. The safety of the
// request is determined by the current state of the Raft group and the type of
// lease request being performed.
func Verify(ctx context.Context, st Settings, i VerifyInput) error {
	switch {
	case i.Acquisition():
		return verifyAcquisition(ctx, st, i)
	case i.Extension():
		return verifyExtension(ctx, st, i)
	case i.Transfer():
		return verifyTransfer(ctx, st, i)
	default:
		return errors.AssertionFailedf("unknown lease operation")
	}
}

func verifyAcquisition(ctx context.Context, st Settings, i VerifyInput) error {
	// Handle an edge case about lease acquisitions: we don't want to forward
	// lease acquisitions to another node (which is what happens when we're not
	// the leader) because:
	// a) if there is a different leader, that leader should acquire the lease
	// itself and thus avoid a change of leadership caused by the leaseholder and
	// leader being different (Raft leadership follows the lease), and
	// b) being a follower, it's possible that this replica is behind in applying
	// the log. Thus, there might be another lease in place that this follower
	// doesn't know about, in which case the lease we're proposing here would be
	// rejected. Not only would proposing such a lease be wasted work, but we're
	// trying to protect against pathological cases where it takes a long time for
	// this follower to catch up (for example because it's waiting for a snapshot,
	// and the snapshot is queued behind many other snapshots). In such a case, we
	// don't want all requests arriving at this node to be blocked on this lease
	// acquisition (which is very likely to eventually fail anyway).
	//
	// Thus, we do one of two things:
	// - if the leader is known, we reject this proposal and make sure the request
	// that needed the lease is redirected to the leaseholder;
	// - if the leader is not known [^1], we don't do anything special here to
	// terminate the proposal, but we know that Raft will reject it with a
	// ErrProposalDropped. We'll eventually re-propose it once a leader is known,
	// at which point it will either go through or be rejected based on whether it
	// is this replica that became the leader.
	//
	// [^1]: however, if the leader is not known and RejectLeaseOnLeaderUnknown
	// cluster setting is true, or if the replica's desired lease type is a leader
	// lease, we reject the proposal.
	// TODO(nathan): make this behaviour default. Right now, it is hidden behind
	// an experimental cluster setting. See #120073 and #118435. We attempted to
	// address this in #127082, but there was fallout, so we reverted that change.
	//
	// A special case is when the leader is known, but is ineligible to get the
	// lease. In that case, we have no choice but to continue with the proposal.
	//
	// Lease extensions for a currently held lease always go through, to keep the
	// lease alive until the normal lease transfer mechanism can colocate it with
	// the leader.
	leader := roachpb.ReplicaID(i.RaftStatus.Lead)
	leaderKnown := leader != roachpb.ReplicaID(raft.None)
	iAmTheLeader := i.RaftStatus.RaftState == raftpb.StateLeader
	if (leader == i.LocalReplicaID) != iAmTheLeader {
		log.KvExec.Fatalf(ctx, "inconsistent Raft state: %s", i.RaftStatus)
	}

	// If the local replica is the raft leader, it can proceed with the lease
	// acquisition.
	if iAmTheLeader || st.AllowLeaseProposalWhenNotLeader {
		return nil
	}

	// If the local replica is the only replica in the range, it can proceed with
	// the lease acquisition, regardless of whether it currently considers itself
	// the raft leader. This is because the replica can become the leader without
	// any remote votes, so it has the ability to grant itself leadership whenever
	// it wants.
	//
	// This affordance for single-replica ranges is important for unit tests, many
	// of which are written with a single-replica range and are not prepared to
	// retry NotLeaseHolderErrors.
	//
	// Note that it is not possible for the local replica to think that it is the
	// only replica in the range but for that not to be the case due to staleness
	// and for an available quorum to exist elsewhere. This is because the replica
	// in a single-replica range must be responsible for any upreplication that
	// might create such a quorum.
	if len(i.Desc.Replicas().VoterDescriptors()) == 1 {
		return nil
	}

	// Else, the local replica is not the leader. We only allow it to proceed with
	// the lease acquisition in rare cases. Some day, we may be able to remove all
	// of them and always reject lease acquisitions when the lease acquirer is not
	// the raft leader.
	var leaderEligibleForLease bool
	if leaderKnown {
		// Figure out if the leader is eligible for getting a lease.
		leaderRep, ok := i.Desc.GetReplicaDescriptorByID(leader)
		if !ok {
			// There is a leader, but it's not part of our descriptor. The descriptor
			// must be stale, so we are behind in applying the log. We don't want the
			// lease ourselves (as we're behind), so let's assume that the leader is
			// eligible. If it proves that it isn't, we might be asked to get the
			// lease again, and by then hopefully we will have caught up.
			leaderEligibleForLease = true
		} else {
			// If the current leader is a VOTER_DEMOTING and it was the last one to
			// hold the lease (according to our possibly stale applied lease state),
			// CheckCanReceiveLease considers it eligible to continue holding the
			// lease, so we don't allow our proposal through. Otherwise, if it was not
			// the last one to hold the lease, it will never be allowed to acquire it
			// again, so we don't consider it eligible.
			leaderWasLastLeaseholder := leaderRep.ReplicaID == i.PrevLease.Replica.ReplicaID
			err := roachpb.CheckCanReceiveLease(leaderRep, i.Desc.Replicas(), leaderWasLastLeaseholder)
			leaderEligibleForLease = err == nil
		}
	}

	desiresLeaderLease := i.DesiredLeaseType == roachpb.LeaseLeader
	reject := false
	if !leaderKnown && (desiresLeaderLease || st.RejectLeaseOnLeaderUnknown) {
		// If a leader lease is desired, and we don't know who the leader is, we do
		// not try proposing a lease acquisition request. The lease should be
		// acquired by the leader, so we want the client to try another replica who
		// might be the leader itself or knows who the leader is.
		log.VEventf(ctx, 2, "not proposing lease acquisition because we're not the leader; the leader is unknown")
		reject = true
	} else if leaderEligibleForLease {
		log.VEventf(ctx, 2, "not proposing lease acquisition because we're not the leader; replica %d is",
			leader)
		reject = true
	}
	if reject {
		if !leaderKnown {
			// We don't know the leader, so pass Lease{} to give no hint.
			return kvpb.NewNotLeaseHolderError(roachpb.Lease{}, i.LocalStoreID, i.Desc,
				"refusing to acquire lease on follower")
		}
		redirectRep, _ /* ok */ := i.Desc.GetReplicaDescriptorByID(leader)
		log.VEventf(ctx, 2, "redirecting proposal to node %s", redirectRep.NodeID)
		return kvpb.NewNotLeaseHolderErrorWithSpeculativeLease(redirectRep, i.LocalStoreID, i.Desc,
			"refusing to acquire lease on follower")
	}

	// If the leader is not known, or if it is known but is ineligible for the
	// lease, continue with the proposal as explained above.
	if !leaderKnown {
		log.VEventf(ctx, 2, "proposing lease acquisition even though we're not the leader; the leader is unknown")
	} else {
		log.VEventf(ctx, 2, "proposing lease acquisition even though we're not the leader; the leader is ineligible")
	}
	return nil
}

func verifyExtension(ctx context.Context, st Settings, i VerifyInput) error {
	// Nothing to verify during an extension if the lease has not expired. We
	// already hold the lease and are extending it. However, if the lease has
	// expired, verify the safety of the extension as if it were an acquisition.
	if i.PrevLeaseExpired {
		return verifyAcquisition(ctx, st, i)
	}
	iAmTheLeader := i.RaftStatus.RaftState == raftpb.StateLeader
	if !iAmTheLeader {
		log.VEventf(ctx, 2, "proposing lease extension even though we're not the leader; we hold the current lease")
	}
	return nil
}

func verifyTransfer(ctx context.Context, st Settings, i VerifyInput) error {
	// When performing a lease transfer, the outgoing leaseholder revokes its
	// lease before proposing the lease transfer request, meaning that it promises
	// to stop using the previous lease to serve reads or writes. The lease
	// transfer request is then proposed and committed to the Raft log, at which
	// point the new lease officially becomes active. However, this new lease is
	// not usable until the incoming leaseholder applies the Raft entry that
	// contains the lease transfer and notices that it is now the leaseholder for
	// the range.
	//
	// The effect of this handoff is that there exists a "power vacuum" time
	// period when the outgoing leaseholder has revoked its previous lease but the
	// incoming leaseholder has not yet applied its new lease. During this time
	// period, a range is effectively unavailable for strong reads and writes,
	// because no replica will act as the leaseholder. Instead, requests that
	// require the lease will be redirected back and forth between the outgoing
	// leaseholder and the incoming leaseholder (the client backs off). To
	// minimize the disruption caused by lease transfers, we need to minimize this
	// time period.
	//
	// We assume that if a lease transfer target is sufficiently caught up on its
	// log such that it will be able to apply the lease transfer through log entry
	// application then this unavailability window will be acceptable. Historically,
	// we did not try to make any determination about sub-snapshot replication lag
	// (see #38065 and #42379, which removed such a heuristic). This left a gap:
	// the proposal-buffer check below only prevents snapshot-level lag, but callers
	// that bypass the allocator (manual lease transfers via AdminTransferLease,
	// AdminRelocateRange, scatter, etc.) could transfer leases to replicas with
	// large send queues — thousands of entries behind — causing proportional
	// unavailability. The allocator paths protect against this via
	// excludeReplicasInNeedOfCatchup, which filters out replicas with send queues,
	// but that protection only applies to allocator-routed transfers.
	//
	// To close this gap, we now also reject lease transfers to replicas that have
	// a send queue, as reported by the RACv2 send stream stats. This is the right
	// signal: it is less strict than requiring Match >= Commit (which would stall
	// transfers on busy or WAN ranges) while still preventing transfers that
	// would cause meaningful unavailability. When send stream stats are
	// unavailable (e.g. the local replica is not the leader, or RACv2 is not
	// active), we conservatively allow the transfer — matching the allocator's
	// fallback behavior in excludeReplicasInNeedOfCatchup.
	//
	// However, we draw a distinction between lease transfer targets that will be
	// able to apply the lease transfer through log entry application and those
	// that will require a Raft snapshot to catch up and apply the lease transfer.
	// Raft snapshots are more expensive than Raft entry replication. They are
	// also significantly more likely to be delayed due to queueing behind other
	// snapshot traffic in the system. This potential for delay makes transferring
	// a lease to a replica that needs a snapshot very risky, as doing so has the
	// effect of inducing range unavailability until the snapshot completes, which
	// could take seconds, minutes, or hours.
	//
	// In the future, we will likely get better at prioritizing snapshots to
	// improve the responsiveness of snapshots that are needed to recover
	// availability. However, even in this world, it is not worth inducing
	// unavailability that can only be recovered through a Raft snapshot. It is
	// better to catch the desired lease target up on the log first and then
	// initiate the lease transfer once its log is connected to the leader's.
	//
	// For this reason, unless we can guarantee that the lease transfer target
	// does not need a Raft snapshot, we don't let it through. This same check
	// lives at higher levels in the stack as well (i.e. in the allocator). The
	// higher level checks avoid wasted work and respond more gracefully to
	// invalid targets (e.g. they pick the next best target). However, this is the
	// only place where the protection is airtight against race conditions because
	// the check is performed:
	// 1. by the current Raft leader, else the proposal will fail
	// 2. while holding latches that prevent interleaving log truncation
	//
	// If an error is thrown in case 2, after the previous lease has been revoked,
	// the outgoing leaseholder still won't be able to use its revoked lease.
	// However, it will be able to immediately request a new lease. This may be
	// disruptive, which is why we try to avoid hitting this airtight protection
	// as much as possible by detecting the failure scenario before revoking the
	// outgoing lease.
	if i.BypassSafetyChecks {
		return nil
	}
	if i.TargetHasSendQueue {
		log.VEventf(ctx, 2, "not initiating lease transfer because the target %s "+
			"has a send queue", i.NextLeaseHolder)
		return NewLeaseTransferRejectedBecauseTargetHasSendQueueError(i.NextLeaseHolder)
	}
	snapStatus := raftutil.ReplicaMayNeedSnapshot(i.RaftStatus, i.RaftCompacted, i.NextLeaseHolder.ReplicaID)
	if snapStatus != raftutil.NoSnapshotNeeded {
		log.VEventf(ctx, 2, "not initiating lease transfer because the target %s may "+
			"need a snapshot: %s", i.NextLeaseHolder, snapStatus)
		return NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(i.NextLeaseHolder, snapStatus)
	}
	return nil
}
