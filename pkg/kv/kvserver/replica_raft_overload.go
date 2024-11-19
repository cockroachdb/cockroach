// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"cmp"
	"context"
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// pauseReplicationIOThreshold is the admission.io.overload threshold at which
// we pause replication to non-essential followers.
var pauseReplicationIOThreshold = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.kv.pause_replication_io_threshold",
	"pause replication to non-essential followers when their I/O admission control score exceeds the given threshold (zero to disable)",
	0,
	settings.FloatWithMinimumOrZeroDisable(0.3),
)

type ioThresholdMapI interface {
	// AbovePauseThreshold returns true if the store's score exceeds the threshold
	// set for trying to pause replication traffic to followers on it.
	AbovePauseThreshold(_ roachpb.StoreID) bool
}

type computeExpendableOverloadedFollowersInput struct {
	self          roachpb.ReplicaID
	replDescs     roachpb.ReplicaSet
	ioOverloadMap ioThresholdMapI
	// getProgressMap returns Raft's view of the progress map. This is only called
	// when needed, and at most once.
	getProgressMap func(context.Context) map[raftpb.PeerID]tracker.Progress
	// seed is used to randomize selection of which followers to pause in case
	// there are multiple followers that qualify, but quorum constraints require
	// picking a subset. In practice, we set this to the RangeID to ensure maximum
	// stability of the selection on a per-Range basis while encouraging randomness
	// across ranges (which in turn should reduce load on all overloaded followers).
	seed int64
	// In addition to being in StateReplicate in the progress map, we also only
	// consider a follower live it its Match index matches or exceeds
	// minLiveMatchIndex. This makes sure that a follower that is behind is not
	// mistaken for one that can meaningfully contribute to quorum in the short
	// term. Without this, it is - at least in theory - possible that as an
	// overloaded formerly expendable store becomes non-overloaded, we will
	// quickly mark another overloaded store as expendable under the assumption
	// that the original store can now contribute to quorum. However, that store
	// is likely behind on the log, and we should consider it as non-live until
	// it has caught up.
	minLiveMatchIndex kvpb.RaftIndex
}

type nonLiveReason byte

const (
	nonLiveReasonInactive nonLiveReason = iota
	nonLiveReasonPaused
	nonLiveReasonBehind
)

// computeExpendableOverloadedFollowers computes a set of followers that we can
// intentionally exempt from replication traffic (MsgApp) to help them avoid I/O
// overload.
//
// In the common case (no store or at least no follower store close to I/O
// overload), this method does very little work.
//
// If at least one follower is (close to being) overloaded, we determine the
// maximum set of such followers that we can afford not to replicate to without
// losing quorum by successively reducing the set of overloaded followers by one
// randomly selected overloaded voter. The randomness makes it more likely that
// when there are multiple overloaded stores in the system that cannot be
// jointly excluded, both stores will in aggregate be relieved from
// approximately 50% of follower raft traffic.
//
// This method uses Raft's view of liveness and in particular will consider
// followers that haven't responded recently (including heartbeats) or are
// waiting for a snapshot as not live. In particular, a follower that is
// initially in the map may transfer out of the map by virtue of being cut off
// from the raft log via a truncation. This is acceptable, since the snapshot
// prevents the replica from receiving log traffic.
func computeExpendableOverloadedFollowers(
	ctx context.Context, d computeExpendableOverloadedFollowersInput,
) (map[roachpb.ReplicaID]struct{}, map[roachpb.ReplicaID]nonLiveReason) {
	var nonLive map[roachpb.ReplicaID]nonLiveReason
	var liveOverloadedVoterCandidates map[roachpb.ReplicaID]struct{}
	var liveOverloadedNonVoterCandidates map[roachpb.ReplicaID]struct{}
	var prs map[raftpb.PeerID]tracker.Progress

	for _, replDesc := range d.replDescs.Descriptors() {
		if pausable := d.ioOverloadMap.AbovePauseThreshold(replDesc.StoreID); !pausable || replDesc.ReplicaID == d.self {
			continue
		}
		// There's at least one overloaded follower, so initialize
		// extra state to determine which traffic we can drop without
		// losing quorum.
		if prs == nil {
			prs = d.getProgressMap(ctx)
			nonLive = map[roachpb.ReplicaID]nonLiveReason{}
			for id, pr := range prs {
				// NB: RecentActive is populated by updateRaftProgressFromActivity().
				if !pr.RecentActive {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonInactive
				}
				if pr.IsPaused() {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonPaused
				}
				if kvpb.RaftIndex(pr.Match) < d.minLiveMatchIndex {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonBehind
				}
			}
			liveOverloadedVoterCandidates = map[roachpb.ReplicaID]struct{}{}
			liveOverloadedNonVoterCandidates = map[roachpb.ReplicaID]struct{}{}
		}

		// Mark replica on overloaded store as possibly pausable.
		//
		// NB: we make no distinction between non-live and live replicas at this
		// point. That is, even if a replica is considered "non-live", we will still
		// consider "additionally" pausing it. The first instinct was to avoid
		// layering anything on top of a non-live follower, however a paused
		// follower immediately becomes non-live, so if we want stable metrics on
		// which followers are "paused", then we need the "pausing" state to
		// overrule the "non-live" state.
		if prs[raftpb.PeerID(replDesc.ReplicaID)].IsLearner {
			liveOverloadedNonVoterCandidates[replDesc.ReplicaID] = struct{}{}
		} else {
			liveOverloadedVoterCandidates[replDesc.ReplicaID] = struct{}{}
		}
	}

	// Start out greedily with all overloaded candidates paused, and remove
	// randomly chosen candidates until we think the raft group can obtain quorum.
	var rnd *rand.Rand
	for len(liveOverloadedVoterCandidates) > 0 {
		up := d.replDescs.CanMakeProgress(func(replDesc roachpb.ReplicaDescriptor) bool {
			rid := replDesc.ReplicaID
			if _, ok := nonLive[rid]; ok {
				return false // not live
			}
			if _, ok := liveOverloadedVoterCandidates[rid]; ok {
				return false // want to drop traffic
			}
			if _, ok := liveOverloadedNonVoterCandidates[rid]; ok {
				return false // want to drop traffic
			}
			return true // live for all we know
		})
		if up {
			// We've found the largest set of voters to drop traffic to
			// without losing quorum.
			break
		}
		var sl []roachpb.ReplicaID
		for sid := range liveOverloadedVoterCandidates {
			sl = append(sl, sid)
		}
		// Sort for determinism during tests.
		slices.Sort(sl)
		// Remove a random voter candidate, and loop around to see if we now have
		// quorum.
		if rnd == nil {
			rnd = rand.New(rand.NewSource(d.seed))
		}
		delete(liveOverloadedVoterCandidates, sl[rnd.Intn(len(sl))])
	}

	// Return union of non-voter and voter candidates.
	for nonVoter := range liveOverloadedNonVoterCandidates {
		liveOverloadedVoterCandidates[nonVoter] = struct{}{}
	}
	return liveOverloadedVoterCandidates, nonLive
}

type ioThresholdMap struct {
	threshold float64 // threshold at which the score indicates pausability
	seq       int     // bumped on creation if pausable set changed
	m         map[roachpb.StoreID]*admissionpb.IOThreshold
}

func (osm ioThresholdMap) String() string {
	return redact.StringWithoutMarkers(osm)
}

var _ redact.SafeFormatter = (*ioThresholdMap)(nil)

func (osm ioThresholdMap) SafeFormat(s redact.SafePrinter, verb rune) {
	var sl []roachpb.StoreID
	for id := range osm.m {
		sl = append(sl, id)
	}
	slices.SortFunc(sl, func(a, b roachpb.StoreID) int {
		aScore, _ := osm.m[a].Score()
		bScore, _ := osm.m[b].Score()
		return cmp.Compare(aScore, bScore)
	})
	for i, id := range sl {
		if i > 0 {
			s.SafeString(", ")
		}
		s.Printf("s%d: %s", id, osm.m[id])
	}
	if len(sl) > 0 {
		s.Printf(" [pausable-threshold=%.2f]", osm.threshold)
	}
}

var _ ioThresholdMapI = (*ioThresholdMap)(nil)

// AbovePauseThreshold implements ioThresholdMapI.
func (osm *ioThresholdMap) AbovePauseThreshold(id roachpb.StoreID) bool {
	sc, _ := osm.m[id].Score()
	return sc > osm.threshold
}

func (osm *ioThresholdMap) AnyAbovePauseThreshold(repls roachpb.ReplicaSet) bool {
	descs := repls.Descriptors()
	for i := range descs {
		if osm.AbovePauseThreshold(descs[i].StoreID) {
			return true
		}
	}
	return false
}

func (osm *ioThresholdMap) IOThreshold(id roachpb.StoreID) *admissionpb.IOThreshold {
	return osm.m[id]
}

// Sequence allows distinguishing sets of overloaded stores. Whenever an
// ioThresholdMap is created, it inherits the sequence of its predecessor,
// incrementing only when the set of pausable stores has changed in the
// transition.
func (osm *ioThresholdMap) Sequence() int {
	return osm.seq
}

type ioThresholds struct {
	mu struct {
		syncutil.Mutex
		inner *ioThresholdMap // always replaced wholesale, so can leak out of mu
	}
}

func (osm *ioThresholds) Current() *ioThresholdMap {
	osm.mu.Lock()
	defer osm.mu.Unlock()
	return osm.mu.inner
}

// Replace replaces the stored view of stores for which we track IOThresholds.
// If the set of overloaded stores (i.e. with a score of >= seqThreshold)
// changes in the process, the updated view will have an incremented Sequence().
func (osm *ioThresholds) Replace(
	m map[roachpb.StoreID]*admissionpb.IOThreshold, seqThreshold float64,
) (prev, cur *ioThresholdMap) {
	osm.mu.Lock()
	defer osm.mu.Unlock()
	last := osm.mu.inner
	if last == nil {
		last = &ioThresholdMap{}
	}
	next := &ioThresholdMap{threshold: seqThreshold, seq: last.seq, m: m}
	var delta int
	for id := range last.m {
		if last.AbovePauseThreshold(id) != next.AbovePauseThreshold(id) {
			delta = 1
			break
		}
	}
	for id := range next.m {
		if last.AbovePauseThreshold(id) != next.AbovePauseThreshold(id) {
			delta = 1
			break
		}
	}
	next.seq += delta
	osm.mu.inner = next
	return last, next
}

func (r *Replica) updatePausedFollowersLocked(ctx context.Context, ioThresholdMap *ioThresholdMap) {
	r.mu.pausedFollowers = nil

	desc := r.descRLocked()
	repls := desc.Replicas()

	if !ioThresholdMap.AnyAbovePauseThreshold(repls) {
		return
	}

	if !r.isRaftLeaderRLocked() {
		// Only the raft leader pauses followers. Followers never send meaningful
		// amounts of data in raft messages, so pausing doesn't make sense on them.
		return
	}

	if r.shouldReplicationAdmissionControlUsePullMode(ctx) {
		// Replication admission control is enabled and is using pull-mode which
		// allows for formation of a send-queue. The send-queue and pull-mode
		// behavior is RAC2 subsumes follower pausing, so do not pause.
		return
	}

	if !quotaPoolEnabledForRange(desc) {
		// If the quota pool isn't enabled (like for the liveness range), play it
		// safe. The range is unlikely to be a major contributor to any follower's
		// I/O and wish to reduce the likelihood of a problem in replication pausing
		// contributing to an outage of that critical range.
		return
	}

	status := r.leaseStatusAtRLocked(ctx, r.Clock().NowAsClockTimestamp())
	if !status.IsValid() || !status.OwnedBy(r.StoreID()) {
		// If we're not the leaseholder (which includes the case in which we just
		// transferred the lease away), leave all followers unpaused. Otherwise, the
		// leaseholder won't learn that the entries it submitted were committed
		// which effectively causes range unavailability.
		return
	}

	// When multiple followers are overloaded, we may not be able to exclude all
	// of them from replication traffic due to quorum constraints. We would like
	// a given Range to deterministically exclude the same store (chosen
	// randomly), so that across multiple Ranges we have a chance of removing
	// load from all overloaded Stores in the cluster. (It would be a bad idea
	// to roll a per-Range dice here on every tick, since that would rapidly
	// include and exclude individual followers from replication traffic, which
	// would be akin to a high rate of packet loss. Once we've decided to ignore
	// a follower, this decision should be somewhat stable for at least a few
	// seconds).
	seed := int64(r.RangeID)
	now := r.store.Clock().Now().GoTime()
	d := computeExpendableOverloadedFollowersInput{
		self:          r.replicaID,
		replDescs:     repls,
		ioOverloadMap: ioThresholdMap,
		getProgressMap: func(_ context.Context) map[raftpb.PeerID]tracker.Progress {
			prs := r.mu.internalRaftGroup.Status().Progress
			updateRaftProgressFromActivity(ctx, prs, repls.Descriptors(), func(id roachpb.ReplicaID) bool {
				return r.mu.lastUpdateTimes.isFollowerActiveSince(id, now, r.store.cfg.RangeLeaseDuration)
			})
			return prs
		},
		minLiveMatchIndex: r.mu.proposalQuotaBaseIndex,
		seed:              seed,
	}
	r.mu.pausedFollowers, _ = computeExpendableOverloadedFollowers(ctx, d)
	bypassFn := r.store.TestingKnobs().RaftReportUnreachableBypass
	for replicaID := range r.mu.pausedFollowers {
		if bypassFn != nil && bypassFn(replicaID) {
			continue
		}
		// We're dropping messages to those followers (see handleRaftReady) but
		// it's a good idea to tell raft not to even bother sending in the first
		// place. Raft will react to this by moving the follower to probing state
		// where it will be contacted only sporadically until it responds to an
		// MsgApp (which it can only do once we stop dropping messages). Something
		// similar would result naturally if we didn't report as unreachable, but
		// with more wasted work.
		r.mu.internalRaftGroup.ReportUnreachable(raftpb.PeerID(replicaID))
	}
	r.mu.replicaFlowControlIntegration.onFollowersPaused(ctx)
}
