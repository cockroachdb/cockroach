// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracker

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// FortificationTracker is used to track fortification from peers. This can
// then be used to compute until when a leader's support expires.
type FortificationTracker struct {
	config        *quorum.Config
	storeLiveness raftstoreliveness.StoreLiveness

	// term is the leadership term associated with fortification tracking. It
	// allows a leader whose leadership term has since ended to keep track of
	// fortification state and take action based on it (e.g. de-fortify
	// followers). It should be reset right before a peer steps up to become a
	// leader again.
	//
	// The term differs from raft.term in that raft.term is the highest term known
	// to a peer, whereas FortificationTracker.term is the highest term a peer was
	// a leader at since it was restarted.
	term uint64

	// fortificationEnabledForTerm tracks whether fortification was enabled or not
	// at any point for the leadership term being tracked in the
	// FortificationTracker. If it was, we must conservatively assume that the
	// raft leader attempted to fortify its term, and save this state.
	fortificationEnabledForTerm bool

	// needsDefortification tracks whether the node should broadcast
	// MsgDeFortifyLeader to all followers to de-fortify the term being tracked in
	// the tracker or not.
	needsDefortification bool

	// fortification contains a map of nodes which have fortified the leader
	// through fortification handshakes, and the corresponding Store Liveness
	// epochs that they have supported the leader in.
	fortification map[pb.PeerID]pb.Epoch

	// leaderMaxSupported is the maximum LeadSupportUntil that the leader has
	// ever claimed to support. Tracking this ensures that LeadSupportUntil
	// never regresses for a raft group. Naively, without any tracking, this
	// can happen around configuration changes[1] and leader step down[2].
	//
	// NB: We use an atomicTimestamp here, which allows us to forward
	// leadMaxSupported on every call to LeadSupportUntil, without requiring
	// callers to acquire a write lock. Typically, LeadSupportUntil is called into
	// by get{LeadSupport,}Status
	//
	// [1] We must ensure that the current LeadSupportUntil is greater than or
	// equal to any previously calculated LeadSupportUntil before proposing a new
	// configuration change.
	// [2] A leader may step down while its LeadSupportUntil is in the future. In
	// such cases, it shouldn't take any action (such as broadcasting
	// de-fortification messages, voting for another peer, or calling an election
	// at a higher term) that could elect a leader until LeadSupportUntil is in
	// the past.
	leaderMaxSupported atomicTimestamp

	logger raftlogger.Logger
}

// NewFortificationTracker initializes a FortificationTracker.
func NewFortificationTracker(
	config *quorum.Config, storeLiveness raftstoreliveness.StoreLiveness, logger raftlogger.Logger,
) *FortificationTracker {
	st := FortificationTracker{
		config:        config,
		storeLiveness: storeLiveness,
		fortification: map[pb.PeerID]pb.Epoch{},
		logger:        logger,
	}
	return &st
}

// FortificationEnabledForTerm returns whether the raft fortification should be
// enabled for the term being tracked in the FortificationTracker.
//
// NB: Fortification may be enabled while a leadership term is in progress.
// However, once fortification has been enabled for a term, it will never flip
// back.
func (ft *FortificationTracker) FortificationEnabledForTerm() bool {
	if !ft.fortificationEnabledForTerm {
		// Check whether fortification has been enabled.
		ft.fortificationEnabledForTerm = ft.storeLiveness.SupportFromEnabled()
	}
	return ft.fortificationEnabledForTerm
}

// RecordFortification records fortification of the given peer for the supplied
// epoch.
func (ft *FortificationTracker) RecordFortification(id pb.PeerID, epoch pb.Epoch) {
	// The supported epoch should never regress. Guard against out of order
	// delivery of fortify responses by using max.
	ft.fortification[id] = max(ft.fortification[id], epoch)
}

// Reset clears out any previously tracked fortification and prepares the
// fortification tracker to be used by a newly elected leader.
func (ft *FortificationTracker) Reset(term uint64) {
	ft.term = term
	ft.fortificationEnabledForTerm = false
	// Whether we need to de-fortify or not is first contingent on whether
	// fortification was enabled for the term being tracked or not. If
	// fortification was attempted, though, we'll need to de-fortify.
	ft.needsDefortification = true
	clear(ft.fortification)
	ft.leaderMaxSupported.Reset()
}

// IsFortifiedBy returns whether the follower fortifies the leader or not.
// If the follower's store doesn't support the leader's store in the store
// liveness fabric, then both isSupported and isFortified will be false.
// If isFortified is true, it implies that isSupported is also true.
func (ft *FortificationTracker) IsFortifiedBy(id pb.PeerID) (isFortified bool, isSupported bool) {
	supportEpoch, curExp := ft.storeLiveness.SupportFrom(id)
	if ft.storeLiveness.SupportExpired(curExp) {
		return false, false
	}

	// At this point we know that the follower's store is providing support
	// at the store liveness fabric.
	fortificationEpoch, exist := ft.fortification[id]
	if !exist {
		// We don't know that the follower is fortified.
		return false, true
	}

	// NB: We can't assert that supportEpoch <= fortificationEpoch because there
	// may be a race between a successful MsgFortifyLeaderResp and the store
	// liveness heartbeat response that lets the leader know the follower's store
	// is supporting the leader's store at the epoch in the MsgFortifyLeaderResp
	// message.
	return fortificationEpoch == supportEpoch, true
}

// LeadSupportUntil returns the timestamp until which the leader is guaranteed
// fortification until based on the fortification being tracked for it by its
// peers.
func (ft *FortificationTracker) LeadSupportUntil(state pb.StateType) hlc.Timestamp {
	if state != pb.StateLeader {
		// We're not the leader, so LeadSupportUntil shouldn't advance.
		return ft.leaderMaxSupported.Load()
	}

	// Compute the lead support using the current configuration and forward the
	// leaderMaxSupported to avoid regressions when the configuration changes.
	leadSupportUntil := ft.computeLeadSupportUntil(state)
	return ft.leaderMaxSupported.Forward(leadSupportUntil)
}

// computeLeadSupportUntil computes the timestamp until which the leader is
// guaranteed fortification using the current quorum configuration.
//
// Unlike LeadSupportUntil, this computation does not provide a guarantee of
// monotonicity. Specifically, its result may regress after a configuration
// change.
func (ft *FortificationTracker) computeLeadSupportUntil(state pb.StateType) hlc.Timestamp {
	if state != pb.StateLeader {
		panic("computeLeadSupportUntil should only be called by the leader")
	}
	if len(ft.fortification) == 0 {
		return hlc.Timestamp{} // fast-path for no fortification
	}

	// TODO(arul): avoid this map allocation as we're calling LeadSupportUntil
	// from hot paths.
	supportExpMap := make(map[pb.PeerID]hlc.Timestamp)
	ft.config.Voters.Visit(func(id pb.PeerID) {
		if supportEpoch, ok := ft.fortification[id]; ok {
			curEpoch, curExp := ft.storeLiveness.SupportFrom(id)
			// NB: We can't assert that supportEpoch <= curEpoch because there may be
			// a race between a successful MsgFortifyLeaderResp and the store liveness
			// heartbeat response that lets the leader know the follower's store is
			// supporting the leader's store at the epoch in the MsgFortifyLeaderResp
			// message.
			if curEpoch == supportEpoch {
				supportExpMap[id] = curExp
			}
		}
	})
	return ft.config.Voters.LeadSupportExpiration(supportExpMap)
}

// CanDefortify returns whether the caller can safely[1] de-fortify the term
// based on the state tracked by the FortificationTracker.
//
// [1] Without risking regressions in the maximum that's ever been indicated to
// the layers above. Or, more simply, without risking regression of leader
// leases.
func (ft *FortificationTracker) CanDefortify() bool {
	if ft.term == 0 {
		return false // nothing is being tracked
	}
	leaderMaxSupported := ft.leaderMaxSupported.Load()
	if leaderMaxSupported.IsEmpty() {
		// If leaderMaxSupported is empty, it means that we've never returned any
		// timestamps to the layers above in calls to LeadSupportUntil. We should be
		// able to de-fortify. If a tree falls in a forrest ...
		ft.logger.Debugf("leaderMaxSupported is empty when computing whether we can de-fortify or not")
	}
	return ft.storeLiveness.SupportExpired(leaderMaxSupported)
}

// NeedsDefortify returns whether the node should still continue to broadcast
// MsgDeFortifyLeader to all followers to de-fortify the term being tracked in
// the tracker or not.
func (ft *FortificationTracker) NeedsDefortify() bool {
	if !ft.fortificationEnabledForTerm {
		// We never attempted to fortify this term, so we don't need to de-fortify.
		return false
	}
	return ft.needsDefortification
}

// InformCommittedTerm informs the fortification tracker that an entry proposed
// in the supplied term has been committed.
func (ft *FortificationTracker) InformCommittedTerm(committedTerm uint64) {
	if committedTerm > ft.term {
		// The committed term (T+1) has advanced beyond the term being tracked in
		// the fortification tracker (T). This means that not only was a new leader
		// elected at term T+1, but it was also able to commit a log entry. This
		// means that a majority of followers are no longer supporting the old
		// leader at term T. This allows us to stop de-fortifying term T.
		//
		// Note that even if a minority of followers are still supporting the old
		// leader at term T, and they never hear from the new leader at term T+1,
		// this shouldn't prevent us from electing a new leader at term T+2 in the
		// future. That's because when campaigning for term T+2, candidates will
		// include their most recent log entry. This must be at term T+1 for any
		// viable candidate. Then, even if a candidate needs a vote from a follower
		// in the minority that is still supporting the leader at term T, the
		// follower will grant its vote when it notices the candidate is
		// campaigning with a log entry that was committed at term T+1.
		//
		// To reiterate, it's safe to stop de-fortifying once a new leader has been
		// elected at term T' > T, and the new leader has commited a log entry at
		// term T'[1]. We're in this case -- save some state, so we can safely say no
		// the next time we're asked whether we need to de-fortify or not.
		//
		// [1] Note that the leader doesn't need to have this log entry committed at
		// T' in its log, it just needs to know such an entry exists.
		ft.needsDefortification = false
	}
}

// ConfigChangeSafe returns whether it is safe to propose a configuration change
// or not, given the current state of lead support.
//
// If the lead support has not caught up from the previous configuration, we
// must not propose another configuration change. Doing so would compromise the
// lead support promise made by the previous configuration and used as an
// expiration of a leader lease. Instead, we wait for the lead support under the
// current configuration to catch up to the maximum lead support reached under
// the previous config. If the lead support is never able to catch up, the
// leader will eventually step down due to CheckQuorum.
//
// The following timeline illustrates the hazard that this check is guarding
// against:
//
// 1. configuration A=(r1, r2, r3), leader=r1
//   - lead_support=20 (r1=30, r2=20, r3=10)
//
// 2. config change #1 adds r4 to the group
//   - configuration B=(r1, r2, r3, r4)
//
// 3. lead support appears to regress to 10
//   - lead_support=10 (r1=30, r2=20, r3=10, r4=0)
//
// 4. any majority quorum for leader election involves r1 or r2
//   - therefore, max lead support of 20 is “safe”
//   - this is analogous to how the raft Leader Completeness invariant works
//     even across config changes, using either (1) single addition/removal
//     at-a-time changes, or (2) joint consensus. Either way, consecutive
//     configs share overlapping majorities.
//
// 5. config change #2 adds r5 to the group
//   - configuration C=(r1, r2, r3, r4, r5)
//
// 6. lead_support still at 10
//   - lead_support=10 (r1=30, r2=20, r3=10, r4=0, r5=0)
//   - however, max lead support of 20 no longer “safe”
//
// 7. r3 can win election with support from r4 and r5 before time 20
//   - neither r1 nor r2 need to be involved
//   - HAZARD! this could violate the original lead support promise
//
// To avoid this hazard, we must wait for the lead support under configuration B
// to catch up to the maximum lead support reached under configuration A before
// allowing the proposal of configuration C. This ensures that the overlapping
// majorities between subsequent configurations preserve the safety of lead
// support.
func (ft *FortificationTracker) ConfigChangeSafe() bool {
	// A configuration change is only safe if the current configuration's lead
	// support has caught up to the maximum lead support reached under the
	// previous configuration, which is reflected in leaderMaxSupported.
	//
	// NB: Only run by the leader.
	return ft.leaderMaxSupported.Load().LessEq(ft.computeLeadSupportUntil(pb.StateLeader))
}

// QuorumActive returns whether the leader is currently supported by a quorum or
// not.
func (ft *FortificationTracker) QuorumActive() bool {
	// NB: Only run by the leader.
	return !ft.storeLiveness.SupportExpired(ft.LeadSupportUntil(pb.StateLeader))
}

// Term returns the leadership term for which the tracker is/was tracking
// fortification state.
func (ft *FortificationTracker) Term() uint64 {
	return ft.term
}

func (ft *FortificationTracker) Empty() bool {
	return len(ft.fortification) == 0
}

func (ft *FortificationTracker) String() string {
	if ft.Empty() {
		return "empty"
	}
	// Print the map in sorted order as we assert on its output in tests.
	ids := make([]pb.PeerID, 0, len(ft.fortification))
	for id := range ft.fortification {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d : %d\n", id, ft.fortification[id])
	}
	return buf.String()
}

// atomicTimestamp is a thin wrapper to provide atomic access to a timestamp.
type atomicTimestamp struct {
	mu syncutil.Mutex

	ts hlc.Timestamp
}

func (a *atomicTimestamp) Load() hlc.Timestamp {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ts
}

func (a *atomicTimestamp) Forward(ts hlc.Timestamp) hlc.Timestamp {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.ts.Forward(ts)
	return a.ts
}

func (a *atomicTimestamp) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ts = hlc.Timestamp{}
}
