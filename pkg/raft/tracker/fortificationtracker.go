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

// FortificationEnabled returns whether the raft fortification protocol is
// enabled or not.
func (ft *FortificationTracker) FortificationEnabled() bool {
	return ft.storeLiveness.SupportFromEnabled()
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

	// TODO(arul): avoid this map allocation as we're calling LeadSupportUntil
	// from hot paths.
	supportExpMap := make(map[pb.PeerID]hlc.Timestamp)
	for id, supportEpoch := range ft.fortification {
		curEpoch, curExp := ft.storeLiveness.SupportFrom(id)
		// NB: We can't assert that supportEpoch <= curEpoch because there may be a
		// race between a successful MsgFortifyLeaderResp and the store liveness
		// heartbeat response that lets the leader know the follower's store is
		// supporting the leader's store at the epoch in the MsgFortifyLeaderResp
		// message.
		if curEpoch == supportEpoch {
			supportExpMap[id] = curExp
		}
	}

	leadSupportUntil := ft.config.Voters.LeadSupportExpiration(supportExpMap)
	return ft.leaderMaxSupported.Forward(leadSupportUntil)
}

// CanDefortify returns whether the caller can safely[1] de-fortify the term
// based on the sate tracked by the FortificationTracker.
//
// [1] Without risking regressions in the maximum that's ever been indicated to
// the layers above. Or, more simply, without risking regression of leader
// leases.
func (ft *FortificationTracker) CanDefortify() bool {
	leaderMaxSupported := ft.leaderMaxSupported.Load()
	if leaderMaxSupported.IsEmpty() {
		// If leaderMaxSupported is empty, it means that we've never returned any
		// timestamps to the layers above in calls to LeadSupportUntil. We should be
		// able to de-fortify. If a tree falls in a forrest ...
		ft.logger.Debugf("leaderMaxSupported is empty when computing whether we can de-fortify or not")
	}
	return ft.storeLiveness.SupportExpired(leaderMaxSupported)
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
