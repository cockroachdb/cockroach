// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/raft/logger"
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// FortificationTracker is used to track fortification from peers. This can
// then be used to compute until when a leader's support expires.
type FortificationTracker struct {
	config        *quorum.Config
	storeLiveness raftstoreliveness.StoreLiveness

	// fortification contains a map of nodes which have fortified the leader
	// through fortification handshakes, and the corresponding Store Liveness
	// epochs that they have supported the leader in.
	fortification map[pb.PeerID]pb.Epoch

	// deFortification contains a map of nodes that are known to have been
	// successfully de-fortified.
	//
	// The presence of a node in this map does not mean the node was definitely
	// fortified at some point. It simply means it is definitely no longer
	// fortified, and therefore we no longer need to send it de-fortification
	// requests.
	deFortification map[pb.PeerID]struct{}

	logger logger.Logger
}

// MakeFortificationTracker initializes a FortificationTracker.
func MakeFortificationTracker(
	config *quorum.Config, storeLiveness raftstoreliveness.StoreLiveness, logger logger.Logger,
) FortificationTracker {
	st := FortificationTracker{
		config:          config,
		storeLiveness:   storeLiveness,
		fortification:   map[pb.PeerID]pb.Epoch{},
		deFortification: map[pb.PeerID]struct{}{},
		logger:          logger,
	}
	return st
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

// AlreadyDeFortified returns whether the supplied peer is known to have been
// de-fortified already.
func (ft *FortificationTracker) AlreadyDeFortified(id pb.PeerID) bool {
	_, isDefortified := ft.deFortification[id]
	return isDefortified
}

// RecordDeFortification records that the peer with the supplied id has been
// successfully de-fortified.
func (ft *FortificationTracker) RecordDeFortification(id pb.PeerID, epoch pb.Epoch) {
	ft.deFortification[id] = struct{}{}

	// Perform some sanity checks before returning.
	curEpoch, found := ft.fortification[id]
	if !found {
		ft.logger.Debugf("recorded de-fortification for %d that we weren't tracking fortification for", id)
		return
	}
	// NB: The follower may have been supporting the epoch we're tracking here, or
	// a higher epoch that we never heard about. However, if the follower tells us
	// it was supporting a lower epoch than what we've been tracking here, we've
	// got a bug.
	//
	// TODO(arul): I think we'll need to make an exception of epoch == 0, where the
	// follower independently de-fortified because it was safe to do so. Make sure
	// something breaks before adding that conditional.
	if epoch < curEpoch {
		panic("leader tracking an epoch that is greater than what the follower says it was supporting")
	}
	// Clear out the fortification epoch we were tracking for this peer. While not
	// necessary, doing so prevents misuse in case someone accesses the
	// fortification state in the future.
	delete(ft.fortification, id)
}

// Reset clears out any previously tracked fortification.
func (ft *FortificationTracker) Reset() {
	clear(ft.fortification)
	// TODO(arul): when we introduce ft.LeadSupportUntil we need to make sure it
	// isn't reset here, because we don't want it to regress when a leader steps
	// down.
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
func (ft *FortificationTracker) LeadSupportUntil() hlc.Timestamp {
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
	return ft.config.Voters.LeadSupportExpiration(supportExpMap)
}

// QuorumActive returns whether the leader is currently supported by a quorum or
// not.
func (ft *FortificationTracker) QuorumActive() bool {
	return !ft.storeLiveness.SupportExpired(ft.LeadSupportUntil())
}

func (ft *FortificationTracker) String() string {
	if len(ft.fortification) == 0 {
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
