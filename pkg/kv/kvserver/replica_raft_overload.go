// Copyright 2022 The Cockroach Authors.
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
	"math/rand"
	"sort"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/tracker"
)

var pauseReplicationIOThreshold = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.kv.pause_replication_io_threshold",
	"pause replication to non-essential followers when their I/O admission control score exceeds the given threshold (zero to disable)",
	// TODO(tbg): set a nonzero default.
	// See: https://github.com/cockroachdb/cockroach/issues/83920
	0.0,
	func(v float64) error {
		if v == 0 {
			return nil
		}
		const min = 0.3
		if v < min {
			return errors.Errorf("minimum admissible nonzero value is %f", min)
		}
		return nil
	},
)

type computeExpendableOverloadedFollowersInput struct {
	replDescs roachpb.ReplicaSet
	// TODO(tbg): all entries are overloaded, so consdier removing the IOThreshold here
	// because it's confusing.
	ioOverloadMap map[roachpb.StoreID]*admissionpb.IOThreshold
	// getProgressMap returns Raft's view of the progress map. This is only called
	// when needed, and at most once.
	getProgressMap func(context.Context) map[uint64]tracker.Progress
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
	minLiveMatchIndex uint64
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
// maximum set of such followers that we can afford not replicating to without
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

	var prs map[uint64]tracker.Progress

	for _, replDesc := range d.replDescs.AsProto() {
		if _, overloaded := d.ioOverloadMap[replDesc.StoreID]; !overloaded {
			continue
		}
		// There's at least one overloaded follower, so initialize
		// extra state to determine which traffic we can drop without
		// losing quorum.
		if prs == nil {
			prs = d.getProgressMap(ctx)
			nonLive = map[roachpb.ReplicaID]nonLiveReason{}
			for id, pr := range prs {
				if !pr.RecentActive {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonInactive
				}
				if pr.IsPaused() {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonPaused
				}
				if pr.Match < d.minLiveMatchIndex {
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
		if prs[uint64(replDesc.ReplicaID)].IsLearner {
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
		sort.Slice(sl, func(i, j int) bool {
			return sl[i] < sl[j]
		})
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

type overloadedStoresMap atomic.Value // map[roachpb.StoreID]*admissionpb.IOThreshold

func (osm *overloadedStoresMap) Load() map[roachpb.StoreID]*admissionpb.IOThreshold {
	v, _ := (*atomic.Value)(osm).Load().(map[roachpb.StoreID]*admissionpb.IOThreshold)
	return v
}

func (osm *overloadedStoresMap) Swap(
	m map[roachpb.StoreID]*admissionpb.IOThreshold,
) map[roachpb.StoreID]*admissionpb.IOThreshold {
	v, _ := (*atomic.Value)(osm).Swap(m).(map[roachpb.StoreID]*admissionpb.IOThreshold)
	return v
}
