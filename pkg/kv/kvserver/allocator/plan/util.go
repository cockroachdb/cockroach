// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plan

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3"
)

// ReplicationChangesForRebalance returns a list of ReplicationChanges to
// execute for a rebalancing decision made by the allocator.
//
// This function assumes that `addTarget` and `removeTarget` are produced by the
// allocator (i.e. they satisfy replica `constraints` and potentially
// `voter_constraints` if we're operating over voter targets).
func ReplicationChangesForRebalance(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	numExistingVoters int,
	addTarget, removeTarget roachpb.ReplicationTarget,
	rebalanceTargetType allocatorimpl.TargetReplicaType,
) (chgs []kvpb.ReplicationChange, performingSwap bool, err error) {
	rdesc, found := desc.GetReplicaDescriptor(addTarget.StoreID)
	if rebalanceTargetType == allocatorimpl.VoterTarget && numExistingVoters == 1 {
		// If there's only one replica, the removal target is the
		// leaseholder and this is unsupported and will fail. However,
		// this is also the only way to rebalance in a single-replica
		// range. If we try the atomic swap here, we'll fail doing
		// nothing, and so we stay locked into the current distribution
		// of replicas. (Note that maybeTransferLeaseAway above will not
		// have found a target, and so will have returned (false, nil).
		//
		// Do the best thing we can, which is carry out the addition
		// only, which should succeed, and the next time we touch this
		// range, we will have one more replica and hopefully it will
		// take the lease and remove the current leaseholder.
		//
		// It's possible that "rebalancing deadlock" can occur in other
		// scenarios, it's really impossible to tell from the code given
		// the constraints we support. However, the lease transfer often
		// does not happen spuriously, and we can't enter dangerous
		// configurations sporadically, so this code path is only hit
		// when we know it's necessary, picking the smaller of two evils.
		//
		// See https://github.com/cockroachdb/cockroach/issues/40333.
		log.KvDistribution.Infof(ctx, "can't swap replica due to lease; falling back to add")

		// Even when there is only 1 existing voter, there may be other replica
		// types in the range. Check if the add target already has a replica, if so
		// it must be a non-voter or the rebalance is invalid.
		if found && rdesc.Type == roachpb.NON_VOTER {
			// The receiving store already has a non-voting replica. Instead of just
			// adding a voter to the receiving store, we *must* promote the non-voting
			// replica to a voter.
			chgs = kvpb.ReplicationChangesForPromotion(addTarget)
		} else if !found {
			chgs = []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: addTarget},
			}
		} else {
			return nil, false, errors.AssertionFailedf(
				"invalid rebalancing decision: trying to"+
					" move voter to a store that already has a replica %s for the range", rdesc,
			)
		}
		return chgs, false, err
	}

	switch rebalanceTargetType {
	case allocatorimpl.VoterTarget:
		// Check if the target being added already has a non-voting replica.
		if found && rdesc.Type == roachpb.NON_VOTER {
			// If the receiving store already has a non-voting replica, we *must*
			// execute a swap between that non-voting replica and the voting replica
			// we're trying to move to it. This swap is executed atomically via
			// joint-consensus.
			//
			// NB: Since voting replicas abide by both the overall `constraints` and
			// the `voter_constraints`, it is copacetic to make this swap since:
			//
			// 1. `addTarget` must already be a valid target for a voting replica
			// (i.e. it must already satisfy both *constraints fields) since an
			// allocator method (`allocateTarget..` or `Rebalance{Non}Voter`) just
			// handed it to us.
			// 2. `removeTarget` may or may not be a valid target for a non-voting
			// replica, but `considerRebalance` takes care to `requeue` the current
			// replica into the replicateQueue. So we expect the replicateQueue's next
			// attempt at rebalancing this range to rebalance the non-voter if it ends
			// up being in violation of the range's constraints.
			promo := kvpb.ReplicationChangesForPromotion(addTarget)
			demo := kvpb.ReplicationChangesForDemotion(removeTarget)
			chgs = append(promo, demo...)
			performingSwap = true
		} else if found {
			return nil, false, errors.AssertionFailedf(
				"programming error:"+
					" store being rebalanced to(%s) already has a voting replica", addTarget.StoreID,
			)
		} else {
			// We have a replica to remove and one we can add, so let's swap them out.
			chgs = []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: addTarget},
				{ChangeType: roachpb.REMOVE_VOTER, Target: removeTarget},
			}
		}
	case allocatorimpl.NonVoterTarget:
		if found {
			// Non-voters should not consider any of the range's existing stores as
			// valid candidates. If we get here, we must have raced with another
			// rebalancing decision.
			return nil, false, errors.AssertionFailedf(
				"invalid rebalancing decision: trying to"+
					" move non-voter to a store that already has a replica %s for the range", rdesc,
			)
		}
		chgs = []kvpb.ReplicationChange{
			{ChangeType: roachpb.ADD_NON_VOTER, Target: addTarget},
			{ChangeType: roachpb.REMOVE_NON_VOTER, Target: removeTarget},
		}
	}
	return chgs, performingSwap, nil
}

// rangeRaftStatus pretty-prints the Raft progress (i.e. Raft log position) of
// the replicas.
func rangeRaftProgress(raftStatus *raft.Status, replicas []roachpb.ReplicaDescriptor) string {
	if raftStatus == nil {
		return "[no raft status]"
	} else if len(raftStatus.Progress) == 0 {
		return "[no raft progress]"
	}
	var buf bytes.Buffer
	buf.WriteString("[")
	for i, r := range replicas {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%d", r.ReplicaID)
		if uint64(r.ReplicaID) == raftStatus.Lead {
			buf.WriteString("*")
		}
		if progress, ok := raftStatus.Progress[uint64(r.ReplicaID)]; ok {
			fmt.Fprintf(&buf, ":%d", progress.Match)
		} else {
			buf.WriteString(":?")
		}
	}
	buf.WriteString("]")
	return buf.String()
}
