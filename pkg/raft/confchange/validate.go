// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package confchange

import (
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/errors"
)

// ValidationContext is a context struct that that is passed to ValidateProp to
// allow it to validate a proposed ConfChange against the current state of the
// Raft group.
type ValidationContext struct {
	CurConfig                         *quorum.Config
	Applied                           uint64
	PendingConfIndex                  uint64
	LeadSupportSafe                   bool
	DisableValidationAgainstCurConfig bool
}

// ValidateProp validates a proposed ConfChange against the current state of the
// Raft group. It returns an error if the proposed change is invalid and must
// not be proposed.
func ValidateProp(ctx ValidationContext, cc pb.ConfChangeV2) error {
	// Per the "Apply" invariant in the config change safety argument[^1],
	// the leader must not append a config change if it hasn't applied all
	// config changes in its log.
	//
	// [^1]: https://github.com/etcd-io/etcd/issues/7625#issuecomment-489232411
	if ctx.PendingConfIndex > ctx.Applied {
		return errors.Errorf("possible unapplied conf change at index %d (applied to %d)",
			ctx.PendingConfIndex, ctx.Applied)
	}

	// If the lead support has not caught up from the previous configuration, we
	// must not propose another configuration change. Doing so would compromise
	// the lead support promise made by the previous configuration and used as an
	// expiration of a leader lease. Instead, we wait for the lead support under
	// the current configuration to catch up to the maximum lead support reached
	// under the previous config. If the lead support is never able to catch up,
	// the leader will eventually step down due to CheckQuorum.
	if !ctx.LeadSupportSafe {
		return errors.Errorf("lead support has not caught up to previous configuration")
	}

	// The DisableValidationAgainstCurConfig flag allows disabling config change
	// constraints that are guaranteed by the upper state machine layer (incorrect
	// ones will apply as no-ops). See raft.Config.DisableConfChangeValidation for
	// an explanation of why this may be used.
	if !ctx.DisableValidationAgainstCurConfig {
		curConfig := ctx.CurConfig

		// Check that the proposed ConfChange is handling the joint state correctly.
		// When in a joint state, the next config change must be to leave the joint
		// state. When not in a joint state, the config change must not be asking us
		// to leave the joint state.
		alreadyJoint := len(curConfig.Voters[1]) > 0
		wantsLeaveJoint := cc.LeaveJoint()
		if alreadyJoint && !wantsLeaveJoint {
			return errors.New("must transition out of joint config first")
		} else if !alreadyJoint && wantsLeaveJoint {
			return errors.New("not in joint state; refusing empty conf change")
		}

		// Check against direct voter removal. Voters must first be demoted to
		// learners before they can be removed.
		voterRemovals := make(map[pb.PeerID]struct{})
		for _, cc := range cc.Changes {
			switch cc.Type {
			case pb.ConfChangeRemoveNode:
				if _, ok := incoming(curConfig.Voters)[cc.NodeID]; ok {
					voterRemovals[cc.NodeID] = struct{}{}
				}
			case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
				// If the node is being added back as a voter or learner, don't consider
				// it a removal. Note that the order of the changes to the voterRemovals
				// map is important here — we need the map manipulation to follow the
				// slice order and mirror the handling of changes in Changer.apply.
				delete(voterRemovals, cc.NodeID)
			case pb.ConfChangeUpdateNode:
			default:
				return errors.Errorf("unexpected conf type %d", cc.Type)
			}
		}
		if len(voterRemovals) > 0 {
			return errors.New("voters must be demoted to learners before being removed")
		}
	}

	return nil
}
