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

	// The DisableValidationAgainstCurConfig flag allows disabling config change
	// constraints that are guaranteed by the upper state machine layer (incorrect
	// ones will apply as no-ops). See raft.Config.DisableConfChangeValidation for
	// an explanation of why this may be used.
	if !ctx.DisableValidationAgainstCurConfig {
		curConfig := ctx.CurConfig

		alreadyJoint := len(curConfig.Voters[1]) > 0
		wantsLeaveJoint := cc.LeaveJoint()
		if alreadyJoint && !wantsLeaveJoint {
			return errors.New("must transition out of joint config first")
		} else if !alreadyJoint && wantsLeaveJoint {
			return errors.New("not in joint state; refusing empty conf change")
		}

		// TODO(nvanbenschoten): check against direct voter removal.
	}

	return nil
}
