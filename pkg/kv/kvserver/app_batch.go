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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type appBatch struct {
	// TODO(tbg): this will absorb the following fields from replicaAppBatch:
	//
	// - batch
	// - state
	// - changeRemovesReplica
}

func (b *appBatch) assertAndCheckCommand(
	ctx context.Context, cmd *raftlog.ReplicatedCmd, state *kvserverpb.ReplicaState, isLocal bool,
) (leaseIndex uint64, _ kvserverbase.ProposalRejectionType, forcedErr *roachpb.Error, _ error) {
	if log.V(4) {
		log.Infof(ctx, "processing command %x: raftIndex=%d maxLeaseIndex=%d closedts=%s",
			cmd.ID, cmd.Index(), cmd.Cmd.MaxLeaseIndex, cmd.Cmd.ClosedTimestamp)
	}

	if cmd.Index() == 0 {
		return 0, 0, nil, errors.AssertionFailedf("processRaftCommand requires a non-zero index")
	}
	if idx, applied := cmd.Index(), state.RaftAppliedIndex; idx != applied+1 {
		// If we have an out-of-order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return.
		return 0, 0, nil, errors.AssertionFailedf("applied index jumped from %d to %d", applied, idx)
	}

	// TODO(sep-raft-log): move the closedts checks from replicaAppBatch here as
	// well. This just needs a bit more untangling as they reference *Replica, but
	// for no super-convincing reason.

	leaseIndex, rej, forcedErr := kvserverbase.CheckForcedErr(ctx, cmd.ID, &cmd.Cmd, isLocal, state)
	return leaseIndex, rej, forcedErr, nil
}

func (b *appBatch) toCheckedCmd(
	ctx context.Context,
	cmd *raftlog.ReplicatedCmd,
	leaseIndex uint64,
	rej kvserverbase.ProposalRejectionType,
	forcedErr *roachpb.Error,
) {
	cmd.LeaseIndex, cmd.Rejection, cmd.ForcedErr = leaseIndex, rej, forcedErr
	if cmd.Rejected() {
		log.VEventf(ctx, 1, "applying command with forced error: %s", cmd.ForcedErr)

		// Apply an empty command.
		cmd.Cmd.ReplicatedEvalResult = kvserverpb.ReplicatedEvalResult{}
		cmd.Cmd.WriteBatch = nil
		cmd.Cmd.LogicalOpLog = nil
		cmd.Cmd.ClosedTimestamp = nil
	} else {
		log.Event(ctx, "applying command")
	}
}
