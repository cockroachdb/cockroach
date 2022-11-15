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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// appBatchNoMethods is the struct on which an implementation of appHandler is
// be provided for use with stand-alone log application (#75729).
//
// The desire to provide both stand-alone and "regular" log application without
// code duplication has led to there being three identical structs:
//
//   - appBatchNoMethods is a pure struct, with no methods attached. This is embedded
//     into replicaAppBatch for convenience, without having to worry about accidentally
//     adopting methods.
//   - appBatchConcrete implements a variant of appHandler but using concrete types
//     (i.e. no interfaces) in the parameters. This is the type used when
//     replicaAppBatch calls through to appBatch to avoid duplicated logic,
//     and avoids repeatedly type asserting.
//   - appBatchHandler implements appHandler. It contains no logic but simply type
//     asserts and calls through to appBatchConcrete. This is currently unused but
//     will be used as replicaAppBatch.h in standalone log application.
type appBatchNoMethods struct {
	// batch accumulates writes implied by the raft entries in this batch.
	batch storage.Batch
	// state is this batch's view of the replica's state. It may be updated
	// as commands are staged into this batch.
	state kvserverpb.ReplicaState
	// changeRemovesReplica tracks whether the command in the batch (there must
	// be only one) removes this replica from the range. If this is the case,
	// the command that does so effectively removes the replica.
	changeRemovesReplica bool
}

// appBatchConcrete implements a variant of apply.Batch, but using concrete
// paremeter types. See appBatchNoMethods for rationale.
type appBatchConcrete appBatchNoMethods

// CheckForcedErr is part of a concrete implementation of appHandler. It passes
// through to kvserverbase.CheckForcedErr.
func (h *appBatchConcrete) CheckForcedErr(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	raftCmd *kvserverpb.RaftCommand,
	isLocal bool,
	state *kvserverpb.ReplicaState,
) (leaseIndex uint64, _ kvserverbase.ProposalRejectionType, _ *roachpb.Error) {
	return kvserverbase.CheckForcedErr(ctx, idKey, raftCmd, isLocal, state)
}

// AssertCheckedCommand is part of a concrete implementation of appHandler. It
// checks that the raft log index is set and equals the previously applied entry
// plus one, i.e. asserts that no entries were skipped.
func (h *appBatchConcrete) AssertCheckedCommand(
	ctx context.Context, cmd *raftlog.ReplicatedCmd,
) error {
	if cmd.Index() == 0 {
		return kvserverbase.NonDeterministicErrorf("processRaftCommand requires a non-zero index")
	}
	if idx, applied := cmd.Index(), h.state.RaftAppliedIndex; idx != applied+1 {
		// If we have an out-of-order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return.
		return kvserverbase.NonDeterministicErrorf("applied index jumped from %d to %d", applied, idx)
	}

	// TODO(sep-raft-log): move the remainder of (*replicaAppBatchHandler).AssertCheckedCommand here; this
	// just needs a bit more untangling.
	return nil
}

// appBatchHandler implements appHandler, by calling the corresponding concrete
// methods on appBatchConcrete. See appBatchNoMethods for rationale.
type appBatchHandler appBatchNoMethods

var _ appHandler = (*appBatchHandler)(nil)

// CheckForcedErr implements appHandler. See the corresponding method on appBatchConcrete.
func (h *appBatchHandler) CheckForcedErr(
	ctx context.Context, cmdI apply.Command,
) (leaseIndex uint64, _ kvserverbase.ProposalRejectionType, _ *roachpb.Error) {
	cmd := cmdI.(*raftlog.ReplicatedCmd)
	const isLocal = false // standalone application is never local
	return (*appBatchConcrete)(h).CheckForcedErr(ctx, cmd.ID, &cmd.Cmd, isLocal, &h.state)
}

// AssertCheckedCommand implements appHandler. See the corresponding method on appBatchConcrete.
func (h *appBatchHandler) AssertCheckedCommand(
	ctx context.Context, cmdI apply.CheckedCommand,
) error {
	return (*appBatchConcrete)(h).AssertCheckedCommand(ctx, cmdI.(*raftlog.ReplicatedCmd))
}
