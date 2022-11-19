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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// standaloneAppBatch is the struct on which an implementation of appHandler is
// be provided for use with stand-alone log application (#75729).
type standaloneAppBatch struct {
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

// TODO(sep-raft-log): also implement Batch (ApplyToStateMachine method),
// using the same approach as used for Stage.
var _ apply.EphemeralBatch = (*standaloneAppBatch)(nil)

func (b *standaloneAppBatch) assertAndCheckCommand(
	ctx context.Context, cmd *raftlog.ReplicatedCmd, isLocal bool,
) (leaseIndex uint64, _ kvserverbase.ProposalRejectionType, forcedErr *roachpb.Error, _ error) {
	if log.V(4) {
		log.Infof(ctx, "processing command %x: raftIndex=%d maxLeaseIndex=%d closedts=%s",
			cmd.ID, cmd.Index(), cmd.Cmd.MaxLeaseIndex, cmd.Cmd.ClosedTimestamp)
	}

	if cmd.Index() == 0 {
		return 0, 0, nil, errors.AssertionFailedf("processRaftCommand requires a non-zero index")
	}
	if idx, applied := cmd.Index(), b.state.RaftAppliedIndex; idx != applied+1 {
		// If we have an out-of-order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return.
		return 0, 0, nil, errors.AssertionFailedf("applied index jumped from %d to %d", applied, idx)
	}

	// TODO(sep-raft-log): move the closedts checks from replicaAppBatch here as
	// well. This just needs a bit more untangling as they reference *Replica, but
	// for no super-convincing reason.

	leaseIndex, rej, forcedErr := kvserverbase.CheckForcedErr(ctx, cmd.ID, &cmd.Cmd, isLocal, &b.state)
	return leaseIndex, rej, forcedErr, nil
}

// These dummy types (softly help) enforce that the two implementations
// of (apply.Batch).Stage don't diverge.
// TODO polish, and discuss the alternatives (interface and generics and
// why we didn't choose either)

type ApplyStepChecked struct{}
type ApplyStepPreRepred struct{}
type ApplyStepRepred struct{}
type ApplyStepPostRepred struct{}

func (b *standaloneAppBatch) toCheckedCmd(
	ctx context.Context,
	cmd *raftlog.ReplicatedCmd,
	leaseIndex uint64,
	rej kvserverbase.ProposalRejectionType,
	forcedErr *roachpb.Error,
) *ApplyStepChecked {
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
	// NB: this is the only place that instantiates this type.
	// TODO: make sure this doesn't allocate every time, can pull it
	// out into a var otherwise.
	return &ApplyStepChecked{}
}

func (b *standaloneAppBatch) preStageTriggers(
	// TODO move to correct file
	_ ApplyStepChecked,
	cmd *raftlog.ReplicatedCmd,
) (*ApplyStepPreRepred, error) {
	// If the command was using the deprecated version of the MVCCStats proto,
	// migrate it to the new version and clear out the field.
	res := cmd.ReplicatedResult()
	if deprecatedDelta := res.DeprecatedDelta; deprecatedDelta != nil {
		if res.Delta != (enginepb.MVCCStatsDelta{}) {
			return nil, errors.AssertionFailedf("stats delta not empty but deprecated delta provided: %+v", cmd)
		}
		res.Delta = deprecatedDelta.ToStatsDelta()
		res.DeprecatedDelta = nil
	}

	// Apply the trivial portions of the command's ReplicatedEvalResult to the
	// batch's ReplicaState.  Any non-trivial commands will be in their own batch,
	// so delaying their non-trivial ReplicatedState updates until later (without
	// ever staging them in the batch) is sufficient. This function modifies the
	// receiver's ReplicaState but does not modify ReplicatedEvalResult in order
	// to give the TestingPostApplyFilter testing knob an opportunity to Stage the
	// command's trivial ReplicatedState updates in the batch.

	b.state.RaftAppliedIndex = cmd.Index()
	b.state.RaftAppliedIndexTerm = cmd.Term

	if leaseAppliedIndex := cmd.LeaseIndex; leaseAppliedIndex != 0 {
		b.state.LeaseAppliedIndex = leaseAppliedIndex
	}
	if cts := cmd.Cmd.ClosedTimestamp; cts != nil && !cts.IsEmpty() {
		b.state.RaftClosedTimestamp = *cts
	}

	// Special-cased MVCC stats handling to exploit commutativity of stats delta
	// upgrades. Thanks to commutativity, the spanlatch manager does not have to
	// serialize on the stats key.
	deltaStats := res.Delta.ToStats()
	b.state.Stats.Add(deltaStats)

	return &ApplyStepPreRepred{}, nil
}

// applyBatchRepr applies the command's write batch to the application batch's
// RocksDB batch. This batch is committed to Pebble in replicaAppBatch.commit.
func (b *standaloneAppBatch) applyBatchRepr(
	ctx context.Context, tok ApplyStepPreRepred, cmd *raftlog.ReplicatedCmd,
) (mutations int, _ *ApplyStepRepred, _ error) {
	wb := cmd.Cmd.WriteBatch
	if wb == nil {
		return 0, &ApplyStepRepred{}, nil
	}
	mutations, err := storage.PebbleBatchCount(wb.Data)
	if err != nil {
		log.Errorf(ctx, "unable to read header of committed WriteBatch: %+v", err)
	}
	if err := b.batch.ApplyBatchRepr(wb.Data, false); err != nil {
		return 0, nil, errors.Wrapf(err, "unable to apply WriteBatch")
	}
	return mutations, &ApplyStepRepred{}, nil
}

func (b *standaloneAppBatch) postStageTriggers(
	ctx context.Context, _ ApplyStepRepred, cmd *raftlog.ReplicatedCmd,
) (*ApplyStepPostRepred, error) {
	// TODO(sep-raft-log): none so far but this is not correct - doesn't handle
	// splits, etc.
	return &ApplyStepPostRepred{}, nil
}

// Stage implements apply.Batch. This closely mirrors (*replicaAppBatch).Stage,
// which is essentially a version of this code interspersed with additional actions
// that don't apply to standalone log application.
func (a *standaloneAppBatch) Stage(
	ctx context.Context, cmdI apply.Command,
) (apply.CheckedCommand, error) {
	cmd := cmdI.(*raftlog.ReplicatedCmd)
	sb := (*standaloneAppBatch)(a)
	const isLocal = false
	leaseIndex, rej, forcedErr, err := sb.assertAndCheckCommand(ctx, cmd, isLocal)
	if err != nil {
		return nil, err
	}
	tok1 := sb.toCheckedCmd(ctx, cmd, leaseIndex, rej, forcedErr)

	tok2, err := sb.preStageTriggers(*tok1, cmd)
	if err != nil {
		return nil, err
	}

	_, tok3, err := sb.applyBatchRepr(ctx, *tok2, cmdI.(*raftlog.ReplicatedCmd))
	if err != nil {
		return nil, err
	}

	tok4, err := sb.postStageTriggers(ctx, *tok3, cmd)
	if err != nil {
		return nil, err
	}

	// There's no last step for standalone application, we're done.
	_ = *tok4

	// We now have a CheckedCommand.
	return cmd, nil
}

// Close implements apply.Batch. It releases the underlying pebble batch.
func (b *standaloneAppBatch) Close() {
	b.batch.Close()
}
