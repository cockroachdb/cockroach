// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// replica_application_*.go files provide concrete implementations of
// the interfaces defined in the storage/apply package:
//
// replica_application_state_machine.go  ->  apply.StateMachine
// replica_application_decoder.go        ->  apply.Decoder
// replica_application_cmd.go            ->  apply.Command         (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandIterator (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandList     (and variants)
//
// These allow Replica to interface with the storage/apply package.

// replicatedCmd stores the state required to apply a single raft entry to a
// replica. The state is accumulated in stages which occur in apply.Task. From
// a high level, the command is decoded from a committed raft entry, then if it
// was proposed locally the proposal is populated from the replica's proposals
// map, then the command is staged into a replicaAppBatch by writing its update
// to the batch's engine.Batch and applying its "trivial" side-effects to the
// batch's view of ReplicaState. Then the batch is committed, the side-effects
// are applied and the local result is processed.
type replicatedCmd struct {
	decodedRaftEntry

	// proposal is populated on the proposing Replica only and comes from the
	// Replica's proposal map.
	proposal *ProposalData

	// ctx is a context that follows from the proposal's context if it was
	// proposed locally. Otherwise, it will follow from the context passed to
	// ApplyCommittedEntries.
	ctx context.Context
	// sp is the tracing span corresponding to ctx. It is closed in
	// finishTracingSpan. This span "follows from" the proposer's span (even
	// when the proposer is remote; we marshall tracing info through the
	// proposal).
	sp *tracing.Span

	// The following fields are set in shouldApplyCommand when we validate that
	// a command applies given the current lease and GC threshold. The process
	// of setting these fields is what transforms an apply.Command into an
	// apply.CheckedCommand.
	leaseIndex    uint64
	forcedErr     *roachpb.Error
	proposalRetry proposalReevaluationReason
	// splitMergeUnlock is acquired for splits and merges when they are staged
	// in the application batch and called after the command's side effects
	// are applied.
	splitMergeUnlock func()

	// The following fields are set after the data has been written to the
	// storage engine in prepareLocalResult. The process of setting these fields
	// is what transforms an apply.CheckedCommand into an apply.AppliedCommand.
	localResult *result.LocalResult
	response    proposalResult
}

// decodedRaftEntry represents the deserialized content of a raftpb.Entry.
type decodedRaftEntry struct {
	*raftlog.Entry
}

// decode the entry e into the replicatedCmd.
func (c *replicatedCmd) decode(ctx context.Context, e *raftpb.Entry) error {
	return c.decodedRaftEntry.decode(ctx, e)
}

// Index implements the apply.Command interface.
func (c *replicatedCmd) Index() uint64 {
	return c.decodedRaftEntry.Index
}

// IsTrivial implements the apply.Command interface.
func (c *replicatedCmd) IsTrivial() bool {
	return isTrivial(c.replicatedResult())
}

// IsLocal implements the apply.Command interface.
func (c *replicatedCmd) IsLocal() bool {
	return c.proposal != nil
}

// Ctx implements the apply.Command interface.
func (c *replicatedCmd) Ctx() context.Context {
	return c.ctx
}

// AckErrAndFinish implements the apply.Command interface.
func (c *replicatedCmd) AckErrAndFinish(ctx context.Context, err error) error {
	if c.IsLocal() {
		c.response.Err = roachpb.NewError(
			roachpb.NewAmbiguousResultError(
				err.Error()))
	}
	return c.AckOutcomeAndFinish(ctx)
}

// Rejected implements the apply.CheckedCommand interface.
func (c *replicatedCmd) Rejected() bool {
	return c.forcedErr != nil
}

// CanAckBeforeApplication implements the apply.CheckedCommand interface.
func (c *replicatedCmd) CanAckBeforeApplication() bool {
	// CanAckBeforeApplication determines whether the request type is compatible
	// with acknowledgement of success before it has been applied. For now, this
	// determination is based on whether the request is performing transactional
	// writes. This could be exposed as an option on the batch header instead.
	//
	// We don't try to ack async consensus writes before application because we
	// know that there isn't a client waiting for the result.
	req := c.proposal.Request
	return req.IsIntentWrite() && !req.AsyncConsensus
}

// AckSuccess implements the apply.CheckedCommand interface.
func (c *replicatedCmd) AckSuccess(ctx context.Context) error {
	if !c.IsLocal() {
		return nil
	}

	// Signal the proposal's response channel with the result.
	// Make a copy of the response to avoid data races between client mutations
	// of the response and use of the response in endCmds.done when the command
	// is finished.
	var resp proposalResult
	reply := *c.proposal.Local.Reply
	reply.Responses = append([]roachpb.ResponseUnion(nil), reply.Responses...)
	resp.Reply = &reply
	resp.EncounteredIntents = c.proposal.Local.DetachEncounteredIntents()
	resp.EndTxns = c.proposal.Local.DetachEndTxns(false /* alwaysOnly */)
	log.Event(ctx, "ack-ing replication success to the client; application will continue async w.r.t. the client")
	c.proposal.signalProposalResult(resp)
	return nil
}

// AckOutcomeAndFinish implements the apply.AppliedCommand interface.
func (c *replicatedCmd) AckOutcomeAndFinish(ctx context.Context) error {
	if c.IsLocal() {
		c.proposal.finishApplication(ctx, c.response)
	}
	c.finishTracingSpan()
	return nil
}

// FinishNonLocal is like AckOutcomeAndFinish, but instead of acknowledging the
// command's proposal if it is local, it asserts that the proposal is not local.
func (c *replicatedCmd) FinishNonLocal(ctx context.Context) {
	if c.IsLocal() {
		log.Fatalf(ctx, "proposal unexpectedly local: %v", c.replicatedResult())
	}
	c.finishTracingSpan()
}

func (c *replicatedCmd) finishTracingSpan() {
	c.sp.Finish()
	c.ctx, c.sp = nil, nil
}

// decode decodes the entry e into the decodedRaftEntry.
func (d *decodedRaftEntry) decode(ctx context.Context, e *raftpb.Entry) error {
	if d.Entry != nil {
		d.Entry.Release()
		*d = decodedRaftEntry{}
	}

	var err error
	d.Entry, err = raftlog.NewEntry(*e)
	if err != nil {
		return wrapWithNonDeterministicFailure(err, "while decoding raft entry")
	}

	// etcd raft sometimes inserts nil commands. Additionally, we sometimes
	// propose empty commands (where e.Data is just the CmdIDKey); both are
	// handled by the caller. In the latter case, we clear the CmdIDKey to
	// make it look like the empty raft command, leaving only one path to
	// handle upstream.
	if d.Cmd.ReplicatedEvalResult.IsZero() {
		d.ID = ""
	}
	return nil
}

func (d *decodedRaftEntry) replicatedResult() *kvserverpb.ReplicatedEvalResult {
	return &d.Cmd.ReplicatedEvalResult
}
