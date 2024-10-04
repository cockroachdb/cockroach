// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	raftlog.ReplicatedCmd

	// proposal is populated on the proposing Replica only and comes from the
	// Replica's proposal map.
	proposal *ProposalData

	// ctx is a non-cancelable context used to apply the command.
	ctx context.Context
	// sp is the tracing span corresponding to ctx. It is closed in
	// finishTracingSpan. This span "follows from" the proposer's span (even
	// when the proposer is remote; we marshall tracing info through the
	// proposal).
	//
	// For local commands, we also have a `ProposalData` which may have a span
	// as well, and if it does, this span will follow from it.
	sp *tracing.Span

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

// IsLocal implements apply.Command.
func (c *replicatedCmd) IsLocal() bool {
	return c.proposal != nil
}

// ApplyAdmissionControl indicates whether the command should be
// subject to replication admission control.
func (c *replicatedCmd) ApplyAdmissionControl() bool {
	return c.Entry.ApplyAdmissionControl
}

// Ctx implements apply.Command.
func (c *replicatedCmd) Ctx() context.Context {
	return c.ctx
}

// AckErrAndFinish implements apply.Command.
func (c *replicatedCmd) AckErrAndFinish(ctx context.Context, err error) error {
	if c.IsLocal() {
		c.response.Err = kvpb.NewError(kvpb.NewAmbiguousResultError(err))
	}
	return c.AckOutcomeAndFinish(ctx)
}

// getStoreWriteByteSizes returns the size of the writes to the store:
// writeBytes is the size of the WriteBatch if any, and ingestedBytes is the
// size of the sstable to ingest, if any.
func (c *replicatedCmd) getStoreWriteByteSizes() (writeBytes int64, ingestedBytes int64) {
	if c.Cmd.WriteBatch != nil {
		writeBytes = int64(len(c.Cmd.WriteBatch.Data))
	}
	if c.Cmd.ReplicatedEvalResult.AddSSTable != nil {
		ingestedBytes = int64(len(c.Cmd.ReplicatedEvalResult.AddSSTable.Data))
	}
	return writeBytes, ingestedBytes
}

// CanAckBeforeApplication implements apply.CheckedCommand.
func (c *replicatedCmd) CanAckBeforeApplication() bool {
	// CanAckBeforeApplication determines whether the request type is compatible
	// with acknowledgement of success before it has been applied. For now, this
	// determination is based on whether the request is performing transactional
	// writes. This could be exposed as an option on the batch header instead.
	//
	// We don't try to ack async consensus writes before application because we
	// know that there isn't a client waiting for the result.
	req := c.proposal.Request
	if !req.IsIntentWrite() || req.AsyncConsensus {
		return false
	}
	if et, ok := req.GetArg(kvpb.EndTxn); ok && et.(*kvpb.EndTxnRequest).InternalCommitTrigger != nil {
		// Don't early-ack for commit triggers, just to keep things simple - the
		// caller is reasonably expecting to be able to run another replication
		// change right away, and some code paths pull the descriptor out of memory
		// where it may not reflect the previous operation yet.
		return false
	}
	return true
}

// AckSuccess implements apply.CheckedCommand.
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
	reply.Responses = append([]kvpb.ResponseUnion(nil), reply.Responses...)
	resp.Reply = &reply
	resp.EncounteredIntents = c.proposal.Local.DetachEncounteredIntents()
	resp.EndTxns = c.proposal.Local.DetachEndTxns(false /* alwaysOnly */)
	log.Event(ctx, "ack-ing replication success to the client; application will continue async w.r.t. the client")
	c.proposal.signalProposalResult(resp)
	return nil
}

// AckOutcomeAndFinish implements the apply.AppliedCommand.
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
		log.Fatalf(ctx, "proposal unexpectedly local: %v", c.ReplicatedResult())
	}
	c.finishTracingSpan()
}

func (c *replicatedCmd) finishTracingSpan() {
	c.sp.Finish()
	c.ctx, c.sp = nil, nil
}
