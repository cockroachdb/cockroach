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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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
	ent              *raftpb.Entry // the raft.Entry being applied
	decodedRaftEntry               // decoded from ent

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
	idKey      kvserverbase.CmdIDKey
	raftCmd    kvserverpb.RaftCommand
	confChange *decodedConfChange // only non-nil for config changes
}

// decodedConfChange represents the fields of a config change raft command.
type decodedConfChange struct {
	raftpb.ConfChangeI
	ConfChangeContext
}

// decode decodes the entry e into the replicatedCmd.
func (c *replicatedCmd) decode(ctx context.Context, e *raftpb.Entry) error {
	c.ent = e
	return c.decodedRaftEntry.decode(ctx, e)
}

// Index implements the apply.Command interface.
func (c *replicatedCmd) Index() uint64 {
	return c.ent.Index
}

// IsTrivial implements the apply.Command interface.
func (c *replicatedCmd) IsTrivial() bool {
	return isTrivial(c.replicatedResult())
}

// IsLocal implements the apply.Command interface.
func (c *replicatedCmd) IsLocal() bool {
	return c.proposal != nil
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
func (c *replicatedCmd) AckSuccess(_ context.Context) error {
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
	*d = decodedRaftEntry{}
	// etcd raft sometimes inserts nil commands, ours are never nil.
	// This case is handled upstream of this call.
	if len(e.Data) == 0 {
		return nil
	}
	switch e.Type {
	case raftpb.EntryNormal:
		return d.decodeNormalEntry(e)
	case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
		return d.decodeConfChangeEntry(e)
	default:
		log.Fatalf(ctx, "unexpected Raft entry: %v", e)
		return nil // unreachable
	}
}

func (d *decodedRaftEntry) decodeNormalEntry(e *raftpb.Entry) error {
	var encodedCommand []byte
	d.idKey, encodedCommand = DecodeRaftCommand(e.Data)
	// An empty command is used to unquiesce a range and wake the
	// leader. Clear commandID so it's ignored for processing.
	if len(encodedCommand) == 0 {
		d.idKey = ""
	} else if err := protoutil.Unmarshal(encodedCommand, &d.raftCmd); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling entry")
	}
	return nil
}

func (d *decodedRaftEntry) decodeConfChangeEntry(e *raftpb.Entry) error {
	d.confChange = &decodedConfChange{}

	switch e.Type {
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := protoutil.Unmarshal(e.Data, &cc); err != nil {
			return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChange")
		}
		d.confChange.ConfChangeI = cc
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := protoutil.Unmarshal(e.Data, &cc); err != nil {
			return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChangeV2")
		}
		d.confChange.ConfChangeI = cc
	default:
		const msg = "unknown entry type"
		err := errors.New(msg)
		return wrapWithNonDeterministicFailure(err, msg)
	}
	if err := protoutil.Unmarshal(d.confChange.AsV2().Context, &d.confChange.ConfChangeContext); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChangeContext")
	}
	if err := protoutil.Unmarshal(d.confChange.Payload, &d.raftCmd); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling RaftCommand")
	}
	d.idKey = kvserverbase.CmdIDKey(d.confChange.CommandID)
	return nil
}

func (d *decodedRaftEntry) replicatedResult() *kvserverpb.ReplicatedEvalResult {
	return &d.raftCmd.ReplicatedEvalResult
}
