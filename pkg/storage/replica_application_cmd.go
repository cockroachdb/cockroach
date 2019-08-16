// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
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
	// ApplyCommittedEntries. sp is the corresponding tracing span, which is
	// closed in FinishAndAckOutcome.
	ctx context.Context
	sp  opentracing.Span

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
	idKey   storagebase.CmdIDKey
	raftCmd storagepb.RaftCommand
	// TODO(tbg): unembed this, it's a nil pointer trap.
	*decodedConfChange // only non-nil for config changes
}

// decodedConfChange represents the fields of a config change raft command.
type decodedConfChange struct {
	cc    raftpb.ConfChangeI // backed by either cc1 or cc2
	cc1   raftpb.ConfChange
	cc2   raftpb.ConfChangeV2
	ccCtx ConfChangeContext
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
	return req.IsTransactionWrite() && !req.AsyncConsensus
}

// AckSuccess implements the apply.CheckedCommand interface.
func (c *replicatedCmd) AckSuccess() error {
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
	resp.Intents = c.proposal.Local.DetachIntents()
	resp.EndTxns = c.proposal.Local.DetachEndTxns(false /* alwaysOnly */)
	c.proposal.signalProposalResult(resp)
	return nil
}

// FinishAndAckOutcome implements the apply.AppliedCommand interface.
func (c *replicatedCmd) FinishAndAckOutcome() error {
	tracing.FinishSpan(c.sp)
	if c.IsLocal() {
		c.proposal.finishApplication(c.response)
	}
	return nil
}

// decode decodes the entry e into the decodedRaftEntry.
func (d *decodedRaftEntry) decode(ctx context.Context, e *raftpb.Entry) error {
	*d = decodedRaftEntry{}
	switch e.Type {
	case raftpb.EntryNormal:
		// etcd raft sometimes inserts nil commands, ours are never nil.
		// This case is handled upstream of this call.
		//
		// NB: e.Data can be nil for EntryConfChangeV2 (the next case).
		if len(e.Data) == 0 {
			return nil
		}
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
	d.decodedConfChange = &decodedConfChange{}
	var err error
	switch e.Type {
	case raftpb.EntryConfChange:
		if err := protoutil.Unmarshal(e.Data, &d.cc1); err != nil {
			return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChange")
		}
		d.cc = d.cc1
	case raftpb.EntryConfChangeV2:
		if err := protoutil.Unmarshal(e.Data, &d.cc2); err != nil {
			return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChangeV2")
		}
		d.cc = d.cc2
	default:
		err = errors.Errorf("unknown conf change entry: %v", e)
	}
	if err != nil {
		return err
	}

	if err := protoutil.Unmarshal(d.cc.AsV2().Context, &d.ccCtx); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChangeContext")
	}
	if err := protoutil.Unmarshal(d.ccCtx.Payload, &d.raftCmd); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling RaftCommand")
	}
	d.idKey = storagebase.CmdIDKey(d.ccCtx.CommandID)
	return nil
}

func (d *decodedRaftEntry) replicatedResult() *storagepb.ReplicatedEvalResult {
	return &d.raftCmd.ReplicatedEvalResult
}
