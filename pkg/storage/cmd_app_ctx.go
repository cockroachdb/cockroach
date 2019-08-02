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
	"go.etcd.io/etcd/raft/raftpb"
)

// cmdAppCtx stores the state required to apply a single raft entry to a
// replica. The state is accumulated in stages which occur in apply.Task. From
// a high level, the command is decoded from a committed raft entry, then if it
// was proposed locally the proposal is populated from the replica's proposals
// map, then the command is staged into a replicaAppBatch by writing its update
// to the batch's engine.Batch and applying its "trivial" side-effects to the
// batch's view of ReplicaState. Then the batch is committed, the side-effects
// are applied and the local result is processed.
type cmdAppCtx struct {
	ent              *raftpb.Entry // the raft.Entry being applied
	decodedRaftEntry               // decoded from ent

	// proposal is populated on the proposing Replica only and comes from the
	// Replica's proposal map.
	proposal *ProposalData
	// ctx will be the proposal's context if proposed locally, otherwise it will
	// be populated with the handleCommittedEntries ctx.
	ctx context.Context

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
	idKey              storagebase.CmdIDKey
	raftCmd            storagepb.RaftCommand
	*decodedConfChange // only non-nil for config changes
}

// decodedConfChange represents the fields of a config change raft command.
type decodedConfChange struct {
	cc    raftpb.ConfChange
	ccCtx ConfChangeContext
}

// decode decodes the entry e into the cmdAppCtx.
func (c *cmdAppCtx) decode(ctx context.Context, e *raftpb.Entry) error {
	c.ent = e
	return c.decodedRaftEntry.decode(ctx, e)
}

func (d *decodedRaftEntry) replicatedResult() *storagepb.ReplicatedEvalResult {
	return &d.raftCmd.ReplicatedEvalResult
}

// Index implements the apply.Command interface.
func (c *cmdAppCtx) Index() uint64 {
	return c.ent.Index
}

// IsTrivial implements the apply.Command interface.
func (c *cmdAppCtx) IsTrivial() bool {
	return isTrivial(c.replicatedResult())
}

// IsLocal implements the apply.Command interface.
func (c *cmdAppCtx) IsLocal() bool {
	return c.proposal != nil
}

// Rejected implements the apply.CheckedCommand interface.
func (c *cmdAppCtx) Rejected() bool {
	return c.forcedErr != nil
}

// FinishAndAckOutcome implements the apply.AppliedCommand interface.
func (c *cmdAppCtx) FinishAndAckOutcome() error {
	if !c.IsLocal() {
		return nil
	}
	c.proposal.finishApplication(c.response)
	return nil
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
	case raftpb.EntryConfChange:
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
	if err := protoutil.Unmarshal(e.Data, &d.cc); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChange")
	}
	if err := protoutil.Unmarshal(d.cc.Context, &d.ccCtx); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling ConfChangeContext")
	}
	if err := protoutil.Unmarshal(d.ccCtx.Payload, &d.raftCmd); err != nil {
		return wrapWithNonDeterministicFailure(err, "while unmarshaling RaftCommand")
	}
	d.idKey = storagebase.CmdIDKey(d.ccCtx.CommandID)
	return nil
}
