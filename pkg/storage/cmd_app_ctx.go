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
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// cmdAppCtx stores the state required to apply a single raft
// entry to a replica. The state is accumulated in stages which occur in
// Replica.handleCommittedEntriesRaftMuLocked. From a high level, a command is
// decoded into an entryApplicationBatch, then if it was proposed locally the
// proposal is populated from the replica's proposals map, then the command
// is staged into the batch by writing its update to the batch's engine.Batch
// and applying its "trivial" side-effects to the batch's view of ReplicaState.
// Then the batch is committed, the side-effects are applied and the local
// result is processed.
type cmdAppCtx struct {
	// e is the Entry being applied.
	e                *raftpb.Entry
	decodedRaftEntry // decoded from e.

	// proposal is populated on the proposing Replica only and comes from the
	// Replica's proposal map.
	proposal *ProposalData
	// ctx will be the proposal's context if proposed locally, otherwise it will
	// be populated with the handleCommittedEntries ctx.
	ctx context.Context

	// The below fields are set during stageRaftCommand when we validate that
	// a command applies given the current lease in checkForcedErr.
	leaseIndex    uint64
	forcedErr     *roachpb.Error
	proposalRetry proposalReevaluationReason
	mutationCount int // number of mutations in the WriteBatch, for writeStats
	// splitMergeUnlock is acquired for splits and merges.
	splitMergeUnlock func()

	// The below fields are set after the data has been written to the storage
	// engine in prepareLocalResult.
	localResult *result.LocalResult
	response    proposalResult
}

func (cmd *cmdAppCtx) proposedLocally() bool {
	return cmd.proposal != nil
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

func (d *decodedRaftEntry) replicatedResult() *storagepb.ReplicatedEvalResult {
	return &d.raftCmd.ReplicatedEvalResult
}

// decode decodes the entry e into the decodedRaftEntry.
func (d *decodedRaftEntry) decode(
	ctx context.Context, e *raftpb.Entry,
) (errExpl string, err error) {
	*d = decodedRaftEntry{}
	// etcd raft sometimes inserts nil commands, ours are never nil.
	// This case is handled upstream of this call.
	if len(e.Data) == 0 {
		return "", nil
	}
	switch e.Type {
	case raftpb.EntryNormal:
		return d.decodeNormalEntry(e)
	case raftpb.EntryConfChange:
		return d.decodeConfChangeEntry(e)
	default:
		log.Fatalf(ctx, "unexpected Raft entry: %v", e)
		return "", nil // unreachable
	}
}

func (d *decodedRaftEntry) decodeNormalEntry(e *raftpb.Entry) (errExpl string, err error) {
	var encodedCommand []byte
	d.idKey, encodedCommand = DecodeRaftCommand(e.Data)
	// An empty command is used to unquiesce a range and wake the
	// leader. Clear commandID so it's ignored for processing.
	if len(encodedCommand) == 0 {
		d.idKey = ""
	} else if err := protoutil.Unmarshal(encodedCommand, &d.raftCmd); err != nil {
		const errExpl = "while unmarshalling entry"
		return errExpl, errors.Wrap(err, errExpl)
	}
	return "", nil
}

func (d *decodedRaftEntry) decodeConfChangeEntry(e *raftpb.Entry) (errExpl string, err error) {
	d.decodedConfChange = &decodedConfChange{}
	if err := protoutil.Unmarshal(e.Data, &d.cc); err != nil {
		const errExpl = "while unmarshaling ConfChange"
		return errExpl, errors.Wrap(err, errExpl)
	}
	if err := protoutil.Unmarshal(d.cc.Context, &d.ccCtx); err != nil {
		const errExpl = "while unmarshaling ConfChangeContext"
		return errExpl, errors.Wrap(err, errExpl)
	}
	if err := protoutil.Unmarshal(d.ccCtx.Payload, &d.raftCmd); err != nil {
		const errExpl = "while unmarshaling RaftCommand"
		return errExpl, errors.Wrap(err, errExpl)
	}
	d.idKey = storagebase.CmdIDKey(d.ccCtx.CommandID)
	return "", nil
}
