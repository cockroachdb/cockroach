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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"go.etcd.io/etcd/raft/raftpb"
)

// cmdAppBatch accumulates state due to the application of raft
// commands. Committed raft commands are applied to the batch in a multi-stage
// process whereby individual commands are decoded, prepared for application
// relative to the current view of replicaState, committed to the storage
// engine, applied to the Replica's in-memory state and then finished by
// releasing their latches and notifying clients.
type cmdAppBatch struct {
	// decodeBuf is used to decode an entry before adding it to the batch.
	// See decode().
	decodeBuf decodedRaftEntry

	// batch accumulates writes implied by the raft entries in this batch.
	batch engine.Batch

	// replicaState is this batch's view of the replica's state.
	// It is copied from under the Replica.mu when the batch is initialized and
	// is updated in stageTrivialReplicatedEvalResult.
	replicaState storagepb.ReplicaState

	// stats is stored on the application batch to avoid an allocation in tracking
	// the batch's view of replicaState. All pointer fields in replicaState other
	// than Stats are overwritten completely rather than updated in-place.
	stats enginepb.MVCCStats

	// updatedTruncatedState tracks whether any command in the batch updated the
	// replica's truncated state. Truncated state updates are considered trivial
	// but represent something of a special case but given their relative
	// frequency and the fact that multiple updates can be trivially coalesced, we
	// treat updates to truncated state as trivial. If the batch updated the
	// truncated state then after it has been committed, then the side-loaded data
	// and raftentry.Cache should be truncated to the index indicated.
	// TODO(ajwerner): consider whether this logic should imply that commands
	// which update truncated state are non-trivial.
	updatedTruncatedState bool

	cmdBuf cmdAppCtxBuf
}

// cmdAppBatch structs are needed to apply raft commands, which is to
// say, frequently, so best to pool them rather than allocated under the raftMu.
var cmdAppBatchSyncPool = sync.Pool{
	New: func() interface{} {
		return new(cmdAppBatch)
	},
}

func getCmdAppBatch() *cmdAppBatch {
	return cmdAppBatchSyncPool.Get().(*cmdAppBatch)
}

func releaseCmdAppBatch(b *cmdAppBatch) {
	b.cmdBuf.clear()
	*b = cmdAppBatch{}
	cmdAppBatchSyncPool.Put(b)
}

// add adds adds the entry and its decoded state to the end of the batch.
func (b *cmdAppBatch) add(e *raftpb.Entry, d decodedRaftEntry) {
	s := b.cmdBuf.allocate()
	s.decodedRaftEntry = d
	s.e = e
}

// decode decodes commands from toProcess until either all of the commands have
// been added to the batch or a non-trivial command is decoded. Non-trivial
// commands must always be in their own batch. If a non-trivial command is
// encountered the batch is returned immediately without adding the newly
// decoded command to the batch or removing it from remaining.
// It is the client's responsibility to deal with non-trivial commands.
//
// numEmptyEntries indicates the number of entries in the consumed portion of
// toProcess contained a zero-byte payload.
func (b *cmdAppBatch) decode(
	ctx context.Context, toProcess []raftpb.Entry, decodeBuf *decodedRaftEntry,
) (
	foundNonTrivialEntry bool,
	numEmptyEntries int,
	remaining []raftpb.Entry,
	errExpl string,
	err error,
) {
	for len(toProcess) > 0 {
		e := &toProcess[0]
		if len(e.Data) == 0 {
			numEmptyEntries++
		}
		if errExpl, err = decodeBuf.decode(ctx, e); err != nil {
			return false, numEmptyEntries, nil, errExpl, err
		}
		// This is a non-trivial entry which needs to be processed alone.
		foundNonTrivialEntry = !isTrivial(decodeBuf.replicatedResult(),
			b.replicaState.UsingAppliedStateKey)
		if foundNonTrivialEntry {
			break
		}
		// We're going to process this entry in this batch so pop it from toProcess
		// and add to appStates.
		toProcess = toProcess[1:]
		b.add(e, *decodeBuf)
	}
	return foundNonTrivialEntry, numEmptyEntries, toProcess, "", nil
}

func (b *cmdAppBatch) reset() {
	b.cmdBuf.clear()
	*b = cmdAppBatch{
		decodeBuf: b.decodeBuf, // preserve the previously decoded entry
	}
}
