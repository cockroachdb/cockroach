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

// entryGen is a generator of raft entries. cmdAppBatch uses the type when
// iterating over committed entries to decode and apply.
//
// The entry and next methods should only be called if valid returns true.
type entryGen []raftpb.Entry

func (g *entryGen) valid() bool          { return len(*g) != 0 }
func (g *entryGen) entry() *raftpb.Entry { return &(*g)[0] }
func (g *entryGen) next()                { *g = (*g)[1:] }

// cmdAppBatch accumulates state due to the application of raft commands.
// Committed raft commands are applied to the batch in a multi-stage process
// whereby individual commands are decoded, prepared for application relative to
// the current view of replicaState, committed to the storage engine, applied to
// the Replica's in-memory state and then finished by releasing their latches
// and notifying clients.
type cmdAppBatch struct {
	// decodeBuf is used to decode an entry before adding it to the batch.
	// See decode().
	decodeBuf     decodedRaftEntry
	decodeBufFull bool

	// cmdBuf is a buffer containing decoded raft entries that are ready to be
	// applied in the same batch.
	cmdBuf cmdAppCtxBuf

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
}

// cmdAppBatch structs are needed to apply raft commands, which is to say,
// frequently, so best to pool them rather than allocated under the raftMu.
var cmdAppBatchSyncPool = sync.Pool{
	New: func() interface{} {
		return new(cmdAppBatch)
	},
}

func getCmdAppBatch() *cmdAppBatch {
	return cmdAppBatchSyncPool.Get().(*cmdAppBatch)
}

func (b *cmdAppBatch) release() {
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

// decode decodes commands from gen until either all of the commands have
// been added to the batch or a non-trivial command is decoded. Non-trivial
// commands must always be in their own batch. If a non-trivial command is
// encountered the batch is returned immediately without adding the newly
// decoded command to the batch or removing it from remaining. It is the
// client's responsibility to deal with non-trivial commands.
func (b *cmdAppBatch) decode(ctx context.Context, gen *entryGen) (errExpl string, err error) {
	for gen.valid() {
		e := gen.entry()
		if errExpl, err = b.decodeBuf.decode(ctx, e); err != nil {
			return errExpl, err
		}
		b.decodeBufFull = true
		// This is a non-trivial entry which needs to be processed alone.
		if !isTrivial(b.decodeBuf.replicatedResult(), b.replicaState.UsingAppliedStateKey) {
			break
		}
		// We're going to process this entry in this batch so add to the
		// cmdBuf and advance the generator.
		b.add(e, b.popDecodeBuf())
		gen.next()
	}
	return "", nil
}

func (b *cmdAppBatch) popDecodeBuf() decodedRaftEntry {
	b.decodeBufFull = false
	return b.decodeBuf
}

// resetBatch resets the accumulate batch state in the cmdAppBatch.
// However, it does not reset the receiver's decode buffer.
func (b *cmdAppBatch) resetBatch() {
	b.cmdBuf.clear()
	*b = cmdAppBatch{
		decodeBuf:     b.decodeBuf, // preserve the previously decoded entry
		decodeBufFull: b.decodeBufFull,
	}
}
