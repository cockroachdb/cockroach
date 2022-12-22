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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type appBatchStats struct {
	// TODO(sep-raft-log):
	// numEntries
	// numEntriesBytes
	// numEntriesEmpty
	numMutations             int
	numEntriesProcessed      int
	numEntriesProcessedBytes int64
	numEmptyEntries          int
	followerStoreWriteBytes  kvadmission.FollowerStoreWriteBytes
	// NB: update `merge` when adding a new field.
}

func (s *appBatchStats) merge(ss appBatchStats) {
	s.numMutations += ss.numMutations
	s.numEntriesProcessed += ss.numEntriesProcessed
	s.numEntriesProcessedBytes += ss.numEntriesProcessedBytes
	ss.numEmptyEntries += ss.numEmptyEntries
	s.followerStoreWriteBytes.Merge(ss.followerStoreWriteBytes)
}

// appBatch is the in-progress foundation for standalone log entry
// application[^1], i.e. the act of applying raft log entries to the state
// machine in a library-style fashion, without a running CockroachDB server.
//
// The intended usage is as follows. Starting with a ReplicatedCmd per Entry,
//
//  1. check it via assertAndCheckCommand followed by toCheckedCmd
//  2. run pre-add triggers (which may augment the WriteBatch)
//  3. stage the WriteBatch into a pebble Batch
//  4. run post-add triggers (metrics, etc)
//
// when all Entries have been added, the batch can be committed. In the course
// of time, appBatch will become an implementation of apply.Batch itself; at the
// time of writing it is only used by the replicaAppBatch implementation of
// apply.Batch, which goes through the above steps while interspersing:
//
//	1a. testing interceptors between assertAndCheckCommand and toCheckedCmd
//	2b. pre-add triggers specific to online command application (e.g. acquiring locks
//	    during replica-spanning operations), and
//	4b. post-add triggers specific to online command application (e.g. updates to
//		  Replica in-mem state)
//
// [^1]: https://github.com/cockroachdb/cockroach/issues/75729
type appBatch struct {
	appBatchStats
	// TODO(tbg): this will absorb the following fields from replicaAppBatch:
	//
	// - batch
	// - state
	// - changeRemovesReplica
}

func (b *appBatch) assertAndCheckCommand(
	ctx context.Context, cmd *raftlog.ReplicatedCmd, state *kvserverpb.ReplicaState, isLocal bool,
) (kvserverbase.ForcedErrResult, error) {
	if log.V(4) {
		log.Infof(ctx, "processing command %x: raftIndex=%d maxLeaseIndex=%d closedts=%s",
			cmd.ID, cmd.Index(), cmd.Cmd.MaxLeaseIndex, cmd.Cmd.ClosedTimestamp)
	}

	if cmd.Index() == 0 {
		return kvserverbase.ForcedErrResult{}, errors.AssertionFailedf("processRaftCommand requires a non-zero index")
	}
	if idx, applied := cmd.Index(), state.RaftAppliedIndex; idx != applied+1 {
		// If we have an out-of-order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return.
		return kvserverbase.ForcedErrResult{}, errors.AssertionFailedf("applied index jumped from %d to %d", applied, idx)
	}

	// TODO(sep-raft-log): move the closedts checks from replicaAppBatch here as
	// well. This just needs a bit more untangling as they reference *Replica, but
	// for no super-convincing reason.

	return kvserverbase.CheckForcedErr(ctx, cmd.ID, &cmd.Cmd, isLocal, state), nil
}

func (b *appBatch) toCheckedCmd(
	ctx context.Context, cmd *raftlog.ReplicatedCmd, fr kvserverbase.ForcedErrResult,
) {
	cmd.ForcedErrResult = fr
	if cmd.Rejected() {
		log.VEventf(ctx, 1, "applying command with forced error: %s", cmd.ForcedError)

		// Apply an empty command.
		cmd.Cmd.ReplicatedEvalResult = kvserverpb.ReplicatedEvalResult{}
		cmd.Cmd.WriteBatch = nil
		cmd.Cmd.LogicalOpLog = nil
		cmd.Cmd.ClosedTimestamp = nil
	} else {
		// If the command was using the deprecated version of the MVCCStats proto,
		// migrate it to the new version and clear out the field.
		res := cmd.ReplicatedResult()
		if deprecatedDelta := res.DeprecatedDelta; deprecatedDelta != nil {
			if res.Delta != (enginepb.MVCCStatsDelta{}) {
				log.Fatalf(ctx, "stats delta not empty but deprecated delta provided: %+v", cmd)
			}
			res.Delta = deprecatedDelta.ToStatsDelta()
			res.DeprecatedDelta = nil
		}
		log.Event(ctx, "applying command")
	}
}

// runPreAddTriggers runs any triggers that must fire before the command is
// added to the appBatch's pebble batch. That is, the pebble batch at this point
// will have materialized the raft log up to but excluding the current command.
func (b *appBatch) runPreAddTriggers(ctx context.Context, cmd *raftlog.ReplicatedCmd) error {
	// None currently.
	return nil
}

// addWriteBatch adds the command's writes to the batch.
func (b *appBatch) addWriteBatch(
	ctx context.Context, batch storage.Batch, cmd *replicatedCmd,
) error {
	wb := cmd.Cmd.WriteBatch
	if wb == nil {
		return nil
	}
	if mutations, err := storage.PebbleBatchCount(wb.Data); err != nil {
		log.Errorf(ctx, "unable to read header of committed WriteBatch: %+v", err)
	} else {
		b.numMutations += mutations
	}
	if err := batch.ApplyBatchRepr(wb.Data, false); err != nil {
		return errors.Wrapf(err, "unable to apply WriteBatch")
	}
	return nil
}

func (b *appBatch) runPostAddTriggers(ctx context.Context, cmd *raftlog.ReplicatedCmd) error {
	// TODO(sep-raft-log): currently they are commingled in runPostAddTriggersReplicaOnly,
	// extract them from that method.
	return nil
}
