// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
)

type appBatchStats struct {
	// TODO(sep-raft-log):
	// numEntries
	// numEntriesBytes
	// numEntriesEmpty

	// numMutations is the number of keys mutated, both via
	// WriteBatch and AddSST.
	numMutations               int
	numEntriesProcessed        int
	numEntriesProcessedBytes   int64
	numEmptyEntries            int
	numAddSST, numAddSSTCopies int

	// NB: update `merge` when adding a new field.
}

func (s *appBatchStats) merge(ss appBatchStats) {
	s.numMutations += ss.numMutations
	s.numEntriesProcessed += ss.numEntriesProcessed
	s.numEntriesProcessedBytes += ss.numEntriesProcessedBytes
	ss.numEmptyEntries += ss.numEmptyEntries
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
	if mutations, err := storage.BatchCount(wb.Data); err != nil {
		log.Errorf(ctx, "unable to read header of committed WriteBatch: %+v", err)
	} else {
		b.numMutations += mutations
	}
	if err := batch.ApplyBatchRepr(wb.Data, false); err != nil {
		return errors.Wrapf(err, "unable to apply WriteBatch")
	}
	return nil
}

type postAddEnv struct {
	st          *cluster.Settings
	eng         storage.Engine
	sideloaded  logstore.SideloadStorage
	bulkLimiter *rate.Limiter
}

func (b *appBatch) runPostAddTriggers(
	ctx context.Context, cmd *raftlog.ReplicatedCmd, env postAddEnv,
) error {
	// TODO(sep-raft-log): currently they are commingled in runPostAddTriggersReplicaOnly,
	// extract them from that method.

	res := cmd.ReplicatedResult()

	// AddSSTable ingestions run before the actual batch gets written to the
	// storage engine. This makes sure that when the Raft command is applied,
	// the ingestion has definitely succeeded. Note that we have taken
	// precautions during command evaluation to avoid having mutations in the
	// WriteBatch that affect the SSTable. Not doing so could result in order
	// reversal (and missing values) here.
	//
	// NB: any command which has an AddSSTable is non-trivial and will be
	// applied in its own batch so it's not possible that any other commands
	// which precede this command can shadow writes from this SSTable.
	if res.AddSSTable != nil {
		copied := addSSTablePreApply(
			ctx,
			env,
			kvpb.RaftTerm(cmd.Term),
			cmd.Index(),
			*res.AddSSTable,
		)
		b.numAddSST++
		if copied {
			b.numAddSSTCopies++
		}
		if added := res.Delta.KeyCount; added > 0 {
			// So far numMutations only tracks the number of keys in
			// WriteBatches but here we have a trivial WriteBatch.
			// Also account for keys added via AddSST. We do this
			// indirectly by relying on the stats, since there isn't
			// a cheap way to get the number of keys in the SST.
			b.numMutations += int(added)
		}
	}
	if res.LinkExternalSSTable != nil {
		linkExternalSStablePreApply(
			ctx,
			env,
			kvpb.RaftTerm(cmd.Term),
			cmd.Index(),
			*res.LinkExternalSSTable)
	}
	return nil
}
