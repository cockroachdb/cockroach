// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	// numWriteAndIngestedBytes is the sum of number of bytes written to the replica and size
	// of the sstable to ingest.
	numWriteAndIngestedBytes int64

	// NB: update `merge` when adding a new field.
}

func (s *appBatchStats) merge(ss appBatchStats) {
	s.numMutations += ss.numMutations
	s.numEntriesProcessed += ss.numEntriesProcessed
	s.numEntriesProcessedBytes += ss.numEntriesProcessedBytes
	s.numEmptyEntries += ss.numEmptyEntries
	s.numWriteAndIngestedBytes += ss.numWriteAndIngestedBytes
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
	// state is the batch's view of the replica's state. It is copied from
	// under Replica.mu when the batch is initialized and is updated in
	// stageTrivialReplicatedEvalResult.
	//
	// This is a shallow copy so any mutations inside of pointer fields need
	// to copy-on-write. The exception to this is `state.Stats`, for which
	// backing memory has already been provided and which may thus be
	// modified directly.
	state kvserverpb.ReplicaState
	// batch accumulates writes implied by the raft entries in this batch, across
	// both the state and raft engines. It is engine separation aware.
	batch kvstorage.Batch[storage.Batch]
	// sl is a cached StateLoader for the range, initialized from
	// raftMu.stateLoader. This avoids re-constructing key prefixes on every
	// addAppliedStateToBatch call. Safe because the entire appBatch lifecycle
	// runs under raftMu.
	sl kvstorage.StateLoader
	// initialForceFlushIndex records the ForceFlushIndex at the start of the
	// batch, used by addAppliedStateToBatch to detect whether it changed.
	initialForceFlushIndex roachpb.ForceFlushIndex
	// asAlloc is reused by addAppliedStateToBatch to avoid heap allocations.
	asAlloc kvserverpb.RangeAppliedState
}

func (b *appBatch) assertAndCheckCommand(
	ctx context.Context, cmd *raftlog.ReplicatedCmd, isLocal bool,
) (kvserverbase.ForcedErrResult, error) {
	if log.V(4) {
		log.KvExec.Infof(ctx, "processing command %x: raftIndex=%d maxLeaseIndex=%d closedts=%s",
			cmd.ID, cmd.Index(), cmd.Cmd.MaxLeaseIndex, cmd.Cmd.ClosedTimestamp)
	}

	if cmd.Index() == 0 {
		return kvserverbase.ForcedErrResult{}, errors.AssertionFailedf("processRaftCommand requires a non-zero index")
	}
	if idx, applied := cmd.Index(), b.state.RaftAppliedIndex; idx != applied+1 {
		// If we have an out-of-order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return.
		return kvserverbase.ForcedErrResult{}, errors.AssertionFailedf("applied index jumped from %d to %d", applied, idx)
	}

	// TODO(sep-raft-log): move the closedts checks from replicaAppBatch here as
	// well. This just needs a bit more untangling as they reference *Replica, but
	// for no super-convincing reason.

	return kvserverbase.CheckForcedErr(ctx, cmd.ID, &cmd.Cmd, isLocal, &b.state), nil
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
func (b *appBatch) addWriteBatch(ctx context.Context, cmd *replicatedCmd) error {
	wb := cmd.Cmd.WriteBatch
	if wb == nil {
		return nil
	}
	if mutations, err := storage.BatchCount(wb.Data); err != nil {
		log.KvExec.Errorf(ctx, "unable to read header of committed WriteBatch: %+v", err)
	} else {
		b.numMutations += mutations
	}
	if err := b.batch.State().ApplyBatchRepr(wb.Data, false); err != nil {
		return errors.Wrapf(err, "unable to apply WriteBatch")
	}
	return nil
}

type postAddEnv struct {
	st          *cluster.Settings
	eng         storage.Engine // StateEngine
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

	if res.Excise != nil {
		if err := env.eng.Excise(ctx, res.Excise.Span); err != nil {
			return errors.Wrapf(err, "error while excising span: %v", res.Excise.Span)
		}
		if err := env.eng.Excise(ctx, res.Excise.LockTableSpan); err != nil {
			return errors.Wrapf(err, "error while excising span: %v", res.Excise.LockTableSpan)
		}
	}

	return nil
}

// applyEntry validates and applies a single decoded raft entry to the state
// machine using the standalone application pipeline. The online path
// (replicaAppBatch.Stage) calls the same underlying methods with replica-only
// steps interleaved.
//
// Commands that fail CheckForcedErr are applied as empty no-ops: their
// WriteBatch and ReplicatedEvalResult are zeroed out, but the applied index
// still advances. This matches the online path's behavior.
//
// TODO(mira): Apply non-trivial side effects such as AddSSTable ingestions
// (runPostAddTriggers). Currently only the WriteBatch is applied.
//
// The caller is responsible for calling addAppliedStateToBatch and committing
// the batch after applying one or more entries.
func (b *appBatch) applyEntry(ctx context.Context, cmd *replicatedCmd) error {
	fr, err := b.assertAndCheckCommand(ctx, &cmd.ReplicatedCmd, false /* isLocal */)
	if err != nil {
		return err
	}
	b.toCheckedCmd(ctx, &cmd.ReplicatedCmd, fr)
	if err := b.addWriteBatch(ctx, cmd); err != nil {
		return err
	}
	b.stageTrivialResult(&cmd.ReplicatedCmd, fr)
	return nil
}

// stageTrivialResult updates the applied state in b.state to reflect a
// successfully checked command. This covers the "trivial" fields that are
// updated for every command: applied index/term, lease applied index, closed
// timestamp, and MVCC stats.
func (b *appBatch) stageTrivialResult(cmd *raftlog.ReplicatedCmd, fr kvserverbase.ForcedErrResult) {
	b.state.RaftAppliedIndex = cmd.Index()
	b.state.RaftAppliedIndexTerm = kvpb.RaftTerm(cmd.Term)
	// NB: since the command is "trivial" we know the LeaseIndex field is set to
	// something meaningful if it's nonzero (e.g. cmd is not a lease request).
	// For a rejected command, LeaseIndex was zeroed out earlier.
	if leaseAppliedIndex := fr.LeaseIndex; leaseAppliedIndex != 0 {
		b.state.LeaseAppliedIndex = leaseAppliedIndex
	}
	if cts := cmd.Cmd.ClosedTimestamp; cts != nil && !cts.IsEmpty() {
		b.state.RaftClosedTimestamp = *cts
	}
	// Special-cased MVCC stats handling to exploit commutativity of stats
	// delta upgrades. Thanks to commutativity, the spanlatch manager does
	// not have to serialize on the stats key.
	res := cmd.ReplicatedResult()
	b.state.Stats.Add(res.Delta.ToStats())
	if res.DoTimelyApplicationToAllReplicas {
		// Update the pending ForceFlushIndex of this batch. Writing is deferred
		// to addAppliedStateToBatch, following the same pattern as AppliedState
		// fields (RaftAppliedIndex, LeaseAppliedIndex). This allows multiple
		// commands in the same batch to update ForceFlushIndex, with only the
		// final value being written.
		b.state.ForceFlushIndex = roachpb.ForceFlushIndex{Index: cmd.Entry.Index}
	}
}

// addAppliedStateToBatch writes the applied state to the batch. This records
// the highest raft and lease index that have been applied, along with the MVCC
// stats. If the ForceFlushIndex changed during this batch, it is written to
// its own key first.
func (b *appBatch) addAppliedStateToBatch(ctx context.Context) error {
	// Write the ForceFlushIndex if it was staged during this batch. This index
	// is stored in a separate RangeForceFlushKey but follows the same
	// deferred-write pattern as RangeAppliedState.
	if b.state.ForceFlushIndex != b.initialForceFlushIndex {
		// NB: this branch goes first, so that MVCC stats are accurate below.
		if err := b.sl.SetForceFlushIndex(
			ctx, b.batch.State(), b.state.Stats, &b.state.ForceFlushIndex,
		); err != nil {
			return err
		}
	}
	// Set the range applied state, which includes the last applied raft and
	// lease index along with the MVCC stats, all in one key.
	b.asAlloc = b.state.ToRangeAppliedState()
	return b.sl.SetRangeAppliedState(ctx, b.batch.State(), &b.asAlloc)
}

// replayBatch implements kvstorage.ReplayBatch using appBatch.
type replayBatch struct {
	ab appBatch
	// cmd is reused across ApplyEntry calls. Decode resets all fields before
	// populating, so no state leaks between entries.
	cmd replicatedCmd
}

// AppliedIndex implements kvstorage.ReplayBatch.
func (rb *replayBatch) AppliedIndex() kvpb.RaftIndex {
	return rb.ab.state.RaftAppliedIndex
}

// ApplyEntry implements kvstorage.ReplayBatch.
func (rb *replayBatch) ApplyEntry(ctx context.Context, ent raftpb.Entry) (bool, error) {
	if err := rb.cmd.Decode(&ent); err != nil {
		return false, err
	}
	if err := rb.ab.applyEntry(ctx, &rb.cmd); err != nil {
		return false, err
	}
	return rb.cmd.IsTrivial(), nil
}

// Commit implements kvstorage.ReplayBatch.
func (rb *replayBatch) Commit(ctx context.Context) error {
	if err := rb.ab.addAppliedStateToBatch(ctx); err != nil {
		return err
	}
	return rb.ab.batch.Commit(false /* sync */)
}

// Close implements kvstorage.ReplayBatch.
func (rb *replayBatch) Close() {
	rb.ab.batch.Close()
}

// MakeNewReplayBatchFn returns a NewReplayBatchFn that creates ReplayBatch
// instances backed by appBatch. Each call loads the range's descriptor via a
// point read using the provided start key, then loads the full ReplicaState.
//
// The state engine (not a snapshot) must be passed so that reads see writes
// committed by previous replay batches.
func MakeNewReplayBatchFn(
	stateEng storage.Engine, bf *kvstorage.BatchFactory,
) kvstorage.NewReplayBatchFn {
	return func(
		ctx context.Context, rangeID roachpb.RangeID, startKey roachpb.RKey,
	) (kvstorage.ReplayBatch, error) {
		desc, err := loadRangeDescriptor(ctx, stateEng, rangeID, startKey)
		if err != nil {
			return nil, err
		}
		// TODO(mira): Consider caching the StateLoader across calls for the
		// same rangeID to avoid re-constructing key prefixes.
		sl := kvstorage.MakeStateLoader(rangeID)
		state, err := sl.Load(ctx, stateEng, &desc)
		if err != nil {
			return nil, err
		}
		return &replayBatch{
			ab: appBatch{
				state:                  state,
				batch:                  bf.NewBatch(),
				sl:                     sl,
				initialForceFlushIndex: state.ForceFlushIndex,
			},
		}, nil
	}
}

// loadRangeDescriptor loads the descriptor for the given range using a point
// read at the provided start key. We read at MaxTimestamp because descriptors
// are versioned MVCC keys. Inconsistent mode reads the latest committed value,
// skipping any intents. This is safe because descriptor intents from
// split/merge transactions are always resolved synchronously with the
// transaction commit, so there is no window in which a committed intent
// could be missed.
func loadRangeDescriptor(
	ctx context.Context, reader kvstorage.StateRO, rangeID roachpb.RangeID, startKey roachpb.RKey,
) (roachpb.RangeDescriptor, error) {
	var desc roachpb.RangeDescriptor
	descKey := keys.RangeDescriptorKey(startKey)
	ok, err := storage.MVCCGetProto(ctx, reader, descKey,
		hlc.MaxTimestamp, &desc, storage.MVCCGetOptions{
			Inconsistent: true, ReadCategory: fs.UnknownReadCategory})
	if err != nil {
		return roachpb.RangeDescriptor{}, errors.Wrapf(err, "reading descriptor for r%d", rangeID)
	}
	if !ok {
		return roachpb.RangeDescriptor{}, errors.Errorf(
			"descriptor not found for r%d at key %s", rangeID, descKey)
	}
	if desc.RangeID != rangeID {
		return roachpb.RangeDescriptor{}, errors.AssertionFailedf(
			"descriptor at key %s has r%d, expected r%d", descKey, desc.RangeID, rangeID)
	}
	return desc, nil
}
