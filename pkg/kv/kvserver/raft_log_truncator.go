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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// pendingLogTruncations tracks proposed truncations for a replica that have
// not yet been enacted due to the corresponding RaftAppliedIndex not yet
// being durable. It is a field in the Replica struct
// (Replica.pendingLogTruncations), but it is declared in this file since
// it is really part of the raftLogTruncator state that is per-replica.
//
// Note that we should not hold pendingLogTruncations.mu for long since it
// could block the raftLogQueue which needs to (only) read the pending
// truncations. This becomes tricky for the raftLogTruncator, which needs to
// do substantial work while reading these pending truncations, and then
// upgrade to mutating these truncations. It cannot allow a mutator to be
// running concurrently. We could add a second mutex to pendingLogTruncations
// to achieve this mutation mutual exclusion. However, we instead rely on the
// fact that all these mutation cases already hold Replica.raftMu (to prevent
// the Replica from being destroyed while the truncation work is happening).
//
// The summary is that we require Replica.raftMu to be additionally held while
// modifying the pending truncations. Hence, either one of those mutexes is
// sufficient for reading. This behavior is abstracted by the definition of
// replicaForTruncator below.
type pendingLogTruncations struct {
	mu struct {
		// From a lock ordering perspective, this mutex is the lowest, i.e., it
		// should not be held when trying to acquire any other mutex.
		syncutil.Mutex
		// We track up to two truncations: the oldest pending truncation, and a
		// merge of all the subsequent pending truncations. We cannot track only
		// one merged truncation since its index may always be ahead of the
		// durable RaftAppliedIndex, and so we may never be able to truncate. We
		// assume liveness of durability advancement, which means that if no new
		// pending truncations are added, the latest one will eventually be
		// enacted.
		//
		// Note that this liveness assumption is not completely true -- if there are
		// no writes happening to the store, the durability (due to memtable
		// flushes) may not advance. We deem this (a) an uninteresting case, since
		// if there are no writes we possibly don't care about aggressively
		// truncating the log, (b) fixing the liveness assumption is not within
		// scope of the truncator (it has to work with what it is given).
		//
		// Invariants:
		// - Queue slot i is empty iff truncs[i] == pendingTruncation{}
		// - Slot 0 represents the first position in the queue. Therefore, it is
		//   not possible for slot 0 to be empty and slot 1 to be non-empty.
		//   An implication is that the queue is empty iff slot 0 is empty.
		// - If slot 0 and 1 are both non-empty, truncs[0].Index < truncs[1].Index
		truncs [2]pendingTruncation
	}
}

// computePostTruncLogSize computes the size of the raft log under the
// pretense that the pending truncations have been enacted.
func (p *pendingLogTruncations) computePostTruncLogSize(raftLogSize int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.iterateLocked(func(_ int, trunc pendingTruncation) {
		raftLogSize += trunc.logDeltaBytes
	})
	if raftLogSize < 0 {
		raftLogSize = 0
	}
	return raftLogSize
}

// computePostTruncFirstIndex computes the first log index that is not
// truncated, under the pretense that the pending truncations have been
// enacted.
func (p *pendingLogTruncations) computePostTruncFirstIndex(firstIndex uint64) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.iterateLocked(func(_ int, trunc pendingTruncation) {
		firstIndexAfterTrunc := trunc.firstIndexAfterTrunc()
		if firstIndex < firstIndexAfterTrunc {
			firstIndex = firstIndexAfterTrunc
		}
	})
	return firstIndex
}

func (p *pendingLogTruncations) isEmptyLocked() bool {
	return p.mu.truncs[0] == (pendingTruncation{})
}

// Returns the front (i.e. the least aggressive truncation) of the pending
// truncations queue, without removing the element.
// REQUIRES: !isEmptyLocked()
func (p *pendingLogTruncations) frontLocked() pendingTruncation {
	return p.mu.truncs[0]
}

// Pops the front (i.e. the least aggressive truncation) of the pending
// truncations queues.
// REQUIRES: !isEmptyLocked()
func (p *pendingLogTruncations) popLocked() {
	p.mu.truncs[0] = p.mu.truncs[1]
	p.mu.truncs[1] = pendingTruncation{}
}

// Iterates over the queued truncations in the queue order, i.e., the oldest
// first.
func (p *pendingLogTruncations) iterateLocked(f func(index int, trunc pendingTruncation)) {
	for i, trunc := range p.mu.truncs {
		if !(trunc == (pendingTruncation{})) {
			f(i, trunc)
		}
	}
}

// Empties the queue of pending truncations.
func (p *pendingLogTruncations) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.isEmptyLocked() {
		p.popLocked()
	}
}

func (p *pendingLogTruncations) capacity() int {
	// Reminder: truncs is a fixed size array.
	return len(p.mu.truncs)
}

type pendingTruncation struct {
	// The pending truncation will truncate entries up to
	// RaftTruncatedState.Index, inclusive.
	roachpb.RaftTruncatedState

	// The logDeltaBytes are computed under the assumption that the
	// truncation is deleting [expectedFirstIndex,RaftTruncatedState.Index]. It
	// originates in ReplicatedEvalResult, where it is accurate.
	// There are two reasons isDeltaTrusted could be considered false here:
	// - The original "accurate" delta does not account for sideloaded files. It
	//   is adjusted on this replica using
	//   SideloadStorage.BytesIfTruncatedFromTo, but it is possible that the
	//   truncated state of this replica is already > expectedFirstIndex. We
	//   don't actually set isDeltaTrusted=false for this case since we will
	//   change Replica.raftLogSizeTrusted to false after enacting this
	//   truncation.
	// - We merge pendingTruncation entries in the pendingTruncations struct. We
	//   are making an effort to have consecutive TruncateLogRequests provide us
	//   stats for index intervals that are adjacent and non-overlapping, but
	//   that behavior is best-effort.
	expectedFirstIndex uint64
	// logDeltaBytes includes the bytes from sideloaded files. Like
	// ReplicatedEvalResult.RaftLogDelta, this is <= 0.
	logDeltaBytes  int64
	isDeltaTrusted bool
}

func (pt *pendingTruncation) firstIndexAfterTrunc() uint64 {
	// Reminder: RaftTruncatedState.Index is inclusive.
	return pt.Index + 1
}

// raftLogTruncator is responsible for actually enacting truncations.
//
// Mutex ordering: The Replica mutexes can be held when acquiring
// raftLogTruncator.mu, but the reverse is not permitted.

type raftLogTruncator struct {
	ambientCtx context.Context
	store      storeForTruncator
	stopper    *stop.Stopper
	mu         struct {
		syncutil.Mutex
		// Ranges are queued into addRanges and batch dequeued by swapping with
		// drainRanges. This avoids holding mu for any work proportional to the
		// number of queued ranges.
		addRanges, drainRanges map[roachpb.RangeID]struct{}
		// State for scheduling the goroutine for background enacting of
		// truncations.
		runningTruncation  bool
		queuedDurabilityCB bool
	}
}

func makeRaftLogTruncator(
	ambientCtx log.AmbientContext, store storeForTruncator, stopper *stop.Stopper,
) *raftLogTruncator {
	t := &raftLogTruncator{
		ambientCtx: ambientCtx.AnnotateCtx(context.Background()),
		store:      store,
		stopper:    stopper,
	}
	t.mu.addRanges = make(map[roachpb.RangeID]struct{})
	t.mu.drainRanges = make(map[roachpb.RangeID]struct{})
	return t
}

// storeForTruncator abstracts the interface of Store needed by the truncator.
type storeForTruncator interface {
	// acquireReplicaForTruncator ensures that the returned replicaForTruncator
	// is not already destroyed. It may return nil. Any mutex protecting
	// raft-state (e.g. Replica.raftMu) is acquired before returning. This
	// method also ensures that the returned replica will not be destroyed until
	// after releaseReplicaForTruncator is called.
	acquireReplicaForTruncator(rangeID roachpb.RangeID) replicaForTruncator
	// releaseReplicaForTruncator releases the replica.
	releaseReplicaForTruncator(r replicaForTruncator)
	// Engine accessor.
	getEngine() storage.Engine
}

// replicaForTruncator abstracts the interface of Replica needed by the
// truncator.
//
// A replica has raft state, including the queue of pending truncations, that
// the truncator is modifying. There is a "raft-state" mutex to mutually
// exclude other actions that are concurrently mutating this state. We assume
// that this "raft-state" mutex is held for the lifetime of
// replicaForTruncator. Hence there are no additional concurrency control
// requirements on the methods that read or write raft-state (this includes
// allowing pendingLogTruncations to be read without holding
// pendingLogTruncations.mu).
//
// We acknowledge that this interface may seem peculiar -- this is due to the
// constraint that it is abstracting Replica.
type replicaForTruncator interface {
	// Returns the Range ID.
	getRangeID() roachpb.RangeID
	// Returns the current truncated state.
	getTruncatedState() roachpb.RaftTruncatedState
	// Updates the replica state after the truncation is enacted.
	setTruncatedStateAndSideEffects(
		_ context.Context, _ *roachpb.RaftTruncatedState, expectedFirstIndexPreTruncation uint64,
	) (expectedFirstIndexWasAccurate bool)
	// Updates the stats related to the raft log size after the truncation is
	// enacted.
	setTruncationDeltaAndTrusted(deltaBytes int64, isDeltaTrusted bool)
	// Returns the pending truncations queue. The caller is allowed to mutate
	// the return value by additionally acquiring pendingLogTruncations.mu.
	getPendingTruncs() *pendingLogTruncations
	// Returns the sideloaded bytes that would be freed if we were to truncate
	// [from, to).
	sideloadedBytesIfTruncatedFromTo(
		_ context.Context, from, to uint64) (freed int64, _ error)
	getStateLoader() stateloader.StateLoader
	// NB: Setting the persistent raft state is via the Engine exposed by
	// storeForTruncator.
}

// raftExpectedFirstIndex and raftLogDelta have the same meaning as in
// ReplicatedEvalResult. Never called before cluster is at
// LooselyCoupledRaftLogTruncation.
func (t *raftLogTruncator) addPendingTruncation(
	ctx context.Context,
	r replicaForTruncator,
	trunc roachpb.RaftTruncatedState,
	raftExpectedFirstIndex uint64,
	raftLogDelta int64,
) {
	pendingTrunc := pendingTruncation{
		RaftTruncatedState: trunc,
		expectedFirstIndex: raftExpectedFirstIndex,
		logDeltaBytes:      raftLogDelta,
		isDeltaTrusted:     true,
	}
	pendingTruncs := r.getPendingTruncs()
	// Need to figure out whether to add this new pendingTrunc to the
	// truncations that are already queued, and if yes, where to add.
	// i is the index of the last already queued truncation.
	i := -1
	// alreadyTruncIndex represents what has been already truncated.
	alreadyTruncIndex := r.getTruncatedState().Index
	// No need to acquire pendingTruncs.mu for read in this case (see
	// replicaForTruncator comment).
	pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
		i = index
		if trunc.Index > alreadyTruncIndex {
			alreadyTruncIndex = trunc.Index
		}
	})
	if alreadyTruncIndex >= pendingTrunc.Index {
		// Noop.
		return
	}
	// This new pending truncation will advance what is truncated.
	// pos is where we will add the new pending truncation.
	pos := i + 1
	mergeWithPending := false
	if pos == pendingTruncs.capacity() {
		// We need to merge with an existing pending truncation.
		pos--
		mergeWithPending = true
	}
	// It is possible that alreadyTruncIndex+1 != raftExpectedFirstIndex. When
	// we merge or enact we will see this problem and set the trusted bit to
	// false. But we can at least correctly count sideloaded entries, which can
	// be large, since we do the computation for the sideloaded entries size
	// here. When alreadyTruncIndex+1 > raftExpectedFirstIndex, this will avoid
	// double counting sideloaded entries that will be freed, and when
	// alreadyTruncIndex+1 < raftExpectedFirstIndex, this will ensure that we
	// don't miss sideloaded entries that will be freed.
	//
	// In the common case of alreadyTruncIndex+1 == raftExpectedFirstIndex, the
	// computation returns the same result regardless of which is plugged in as
	// the lower bound.
	sideloadedFreed, err := r.sideloadedBytesIfTruncatedFromTo(
		ctx, alreadyTruncIndex+1, pendingTrunc.firstIndexAfterTrunc())
	if err != nil {
		// Log a loud error since we need to continue enqueuing the truncation.
		log.Errorf(ctx, "while computing size of sideloaded files to truncate: %+v", err)
		pendingTrunc.isDeltaTrusted = false
	}
	pendingTrunc.logDeltaBytes -= sideloadedFreed
	if mergeWithPending {
		// Merge the existing entry into the new one.
		// No need to acquire pendingTruncs.mu for read in this case.
		pendingTrunc.isDeltaTrusted = pendingTrunc.isDeltaTrusted &&
			pendingTruncs.mu.truncs[pos].isDeltaTrusted
		if pendingTruncs.mu.truncs[pos].firstIndexAfterTrunc() != pendingTrunc.expectedFirstIndex {
			pendingTrunc.isDeltaTrusted = false
		}
		pendingTrunc.logDeltaBytes += pendingTruncs.mu.truncs[pos].logDeltaBytes
		pendingTrunc.expectedFirstIndex = pendingTruncs.mu.truncs[pos].expectedFirstIndex
	}
	pendingTruncs.mu.Lock()
	// Install the new pending truncation.
	pendingTruncs.mu.truncs[pos] = pendingTrunc
	pendingTruncs.mu.Unlock()

	if pos == 0 {
		if mergeWithPending {
			panic("should never be merging pending truncations at pos 0")
		}
		// First entry in queue of pending truncations for this replica, so add
		// the RangeID to the map.
		t.enqueueRange(r.getRangeID())
	}
}

type rangesByRangeID []roachpb.RangeID

func (r rangesByRangeID) Len() int {
	return len(r)
}
func (r rangesByRangeID) Less(i, j int) bool {
	return r[i] < r[j]
}
func (r rangesByRangeID) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// Invoked whenever the durability of the store advances. We assume that this
// is coarse in that the advancement of durability will apply to all ranges in
// this store, and most of the preceding pending truncations have their goal
// truncated index become durable in RangeAppliedState.RaftAppliedIndex. This
// coarseness assumption is important for not wasting much work being done in
// this method.
//
// This method schedules the actual work for asynchronous execution as we need
// to return quickly, and not call into the engine since that could risk
// deadlock (see storage.Engine.RegisterFlushCompletedCallback).
func (t *raftLogTruncator) durabilityAdvancedCallback() {
	runTruncation := false
	t.mu.Lock()
	if !t.mu.runningTruncation && len(t.mu.addRanges) > 0 {
		runTruncation = true
		t.mu.runningTruncation = true
	}
	if !runTruncation && len(t.mu.addRanges) > 0 {
		t.mu.queuedDurabilityCB = true
	}
	t.mu.Unlock()
	if !runTruncation {
		return
	}
	if err := t.stopper.RunAsyncTask(t.ambientCtx, "raft-log-truncation",
		func(ctx context.Context) {
			for {
				t.durabilityAdvanced(ctx)
				shouldReturn := false
				t.mu.Lock()
				queued := t.mu.queuedDurabilityCB
				t.mu.queuedDurabilityCB = false
				if !queued {
					t.mu.runningTruncation = false
					shouldReturn = true
				}
				t.mu.Unlock()
				if shouldReturn {
					return
				}
			}
		}); err != nil {
		// Task did not run because stopper is stopped.
		func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			if !t.mu.runningTruncation {
				panic("expected runningTruncation")
			}
			t.mu.runningTruncation = false
		}()
	}
}

// Synchronously does the work to truncate the queued replicas.
func (t *raftLogTruncator) durabilityAdvanced(ctx context.Context) {
	t.mu.Lock()
	t.mu.addRanges, t.mu.drainRanges = t.mu.drainRanges, t.mu.addRanges
	// If another pendingTruncation is added to this Replica, it will not be
	// added to the addRanges map since the Replica already has pending
	// truncations. That is ok: we will try to enact all pending truncations for
	// that Replica below, since there typically will only be one pending, and
	// if there are any remaining we will add it back to the addRanges map.
	//
	// We can modify drainRanges after releasing t.mu since we are guaranteed
	// that there is at most one durabilityAdvanced running at a time.
	drainRanges := t.mu.drainRanges
	t.mu.Unlock()
	if len(drainRanges) == 0 {
		return
	}
	ranges := make([]roachpb.RangeID, 0, len(drainRanges))
	for k := range drainRanges {
		ranges = append(ranges, k)
		delete(drainRanges, k)
	}
	// Sort it for deterministic testing output.
	sort.Sort(rangesByRangeID(ranges))
	// Create an engine Reader to provide a safe lower bound on what is durable.
	reader := t.store.getEngine().NewReadOnly(storage.GuaranteedDurability)
	defer reader.Close()
	shouldQuiesce := t.stopper.ShouldQuiesce()
	quiesced := false
	for _, rangeID := range ranges {
		t.tryEnactTruncations(ctx, rangeID, reader)
		// Check if the stopper is quiescing. This isn't strictly necessary, but
		// if there are a huge number of ranges that need to be truncated, this
		// will cause us to stop faster.
		select {
		case <-shouldQuiesce:
			quiesced = true
		default:
		}
		if quiesced {
			break
		}
	}
}

// TODO(tbg): Instead of directly calling tryEnactTruncations from the
// raftLogTruncator, we would like to use the Store.processReady path to
// centralize error handling and timing of all raft related processing. We
// will continue to enact truncations across all replicas using a single
// goroutine per store, and not use the raftScheduler workers -- this is
// because a durabilityAdvanced callback triggers truncations for all queued
// replicas and we don't want to use up all the workers for truncation
// activity at the same time and starve foreground activity. We considered
// localizing all changes in handleRaftReadyRaftMuLocked (not touching the
// plumbing in processReady etc. that leads up to it), by marking the Replica
// that it should try doing truncation, and calling processReady from the
// truncator's goroutine. However, we are concerned that by marking the
// Replica we allow for a race between the the truncator's goroutine and the
// raftScheduler worker that can cause the latter to pick up the truncation
// work. This race is not a correctness problem, but can cause needless
// surprise. The current plan is to some refactoring of processReady so that
// we can have a second entry point (processTruncation) that also goes through
// most of the code that lives in processReady today (and passes a truncation
// through to handleRaftReadyRaftMuLocked). The code in
// handleRaftReadyRaftMyLocked would call something akin to
// tryEnactTruncations. Note that tryEnactTruncations needs a storage.Reader
// for reading only durable state -- currently we share it across replicas
// since it is easy to do so. But in the future code we can construct such a
// Reader in tryEnactTruncations.

func (t *raftLogTruncator) tryEnactTruncations(
	ctx context.Context, rangeID roachpb.RangeID, reader storage.Reader,
) {
	r := t.store.acquireReplicaForTruncator(rangeID)
	if r == nil {
		// Not found.
		return
	}
	defer t.store.releaseReplicaForTruncator(r)
	truncState := r.getTruncatedState()
	pendingTruncs := r.getPendingTruncs()
	// Remove the noop pending truncations.
	pendingTruncs.mu.Lock()
	for !pendingTruncs.isEmptyLocked() {
		pendingTrunc := pendingTruncs.frontLocked()
		if pendingTrunc.Index <= truncState.Index {
			// The pending truncation is a noop. Even though we avoid queueing
			// noop truncations, this is possible because a snapshot could have
			// been applied to the replica after enqueueing the truncations.
			pendingTruncs.popLocked()
		} else {
			break
		}
	}
	// NB: Unlocking but can keep reading pendingTruncs due to
	// replicaForTruncator contract.
	pendingTruncs.mu.Unlock()
	if pendingTruncs.isEmptyLocked() {
		// Nothing to do for this replica.
		return
	}
	// Have some useful pending truncations.

	// Use the reader to decide what is durable.
	stateLoader := r.getStateLoader()
	as, err := stateLoader.LoadRangeAppliedState(ctx, reader)
	if err != nil {
		log.Errorf(ctx, "error loading RangeAppliedState, dropping all pending log truncations: %s",
			err)
		pendingTruncs.reset()
		return
	}
	// enactIndex represents the index of the latest queued truncation that
	// can be enacted. We start with -1 since it is possible that nothing can
	// be enacted.
	enactIndex := -1
	pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
		if trunc.Index > as.RaftAppliedIndex {
			return
		}
		enactIndex = index
	})
	if enactIndex < 0 {
		// Enqueue the rangeID for the future.
		t.enqueueRange(rangeID)
		return
	}
	// Do the truncation of persistent raft entries, specified by enactIndex
	// (this subsumes all the preceding queued truncations).
	batch := t.store.getEngine().NewUnindexedBatch(false /* writeOnly */)
	defer batch.Close()
	apply, err := handleTruncatedStateBelowRaftPreApply(ctx, &truncState,
		&pendingTruncs.mu.truncs[enactIndex].RaftTruncatedState, stateLoader, batch)
	if err != nil || !apply {
		if err != nil {
			log.Errorf(ctx, "while attempting to truncate raft log: %+v", err)
		} else {
			err := errors.AssertionFailedf(
				"unexpected !apply from handleTruncatedStateBelowRaftPreApply")
			if buildutil.CrdbTestBuild || util.RaceEnabled {
				log.Fatalf(ctx, "%s", err)
			} else {
				log.Errorf(ctx, "%s", err)
			}
		}
		pendingTruncs.reset()
		return
	}
	// sync=false since we don't need a guarantee that the truncation is
	// durable. Loss of a truncation means we have more of the suffix of the
	// raft log, which does not affect correctness.
	if err := batch.Commit(false /* sync */); err != nil {
		log.Errorf(ctx, "while committing batch to truncate raft log: %+v", err)
		pendingTruncs.reset()
		return
	}
	// Truncation done. Need to update the Replica state. This requires iterating
	// over all the enacted entries.
	pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
		if index > enactIndex {
			return
		}
		isDeltaTrusted := true
		expectedFirstIndexWasAccurate := r.setTruncatedStateAndSideEffects(
			ctx, &trunc.RaftTruncatedState, trunc.expectedFirstIndex)
		if !expectedFirstIndexWasAccurate || !trunc.isDeltaTrusted {
			isDeltaTrusted = false
		}
		r.setTruncationDeltaAndTrusted(trunc.logDeltaBytes, isDeltaTrusted)
	})
	// Now remove the enacted truncations. It is the same iteration as the
	// previous one, but we do it while holding pendingTruncs.mu. Note that
	// since we have updated the raft log size but not yet removed the pending
	// truncations, a concurrent thread could race and compute a lower post
	// truncation size. We ignore this race since it seems harmless, and closing
	// it requires a more complicated replicaForTruncator interface.
	pendingTruncs.mu.Lock()
	for i := 0; i <= enactIndex; i++ {
		pendingTruncs.popLocked()
	}
	pendingTruncs.mu.Unlock()
	if !pendingTruncs.isEmptyLocked() {
		t.enqueueRange(rangeID)
	}
}

func (t *raftLogTruncator) enqueueRange(rangeID roachpb.RangeID) {
	t.mu.Lock()
	t.mu.addRanges[rangeID] = struct{}{}
	t.mu.Unlock()
}
