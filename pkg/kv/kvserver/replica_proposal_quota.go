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
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/tracker"
)

func (r *Replica) maybeAcquireProposalQuota(
	ctx context.Context, ba *kvpb.BatchRequest, quota uint64,
) (*quotapool.IntAlloc, error) {
	// We don't want to delay lease requests or transfers, in particular
	// expiration lease extensions. These are small and latency-sensitive.
	if ba.IsSingleRequestLeaseRequest() || ba.IsSingleTransferLeaseRequest() {
		return nil, nil
	}

	r.mu.RLock()
	quotaPool := r.mu.proposalQuota
	desc := *r.mu.state.Desc
	r.mu.RUnlock()

	// Quota acquisition only takes place on the leader replica,
	// r.mu.proposalQuota is set to nil if a node is a follower (see
	// updateProposalQuotaRaftMuLocked). For the cases where the range lease
	// holder is not the same as the range leader, i.e. the lease holder is a
	// follower, r.mu.proposalQuota == nil. This means all quota acquisitions
	// go through without any throttling whatsoever but given how short lived
	// these scenarios are we don't try to remedy any further.
	//
	// NB: It is necessary to allow proposals with a nil quota pool to go
	// through, for otherwise a follower could never request the lease.

	if quotaPool == nil {
		return nil, nil
	}

	if !quotaPoolEnabledForRange(desc) {
		return nil, nil
	}

	// Trace if we're running low on available proposal quota; it might explain
	// why we're taking so long.
	if log.HasSpan(ctx) {
		if q := quotaPool.ApproximateQuota(); q < quotaPool.Capacity()/10 {
			log.Eventf(ctx, "quota running low, currently available ~%d", q)
		}
	}
	alloc, err := quotaPool.Acquire(ctx, quota)
	// Let quotapool errors due to being closed pass through.
	if errors.HasType(err, (*quotapool.ErrClosed)(nil)) {
		err = nil
	}
	return alloc, err
}

func quotaPoolEnabledForRange(desc roachpb.RangeDescriptor) bool {
	// The NodeLiveness range does not use a quota pool. We don't want to
	// throttle updates to the NodeLiveness range even if a follower is falling
	// behind because this could result in cascading failures.
	return !bytes.HasPrefix(desc.StartKey, keys.NodeLivenessPrefix)
}

var logSlowRaftProposalQuotaAcquisition = quotapool.OnSlowAcquisition(
	base.SlowRequestThreshold, quotapool.LogSlowAcquisition,
)

func (r *Replica) updateProposalQuotaRaftMuLocked(
	ctx context.Context, lastLeaderID roachpb.ReplicaID,
) {
	now := r.Clock().PhysicalTime()
	r.mu.Lock()
	defer r.mu.Unlock()

	status := r.mu.internalRaftGroup.BasicStatus()
	if r.mu.leaderID != lastLeaderID {
		if r.replicaID == r.mu.leaderID {
			// We're becoming the leader.
			// Initialize the proposalQuotaBaseIndex at the applied index.
			// After the proposal quota is enabled all entries applied by this replica
			// will be appended to the quotaReleaseQueue. The proposalQuotaBaseIndex
			// and the quotaReleaseQueue together track status.Applied exactly.
			r.mu.proposalQuotaAndDelayTracker.init(ctx, kvpb.RaftIndex(status.Applied))
			if r.mu.proposalQuota != nil {
				log.Fatal(ctx, "proposalQuota was not nil before becoming the leader")
			}

			// Raft may propose commands itself (specifically the empty
			// commands when leadership changes), and these commands don't go
			// through the code paths where we acquire quota from the pool. To
			// offset this we reset the quota pool whenever leadership changes
			// hands.
			r.mu.proposalQuota = quotapool.NewIntPool(
				"raft proposal",
				uint64(r.store.cfg.RaftProposalQuota),
				logSlowRaftProposalQuotaAcquisition,
			)
			r.mu.lastUpdateTimes = make(map[roachpb.ReplicaID]time.Time)
			r.mu.lastUpdateTimes.updateOnBecomeLeader(r.mu.state.Desc.Replicas().Descriptors(), now)
			r.mu.replicaFlowControlIntegration.onBecameLeader(ctx)
			r.mu.lastProposalAtTicks = r.mu.ticks // delay imminent quiescence
		} else if r.mu.proposalQuota != nil {
			// We're becoming a follower.
			// We unblock all ongoing and subsequent quota acquisition goroutines
			// (if any) and release the quotaReleaseQueue so its allocs are pooled.
			r.mu.proposalQuota.Close("leader change")
			r.mu.proposalQuota.Release(r.mu.proposalQuotaAndDelayTracker.drain()...)
			r.mu.proposalQuota = nil
			r.mu.lastUpdateTimes = nil
			r.mu.replicaFlowControlIntegration.onBecameFollower(ctx)
		}
		return
	} else if r.mu.proposalQuota == nil {
		if r.replicaID == r.mu.leaderID {
			log.Fatal(ctx, "leader has uninitialized proposalQuota pool")
		}
		// We're a follower.
		return
	}

	// We're still the leader.
	// Find the minimum index that active followers have acknowledged.

	// commitIndex is used to determine whether a newly added replica has fully
	// caught up.
	commitIndex := kvpb.RaftIndex(status.Commit)
	appliedIndex := kvpb.RaftIndex(status.Applied)

	r.mu.proposalQuotaAndDelayTracker.prepareForReplicaIteration(
		appliedIndex, now, r.mu.proposalQuota.Len() > 0)
	r.mu.internalRaftGroup.WithProgress(func(id uint64, _ raft.ProgressType, progress tracker.Progress) {
		rep, ok := r.mu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(id))
		if !ok {
			return
		}

		// TODO(sumeer): should we add some observability for stores that are
		// inactive, or behind the proposalQuotaBaseIndex, or paused, that are
		// resulting in a range not having quorum.

		// Only consider followers that are active. Inactive ones don't decrease
		// minIndex - i.e. they don't hold up releasing quota.
		//
		// The policy for determining who's active is stricter than the one used
		// for purposes of quiescing. Failure to consider a dead/stuck node as
		// such for the purposes of releasing quota can have bad consequences
		// (writes will stall), whereas for quiescing the downside is lower.

		if !r.mu.lastUpdateTimes.isFollowerActiveSince(rep.ReplicaID, now, r.store.cfg.RangeLeaseDuration) {
			return
		}
		// At this point, we know that either we communicated with this replica
		// recently, or we became the leader recently. The latter case is ambiguous
		// w.r.t. the actual state of that replica, but it is temporary.

		// Note that the Match field has different semantics depending on
		// the State.
		//
		// In state ProgressStateReplicate, the Match index is optimistically
		// updated whenever a message is *sent* (not received). Due to Raft
		// flow control, only a reasonably small amount of data can be en
		// route to a given follower at any point in time.
		//
		// In state ProgressStateProbe, the Match index equals Next-1, and
		// it tells us the leader's optimistic best guess for the right log
		// index (and will try once per heartbeat interval to update its
		// estimate). In the usual case, the follower responds with a hint
		// when it rejects the first probe and the leader replicates or
		// sends a snapshot. In the case in which the follower does not
		// respond, the leader reduces Match by one each heartbeat interval.
		// But if the follower does not respond, we've already filtered it
		// out above. We use the Match index as is, even though the follower
		// likely isn't there yet because that index won't go up unless the
		// follower is actually catching up, so it won't cause it to fall
		// behind arbitrarily.
		//
		// Another interesting tidbit about this state is that the Paused
		// field is usually true as it is used to limit the number of probes
		// (i.e. appends) sent to this follower to one per heartbeat
		// interval.
		//
		// In state ProgressStateSnapshot, the Match index is the last known
		// (possibly optimistic, depending on previous state) index before
		// the snapshot went out. Once the snapshot applies, the follower
		// will enter ProgressStateReplicate again. So here the Match index
		// works as advertised too.

		// Only consider followers who are in advance of the quota base
		// index. This prevents a follower from coming back online and
		// preventing throughput to the range until it has caught up.
		if kvpb.RaftIndex(progress.Match) < r.mu.proposalQuotaAndDelayTracker.getProposalQuotaBaseIndex() {
			return
		}
		if _, paused := r.mu.pausedFollowers[roachpb.ReplicaID(id)]; paused {
			// We are dropping MsgApp to this store, so we are effectively treating
			// it as non-live for the purpose of replication and are letting it fall
			// behind intentionally.
			//
			// See #79215.
			return
		}
		r.mu.proposalQuotaAndDelayTracker.updateReplicaAtStore(
			rep.StoreID, kvpb.RaftIndex(progress.Match))
		// If this is the most recently added replica, and it has caught up, clear
		// our state that was tracking it. This is unrelated to managing proposal
		// quota, but this is a convenient place to do so.
		if rep.ReplicaID == r.mu.lastReplicaAdded && kvpb.RaftIndex(progress.Match) >= commitIndex {
			r.mu.lastReplicaAdded = 0
			r.mu.lastReplicaAddedTime = time.Time{}
		}
	})

	toRelease := r.mu.proposalQuotaAndDelayTracker.finishReplicaIteration(ctx)
	if len(toRelease) > 0 {
		// NB: Release deals with cases where allocs being released do not originate
		// from this incarnation of quotaReleaseQueue, which can happen if a
		// proposal acquires quota while this replica is the raft leader in some
		// term and then commits while at a different term.
		r.mu.proposalQuota.Release(toRelease...)
	}

	// Tick the replicaFlowControlIntegration interface. This is as convenient a
	// place to do it as any other. Much like the quota pool code above, the
	// flow control integration layer considers raft progress state for
	// individual replicas, and whether they've been recently active.
	r.mu.replicaFlowControlIntegration.onRaftTicked(ctx)
}

// TODO(sumeer): this is completely untested. Add tests after initial
// review/sanity check.

// proposalQuotaAndDelayTracker tracks the (a) pending quota to be returned to
// the quota pool, and (b) various delay stats per replica, at the leader. (b)
// is done in a best-effort manner. The stats include:
//   - delay incurred per-entry in reaching quorum and the replica/store
//     responsible
//   - per-store mean delay in persisting raft log entries
//   - duration of quota pool exhaustion caused by a store
//   - quota consumed due to a store
//
// We put this functionality in one place since doing all this tracking
// requires maintaining some state per raft log entry (and GC'ing that entry
// state when the quota is released, since by then no one cares about that
// entry), and state per store.
type proposalQuotaAndDelayTracker struct {
	// The base index is the index up to (including) which quota was already
	// released. That is, the first element in quotaReleaseQueue below is
	// released as the base index moves up by one, etc.
	proposalQuotaBaseIndex kvpb.RaftIndex
	// Once the leader observes a proposal come 'out of Raft', we add the size
	// of the associated command to a queue of quotas we have yet to release
	// back to the quota pool. At that point ownership of the quota is
	// transferred from Replica.mu.proposals to this queue.
	//
	// We'll release the respective quota once all replicas have persisted the
	// corresponding entry into their logs (or once we give up waiting on some
	// replica because it looks like it's dead).
	//
	// The tail of the quotaReleaseQueue is entries for which we have not yet
	// been given ownership of the quota, since that happens for entries that
	// have been applied. The tail consists of entries that are being locally
	// appended to the raft log.
	quotaReleaseQueue []queueEntry
	// numQuotaEntries tracks the prefix of quotaReleaseQueue for which we have
	// been handed ownership of the quota.
	numQuotaEntries int
	// iterState is used for replica iteration.
	iterState struct {
		iterStartTimeNanos int64
		appliedIndex       kvpb.RaftIndex
		toReleaseScratch   []*quotapool.IntAlloc
	}
	// quotaConsumed is the sum of the bytes in quotaReleaseQueue[:numQuotaEntries].
	quotaConsumed int64
	// quotaUnavailableStart is the start time of when quota was unavailable. An
	// empty time represents quota is available.
	quotaUnavailableStart time.Time
	// storeState tracks state for each active replica. We key by StoreID since
	// there is at most one replica per StoreID.
	storeState map[roachpb.StoreID]*perStoreTrackerState
}

type queueEntry struct {
	// alloc is nil if there was no quota allocated, or we have not yet been
	// given ownership of the quota.
	alloc *quotapool.IntAlloc
	// queuedUnixNanos represents the time when the entry was added to the local
	// raft log. We count delay from this time.
	queuedUnixNanos int64
	// latestProgressMatchIndex is updated whenever etcd/raft's
	// tracker.Progress.Match for a replica advances up to the entry,
	// representing that the replica has been sent the entry.
	// tracker.Progress.Match does not imply that the replica has persisted the
	// entry, but Raft flow control limits the entries en route to a replica to
	// a small amount. We assume here that whenever a replica's
	// tracker.Progress.Match advances up to this entry, it will soon be
	// persisted (this may not be a good assumption for slowness outliers, but
	// reasonable for chronic slowness). We track only the latest replica to
	// advance up to this index since that is potentially the slow one, if
	// quorum is delayed. This value is updated only until the entry is applied
	// to the local state machine, since (a) we know that quorum has been
	// achieved at that point, (b) it is used only for computing the return
	// value in lastStoreToProgress(), which is called after local application
	// for tracing.
	//
	// TODO(sumeer): is there a signal of what has been persisted in a replica,
	// instead of using tracker.Progress.Match.
	latestProgressMatchIndex struct {
		storeID   roachpb.StoreID
		unixNanos int64
	}
}

// perStoreTrackerState is the state maintained per replica (actually per
// store, since there can't be multiple replicas at a store).
type perStoreTrackerState struct {
	// iterTimeNanos is the iteration time when this replica was last considered
	// active. It is used to GC replica entries.
	iterTimeNanos int64
	// notProgressedIndex is the latest value of etcd/raft's
	// tracker.Progress.Match observed for this replica plus one. This is
	// upper-bounded by proposalQuotaBaseIndex + len(quotaReleaseQueue).
	// Unlike quotaConsumedStartIndex, notProgressedIndex is not monotonic,
	// since it can decrease due to failures.
	notProgressedIndex kvpb.RaftIndex
	// quotaConsumedStartIndex is the first raft entry index whose quota counts
	// towards this store. This is upper-bounded by proposalQuotaBaseIndex +
	// numQuotaEntries. Monotonically non-decreasing.
	quotaConsumedStartIndex kvpb.RaftIndex
	perStoreStats
}

// perStoreStats is the stats in perStoreTrackerState.
type perStoreStats struct {
	// quotaConsumed is the quota consumed (a gauge) that can be attributed to
	// this replica. It is always <= proposalQuotaAndDelayTracker.quotaConsumed.
	// This is used for two purposes: (a) when the quotapool is full whether
	// this store is one of those responsible, (b) to aggregate the quota
	// consumed by this store across all ranges.
	quotaConsumed int64
	// quotaUnavailableDuration is the duration that quota was unavailable due
	// to this replica.
	quotaUnavailableDuration time.Duration
	// progressMatchDuration and progressMatchCount can be used to compute the
	// mean duration before tracker.Progress.Match advanced past each entry in
	// the raft log. This is not useful for outliers. Outliers are currently
	// handled via queueEntry.latestProgressMatchIndex and tracing. If that is
	// insufficient, we could change this to a histogram.
	progressMatchDuration time.Duration
	progressMatchCount    int64
}

func (ss *perStoreStats) merge(other perStoreStats) {
	ss.quotaConsumed += other.quotaConsumed
	ss.quotaUnavailableDuration += other.quotaUnavailableDuration
	ss.progressMatchDuration += other.progressMatchDuration
	ss.progressMatchCount += other.progressMatchCount
}

// init the tracker, when the local replica becomes the leader.
func (pq *proposalQuotaAndDelayTracker) init(
	ctx context.Context, proposalQuotaBaseIndex kvpb.RaftIndex,
) {
	if queueLen := len(pq.quotaReleaseQueue); queueLen != 0 || pq.numQuotaEntries != 0 {
		log.Fatalf(ctx, "len(quotaReleaseQueue) = %d, expected 0", queueLen)
	}
	*pq = proposalQuotaAndDelayTracker{}
	pq.proposalQuotaBaseIndex = proposalQuotaBaseIndex
	pq.storeState = map[roachpb.StoreID]*perStoreTrackerState{}
}

// drain the tracker, when the local replica becomes a follower.
func (pq *proposalQuotaAndDelayTracker) drain() []*quotapool.IntAlloc {
	toRelease := pq.releaseQuotaEntries(kvpb.RaftIndex(pq.numQuotaEntries))
	*pq = proposalQuotaAndDelayTracker{}
	return toRelease
}

// Internal helper.
func (pq *proposalQuotaAndDelayTracker) isActive() bool {
	return pq.storeState != nil
}

func (pq *proposalQuotaAndDelayTracker) getProposalQuotaBaseIndex() kvpb.RaftIndex {
	return pq.proposalQuotaBaseIndex
}

func (pq *proposalQuotaAndDelayTracker) getQuotaReleaseQueueLen() int {
	return pq.numQuotaEntries
}

// iterateQuotaQueue is for reading the entries that have quota.
func (pq *proposalQuotaAndDelayTracker) iterateQuotaQueue(f func(alloc *quotapool.IntAlloc)) {
	for i := 0; i < pq.numQuotaEntries; i++ {
		f(pq.quotaReleaseQueue[i].alloc)
	}
}

// raftAppend is called when [minIndex, maxIndex] are being appended to the
// local raft log. The callee will start tracking the delay from the latest
// time an index is mentioned in raftAppend -- this is because previous
// appends may be overwritten if quorum was never achieved.
func (pq *proposalQuotaAndDelayTracker) raftAppend(
	minIndex, maxIndex kvpb.RaftIndex, now time.Time,
) {
	if !pq.isActive() {
		return
	}
	desiredQueueLen := int(maxIndex - pq.proposalQuotaBaseIndex)
	queueLen := len(pq.quotaReleaseQueue)
	if queueLen > desiredQueueLen {
		truncatedLen := desiredQueueLen
		if truncatedLen < pq.numQuotaEntries {
			panic("entry already applied")
		}
		notProgressIndexUpperBound := kvpb.RaftIndex(truncatedLen) + pq.proposalQuotaBaseIndex + 1
		pq.quotaReleaseQueue = pq.quotaReleaseQueue[:truncatedLen]
		for _, ss := range pq.storeState {
			if ss.notProgressedIndex > notProgressIndexUpperBound {
				ss.notProgressedIndex = notProgressIndexUpperBound
			}
		}
	} else if cap(pq.quotaReleaseQueue) < desiredQueueLen {
		qrq := make([]queueEntry, desiredQueueLen, 2*desiredQueueLen)
		copy(qrq, pq.quotaReleaseQueue)
		pq.quotaReleaseQueue = qrq
	} else {
		pq.quotaReleaseQueue = pq.quotaReleaseQueue[:desiredQueueLen]
	}
	startIndex := int(minIndex - pq.proposalQuotaBaseIndex - 1)
	nowInNanos := now.UnixNano()
	for i := startIndex; i < desiredQueueLen; i++ {
		if pq.quotaReleaseQueue[i].alloc != nil {
			// This should not happen, since this entry has already been applied.
			// Should we ignore, given this is best-effort bookkeeping?
			panic("entry already applied")
		}
		pq.quotaReleaseQueue[i] = queueEntry{
			queuedUnixNanos: nowInNanos,
		}
	}
}

// enqueueAlloc is called to enqueue an alloc for an entry whose quota is
// being transferred to this tracker.
// REQUIRES: isActive()
func (pq *proposalQuotaAndDelayTracker) enqueueAlloc(alloc *quotapool.IntAlloc, now time.Time) {
	if len(pq.quotaReleaseQueue) > pq.numQuotaEntries {
		pq.quotaReleaseQueue[pq.numQuotaEntries].alloc = alloc
	} else {
		pq.quotaReleaseQueue = append(pq.quotaReleaseQueue, queueEntry{
			alloc:           alloc,
			queuedUnixNanos: now.UnixNano(),
		})
	}
	pq.numQuotaEntries++
	entryIndex := pq.proposalQuotaBaseIndex + kvpb.RaftIndex(pq.numQuotaEntries)
	if alloc != nil {
		acquired := int64(alloc.Acquired())
		pq.quotaConsumed += acquired
		for _, ss := range pq.storeState {
			if entryIndex < ss.quotaConsumedStartIndex {
				panic("quotaConsumedStartIndex upper bound was violated")
			}
			ss.quotaConsumed += acquired
		}
	}
}

// prepareForReplicaIteration prepares the tracker for iteration over the
// replicas. An iteration causes quota release and updating of various stats.
func (pq *proposalQuotaAndDelayTracker) prepareForReplicaIteration(
	appliedIndex kvpb.RaftIndex, now time.Time, isQueueEmpty bool,
) {
	pq.iterState.iterStartTimeNanos = now.UnixNano()
	pq.iterState.appliedIndex = appliedIndex
	if !isQueueEmpty {
		if pq.quotaUnavailableStart.IsZero() {
			pq.quotaUnavailableStart = now
		} else {
			// Quota was already unavailable, so extend the unavailable duration.

			// 0.9 is somewhat arbitrary. We want to capture replicas that are close
			// to quota exhaustion.
			fullThreshold := int64(0.9 * float64(pq.quotaConsumed))
			duration := now.Sub(pq.quotaUnavailableStart)
			pq.quotaUnavailableStart = now
			for _, ss := range pq.storeState {
				if ss.quotaConsumed > fullThreshold {
					ss.quotaUnavailableDuration += duration
				}
			}
		}
	} else if !pq.quotaUnavailableStart.IsZero() {
		pq.quotaUnavailableStart = time.Time{}
	}
}

// updateReplicaAtStore is called for each replica that is considered active,
// during iteration over the replicas.
//
// REQUIRES: progressMatchIndex >= pq.proposalQuotaBaseIndex.
func (pq *proposalQuotaAndDelayTracker) updateReplicaAtStore(
	storeID roachpb.StoreID, progressMatchIndex kvpb.RaftIndex,
) {
	notProgressIndex := progressMatchIndex + 1
	upperBoundNotProgressIndex :=
		pq.proposalQuotaBaseIndex + 1 + kvpb.RaftIndex(len(pq.quotaReleaseQueue))
	if upperBoundNotProgressIndex > notProgressIndex {
		notProgressIndex = upperBoundNotProgressIndex
	}
	quotaConsumedStartIndex := notProgressIndex
	upperBoundQuotaConsumedStartIndex :=
		pq.proposalQuotaBaseIndex + 1 + kvpb.RaftIndex(pq.numQuotaEntries)
	if upperBoundQuotaConsumedStartIndex > quotaConsumedStartIndex {
		quotaConsumedStartIndex = upperBoundQuotaConsumedStartIndex
	}
	ss := pq.storeState[storeID]
	if ss == nil {
		// New active replica.
		ss = &perStoreTrackerState{
			iterTimeNanos:           pq.iterState.iterStartTimeNanos,
			notProgressedIndex:      notProgressIndex,
			quotaConsumedStartIndex: quotaConsumedStartIndex,
			perStoreStats: perStoreStats{
				quotaConsumed: pq.quotaConsumed,
			},
		}
		for i := 0; kvpb.RaftIndex(i)+pq.proposalQuotaBaseIndex+1 < ss.quotaConsumedStartIndex; i++ {
			if pq.quotaReleaseQueue[i].alloc != nil {
				ss.quotaConsumed -= int64(pq.quotaReleaseQueue[i].alloc.Acquired())
			}
		}
		pq.storeState[storeID] = ss
		// We don't fiddle with progressMatchDuration, progressMatchCount, or for
		// the queue entries the queueEntry.lastProgressMatchIndex since those are
		// updated when notProgressedIndex advances, and here we have initialized
		// notProgressedIndex.
		return
	}
	// Common case. Existing active replica.
	ss.iterTimeNanos = pq.iterState.iterStartTimeNanos
	prevNotProgressedIndex := ss.notProgressedIndex
	ss.notProgressedIndex = notProgressIndex
	for i := prevNotProgressedIndex; i < notProgressIndex; i++ {
		j := i - pq.proposalQuotaBaseIndex - 1
		ss.progressMatchDuration += time.Duration(
			pq.iterState.iterStartTimeNanos - pq.quotaReleaseQueue[j].queuedUnixNanos)
		ss.progressMatchCount++
		pq.quotaReleaseQueue[j].latestProgressMatchIndex.storeID = storeID
		pq.quotaReleaseQueue[j].latestProgressMatchIndex.unixNanos = pq.iterState.iterStartTimeNanos
	}
	prevQuotaConsumedStartIndex := ss.quotaConsumedStartIndex
	ss.quotaConsumedStartIndex = quotaConsumedStartIndex
	for i := prevQuotaConsumedStartIndex; i < quotaConsumedStartIndex; i++ {
		j := i - pq.proposalQuotaBaseIndex - 1
		entry := pq.quotaReleaseQueue[j]
		if entry.alloc != nil {
			ss.quotaConsumed -= int64(entry.alloc.Acquired())
		}
	}
}

// finishReplicaIteration is called at the end of replica iteration. Returns
// the allocs to release to the quotapool.
func (pq *proposalQuotaAndDelayTracker) finishReplicaIteration(
	ctx context.Context,
) []*quotapool.IntAlloc {
	// Initialize toReleaseIndex to the currently applied index. The below
	// progress checks will only decrease the toReleaseIndex. Given that the
	// quotaReleaseQueue cannot have allocs beyond the applied index there's no
	// reason to consider progress beyond it as meaningful.
	toReleaseIndex := pq.iterState.appliedIndex
	for s, ss := range pq.storeState {
		if ss.iterTimeNanos != pq.iterState.iterStartTimeNanos {
			// Remove ones that were not iterated over.
			delete(pq.storeState, s)
		} else if ss.notProgressedIndex <= toReleaseIndex {
			toReleaseIndex = ss.notProgressedIndex - 1
		}
	}
	if pq.proposalQuotaBaseIndex < toReleaseIndex {
		// We've persisted at least toReleaseIndex-pq.proposalQuotaBaseIndex
		// entries to the raft log on all 'active' replicas and applied these
		// entries locally, so we are able to release these back to the quota
		// pool.
		numReleases := toReleaseIndex - pq.proposalQuotaBaseIndex
		return pq.releaseQuotaEntries(numReleases)
	}
	// Assert the sanity of the base index and the queue. Queue entries should
	// correspond to applied entries. It should not be possible for the base
	// index and the not yet released applied entries to not equal the applied
	// index.
	releasableIndex := pq.proposalQuotaBaseIndex + kvpb.RaftIndex(pq.numQuotaEntries)
	if releasableIndex != pq.iterState.appliedIndex {
		log.Fatalf(ctx, "proposalQuotaBaseIndex (%d) + quotaReleaseQueueLen (%d) = %d"+
			" must equal the applied index (%d)",
			pq.proposalQuotaBaseIndex, len(pq.quotaReleaseQueue), releasableIndex,
			pq.iterState.appliedIndex)
	}
	return nil
}

// Internal helper.
func (pq *proposalQuotaAndDelayTracker) releaseQuotaEntries(
	numReleases kvpb.RaftIndex,
) []*quotapool.IntAlloc {
	pq.iterState.toReleaseScratch = pq.iterState.toReleaseScratch[:0]
	for i := kvpb.RaftIndex(0); i < numReleases; i++ {
		pq.iterState.toReleaseScratch = append(
			pq.iterState.toReleaseScratch, pq.quotaReleaseQueue[i].alloc)
	}
	pq.quotaReleaseQueue = pq.quotaReleaseQueue[numReleases:]
	pq.proposalQuotaBaseIndex += numReleases
	pq.numQuotaEntries -= int(numReleases)
	return pq.iterState.toReleaseScratch
}

// lastStoreToProgress is called to retrieve information about the last store
// that progressed past a raft entry. It is used in per-request tracing. It is
// best-effort, and StoreID == 0 means no data.
func (pq *proposalQuotaAndDelayTracker) lastStoreToProgress(
	raftIndex kvpb.RaftIndex,
) (storeID roachpb.StoreID, delay time.Duration) {
	if raftIndex <= pq.proposalQuotaBaseIndex ||
		raftIndex > pq.proposalQuotaBaseIndex+kvpb.RaftIndex(len(pq.quotaReleaseQueue)) {
		return 0, 0
	}
	entry := pq.quotaReleaseQueue[raftIndex-pq.proposalQuotaBaseIndex-1]
	duration := entry.latestProgressMatchIndex.unixNanos - entry.queuedUnixNanos
	if duration < 0 {
		duration = 0
	}
	return entry.latestProgressMatchIndex.storeID, time.Duration(duration)
}

type storeQueueAndDelayStats struct {
	roachpb.StoreID
	numReplicas int
	perStoreStats
}

// getAndResetStoreStats should be called periodically, to retrieve gauges and
// delta stats. The deltas tracked in the tracker will be reset to 0.
func (pq *proposalQuotaAndDelayTracker) getAndResetStoreStats(
	scratch []storeQueueAndDelayStats,
) []storeQueueAndDelayStats {
	scratch = scratch[:0]
	for s, ss := range pq.storeState {
		scratch = append(scratch, storeQueueAndDelayStats{
			StoreID:       s,
			numReplicas:   1,
			perStoreStats: ss.perStoreStats,
		})
		qc := ss.perStoreStats.quotaConsumed
		ss.perStoreStats = perStoreStats{quotaConsumed: qc}
	}
	return scratch
}
