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
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"go.etcd.io/raft/v3"
)

// ReplicaMetrics contains details on the current status of the replica.
type ReplicaMetrics struct {
	Leader                    bool
	LeaseValid                bool
	Leaseholder               bool
	LeaseType                 roachpb.LeaseType
	LeaseStatus               kvserverpb.LeaseStatus
	LivenessLease             bool
	ViolatingLeasePreferences bool
	LessPreferredLease        bool

	// Quiescent indicates whether the replica believes itself to be quiesced.
	Quiescent bool
	// Ticking indicates whether the store is ticking the replica. It should be
	// the opposite of Quiescent.
	Ticking bool

	// RangeCounter is true if the current replica is responsible for range-level
	// metrics (generally the leaseholder, if live, otherwise the first replica in the
	// range descriptor).
	RangeCounter          bool
	Unavailable           bool
	Underreplicated       bool
	Overreplicated        bool
	RaftLogTooLarge       bool
	BehindCount           int64
	PausedFollowerCount   int64
	SlowRaftProposalCount int64

	QuotaPoolPercentUsed int64 // [0,100]

	// Latching and locking metrics.
	LatchMetrics     concurrency.LatchMetrics
	LockTableMetrics concurrency.LockTableMetrics
}

// Metrics returns the current metrics for the replica.
func (r *Replica) Metrics(
	ctx context.Context,
	now hlc.ClockTimestamp,
	vitalityMap livenesspb.NodeVitalityMap,
	clusterNodes int,
) ReplicaMetrics {
	r.store.unquiescedReplicas.Lock()
	_, ticking := r.store.unquiescedReplicas.m[r.RangeID]
	r.store.unquiescedReplicas.Unlock()

	latchMetrics := r.concMgr.LatchMetrics()
	lockTableMetrics := r.concMgr.LockTableMetrics()

	storeAttrs := r.store.Attrs()
	nodeAttrs := r.store.nodeDesc.Attrs
	nodeLocality := r.store.nodeDesc.Locality

	conf, err := r.LoadSpanConfig(ctx)
	if err != nil {
		return ReplicaMetrics{}
	}

	r.mu.RLock()

	var qpUsed, qpCap int64
	if q := r.mu.proposalQuota; q != nil {
		qpAvail := int64(q.ApproximateQuota())
		qpCap = int64(q.Capacity()) // NB: max capacity is MaxInt64, see NewIntPool
		qpUsed = qpCap - qpAvail
	}
	input := calcReplicaMetricsInput{
		raftCfg:               &r.store.cfg.RaftConfig,
		conf:                  conf,
		vitalityMap:           vitalityMap,
		clusterNodes:          clusterNodes,
		desc:                  r.mu.state.Desc,
		raftStatus:            r.raftSparseStatusRLocked(),
		leaseStatus:           r.leaseStatusAtRLocked(ctx, now),
		storeID:               r.store.StoreID(),
		storeAttrs:            storeAttrs,
		nodeAttrs:             nodeAttrs,
		nodeLocality:          nodeLocality,
		quiescent:             r.mu.quiescent,
		ticking:               ticking,
		latchMetrics:          latchMetrics,
		lockTableMetrics:      lockTableMetrics,
		raftLogSize:           r.mu.raftLogSize,
		raftLogSizeTrusted:    r.mu.raftLogSizeTrusted,
		qpUsed:                qpUsed,
		qpCapacity:            qpCap,
		paused:                r.mu.pausedFollowers,
		slowRaftProposalCount: r.mu.slowProposalCount,
	}

	r.mu.RUnlock()

	return calcReplicaMetrics(input)
}

type calcReplicaMetricsInput struct {
	raftCfg               *base.RaftConfig
	conf                  *roachpb.SpanConfig
	vitalityMap           livenesspb.NodeVitalityMap
	clusterNodes          int
	desc                  *roachpb.RangeDescriptor
	raftStatus            *raftSparseStatus
	leaseStatus           kvserverpb.LeaseStatus
	storeID               roachpb.StoreID
	storeAttrs, nodeAttrs roachpb.Attributes
	nodeLocality          roachpb.Locality
	quiescent             bool
	ticking               bool
	latchMetrics          concurrency.LatchMetrics
	lockTableMetrics      concurrency.LockTableMetrics
	raftLogSize           int64
	raftLogSizeTrusted    bool
	qpUsed, qpCapacity    int64 // quota pool used and capacity bytes
	paused                map[roachpb.ReplicaID]struct{}
	slowRaftProposalCount int64
}

func calcReplicaMetrics(d calcReplicaMetricsInput) ReplicaMetrics {
	var validLease, validLeaseOwner, livenessLease, violatingLeasePreferences, lessPreferredLease bool
	var validLeaseType roachpb.LeaseType
	if d.leaseStatus.IsValid() {
		validLease = true
		validLeaseOwner = d.leaseStatus.Lease.OwnedBy(d.storeID)
		validLeaseType = d.leaseStatus.Lease.Type()
		if validLeaseOwner {
			livenessLease = keys.NodeLivenessSpan.Overlaps(d.desc.RSpan().AsRawSpanWithNoLocals())
			switch CheckStoreAgainstLeasePreferences(
				d.storeID, d.storeAttrs, d.nodeAttrs,
				d.nodeLocality, d.conf.LeasePreferences) {
			case LeasePreferencesViolating:
				violatingLeasePreferences = true
			case LeasePreferencesLessPreferred:
				lessPreferredLease = true
			}
		}
	}

	rangeCounter, unavailable, underreplicated, overreplicated := calcRangeCounter(
		d.storeID, d.desc, d.leaseStatus, d.vitalityMap, d.conf.GetNumVoters(), d.conf.NumReplicas, d.clusterNodes)

	// The raft leader computes the number of raft entries that replicas are
	// behind.
	leader := d.raftStatus != nil && d.raftStatus.RaftState == raft.StateLeader
	var leaderBehindCount, leaderPausedFollowerCount int64
	if leader {
		leaderBehindCount = calcBehindCount(d.raftStatus, d.desc, d.vitalityMap)
		leaderPausedFollowerCount = int64(len(d.paused))
	}

	const raftLogTooLargeMultiple = 4
	return ReplicaMetrics{
		Leader:                    leader,
		LeaseValid:                validLease,
		Leaseholder:               validLeaseOwner,
		LeaseType:                 validLeaseType,
		LeaseStatus:               d.leaseStatus,
		LivenessLease:             livenessLease,
		ViolatingLeasePreferences: violatingLeasePreferences,
		LessPreferredLease:        lessPreferredLease,
		Quiescent:                 d.quiescent,
		Ticking:                   d.ticking,
		RangeCounter:              rangeCounter,
		Unavailable:               unavailable,
		Underreplicated:           underreplicated,
		Overreplicated:            overreplicated,
		RaftLogTooLarge: d.raftLogSizeTrusted &&
			d.raftLogSize > raftLogTooLargeMultiple*d.raftCfg.RaftLogTruncationThreshold,
		BehindCount:           leaderBehindCount,
		PausedFollowerCount:   leaderPausedFollowerCount,
		SlowRaftProposalCount: d.slowRaftProposalCount,
		QuotaPoolPercentUsed:  calcQuotaPoolPercentUsed(d.qpUsed, d.qpCapacity),
		LatchMetrics:          d.latchMetrics,
		LockTableMetrics:      d.lockTableMetrics,
	}
}

func calcQuotaPoolPercentUsed(qpUsed, qpCapacity int64) int64 {
	if qpCapacity < 1 {
		qpCapacity++ // defense in depth against divide by zero below
	}
	// NB: assumes qpUsed < qpCapacity. If this isn't the case, below returns more
	// than 100, but that's better than rounding it (and thus hiding a problem).
	// Also this might be expected if a quota pool overcommits (for example, due
	// to a single proposal that clocks in above the total capacity).
	return int64(math.Round(float64(100*qpUsed) / float64(qpCapacity))) // 0-100
}

// calcRangeCounter returns whether this replica is designated as the replica in
// the range responsible for range-level metrics, whether the range doesn't have
// a quorum of live voting replicas, and whether the range is currently
// under-replicated (with regards to either the number of voting replicas or the
// number of non-voting replicas).
//
// Note: we compute an estimated range count across the cluster by counting the
// leaseholder of each descriptor if it's live, otherwise the first live
// replica. This heuristic can double count, as all nodes may not agree on who
// the leaseholder is, nor whether it is live (e.g. during a network partition).
func calcRangeCounter(
	storeID roachpb.StoreID,
	desc *roachpb.RangeDescriptor,
	leaseStatus kvserverpb.LeaseStatus,
	vitalityMap livenesspb.NodeVitalityMap,
	numVoters, numReplicas int32,
	clusterNodes int,
) (rangeCounter, unavailable, underreplicated, overreplicated bool) {
	// If there is a live leaseholder (regardless of whether the lease is still
	// valid) that leaseholder is responsible for range-level metrics.
	if vitalityMap[leaseStatus.Lease.Replica.NodeID].IsLive(livenesspb.Metrics) {
		rangeCounter = leaseStatus.OwnedBy(storeID)
	} else {
		// Otherwise, use the first live replica.
		for _, rd := range desc.Replicas().Descriptors() {
			if vitalityMap[rd.NodeID].IsLive(livenesspb.Metrics) {
				rangeCounter = rd.StoreID == storeID
				break
			}
		}
	}

	// We also compute an estimated per-range count of under-replicated and
	// unavailable ranges for each range based on the liveness table.
	if rangeCounter {
		neededVoters := allocatorimpl.GetNeededVoters(numVoters, clusterNodes)
		neededNonVoters := allocatorimpl.GetNeededNonVoters(int(numVoters), int(numReplicas-numVoters), clusterNodes)
		status := desc.Replicas().ReplicationStatus(func(rDesc roachpb.ReplicaDescriptor) bool {
			return vitalityMap[rDesc.NodeID].IsLive(livenesspb.Metrics)
		},
			// needed{Voters,NonVoters} - we don't care about the
			// under/over-replication determinations from the report because
			// it's too magic. We'll do our own determination below.
			0, -1)
		unavailable = !status.Available
		liveVoters := calcLiveVoterReplicas(desc, vitalityMap)
		liveNonVoters := calcLiveNonVoterReplicas(desc, vitalityMap)
		if neededVoters > liveVoters || neededNonVoters > liveNonVoters {
			underreplicated = true
		} else if neededVoters < liveVoters || neededNonVoters < liveNonVoters {
			overreplicated = true
		}
	}
	return
}

// calcLiveVoterReplicas returns a count of the live voter replicas; a live
// replica is determined by checking its node in the provided liveness map. This
// method is used when indicating under-replication so only voter replicas are
// considered.
func calcLiveVoterReplicas(
	desc *roachpb.RangeDescriptor, vitalityMap livenesspb.NodeVitalityMap,
) int {
	return calcLiveReplicas(desc.Replicas().VoterDescriptors(), vitalityMap)
}

// calcLiveNonVoterReplicas returns a count of the live non-voter replicas; a live
// replica is determined by checking its node in the provided liveness map.
func calcLiveNonVoterReplicas(
	desc *roachpb.RangeDescriptor, vitalityMap livenesspb.NodeVitalityMap,
) int {
	return calcLiveReplicas(desc.Replicas().NonVoterDescriptors(), vitalityMap)
}

func calcLiveReplicas(
	repls []roachpb.ReplicaDescriptor, vitalityMap livenesspb.NodeVitalityMap,
) int {
	var live int
	for _, rd := range repls {
		if vitalityMap[rd.NodeID].IsLive(livenesspb.Metrics) {
			live++
		}
	}
	return live
}

// calcBehindCount returns a total count of log entries that follower replicas
// are behind. This can only be computed on the raft leader.
func calcBehindCount(
	raftStatus *raftSparseStatus,
	desc *roachpb.RangeDescriptor,
	vitalityMap livenesspb.NodeVitalityMap,
) int64 {
	var behindCount int64
	for _, rd := range desc.Replicas().Descriptors() {
		if progress, ok := raftStatus.Progress[uint64(rd.ReplicaID)]; ok {
			if progress.Match > 0 &&
				progress.Match < raftStatus.Commit {
				behindCount += int64(raftStatus.Commit) - int64(progress.Match)
			}
		}
	}

	return behindCount
}

// LoadStats returns the load statistics for the replica.
func (r *Replica) LoadStats() load.ReplicaLoadStats {
	return r.loadStats.Stats()
}

func (r *Replica) needsSplitBySizeRLocked(conf *roachpb.SpanConfig) bool {
	exceeded, _ := r.exceedsMultipleOfSplitSizeRLocked(conf, 1)
	return exceeded
}

func (r *Replica) needsRaftLogTruncationLocked() bool {
	// We don't want to check the Raft log for truncation on every write
	// operation or even every operation which occurs after the Raft log exceeds
	// RaftLogQueueStaleSize. The logic below queues the replica for possible
	// Raft log truncation whenever an additional RaftLogQueueStaleSize bytes
	// have been written to the Raft log. Note that it does not matter if some
	// of the bytes in raftLogLastCheckSize are already part of pending
	// truncations since this comparison is looking at whether the raft log has
	// grown sufficiently.
	checkRaftLog := r.mu.raftLogSize-r.mu.raftLogLastCheckSize >= RaftLogQueueStaleSize
	if checkRaftLog {
		r.mu.raftLogLastCheckSize = r.mu.raftLogSize
	}
	return checkRaftLog
}

// exceedsMultipleOfSplitSizeRLocked returns whether the current size of the
// range exceeds the max size times mult. If so, the bytes overage is also
// returned. Note that the max size is determined by either the current maximum
// size as dictated by the span config or a previous max size indicating that
// the max size has changed relatively recently and thus we should not
// backpressure for being over.
func (r *Replica) exceedsMultipleOfSplitSizeRLocked(
	conf *roachpb.SpanConfig, mult float64,
) (exceeded bool, bytesOver int64) {
	maxBytes := conf.RangeMaxBytes
	if r.mu.largestPreviousMaxRangeSizeBytes > maxBytes {
		maxBytes = r.mu.largestPreviousMaxRangeSizeBytes
	}
	size := r.mu.state.Stats.Total()
	maxSize := int64(float64(maxBytes)*mult) + 1
	if maxBytes <= 0 || size <= maxSize {
		return false, 0
	}
	return true, size - maxSize
}
