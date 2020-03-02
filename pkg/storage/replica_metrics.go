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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"go.etcd.io/etcd/raft"
)

// ReplicaMetrics contains details on the current status of the replica.
type ReplicaMetrics struct {
	Leader      bool
	LeaseValid  bool
	Leaseholder bool
	LeaseType   roachpb.LeaseType
	LeaseStatus storagepb.LeaseStatus

	// Quiescent indicates whether the replica believes itself to be quiesced.
	Quiescent bool
	// Ticking indicates whether the store is ticking the replica. It should be
	// the opposite of Quiescent.
	Ticking bool

	// Is this the replica which collects per-range metrics? This is done either
	// on the leader or, if there is no leader, on the largest live replica ID.
	RangeCounter    bool
	Unavailable     bool
	Underreplicated bool
	Overreplicated  bool
	BehindCount     int64
	LatchInfoLocal  storagepb.LatchManagerInfo
	LatchInfoGlobal storagepb.LatchManagerInfo
	RaftLogTooLarge bool
}

// Metrics returns the current metrics for the replica.
func (r *Replica) Metrics(
	ctx context.Context, now hlc.Timestamp, livenessMap IsLiveMap, clusterNodes int,
) ReplicaMetrics {
	r.mu.RLock()
	raftStatus := r.raftStatusRLocked()
	leaseStatus := r.leaseStatus(*r.mu.state.Lease, now, r.mu.minLeaseProposedTS)
	quiescent := r.mu.quiescent || r.mu.internalRaftGroup == nil
	desc := r.mu.state.Desc
	zone := r.mu.zone
	raftLogSize := r.mu.raftLogSize
	r.mu.RUnlock()

	r.store.unquiescedReplicas.Lock()
	_, ticking := r.store.unquiescedReplicas.m[r.RangeID]
	r.store.unquiescedReplicas.Unlock()

	latchInfoGlobal, latchInfoLocal := r.latchMgr.Info()

	return calcReplicaMetrics(
		ctx,
		now,
		&r.store.cfg.RaftConfig,
		zone,
		livenessMap,
		clusterNodes,
		desc,
		raftStatus,
		leaseStatus,
		r.store.StoreID(),
		quiescent,
		ticking,
		latchInfoLocal,
		latchInfoGlobal,
		raftLogSize,
	)
}

func calcReplicaMetrics(
	_ context.Context,
	_ hlc.Timestamp,
	raftCfg *base.RaftConfig,
	zone *zonepb.ZoneConfig,
	livenessMap IsLiveMap,
	clusterNodes int,
	desc *roachpb.RangeDescriptor,
	raftStatus *raft.Status,
	leaseStatus storagepb.LeaseStatus,
	storeID roachpb.StoreID,
	quiescent bool,
	ticking bool,
	latchInfoLocal storagepb.LatchManagerInfo,
	latchInfoGlobal storagepb.LatchManagerInfo,
	raftLogSize int64,
) ReplicaMetrics {
	var m ReplicaMetrics

	var leaseOwner bool
	m.LeaseStatus = leaseStatus
	if leaseStatus.State == storagepb.LeaseState_VALID {
		m.LeaseValid = true
		leaseOwner = leaseStatus.Lease.OwnedBy(storeID)
		m.LeaseType = leaseStatus.Lease.Type()
	}
	m.Leaseholder = m.LeaseValid && leaseOwner
	m.Leader = isRaftLeader(raftStatus)
	m.Quiescent = quiescent
	m.Ticking = ticking

	m.RangeCounter, m.Unavailable, m.Underreplicated, m.Overreplicated =
		calcRangeCounter(storeID, desc, livenessMap, *zone.NumReplicas, clusterNodes)

	// The raft leader computes the number of raft entries that replicas are
	// behind.
	if m.Leader {
		m.BehindCount = calcBehindCount(raftStatus, desc, livenessMap)
	}

	m.LatchInfoLocal = latchInfoLocal
	m.LatchInfoGlobal = latchInfoGlobal

	const raftLogTooLargeMultiple = 4
	m.RaftLogTooLarge = raftLogSize > (raftLogTooLargeMultiple * raftCfg.RaftLogTruncationThreshold)

	return m
}

// calcRangeCounter returns whether this replica is designated as the
// replica in the range responsible for range-level metrics, whether
// the range doesn't have a quorum of live replicas, and whether the
// range is currently under-replicated.
//
// Note: we compute an estimated range count across the cluster by counting the
// first live replica in each descriptor. Note that the first live replica is
// an arbitrary choice. We want to select one live replica to do the counting
// that all replicas can agree on.
//
// Note that this heuristic can double count. If the first live replica is on
// a node that is partitioned from the other replicas in the range, there may
// be multiple nodes which believe they are the first live replica. This
// scenario seems rare as it requires the partitioned node to be alive enough
// to be performing liveness heartbeats.
func calcRangeCounter(
	storeID roachpb.StoreID,
	desc *roachpb.RangeDescriptor,
	livenessMap IsLiveMap,
	numReplicas int32,
	clusterNodes int,
) (rangeCounter, unavailable, underreplicated, overreplicated bool) {
	// It seems unlikely that a learner replica would be the first live one, but
	// there's no particular reason to exclude them. Note that `All` returns the
	// voters first.
	for _, rd := range desc.Replicas().All() {
		if livenessMap[rd.NodeID].IsLive {
			rangeCounter = rd.StoreID == storeID
			break
		}
	}
	// We also compute an estimated per-range count of under-replicated and
	// unavailable ranges for each range based on the liveness table.
	if rangeCounter {
		unavailable = !desc.Replicas().CanMakeProgress(func(rDesc roachpb.ReplicaDescriptor) bool {
			return livenessMap[rDesc.NodeID].IsLive
		})
		needed := GetNeededReplicas(numReplicas, clusterNodes)
		liveVoterReplicas := calcLiveVoterReplicas(desc, livenessMap)
		if needed > liveVoterReplicas {
			underreplicated = true
		} else if needed < liveVoterReplicas {
			overreplicated = true
		}
	}
	return
}

// calcLiveVoterReplicas returns a count of the live voter replicas; a live
// replica is determined by checking its node in the provided liveness map. This
// method is used when indicating under-replication so only voter replicas are
// considered.
func calcLiveVoterReplicas(desc *roachpb.RangeDescriptor, livenessMap IsLiveMap) int {
	var live int
	for _, rd := range desc.Replicas().Voters() {
		if livenessMap[rd.NodeID].IsLive {
			live++
		}
	}
	return live
}

// calcBehindCount returns a total count of log entries that follower replicas
// are behind. This can only be computed on the raft leader.
func calcBehindCount(
	raftStatus *raft.Status, desc *roachpb.RangeDescriptor, livenessMap IsLiveMap,
) int64 {
	var behindCount int64
	for _, rd := range desc.Replicas().All() {
		if progress, ok := raftStatus.Progress[uint64(rd.ReplicaID)]; ok {
			if progress.Match > 0 &&
				progress.Match < raftStatus.Commit {
				behindCount += int64(raftStatus.Commit) - int64(progress.Match)
			}
		}
	}

	return behindCount
}

// QueriesPerSecond returns the range's average QPS if it is the current
// leaseholder. If it isn't, this will return 0 because the replica does not
// know about the reads that the leaseholder is serving.
//
// A "Query" is a BatchRequest (regardless of its contents) arriving at the
// leaseholder with a gateway node set in the header (i.e. excluding requests
// that weren't sent through a DistSender, which in practice should be
// practically none).
func (r *Replica) QueriesPerSecond() float64 {
	qps, _ := r.leaseholderStats.avgQPS()
	return qps
}

// WritesPerSecond returns the range's average keys written per second. A
// "Write" is a mutation applied by Raft as measured by
// engine.RocksDBBatchCount(writeBatch). This corresponds roughly to the number
// of keys mutated by a write. For example, writing 12 intents would count as 24
// writes (12 for the metadata, 12 for the versions). A DeleteRange that
// ultimately only removes one key counts as one (or two if it's transactional).
func (r *Replica) WritesPerSecond() float64 {
	wps, _ := r.writeStats.avgQPS()
	return wps
}

func (r *Replica) needsSplitBySizeRLocked() bool {
	return r.exceedsMultipleOfSplitSizeRLocked(1)
}

func (r *Replica) needsMergeBySizeRLocked() bool {
	return r.mu.state.Stats.Total() < *r.mu.zone.RangeMinBytes
}

func (r *Replica) exceedsMultipleOfSplitSizeRLocked(mult float64) bool {
	maxBytes := *r.mu.zone.RangeMaxBytes
	size := r.mu.state.Stats.Total()
	return maxBytes > 0 && float64(size) > float64(maxBytes)*mult
}
