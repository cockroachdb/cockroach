// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TODO(kvoli): Recording per-second stats on every replica costs unnecessary
// memory. Only a counter should be maintained per replica along with a low
// cost ewma if necessary. A central, fixed memory cost structure should track
// finer grained moving averages of interesting replicas.

// LoadStat represents an ordinal position in replica load for a specific type
// of load statistic.
type LoadStat int

const (
	Queries LoadStat = iota
	Requests
	WriteKeys
	ReadKeys
	WriteBytes
	ReadBytes
	RaftCPUNanos
	ReqCPUNanos

	numLoadStats = 8
)

// ReplicaLoadStats contains per-second average statistics for load upon a
// replica. All fields will return 0 until after replicastats.MinStatsDuration
// has passed, despite any values recorded. This is done to avoid reporting
// erroneous stats that can arise when the sample time is small.
type ReplicaLoadStats struct {
	// QueriesPerSecond is the replica's average QPS if it is the current leaseholder. If
	// it isn't, this will return 0 because the replica does not know about the
	// reads that the leaseholder is serving.
	//
	// A "Query" is a BatchRequest (regardless of its contents) arriving at the
	// leaseholder with a gateway node set in the header (i.e. excluding
	// requests that weren't sent through a DistSender, which in practice
	// should be practically none). See Replica.getBatchRequestQPS() for how
	// this is accounted for.
	QueriesPerSecond float64
	// RequestsPerSecond is the replica's average requests received per second. A
	// batch request may have one to many requests.
	RequestsPerSecond float64
	// WriteKeysPerSecond is the replica's average keys written per second. A
	// "Write" is a mutation applied by Raft as measured by
	// engine.RocksDBBatchCount(writeBatch). This corresponds roughly to the number
	// of keys mutated by a write. For example, writing 12 intents would count as 24
	// writes (12 for the metadata, 12 for the versions). A DeleteRange that
	// ultimately only removes one key counts as one (or two if it's transactional).
	WriteKeysPerSecond float64
	// ReadKeysPerSecond is the replica's average keys read per second. A "Read"
	// is a key access during evaluation of a batch request. This includes both
	// follower and leaseholder reads.
	ReadKeysPerSecond float64
	// WriteBytesPerSecond is the replica's average bytes written per second. A
	// "Write" is as described in WritesPerSecond. If the replica is a leaseholder,
	// this is recorded as the bytes that will be written by the replica during the
	// application of the Raft command including write bytes and ingested bytes for
	// AddSSTable requests. If the replica is a follower, this is recorded right
	// before a command is applied to the state machine.
	WriteBytesPerSecond float64
	// ReadBytesPerSecond is the replica's average bytes read per second. A "Read" is as
	// described in ReadsPerSecond.
	ReadBytesPerSecond float64
	// RaftCPUNanos is the replica's time spent on-processor for raft averaged
	// per second.
	RaftCPUNanosPerSecond float64
	// RequestCPUNanos is the replica's time spent on-processor for requests
	// averaged per second.
	RequestCPUNanosPerSecond float64
}

// ReplicaLoad tracks a sliding window of throughput on a replica.
type ReplicaLoad struct {
	clock *hlc.Clock

	mu struct {
		syncutil.Mutex
		stats [numLoadStats]*replicastats.ReplicaStats
	}
}

// NewReplicaLoad returns a new ReplicaLoad, which may be used to track the
// request throughput of a replica.
func NewReplicaLoad(clock *hlc.Clock, getNodeLocality replicastats.LocalityOracle) *ReplicaLoad {
	rl := &ReplicaLoad{
		clock: clock,
	}

	stats := [numLoadStats]*replicastats.ReplicaStats{}
	now := timeutil.Unix(0, clock.PhysicalNow())

	// NB: We only wish to record the locality of a request for QPS, where it
	// as only follow-the-workload lease transfers use this per-locality
	// request count. Maintaining more than one bucket for client requests
	// increases the memory footprint O(localities).
	stats[Queries] = replicastats.NewReplicaStats(now, getNodeLocality)

	// For all other stats, we don't include a locality oracle.
	for i := 1; i < numLoadStats; i++ {
		stats[i] = replicastats.NewReplicaStats(now, nil)
	}

	rl.mu.stats = stats
	return rl

}

func (rl *ReplicaLoad) record(stat LoadStat, val float64, nodeID roachpb.NodeID) {
	now := timeutil.Unix(0, rl.clock.PhysicalNow())
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.mu.stats[stat].RecordCount(now, val, nodeID)
}

// Split will distribute the load from the calling struct, evenly between itself
// and other.
// NB: Split could create a cycle where a.split(b) || b.split(c) || c.split(a)
// (where || = concurrent). Split should not be called on another replica load
// object unless a higher level lock (raftMu) is also held on the given
// replicas. See SplitRange() in store_split.go.
func (rl *ReplicaLoad) Split(other *ReplicaLoad) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()

	// TODO(kvoli): Make this method immutable, returning two new replica load
	// structs rather than mutating the original and other to split
	// attribution.
	for i := range rl.mu.stats {
		rl.mu.stats[i].SplitRequestCounts(other.mu.stats[i])
	}
}

// Merge will combine the tracked load from other, into the calling struct.
// NB: Merging could create a cycle in theory where a.merge(b) || b.merge(c) ||
// c.merge(a) (where || = concurrent). Merge should not be called on another
// replica load object unless a higher level lock (raftMu) is also held on the
// given replicas. See MergeRange() in store_merge.go.
func (rl *ReplicaLoad) Merge(other *ReplicaLoad) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()

	// TODO(kvoli): Make this method immutable, returning one new replica load
	// struct rather than mutating the original and other to merge attribution.
	for i := range rl.mu.stats {
		rl.mu.stats[i].MergeRequestCounts(other.mu.stats[i])
	}
}

// Reset will clear all recorded history.
func (rl *ReplicaLoad) Reset() {
	now := timeutil.Unix(0, rl.clock.PhysicalNow())
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for i := range rl.mu.stats {
		rl.mu.stats[i].ResetRequestCounts(now)
	}
}

// getLockedWithNow returns the current value for the LoadStat with ordinal
// stat using the provided time. It requires rl.mu to be held.
func (rl *ReplicaLoad) getLockedWithNow(stat LoadStat, now time.Time) float64 {
	rl.mu.AssertHeld()
	var ret float64
	// Only return the value if the statistics have been gathered for longer
	// than the minimum duration.
	if val, dur := rl.mu.stats[stat].AverageRatePerSecond(now); dur >= replicastats.MinStatsDuration {
		ret = val
	}
	return ret
}

// statsLockedWithNow returns a ReplicaLoadStats populated using the provided
// time. It requires rl.mu to be held.
func (rl *ReplicaLoad) statsLockedWithNow(now time.Time) ReplicaLoadStats {
	return ReplicaLoadStats{
		QueriesPerSecond:         rl.getLockedWithNow(Queries, now),
		RequestsPerSecond:        rl.getLockedWithNow(Requests, now),
		WriteKeysPerSecond:       rl.getLockedWithNow(WriteKeys, now),
		ReadKeysPerSecond:        rl.getLockedWithNow(ReadKeys, now),
		WriteBytesPerSecond:      rl.getLockedWithNow(WriteBytes, now),
		ReadBytesPerSecond:       rl.getLockedWithNow(ReadBytes, now),
		RequestCPUNanosPerSecond: rl.getLockedWithNow(ReqCPUNanos, now),
		RaftCPUNanosPerSecond:    rl.getLockedWithNow(RaftCPUNanos, now),
	}
}

// Stats returns a current stat summary of replica load.
func (rl *ReplicaLoad) Stats() ReplicaLoadStats {
	now := timeutil.Unix(0, rl.clock.PhysicalNow())
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.statsLockedWithNow(now)
}

// StatsWithLocalityInfo returns load stats and the Queries dimension's
// locality info in a single locked operation. This avoids the overhead of
// acquiring the mutex twice and computing the current time repeatedly,
// which matters when called per-replica during Store.Capacity scans
// (30k+ replicas).
func (rl *ReplicaLoad) StatsWithLocalityInfo() (ReplicaLoadStats, replicastats.RatedSummary) {
	now := timeutil.Unix(0, rl.clock.PhysicalNow())
	rl.mu.Lock()
	defer rl.mu.Unlock()

	return rl.statsLockedWithNow(now), rl.mu.stats[Queries].SnapshotRatedSummary(now)
}

// RequestLocalityInfo returns the summary of client localities for requests
// made to this replica.
func (rl *ReplicaLoad) RequestLocalityInfo() replicastats.RatedSummary {
	now := timeutil.Unix(0, rl.clock.PhysicalNow())
	rl.mu.Lock()
	defer rl.mu.Unlock()

	return rl.mu.stats[Queries].SnapshotRatedSummary(now)
}

// TestingGetSum returns the sum of recorded values for the LoadStat with
// ordinal stat. The sum is used in testing in place of any averaging in order
// to assert precisely on the values that have been recorded.
func (rl *ReplicaLoad) TestingGetSum(stat LoadStat) float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	val, _ := rl.mu.stats[stat].Sum()
	return val
}

// TestingSetStat sets the value for the LoadStat with ordinal stat to be the
// value given. This value will then be returned in all future Stats() calls
// unless overriden by another call to TestingSetStat.
func (rl *ReplicaLoad) TestingSetStat(stat LoadStat, val float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.mu.stats[stat].SetMeanRateForTesting(val)
}
