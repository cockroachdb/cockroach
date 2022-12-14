// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package load

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	// "Write" is as described in WritesPerSecond.
	WriteBytesPerSecond float64
	// ReadBytesPerSecond is the replica's average bytes read per second. A "Read" is as
	// described in ReadsPerSecond.
	ReadBytesPerSecond float64
	// CPUNanosPerSecond is the range's time spent on-processor averaged per second.
	CPUNanosPerSecond float64
}

// ReplicaLoad tracks a sliding window of throughput on a replica.
type ReplicaLoad struct {
	stats [numLoadStats]*replicastats.ReplicaStats
}

// NewReplicaLoad returns a new ReplicaLoad, which may be used to track the
// request throughput of a replica.
func NewReplicaLoad(clock *hlc.Clock, getNodeLocality replicastats.LocalityOracle) *ReplicaLoad {
	stats := [numLoadStats]*replicastats.ReplicaStats{}

	// NB: We only wish to record the locality of a request for QPS, where it
	// as only follow-the-workload lease transfers use this per-locality
	// request count. Maintaining more than one bucket for client requests
	// increases the memory footprint O(localities).
	stats[Queries] = replicastats.NewReplicaStats(clock, getNodeLocality)

	// For all other stats, we don't include a locality oracle.
	for i := 1; i < numLoadStats; i++ {
		stats[i] = replicastats.NewReplicaStats(clock, nil)
	}

	return &ReplicaLoad{
		stats: stats,
	}
}

func (rl *ReplicaLoad) record(stat LoadStat, val float64, nodeID roachpb.NodeID) {
	rl.stats[stat].RecordCount(val, nodeID)
}

// Split will distribute the load in the calling struct, evenly between itself
// and other.
func (rl *ReplicaLoad) Split(other *ReplicaLoad) {
	for i := range rl.stats {
		rl.stats[i].SplitRequestCounts(other.stats[i])
	}
}

// Merge will combine the tracked load in other, into the calling struct.
func (rl *ReplicaLoad) Merge(other *ReplicaLoad) {
	for i := range rl.stats {
		rl.stats[i].MergeRequestCounts(other.stats[i])
	}
}

// Reset will clear all recorded history.
func (rl *ReplicaLoad) Reset() {
	for i := range rl.stats {
		rl.stats[i].ResetRequestCounts()
	}
}

// get returns the current value for the LoadStat with ordinal stat.
func (rl *ReplicaLoad) get(stat LoadStat) float64 {
	var ret float64
	// Only return the value if the statistics have been gathered for longer
	// than the minimum duration.
	if val, dur := rl.stats[stat].AverageRatePerSecond(); dur >= replicastats.MinStatsDuration {
		ret = val
	}
	return ret
}

// Stats returns a current stat summary of replica load.
func (rl *ReplicaLoad) Stats() ReplicaLoadStats {
	return ReplicaLoadStats{
		QueriesPerSecond:    rl.get(Queries),
		RequestsPerSecond:   rl.get(Requests),
		WriteKeysPerSecond:  rl.get(WriteKeys),
		ReadKeysPerSecond:   rl.get(ReadKeys),
		WriteBytesPerSecond: rl.get(WriteBytes),
		ReadBytesPerSecond:  rl.get(ReadBytes),
		CPUNanosPerSecond:   rl.get(RaftCPUNanos) + rl.get(ReqCPUNanos),
	}
}

// RequestLocalityInfo returns the summary of client localities for requests
// made to this replica.
func (rl *ReplicaLoad) RequestLocalityInfo() *replicastats.RatedSummary {
	return rl.stats[Queries].SnapshotRatedSummary()
}

// TestingGetSum returns the sum of recorded values for the LoadStat with
// ordinal stat. The sum is used in testing in place of any averaging in order
// to assert precisely on the values that have been recorded.
func (rl *ReplicaLoad) TestingGetSum(stat LoadStat) float64 {
	val, _ := rl.stats[stat].SumLocked()
	return val
}

// TestingSetStat sets the value for the LoadStat with ordinal stat to be the
// value given. This value will then be returned in all future Stats() calls
// unless overriden by another call to TestingSetStat.
func (rl *ReplicaLoad) TestingSetStat(stat LoadStat, val float64) {
	rl.stats[stat].SetMeanRateForTesting(val)
}
