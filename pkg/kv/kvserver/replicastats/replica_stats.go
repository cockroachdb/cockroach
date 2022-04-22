// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replicastats

import (
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	replStatsRotateInterval = 5 * time.Minute
	decayFactor             = 0.8

	// MinStatsDuration defines a lower bound on how long users of replica stats
	// should wait before using those stats for anything. If the duration of a
	// measurement has been less than MinStatsDuration, these methods could easily
	// return outlier/anomalous data.
	MinStatsDuration = 5 * time.Second
)

// AddSSTableRequestSizeFactor wraps
// "kv.replica_stats.addsst_request_size_factor". When this setting is set to
// 0, all batch requests are treated uniformly as 1 QPS. When this setting is
// greater than or equal to 1, AddSSTable requests will add additional QPS,
// when present within a batch request. The additional QPS is size of the
// SSTable data, divided by this factor. Thereby, the magnitude of this factor
// is inversely related to QPS sensitivity to AddSSTableRequests.
var AddSSTableRequestSizeFactor = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.replica_stats.addsst_request_size_factor",
	"the divisor that is applied to addsstable request sizes, then recorded in a leaseholders QPS; 0 means all requests are treated as cost 1",
	// The default value of 50,000 was chosen as the default divisor, following manual testing that
	// is discussed in this pull request: #76252. Every additional 50,000 AddSSTable bytes will
	// increase accounted QPS by 1. Typically AddSSTableRequests are ~1mb in size, accounted as 20
	// QPS.
	50000,
).WithPublic()

// LocalityOracle provides a mapping between a node ID and it's corresponding
// locality.
type LocalityOracle func(roachpb.NodeID) string

// PerLocalityCounts maps from the string representation of a locality to count.
type PerLocalityCounts map[string]float64

// ReplicaStats maintains statistics about the work done by a replica. Its
// initial use is tracking the number of requests received from each
// cluster locality in order to inform lease transfer decisions.
type ReplicaStats struct {
	clock           *hlc.Clock
	getNodeLocality LocalityOracle

	// We use a set of time windows in order to age out old stats without having
	// to do hard resets. The `requests` array is a circular buffer of the last
	// N windows of stats. We rotate through the circular buffer every so often
	// as determined by `replStatsRotateInterval`.
	//
	// We could alternatively use a forward decay approach here, but it would
	// require more memory than this slightly less precise windowing method:
	//   http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
	Mu struct {
		syncutil.Mutex
		idx        int
		records    [6]*replicaStatsRecord
		lastRotate time.Time
		lastReset  time.Time

		// Testing only.
		avgRateForTesting float64
	}
}

type replicaStatsRecord struct {
	localityCounts PerLocalityCounts
	sum, max, min  float64
	count          int64
}

func newReplicaStatsRecord() *replicaStatsRecord {
	return &replicaStatsRecord{
		localityCounts: make(PerLocalityCounts),
		max:            -math.MaxFloat64,
		min:            math.MaxFloat64,
	}
}

// mergeReplicaStatsRecords combines two records and returns a new record with
// the merged data. When this is called with nil records, a nil record is
// returned; otherwise a new record instead.
func mergeReplicaStatsRecords(left, right *replicaStatsRecord) *replicaStatsRecord {
	if left == nil && right == nil {
		return nil
	}
	if left == nil {
		left = newReplicaStatsRecord()
	}
	if right == nil {
		right = newReplicaStatsRecord()
	}

	mergedStats := newReplicaStatsRecord()

	mergedStats.max = math.Max(left.max, right.max)
	mergedStats.min = math.Min(left.min, right.min)
	mergedStats.sum = left.sum + right.sum
	mergedStats.count = left.count + right.count

	for locality, count := range left.localityCounts {
		mergedStats.localityCounts[locality] += count
	}

	for locality, count := range right.localityCounts {
		mergedStats.localityCounts[locality] += count
	}

	return mergedStats
}

func (rsr *replicaStatsRecord) split(other *replicaStatsRecord) {
	other.max = rsr.max
	other.min = rsr.min

	rsr.count = rsr.count / 2
	other.count = rsr.count

	rsr.sum = rsr.sum / 2.0
	other.sum = rsr.sum

	for locality, count := range rsr.localityCounts {
		rsr.localityCounts[locality] = count / 2.0
		other.localityCounts[locality] = rsr.localityCounts[locality]
	}
}

// NewReplicaStats constructs a new ReplicaStats tracker.
func NewReplicaStats(clock *hlc.Clock, getNodeLocality LocalityOracle) *ReplicaStats {
	rs := &ReplicaStats{
		clock:           clock,
		getNodeLocality: getNodeLocality,
	}

	rs.Mu.lastRotate = timeutil.Unix(0, rs.clock.PhysicalNow())
	rs.Mu.records[rs.Mu.idx] = newReplicaStatsRecord()
	rs.Mu.lastReset = rs.Mu.lastRotate
	return rs
}

// MergeRequestCounts joins the current ReplicaStats object with other, for the
// purposes of merging a range.
func (rs *ReplicaStats) MergeRequestCounts(other *ReplicaStats) {
	other.Mu.Lock()
	defer other.Mu.Unlock()
	rs.Mu.Lock()
	defer rs.Mu.Unlock()

	// Sanity check that the request lengths are correct, if not we cannot
	// merge them so reset both.
	if len(rs.Mu.records) != len(other.Mu.records) {
		rs.ResetRequestCounts()
		other.ResetRequestCounts()
		return
	}

	n := len(rs.Mu.records)

	for i := range other.Mu.records {

		rsIdx := (rs.Mu.idx + n - i) % n
		otherIdx := (other.Mu.idx + n - i) % n

		rs.Mu.records[rsIdx] = mergeReplicaStatsRecords(rs.Mu.records[rsIdx], other.Mu.records[otherIdx])

		// Reset the stats on other.
		other.Mu.records[otherIdx] = newReplicaStatsRecord()
	}

	// Update the last rotate time to be the lesser of the two, so that a
	// rotation occurs as early as possible.
	if rs.Mu.lastRotate.After(other.Mu.lastRotate) {
		rs.Mu.lastRotate = other.Mu.lastRotate
	}
}

// SplitRequestCounts divides the current ReplicaStats object in two for the
// purposes of splitting a range. It modifies itself to have half its requests
// and the provided other to have the other half.
//
// Note that assuming a 50/50 split is optimistic, but it's much better than
// resetting both sides upon a split.
func (rs *ReplicaStats) SplitRequestCounts(other *ReplicaStats) {
	other.Mu.Lock()
	defer other.Mu.Unlock()
	rs.Mu.Lock()
	defer rs.Mu.Unlock()

	other.Mu.idx = rs.Mu.idx
	other.Mu.lastRotate = rs.Mu.lastRotate
	other.Mu.lastReset = rs.Mu.lastReset

	for i := range rs.Mu.records {
		if rs.Mu.records[i] == nil {
			other.Mu.records[i] = nil
			continue
		}
		other.Mu.records[i] = newReplicaStatsRecord()
		rs.Mu.records[i].split(other.Mu.records[i])
	}
}

// RecordCount records the given count against the given node ID.
func (rs *ReplicaStats) RecordCount(count float64, nodeID roachpb.NodeID) {
	var locality string
	if rs.getNodeLocality != nil {
		locality = rs.getNodeLocality(nodeID)
	}
	now := timeutil.Unix(0, rs.clock.PhysicalNow())

	rs.Mu.Lock()
	defer rs.Mu.Unlock()

	rs.maybeRotateLocked(now)

	record := rs.Mu.records[rs.Mu.idx]
	record.sum += count
	record.max = math.Max(record.max, count)
	record.min = math.Min(record.min, count)
	record.localityCounts[locality] += count
	record.count++
}

func (rs *ReplicaStats) maybeRotateLocked(now time.Time) {
	if now.Sub(rs.Mu.lastRotate) >= replStatsRotateInterval {
		rs.rotateLocked()
		rs.Mu.lastRotate = now
	}
}

func (rs *ReplicaStats) rotateLocked() {
	rs.Mu.idx = (rs.Mu.idx + 1) % len(rs.Mu.records)
	rs.Mu.records[rs.Mu.idx] = newReplicaStatsRecord()
}

// PerLocalityDecayingRate returns the per-locality counts-per-second and the
// amount of time over which the stats were accumulated. Note that the replica stats
// stats are exponentially decayed such that newer requests are weighted more
// heavily than older requests.
func (rs *ReplicaStats) PerLocalityDecayingRate() (PerLocalityCounts, time.Duration) {
	now := timeutil.Unix(0, rs.clock.PhysicalNow())

	rs.Mu.Lock()
	defer rs.Mu.Unlock()

	rs.maybeRotateLocked(now)

	// Use the fraction of time since the last rotation as a smoothing factor to
	// avoid jarring changes in request count immediately before/after a rotation.
	timeSinceRotate := now.Sub(rs.Mu.lastRotate)
	fractionOfRotation := float64(timeSinceRotate) / float64(replStatsRotateInterval)

	counts := make(PerLocalityCounts)
	var duration time.Duration
	for i := range rs.Mu.records {
		// We have to add len(rs.mu.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.mu.idx is small.
		requestsIdx := (rs.Mu.idx + len(rs.Mu.records) - i) % len(rs.Mu.records)
		if cur := rs.Mu.records[requestsIdx]; cur != nil {
			decay := math.Pow(decayFactor, float64(i)+fractionOfRotation)
			if i == 0 {
				duration += time.Duration(float64(timeSinceRotate) * decay)
			} else {
				duration += time.Duration(float64(replStatsRotateInterval) * decay)
			}
			for k, v := range cur.localityCounts {
				counts[k] += v * decay
			}
		}
	}

	if duration.Seconds() > 0 {
		for k := range counts {
			counts[k] = counts[k] / duration.Seconds()
		}
	}
	return counts, now.Sub(rs.Mu.lastReset)
}

// SumLocked returns the sum of all queries currently recorded.
// Calling this method requires holding a lock on mu.
func (rs *ReplicaStats) SumLocked() (float64, int) {
	var sum float64
	var windowsUsed int
	for i := range rs.Mu.records {
		// We have to add len(rs.mu.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.mu.idx is small.
		requestsIdx := (rs.Mu.idx + len(rs.Mu.records) - i) % len(rs.Mu.records)
		if cur := rs.Mu.records[requestsIdx]; cur != nil {
			windowsUsed++
			sum += cur.sum
		}
	}
	return sum, windowsUsed
}

// AverageRatePerSecond returns the average counts-per-second and the amount of time
// over which the stat was accumulated. Note that these averages are exact,
// not exponentially decayed (there isn't a ton of justification for going
// one way or the other, but not decaying makes the average more stable,
// which is probably better for avoiding rebalance thrashing).
func (rs *ReplicaStats) AverageRatePerSecond() (float64, time.Duration) {
	now := timeutil.Unix(0, rs.clock.PhysicalNow())

	rs.Mu.Lock()
	defer rs.Mu.Unlock()
	if rs.Mu.avgRateForTesting != 0 {
		return rs.Mu.avgRateForTesting, 0
	}

	rs.maybeRotateLocked(now)

	// First accumulate the counts, then divide by the total number of seconds.
	sum, windowsUsed := rs.SumLocked()
	if windowsUsed <= 0 {
		return 0, 0
	}
	duration := now.Sub(rs.Mu.lastRotate) + time.Duration(windowsUsed-1)*replStatsRotateInterval
	if duration == 0 {
		return 0, 0
	}
	return sum / duration.Seconds(), duration
}

// ResetRequestCounts resets the underlying request counts.
func (rs *ReplicaStats) ResetRequestCounts() {
	rs.Mu.Lock()
	defer rs.Mu.Unlock()

	for i := range rs.Mu.records {
		rs.Mu.records[i] = nil
	}
	rs.Mu.records[rs.Mu.idx] = newReplicaStatsRecord()
	rs.Mu.lastRotate = timeutil.Unix(0, rs.clock.PhysicalNow())
	rs.Mu.lastReset = rs.Mu.lastRotate
}

// SetMeanRateForTesting is a testing helper to directly sey the mean rate.
func (rs *ReplicaStats) SetMeanRateForTesting(rate float64) {
	rs.Mu.Lock()
	defer rs.Mu.Unlock()
	rs.Mu.avgRateForTesting = rate
}
