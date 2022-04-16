// Copyright 2017 The Cockroach Authors.
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

type localityOracle func(roachpb.NodeID) string

// perLocalityCounts maps from the string representation of a locality to count.
type perLocalityCounts map[string]float64

// replicaStats maintains statistics about the work done by a replica. Its
// initial use is tracking the number of requests received from each
// cluster locality in order to inform lease transfer decisions.
type replicaStats struct {
	clock           *hlc.Clock
	getNodeLocality localityOracle

	// We use a set of time windows in order to age out old stats without having
	// to do hard resets. The `requests` array is a circular buffer of the last
	// N windows of stats. We rotate through the circular buffer every so often
	// as determined by `replStatsRotateInterval`.
	//
	// We could alternatively use a forward decay approach here, but it would
	// require more memory than this slightly less precise windowing method:
	//   http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
	mu struct {
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
	localityCounts perLocalityCounts
	sum, max, min  float64
	count          int64
}

func newReplicaStatsRecord() *replicaStatsRecord {
	return &replicaStatsRecord{
		localityCounts: make(perLocalityCounts),
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

func newReplicaStats(clock *hlc.Clock, getNodeLocality localityOracle) *replicaStats {
	rs := &replicaStats{
		clock:           clock,
		getNodeLocality: getNodeLocality,
	}

	rs.mu.lastRotate = timeutil.Unix(0, rs.clock.PhysicalNow())
	rs.mu.records[rs.mu.idx] = newReplicaStatsRecord()
	rs.mu.lastReset = rs.mu.lastRotate
	return rs
}

// mergeRequestCounts joins the current replicaStats object with other, for the
// purposes of merging a range.
func (rs *replicaStats) mergeRequestCounts(other *replicaStats) {
	other.mu.Lock()
	defer other.mu.Unlock()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Sanity check that the request lengths are correct, if not we cannot
	// merge them so reset both.
	if len(rs.mu.records) != len(other.mu.records) {
		rs.resetRequestCounts()
		other.resetRequestCounts()
		return
	}

	n := len(rs.mu.records)

	for i := range other.mu.records {

		rsIdx := (rs.mu.idx + n - i) % n
		otherIdx := (other.mu.idx + n - i) % n

		rs.mu.records[rsIdx] = mergeReplicaStatsRecords(rs.mu.records[rsIdx], other.mu.records[otherIdx])

		// Reset the stats on other.
		other.mu.records[otherIdx] = newReplicaStatsRecord()
	}

	// Update the last rotate time to be the lesser of the two, so that a
	// rotation occurs as early as possible.
	if rs.mu.lastRotate.After(other.mu.lastRotate) {
		rs.mu.lastRotate = other.mu.lastRotate
	}
}

// splitRequestCounts divides the current replicaStats object in two for the
// purposes of splitting a range. It modifies itself to have half its requests
// and the provided other to have the other half.
//
// Note that assuming a 50/50 split is optimistic, but it's much better than
// resetting both sides upon a split.
func (rs *replicaStats) splitRequestCounts(other *replicaStats) {
	other.mu.Lock()
	defer other.mu.Unlock()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	other.mu.idx = rs.mu.idx
	other.mu.lastRotate = rs.mu.lastRotate
	other.mu.lastReset = rs.mu.lastReset

	for i := range rs.mu.records {
		if rs.mu.records[i] == nil {
			other.mu.records[i] = nil
			continue
		}
		other.mu.records[i] = newReplicaStatsRecord()
		rs.mu.records[i].split(other.mu.records[i])
	}
}

func (rs *replicaStats) recordCount(count float64, nodeID roachpb.NodeID) {
	var locality string
	if rs.getNodeLocality != nil {
		locality = rs.getNodeLocality(nodeID)
	}
	now := timeutil.Unix(0, rs.clock.PhysicalNow())

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.maybeRotateLocked(now)

	record := rs.mu.records[rs.mu.idx]
	record.sum += count
	record.max = math.Max(record.max, count)
	record.min = math.Min(record.min, count)
	record.localityCounts[locality] += count
	record.count++
}

func (rs *replicaStats) maybeRotateLocked(now time.Time) {
	if now.Sub(rs.mu.lastRotate) >= replStatsRotateInterval {
		rs.rotateLocked()
		rs.mu.lastRotate = now
	}
}

func (rs *replicaStats) rotateLocked() {
	rs.mu.idx = (rs.mu.idx + 1) % len(rs.mu.records)
	rs.mu.records[rs.mu.idx] = newReplicaStatsRecord()
}

// perLocalityDecayingRate() returns the per-locality counts-per-second and the
// amount of time over which the stats were accumulated. Note that the replica stats
// stats are exponentially decayed such that newer requests are weighted more
// heavily than older requests.
func (rs *replicaStats) perLocalityDecayingRate() (perLocalityCounts, time.Duration) {
	now := timeutil.Unix(0, rs.clock.PhysicalNow())

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.maybeRotateLocked(now)

	// Use the fraction of time since the last rotation as a smoothing factor to
	// avoid jarring changes in request count immediately before/after a rotation.
	timeSinceRotate := now.Sub(rs.mu.lastRotate)
	fractionOfRotation := float64(timeSinceRotate) / float64(replStatsRotateInterval)

	counts := make(perLocalityCounts)
	var duration time.Duration
	for i := range rs.mu.records {
		// We have to add len(rs.mu.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.mu.idx is small.
		requestsIdx := (rs.mu.idx + len(rs.mu.records) - i) % len(rs.mu.records)
		if cur := rs.mu.records[requestsIdx]; cur != nil {
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
	return counts, now.Sub(rs.mu.lastReset)
}

// sumQueriesLocked returns the sum of all queries currently recorded.
// Calling this method requires holding a lock on mu.
func (rs *replicaStats) sumLocked() (float64, int) {
	var sum float64
	var windowsUsed int
	for i := range rs.mu.records {
		// We have to add len(rs.mu.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.mu.idx is small.
		requestsIdx := (rs.mu.idx + len(rs.mu.records) - i) % len(rs.mu.records)
		if cur := rs.mu.records[requestsIdx]; cur != nil {
			windowsUsed++
			sum += cur.sum
		}
	}
	return sum, windowsUsed
}

// averageRatePerSecond returns the average counts-per-second and the amount of time
// over which the stat was accumulated. Note that these averages are exact,
// not exponentially decayed (there isn't a ton of justification for going
// one way or the other, but not decaying makes the average more stable,
// which is probably better for avoiding rebalance thrashing).
func (rs *replicaStats) averageRatePerSecond() (float64, time.Duration) {
	now := timeutil.Unix(0, rs.clock.PhysicalNow())

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.mu.avgRateForTesting != 0 {
		return rs.mu.avgRateForTesting, 0
	}

	rs.maybeRotateLocked(now)

	// First accumulate the counts, then divide by the total number of seconds.
	sum, windowsUsed := rs.sumLocked()
	if windowsUsed <= 0 {
		return 0, 0
	}
	duration := now.Sub(rs.mu.lastRotate) + time.Duration(windowsUsed-1)*replStatsRotateInterval
	if duration == 0 {
		return 0, 0
	}
	return sum / duration.Seconds(), duration
}

func (rs *replicaStats) resetRequestCounts() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	for i := range rs.mu.records {
		rs.mu.records[i] = nil
	}
	rs.mu.records[rs.mu.idx] = newReplicaStatsRecord()
	rs.mu.lastRotate = timeutil.Unix(0, rs.clock.PhysicalNow())
	rs.mu.lastReset = rs.mu.lastRotate
}

func (rs *replicaStats) setMeanRateForTesting(rate float64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.mu.avgRateForTesting = rate
}
