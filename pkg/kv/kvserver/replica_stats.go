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
		requests   [6]perLocalityCounts
		lastRotate time.Time
		lastReset  time.Time

		// Testing only.
		avgQPSForTesting float64
	}
}

func newReplicaStats(clock *hlc.Clock, getNodeLocality localityOracle) *replicaStats {
	rs := &replicaStats{
		clock:           clock,
		getNodeLocality: getNodeLocality,
	}
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
	rs.mu.lastRotate = timeutil.Unix(0, rs.clock.PhysicalNow())
	rs.mu.lastReset = rs.mu.lastRotate
	return rs
}

// splitRequestCounts divides the current replicaStats object in two for the
// purposes of splitting a range. It modifies itself to have half its requests
// and the provided other to have the other half.
//
// Note that assuming a 50/50 split is optimistic, but it's much better than
// resetting both sides upon a split.
// TODO(a-robinson): Write test for this.
func (rs *replicaStats) splitRequestCounts(other *replicaStats) {
	other.mu.Lock()
	defer other.mu.Unlock()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	other.mu.idx = rs.mu.idx
	other.mu.lastRotate = rs.mu.lastRotate
	other.mu.lastReset = rs.mu.lastReset

	for i := range rs.mu.requests {
		if rs.mu.requests[i] == nil {
			other.mu.requests[i] = nil
			continue
		}
		other.mu.requests[i] = make(perLocalityCounts)
		for k := range rs.mu.requests[i] {
			newVal := rs.mu.requests[i][k] / 2.0
			rs.mu.requests[i][k] = newVal
			other.mu.requests[i][k] = newVal
		}
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
	rs.mu.requests[rs.mu.idx][locality] += count
}

func (rs *replicaStats) maybeRotateLocked(now time.Time) {
	if now.Sub(rs.mu.lastRotate) >= replStatsRotateInterval {
		rs.rotateLocked()
		rs.mu.lastRotate = now
	}
}

func (rs *replicaStats) rotateLocked() {
	rs.mu.idx = (rs.mu.idx + 1) % len(rs.mu.requests)
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
}

// perLocalityDecayingQPS returns the per-locality QPS and the amount of time
// over which the stats were accumulated.
// Note that the QPS stats are exponentially decayed such that newer requests
// are weighted more heavily than older requests.
func (rs *replicaStats) perLocalityDecayingQPS() (perLocalityCounts, time.Duration) {
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
	for i := range rs.mu.requests {
		// We have to add len(rs.mu.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.mu.idx is small.
		requestsIdx := (rs.mu.idx + len(rs.mu.requests) - i) % len(rs.mu.requests)
		if cur := rs.mu.requests[requestsIdx]; cur != nil {
			decay := math.Pow(decayFactor, float64(i)+fractionOfRotation)
			if i == 0 {
				duration += time.Duration(float64(timeSinceRotate) * decay)
			} else {
				duration += time.Duration(float64(replStatsRotateInterval) * decay)
			}
			for k, v := range cur {
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
func (rs *replicaStats) sumQueriesLocked() (float64, int) {
	var sum float64
	var windowsUsed int
	for i := range rs.mu.requests {
		// We have to add len(rs.mu.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.mu.idx is small.
		requestsIdx := (rs.mu.idx + len(rs.mu.requests) - i) % len(rs.mu.requests)
		if cur := rs.mu.requests[requestsIdx]; cur != nil {
			windowsUsed++
			for _, v := range cur {
				sum += v
			}
		}
	}
	return sum, windowsUsed
}

// avgQPS returns the average requests-per-second and the amount of time
// over which the stat was accumulated. Note that these averages are exact,
// not exponentially decayed (there isn't a ton of justification for going
// one way or the other, but not decaying makes the average more stable,
// which is probably better for avoiding rebalance thrashing).
func (rs *replicaStats) avgQPS() (float64, time.Duration) {
	now := timeutil.Unix(0, rs.clock.PhysicalNow())

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.mu.avgQPSForTesting != 0 {
		return rs.mu.avgQPSForTesting, 0
	}

	rs.maybeRotateLocked(now)

	// First accumulate the counts, then divide by the total number of seconds.
	sum, windowsUsed := rs.sumQueriesLocked()
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

	for i := range rs.mu.requests {
		rs.mu.requests[i] = nil
	}
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
	rs.mu.lastRotate = timeutil.Unix(0, rs.clock.PhysicalNow())
	rs.mu.lastReset = rs.mu.lastRotate
}

func (rs *replicaStats) setAvgQPSForTesting(qps float64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.mu.avgQPSForTesting = qps
}
