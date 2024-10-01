// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicastats

import (
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
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
	settings.SystemOnly,
	"kv.replica_stats.addsst_request_size_factor",
	"the divisor that is applied to addsstable request sizes, then recorded in a leaseholders QPS; 0 means all requests are treated as cost 1",
	// The default value of 50,000 was chosen as the default divisor, following manual testing that
	// is discussed in this pull request: #76252. Every additional 50,000 AddSSTable bytes will
	// increase accounted QPS by 1. Typically AddSSTableRequests are ~1mb in size, accounted as 20
	// QPS.
	50000,
	settings.WithPublic)

// LocalityOracle provides a mapping between a node ID and it's corresponding
// locality.
type LocalityOracle func(roachpb.NodeID) string

// PerLocalityCounts maps from the string representation of a locality to count.
type PerLocalityCounts map[string]float64

// RatedSummary returns a rated summary representing a snapshot of the current
// replicastats state.
type RatedSummary struct {
	QPS            float64
	LocalityCounts PerLocalityCounts
	Duration       time.Duration
}

// ReplicaStats maintains statistics about the work done by a replica. Its
// initial use is tracking the number of requests received from each
// cluster locality in order to inform lease transfer decisions.
type ReplicaStats struct {
	getNodeLocality LocalityOracle

	// We use a set of time windows in order to age out old stats without having
	// to do hard resets. The `requests` array is a circular buffer of the last
	// N windows of stats. We rotate through the circular buffer every so often
	// as determined by `replStatsRotateInterval`.
	//
	// We could alternatively use a forward decay approach here, but it would
	// require more memory than this slightly less precise windowing method:
	//   http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
	idx int
	// Each window record is reused internally by flipping an active field and
	// clearing the fields.
	records    [6]replicaStatsRecord
	lastRotate time.Time

	// Testing only.
	avgRateForTesting float64
}

type replicaStatsRecord struct {
	localityCounts *PerLocalityCounts
	sum            float64
	active         bool
}

func (rsr *replicaStatsRecord) reset() {
	rsr.sum = 0

	if rsr.localityCounts == nil || len(*rsr.localityCounts) == 0 {
		return
	}

	// Reuse the existing map to avoid heap allocations. Additionally, Instead
	// of deleting the locality entries, it is likely that the same entries
	// will occur again in the next use of this window. Zero out any existing
	// values to avoid churning memory. We could also delete elements that
	// haven't been seen in a while, however it makes no difference as the
	// memory wouldn't be released (https://github.com/golang/go/issues/20135).
	// To protect against a memory leak, where many unused localities
	// accumulate, create a new map if there are any unused localities. This is
	// important when there are many quiesced ranges, that may have previously
	// had just one request.
	for _, v := range *rsr.localityCounts {
		if v == 0 {
			rsr.localityCounts = &PerLocalityCounts{}
			return
		}
	}

	for k := range *rsr.localityCounts {
		(*rsr.localityCounts)[k] = 0
	}

}

// activate sets the active indicator on the record. It assumes that the record
// is empty and does not clear any fields.
func (rsr *replicaStatsRecord) activate() {
	rsr.active = true
}

// deactivate sets the active indicator on a record to false. It clears the
// data, emptying the record.
func (rsr *replicaStatsRecord) deactivate() {
	rsr.reset()
	rsr.active = false
}

// mergeReplicaStatsRecords combines the record passed in with the receiver.
func (rsr *replicaStatsRecord) mergeReplicaStatsRecords(other replicaStatsRecord) {
	// If the other record is inactive, then there is nothing to merge into the
	// resulting record. Otherwise, regardless of whether the resulting record
	// was originally active or inactive, we should merge the stats in and
	// ensure it is set to active.
	if !other.active {
		return
	}

	rsr.sum += other.sum
	rsr.activate()

	// When the other record's locality counts or calling record's locality
	// counts are not set, there is nothing to merge togther.
	if other.localityCounts == nil || rsr.localityCounts == nil {
		return
	}

	for locality, count := range *other.localityCounts {
		(*rsr.localityCounts)[locality] += count
	}
}

func (rsr *replicaStatsRecord) split(other *replicaStatsRecord) {
	rsr.sum = rsr.sum / 2.0
	other.sum = rsr.sum

	// When the calling record's locality counts are not set, there is nothing
	// to split onto the other record's localityCounts.
	if rsr.localityCounts == nil {
		return
	}

	// Halve the calling record's locality counts, so that even if the other
	// record won't be updated, the calling record has at least halved.
	for locality, count := range *rsr.localityCounts {
		(*rsr.localityCounts)[locality] = count / 2.0
	}

	// When the other record's locality counts are not set, there is nothing to
	// split into.
	if other.localityCounts == nil {
		return
	}

	for locality := range *rsr.localityCounts {
		(*other.localityCounts)[locality] = (*rsr.localityCounts)[locality]
	}
}

// NewReplicaStats constructs a new ReplicaStats tracker.
func NewReplicaStats(now time.Time, getNodeLocality LocalityOracle) *ReplicaStats {
	rs := &ReplicaStats{
		getNodeLocality: getNodeLocality,
	}
	// Only create the locality counts when a locality oracle is given.
	if getNodeLocality != nil {
		for i := range rs.records {
			rs.records[i].localityCounts = &PerLocalityCounts{}
		}
	}
	// Set the first record to active. All other records will be initially
	// inactive and empty.
	rs.records[rs.idx].activate()
	rs.lastRotate = now
	return rs
}

// MergeRequestCounts joins the current ReplicaStats object with other, for the
// purposes of merging a range.
func (rs *ReplicaStats) MergeRequestCounts(other *ReplicaStats) {
	n := len(rs.records)
	for i := range other.records {
		rsIdx := (rs.idx + n - i) % n
		otherIdx := (other.idx + n - i) % n

		rs.records[rsIdx].mergeReplicaStatsRecords(other.records[otherIdx])
		// We merged the other records counts into rs, deactivate the other
		// record to avoid double counting with repeated calls to merge.
		other.records[otherIdx].deactivate()
	}

	// Update the last rotate time to be the lesser of the two, so that a
	// rotation occurs as early as possible.
	if rs.lastRotate.After(other.lastRotate) {
		rs.lastRotate = other.lastRotate
	}

}

// SplitRequestCounts divides the current ReplicaStats object in two for the
// purposes of splitting a range. It modifies itself to have half its requests
// and the provided other to have the other half.
//
// Note that assuming a 50/50 split is optimistic, but it's much better than
// resetting both sides upon a split.
func (rs *ReplicaStats) SplitRequestCounts(other *ReplicaStats) {
	other.idx = rs.idx
	other.lastRotate = rs.lastRotate

	for i := range rs.records {
		// When the lhs isn't active, set the rhs to inactive as well.
		if !rs.records[i].active {
			other.records[i].deactivate()
			continue
		}
		// Otherwise, activate the rhs record and split the request count
		// between the two.
		other.records[i].activate()
		rs.records[i].split(&other.records[i])
	}
}

// RecordCount records the given count against the given node ID.
func (rs *ReplicaStats) RecordCount(now time.Time, count float64, nodeID roachpb.NodeID) {
	rs.maybeRotate(now)

	record := &rs.records[rs.idx]
	record.sum += count

	if rs.getNodeLocality != nil {
		(*record.localityCounts)[rs.getNodeLocality(nodeID)] += count
	}

}

func (rs *ReplicaStats) maybeRotate(now time.Time) {
	if now.Sub(rs.lastRotate) >= replStatsRotateInterval {
		rs.rotate()
		rs.lastRotate = now
	}
}

func (rs *ReplicaStats) rotate() {
	rs.idx = (rs.idx + 1) % len(rs.records)
	// Reset the next idx and set the record to active.
	rs.records[rs.idx].reset()
	rs.records[rs.idx].activate()
}

// PerLocalityDecayingRate returns the per-locality counts-per-second and the
// amount of time over which the stats were accumulated. Note that the replica stats
// stats are exponentially decayed such that newer requests are weighted more
// heavily than older requests.
func (rs *ReplicaStats) PerLocalityDecayingRate(now time.Time) PerLocalityCounts {
	rs.maybeRotate(now)
	counts := make(PerLocalityCounts)

	// When the locality oracle isn't set, return early as there could not be
	// any locality specific values.
	if rs.getNodeLocality == nil {
		return counts
	}

	// Use the fraction of time since the last rotation as a smoothing factor to
	// avoid jarring changes in request count immediately before/after a rotation.
	timeSinceRotate := now.Sub(rs.lastRotate)
	fractionOfRotation := float64(timeSinceRotate) / float64(replStatsRotateInterval)

	var duration time.Duration
	for i := range rs.records {
		// We have to add len(rs.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.idx is small.
		requestsIdx := (rs.idx + len(rs.records) - i) % len(rs.records)
		if cur := rs.records[requestsIdx]; cur.active {
			decay := math.Pow(decayFactor, float64(i)+fractionOfRotation)
			if i == 0 {
				duration += time.Duration(float64(timeSinceRotate) * decay)
			} else {
				duration += time.Duration(float64(replStatsRotateInterval) * decay)
			}
			for k, v := range *cur.localityCounts {
				counts[k] += v * decay
			}
		}
	}

	if duration.Seconds() > 0 {
		for k := range counts {
			counts[k] = counts[k] / duration.Seconds()
		}
	}
	return counts
}

// Sum returns the sum of all queries currently recorded.
func (rs *ReplicaStats) Sum() (float64, int) {
	var sum float64
	var windowsUsed int
	for i := range rs.records {
		// We have to add len(rs.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.idx is small.
		requestsIdx := (rs.idx + len(rs.records) - i) % len(rs.records)
		if cur := rs.records[requestsIdx]; cur.active {
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
func (rs *ReplicaStats) AverageRatePerSecond(now time.Time) (float64, time.Duration) {
	if rs.avgRateForTesting != 0 {
		return rs.avgRateForTesting, MinStatsDuration
	}

	rs.maybeRotate(now)

	// First accumulate the counts, then divide by the total number of seconds.
	sum, windowsUsed := rs.Sum()
	if windowsUsed <= 0 {
		return 0, 0
	}
	duration := now.Sub(rs.lastRotate) + time.Duration(windowsUsed-1)*replStatsRotateInterval
	if duration == 0 {
		return 0, 0
	}
	return sum / duration.Seconds(), duration
}

// ResetRequestCounts resets the underlying request counts.
func (rs *ReplicaStats) ResetRequestCounts(now time.Time) {
	// Reset the individual records and set their state to inactive.
	for i := range rs.records {
		rs.records[i].deactivate()
	}
	// Update the current idx record to be active.
	rs.records[rs.idx].activate()
	rs.lastRotate = now
}

// SnapshotRatedSummary returns a RatedSummary representing a snapshot of the
// current replica stats state, summarized by arithmetic mean count,
// per-locality count and duration recorded over.
func (rs *ReplicaStats) SnapshotRatedSummary(now time.Time) *RatedSummary {
	qps, duration := rs.AverageRatePerSecond(now)
	localityCounts := rs.PerLocalityDecayingRate(now)
	return &RatedSummary{
		QPS:            qps,
		LocalityCounts: localityCounts,
		Duration:       duration,
	}
}

// SetMeanRateForTesting is a testing helper to directly sey the mean rate.
func (rs *ReplicaStats) SetMeanRateForTesting(rate float64) {
	rs.avgRateForTesting = rate
}
