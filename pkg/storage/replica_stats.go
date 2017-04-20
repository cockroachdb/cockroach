// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	rotateInterval = 5 * time.Minute
	decayFactor    = 0.8
)

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
	// as determined by `rotateInterval`.
	//
	// We could alternatively use a forward decay approach here, but it would
	// require more memory than this slightly less precise windowing method:
	//   http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
	mu struct {
		syncutil.Mutex
		idx        int
		requests   [5]perLocalityCounts
		lastRotate time.Time
		lastReset  time.Time
	}
}

func newReplicaStats(clock *hlc.Clock, getNodeLocality localityOracle) *replicaStats {
	rs := &replicaStats{
		clock:           clock,
		getNodeLocality: getNodeLocality,
	}
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
	rs.mu.lastRotate = time.Unix(0, rs.clock.PhysicalNow())
	rs.mu.lastReset = rs.mu.lastRotate
	return rs
}

func (rs *replicaStats) record(nodeID roachpb.NodeID) {
	locality := rs.getNodeLocality(nodeID)
	now := time.Unix(0, rs.clock.PhysicalNow())

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.maybeRotateLocked(now)
	rs.mu.requests[rs.mu.idx][locality]++
}

func (rs *replicaStats) maybeRotateLocked(now time.Time) {
	if now.Sub(rs.mu.lastRotate) >= rotateInterval {
		rs.rotateLocked()
		rs.mu.lastRotate = now
	}
}

func (rs *replicaStats) rotateLocked() {
	rs.mu.idx = (rs.mu.idx + 1) % len(rs.mu.requests)
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
}

// getRequestCounts returns the current per-locality request counts and the
// amount of time over which the counts were accumulated.
func (rs *replicaStats) getRequestCounts() (perLocalityCounts, time.Duration) {
	now := time.Unix(0, rs.clock.PhysicalNow())

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.maybeRotateLocked(now)

	// Use the fraction of time since the last rotation as a smoothing factor to
	// avoid jarring changes in request count immediately before/after a rotation.
	fractionOfRotation := float64(now.Sub(rs.mu.lastRotate)) / float64(rotateInterval)

	counts := make(perLocalityCounts)
	for i := range rs.mu.requests {
		// We have to add len(rs.mu.requests) to the numerator to avoid getting a
		// negative result from the modulus operation when rs.mu.idx is small.
		requestsIdx := (rs.mu.idx + len(rs.mu.requests) - i) % len(rs.mu.requests)
		if cur := rs.mu.requests[requestsIdx]; cur != nil {
			for k, v := range cur {
				counts[k] += v * math.Pow(decayFactor, float64(i)+fractionOfRotation)
			}
		}
	}

	return counts, now.Sub(rs.mu.lastReset)
}

func (rs *replicaStats) resetRequestCounts() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	for i := range rs.mu.requests {
		rs.mu.requests[i] = nil
	}
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
	rs.mu.lastRotate = time.Unix(0, rs.clock.PhysicalNow())
	rs.mu.lastReset = rs.mu.lastRotate
}
