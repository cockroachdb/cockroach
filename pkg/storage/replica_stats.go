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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	numWindows     = 5
	rotateInterval = 5 * time.Minute
	decayFactor    = 0.75
)

type localityOracle func(roachpb.NodeID) string

// perLocalityCounts maps from the string representation of a locality to count.
type perLocalityCounts map[string]float64

// replicaStats maintains statistics about the work done by a replica. Its
// initial use is tracking the number of requests received from each
// cluster locality in order to inform lease transfer decisions.
type replicaStats struct {
	getNodeLocality localityOracle

	// We could alternatively use a forward decay approach here, but it would
	// require more memory than this slightly less precise windowing method:
	//   http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
	mu struct {
		syncutil.Mutex
		idx        int
		requests   []perLocalityCounts
		lastRotate time.Time
		lastReset  time.Time
	}
}

func newReplicaStats(getNodeLocality localityOracle) *replicaStats {
	rs := &replicaStats{
		getNodeLocality: getNodeLocality,
	}
	rs.mu.requests = make([]perLocalityCounts, numWindows)
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
	rs.mu.lastRotate = timeutil.Now()
	rs.mu.lastReset = timeutil.Now()
	return rs
}

func (rs *replicaStats) record(nodeID roachpb.NodeID) {
	locality := rs.getNodeLocality(nodeID)
	now := timeutil.Now()

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if now.Sub(rs.mu.lastRotate) > rotateInterval {
		rs.rotateLocked()
		rs.mu.lastRotate = now
	}
	rs.mu.requests[rs.mu.idx][locality]++
}

func (rs *replicaStats) rotateLocked() {
	rs.mu.idx = (rs.mu.idx + 1) % numWindows
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
}

// getRequestCounts returns the current per-locality request counts and the
// amount of time over which the counts were accumulated.
func (rs *replicaStats) getRequestCounts() (perLocalityCounts, time.Duration) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	dur := timeutil.Now().Sub(rs.mu.lastReset)

	counts := make(perLocalityCounts)

	for i := 0; i < numWindows; i++ {
		if cur := rs.mu.requests[(rs.mu.idx+numWindows-i)%numWindows]; cur != nil {
			for k, v := range cur {
				counts[k] += v * math.Pow(decayFactor, float64(i))
			}
		}
	}

	return counts, dur
}

func (rs *replicaStats) resetRequestCounts() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	for i := 0; i < numWindows; i++ {
		rs.mu.requests[i] = nil
	}
	rs.mu.requests[rs.mu.idx] = make(perLocalityCounts)
	rs.mu.lastRotate = timeutil.Now()
	rs.mu.lastReset = timeutil.Now()
}
