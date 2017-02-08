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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type localityOracle func(roachpb.NodeID) string

// perLocalityCounts maps from the string representation of a locality to count.
type perLocalityCounts map[string]int64

// replicaStats maintains statistics about the work done by a replica. Its
// initial use is tracking the number of requests received from each
// cluster locality in order to inform lease transfer decisions.
type replicaStats struct {
	getNodeLocality localityOracle

	mu struct {
		syncutil.Mutex
		// TODO(a-robinson): Use a form of exponential decay on the counts rather
		// than hard resets back to zero?
		requests  perLocalityCounts
		lastReset time.Time
	}
}

func newReplicaStats(getNodeLocality localityOracle) *replicaStats {
	rs := &replicaStats{
		getNodeLocality: getNodeLocality,
	}
	rs.mu.requests = make(perLocalityCounts)
	rs.mu.lastReset = timeutil.Now()
	return rs
}

func (rs *replicaStats) record(nodeID roachpb.NodeID) {
	locality := rs.getNodeLocality(nodeID)

	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.mu.requests[locality]++
}

// getRequestCounts returns the current per-locality request counts and the
// amount of time over which the counts were accumulated.
func (rs *replicaStats) getRequestCounts() (perLocalityCounts, time.Duration) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	dur := timeutil.Now().Sub(rs.mu.lastReset)

	counts := make(perLocalityCounts)
	for k := range rs.mu.requests {
		counts[k] = rs.mu.requests[k]
	}

	return counts, dur
}

func (rs *replicaStats) resetRequestCounts() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.mu.requests = make(perLocalityCounts)
	rs.mu.lastReset = timeutil.Now()
}
