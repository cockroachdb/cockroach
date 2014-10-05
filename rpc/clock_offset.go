// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package rpc

import (
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
)

// RemoteClockMonitor keeps track of the most recent measurements of remote
// offsets from this node to connected nodes.
type RemoteClockMonitor struct {
	offsets  map[string]RemoteOffset // Maps remote string addr to offset.
	maxDrift time.Duration           // The max drift of a node in the cluster.
	mu       sync.Mutex
}

// ClusterOffsetInterval is the best interval we can construct to estimate this
// node's offset from the cluster.
type ClusterOffsetInterval struct {
	Lowerbound int64 // The lowerbound on the offset in nanoseconds.
	Upperbound int64 // The upperbound on the offset in nanoseconds.
}

// endpoint represents an endpoint in the interval estimation of a single
// remote clock. It could be either the lowpoint or the highpoint of the
// interval.
//
// For example, if the remote clock offset bounds are [-5, 10], then it
// will be converted into two endpoints:
// endpoint{offset: -5, endType: -1}
// endpoint{offset: 10, endType: +1}
type endpoint struct {
	offset  int64 // The boundary offset represented by this endpoint.
	endType int   // -1 if lowpoint, +1 if highpoint.
}

// endpointList is a slice of endpoints, sorted by endpoint offset.
type endpointList []endpoint

// Implementation of sort.Interface.
func (l endpointList) Len() int {
	return len(l)
}
func (l endpointList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l endpointList) Less(i, j int) bool {
	if l[i].offset == l[j].offset {
		return l[i].endType < l[j].endType
	}
	return l[i].offset < l[j].offset
}

// newRemoteClockMonitor returns a monitor with the value of maxDrift, which
// should be the maximum drift of all nodes in the server's cluster.
func newRemoteClockMonitor(maxDrift time.Duration) *RemoteClockMonitor {
	return &RemoteClockMonitor{
		offsets:  make(map[string]RemoteOffset),
		maxDrift: maxDrift,
	}
}

// UpdateOffset is a thread-safe way to update the remote clock measurements.
// It only updates if the new measurement is no older than the value currently
// in the map.
func (r *RemoteClockMonitor) UpdateOffset(addr string, offset RemoteOffset) {
	r.mu.Lock()
	defer r.mu.Unlock()
	oldOffset, ok := r.offsets[addr]
	if !ok {
		r.offsets[addr] = offset
	}

	if offset.MeasuredAt >= oldOffset.MeasuredAt {
		r.offsets[addr] = offset
	}
}

// The routine that measures this node's probable offset from the rest of the
// cluster. This offset is measured as a ClusterOffsetInterval. For example,
// the output might be [-5, 10], which would indicate that this node's offset
// is likely between -5 and 10 nanoseconds from the average clock of the
// cluster.
//
// The intersection algorithm used here is documented at:
// https://www.eecis.udel.edu/~mills/ntp/html/select.html
// If a remote clock is correct, its offset interval should encompass this
// clock's offset from the cluster time (see buildEndpointList()).
// If the majority of remote clock are correct, then their intevals should
// overlap over some region, which should include the true offset from the
// cluster time. This algorithm ought to return this region.
func (r *RemoteClockMonitor) findOffsetInterval() ClusterOffsetInterval {
	log.Infof("in offsets: %v", r.offsets)
	endpoints := r.buildEndpointList()
	sort.Sort(endpoints)
	log.Infof("endpoints: %v", endpoints)
	numClocks := len(endpoints) / 2
	// falsechimers are remote clocks which appear to have too great an offset
	// from the cluster time. Their offset measurement is probably misleading,
	// and should be ignored.
	falsechimers := 0
	endcount := 0
	var lowerBound int64
	var upperBound int64
	for falsechimers = 0; falsechimers < numClocks/2; falsechimers++ {
		// Find lowerbound of the interval.
		endcount = 0
		for _, endpoint := range endpoints {
			endcount -= endpoint.endType
			lowerBound = endpoint.offset
			if endcount >= numClocks-falsechimers {
				break
			}
		}

		// Find upperbound of the interval.
		endcount = 0
		for i := len(endpoints) - 1; i >= 0; i-- {
			endpoint := endpoints[i]
			endcount += endpoint.endType
			upperBound = endpoint.offset
			if endcount >= numClocks-falsechimers {
				break
			}
		}

		if lowerBound < upperBound {
			return ClusterOffsetInterval{
				Lowerbound: lowerBound,
				Upperbound: upperBound,
			}
		}
	}
	// TODO(embark): If execution gets to here, that indicates that fewer than
	// a majority of connected remote clocks seem to encompass the central
	// time of the cluster. It's not clear what should be done in this case.
	return ClusterOffsetInterval{}
}

// buildEndpointList() takes all the RemoteOffsets that are in the monitor, and
// turns these offsets into intervals which should encompass this node's true
// offset from the cluster time. It returns a list including the two endpoints
// of each interval.
//
// A RemoteOffset r is represented by this interval:
// [r.Offset - r.Error - MaxDrift, r.Offset + r.Error + MaxDrift],
// where MaxDrift is the furthest a node's clock can deviate from the cluster
// time. While the offset between this node and the remote time is actually
// within [r.Offset - r.Error, r.Offset + r.Error], we also must expand the
// interval by MaxDrift. This accounts for the fact that the remote clock is at
// most MaxDrift distance from the cluster time. Thus the expanded interval
// ought to contain this node's offset from the true cluster time, not just the
// offset from the remote clock's time.
func (r *RemoteClockMonitor) buildEndpointList() endpointList {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO(embark) Figure out when we need to remove entries from r.offsets.
	endpoints := make(endpointList, 0, len(r.offsets)*2)
	for _, o := range r.offsets {
		lowpoint := endpoint{
			offset:  o.Offset - o.Error - r.maxDrift.Nanoseconds(),
			endType: -1,
		}
		highpoint := endpoint{
			offset:  o.Offset + o.Error + r.maxDrift.Nanoseconds(),
			endType: +1,
		}
		endpoints = append(endpoints, lowpoint, highpoint)
	}
	return endpoints
}
