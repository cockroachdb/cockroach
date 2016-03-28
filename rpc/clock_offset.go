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
// permissions and limitations under the License.
//
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package rpc

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
)

type remoteClockMetrics struct {
	clusterOffsetLowerBound *metric.Gauge
	clusterOffsetUpperBound *metric.Gauge
}

// RemoteClockMonitor keeps track of the most recent measurements of remote
// offsets from this node to connected nodes.
type RemoteClockMonitor struct {
	clock           *hlc.Clock
	monitorInterval time.Duration

	mu struct {
		sync.Mutex
		offsets         map[string]RemoteOffset
		lastMonitoredAt time.Time
	}

	metrics  remoteClockMetrics
	registry *metric.Registry
}

type clusterOffsetInterval struct {
	lowerbound, upperbound time.Duration
}

func (i clusterOffsetInterval) String() string {
	return fmt.Sprintf("[%s, %s]", i.lowerbound, i.upperbound)
}

// majorityIntervalNotFoundError indicates that we could not find a majority
// overlap in our estimate of remote clocks.
type majorityIntervalNotFoundError struct {
	endpoints endpointList
}

func (m *majorityIntervalNotFoundError) Error() string {
	return fmt.Sprintf("unable to determine the true cluster time from remote clock endpoints %v", m.endpoints)
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
	offset  time.Duration // The boundary offset represented by this endpoint.
	endType int           // -1 if lowpoint, +1 if highpoint.
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

// newRemoteClockMonitor returns a monitor with the given server clock.
func newRemoteClockMonitor(clock *hlc.Clock) *RemoteClockMonitor {
	r := RemoteClockMonitor{
		clock:           clock,
		monitorInterval: defaultHeartbeatInterval * 10,
		registry:        metric.NewRegistry(),
	}
	r.mu.offsets = make(map[string]RemoteOffset)
	r.metrics = remoteClockMetrics{
		clusterOffsetLowerBound: r.registry.Gauge("lower-bound-nanos"),
		clusterOffsetUpperBound: r.registry.Gauge("upper-bound-nanos"),
	}
	return &r
}

// UpdateOffset is a thread-safe way to update the remote clock measurements.
//
// It only updates the offset for addr if one the following three cases holds:
// 1. There is no prior offset for that address.
// 2. The old offset for addr was measured before r.mu.lastMonitoredAt. We never
// use values during monitoring that are older than r.mu.lastMonitoredAt.
// 3. The new offset's error is smaller than the old offset's error.
//
// The third case allows the monitor to use the most precise clock reading of
// the remote addr during the next findOffsetInterval() invocation. We may
// measure the remote clock several times before we next calculate the cluster
// offset. When we do the measurement, we want to use the reading with the
// smallest error. Because r.monitorInterval > heartbeatInterval, this gives us
// several chances to accurately read the remote clock. Note that we don't want
// r.monitorInterval to be too large, else we might end up relying on old
// information.
func (r *RemoteClockMonitor) UpdateOffset(addr string, offset RemoteOffset) {
	emptyOffset := offset == RemoteOffset{}

	r.mu.Lock()
	defer r.mu.Unlock()

	if oldOffset, ok := r.mu.offsets[addr]; !ok {
		// We don't have a measurement - if the incoming measurement is not empty,
		// set it.
		if !emptyOffset {
			r.mu.offsets[addr] = offset
		}
	} else if oldOffset.measuredAt().Before(r.mu.lastMonitoredAt) {
		// We have a measurement but it's old - if the incoming measurement is not empty,
		// set it, otherwise delete the old measurement.
		if !emptyOffset {
			r.mu.offsets[addr] = offset
		} else {
			delete(r.mu.offsets, addr)
		}
	} else if offset.Uncertainty < oldOffset.Uncertainty {
		// We have a measurement but its uncertainty is greater than that of the
		// incoming measurement - if the incoming measurement is not empty, set it.
		if !emptyOffset {
			r.mu.offsets[addr] = offset
		}
	}

	if log.V(2) {
		log.Infof("update offset: %s %v", addr, r.mu.offsets[addr])
	}
}

// MonitorRemoteOffsets periodically checks that the offset of this server's
// clock from the true cluster time is within MaxOffset. If the offset exceeds
// MaxOffset, then this method will trigger a fatal error, causing the node to
// suicide.
func (r *RemoteClockMonitor) MonitorRemoteOffsets(stopper *stop.Stopper) error {
	if log.V(1) {
		log.Infof("monitoring cluster offset every %s", r.monitorInterval)
	}
	var monitorTimer util.Timer
	defer monitorTimer.Stop()
	for {
		monitorTimer.Reset(r.monitorInterval)
		select {
		case <-stopper.ShouldStop():
			return nil
		case <-monitorTimer.C:
			monitorTimer.Read = true
			offsetInterval, err := r.findOffsetInterval()
			// By the contract of the hlc, if the value is 0, then safety checking
			// of the max offset is disabled. However we may still want to
			// propagate the information to a status node.
			// TODO(embark): once there is a framework for collecting timeseries
			// data about the db, propagate the offset status to that.
			if maxOffset := r.clock.MaxOffset(); maxOffset != 0 {
				if err != nil {
					return util.Errorf("clock offset could not be determined: %s", err)
				}

				if !isHealthyOffsetInterval(offsetInterval, maxOffset) {
					return util.Errorf(
						"clock offset is in interval: %s, which indicates that the true offset is greater than the max offset: %s",
						offsetInterval, maxOffset,
					)
				}
				if log.V(1) {
					log.Infof("healthy cluster offset: %s", offsetInterval)
				}
			}

			r.metrics.clusterOffsetLowerBound.Update(int64(offsetInterval.lowerbound))
			r.metrics.clusterOffsetUpperBound.Update(int64(offsetInterval.upperbound))

			r.mu.Lock()
			r.mu.lastMonitoredAt = r.clock.PhysicalTime()
			r.mu.Unlock()
		}
	}
}

// isHealthyOffsetInterval returns true if the clusterOffsetInterval indicates
// that the node's offset is within maxOffset, else false. For example, if the
// offset interval is [-20, -11] and the maxOffset is 10 nanoseconds, then the
// clock offset must be too great, because no point in the interval is within
// the maxOffset.
func isHealthyOffsetInterval(i clusterOffsetInterval, maxOffset time.Duration) bool {
	return i.lowerbound <= maxOffset && i.upperbound >= -maxOffset
}

// The routine that measures this node's probable offset from the rest of the
// cluster. This offset is measured as a clusterOffsetInterval. For example,
// the output might be [-5, 10], which would indicate that this node's offset
// is likely between -5 and 10 nanoseconds from the average clock of the
// cluster.
//
// The intersection algorithm used here is documented at:
// http://infolab.stanford.edu/pub/cstr/reports/csl/tr/83/247/CSL-TR-83-247.pdf,
// commonly known as Marzullo's algorithm. If a remote clock is correct, its
// offset interval should encompass this clock's offset from the cluster time
// (see buildEndpointList()). If the majority of remote clock are correct, then
// their intervals should overlap over some region, which should include the
// true offset from the cluster time. This algorithm returns this region.
//
// If an interval cannot be found, an error is returned, indicating that
// a majority of remote node offset intervals do not overlap the cluster time.
func (r *RemoteClockMonitor) findOffsetInterval() (clusterOffsetInterval, error) {
	endpoints := r.buildEndpointList()
	numClocks := len(endpoints) / 2
	if log.V(1) {
		log.Infof("finding offset interval for monitor interval: %s, numOffsets %d",
			r.monitorInterval, numClocks)
	}
	if numClocks == 0 {
		return clusterOffsetInterval{}, nil
	}
	sort.Sort(endpoints)

	best := 0
	count := 0
	var interval clusterOffsetInterval

	// Find the interval which the most offset intervals overlap.
	for i, endpoint := range endpoints {
		count -= endpoint.endType
		if count > best {
			best = count
			interval.lowerbound = endpoint.offset
			// Note the endType of the last endpoint is +1, so count < best.
			// Thus this code will never run when i = len(endpoint)-1.
			interval.upperbound = endpoints[i+1].offset
		}
	}

	// Indicates that fewer than a majority of connected remote clocks seem to
	// encompass the central offset from the cluster, an error condition.
	if best <= numClocks/2 {
		return interval, &majorityIntervalNotFoundError{endpoints: endpoints}
	}

	// A majority of offset intervals overlap at this interval, which should
	// contain the true cluster offset.
	return interval, nil
}

// buildEndpointList() takes all the RemoteOffsets that are in the monitor, and
// turns these offsets into intervals which should encompass this node's true
// offset from the cluster time. It returns a list including the two endpoints
// of each interval.
//
// As a side effect, any RemoteOffsets that haven't been
// updated since the last monitoring are removed. (Side effects are nasty, but
// prevent us from running through the list an extra time under a lock).
//
// A RemoteOffset r is represented by this interval:
// [r.Offset - r.Uncertainty, r.Offset + r.Uncertainty]
func (r *RemoteClockMonitor) buildEndpointList() endpointList {
	r.mu.Lock()
	defer r.mu.Unlock()

	endpoints := make(endpointList, 0, len(r.mu.offsets)*2)
	for addr, offset := range r.mu.offsets {
		// Remove anything that hasn't been updated since the last time offest
		// was measured. This indicates that we no longer have a connection to
		// that addr.
		if offset.measuredAt().Before(r.mu.lastMonitoredAt) {
			delete(r.mu.offsets, addr)
			continue
		}

		lowpoint := endpoint{
			offset:  time.Duration(offset.Offset-offset.Uncertainty) * time.Nanosecond,
			endType: -1,
		}
		highpoint := endpoint{
			offset:  time.Duration(offset.Offset+offset.Uncertainty) * time.Nanosecond,
			endType: +1,
		}
		endpoints = append(endpoints, lowpoint, highpoint)
	}
	return endpoints
}

// Registry returns a registry with the metrics tracked by this server, which can be used to
// access its stats or be added to another registry.
func (r *RemoteClockMonitor) Registry() *metric.Registry {
	return r.registry
}
