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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
)

type remoteClockMetrics struct {
	clusterOffsetLowerBound *metric.Gauge
	clusterOffsetUpperBound *metric.Gauge
}

// RemoteClockMonitor keeps track of the most recent measurements of remote
// offsets from this node to connected nodes.
type RemoteClockMonitor struct {
	clock     *hlc.Clock
	offsetTTL time.Duration

	mu struct {
		sync.Mutex
		offsets map[string]RemoteOffset
	}

	metrics  remoteClockMetrics
	registry *metric.Registry
}

// newRemoteClockMonitor returns a monitor with the given server clock.
func newRemoteClockMonitor(clock *hlc.Clock, offsetTTL time.Duration) *RemoteClockMonitor {
	r := RemoteClockMonitor{
		clock:     clock,
		offsetTTL: offsetTTL,
		registry:  metric.NewRegistry(),
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
// It only updates the offset for addr if one of the following cases holds:
// 1. There is no prior offset for that address.
// 2. The old offset for addr was measured long enough ago to be considered
// stale.
// 3. The new offset's error is smaller than the old offset's error.
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
	} else if oldOffset.isStale(r.offsetTTL, r.clock.PhysicalTime()) {
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

// VerifyClockOffset calculates the number of nodes to which the known offset
// is healthy (as defined by RemoteOffset.isHealthy). It returns nil iff more
// than half the known offsets are healthy, and an error otherwise. A non-nil
// return indicates that this node's clock is unreliable, and that the node
// should terminate.
func (r *RemoteClockMonitor) VerifyClockOffset() error {
	// By the contract of the hlc, if the value is 0, then safety checking
	// of the max offset is disabled. However we may still want to
	// propagate the information to a status node.
	if maxOffset := r.clock.MaxOffset(); maxOffset != 0 {
		now := r.clock.PhysicalTime()

		healthyOffsetCount := 0

		r.mu.Lock()
		for addr, offset := range r.mu.offsets {
			if offset.isStale(r.offsetTTL, now) {
				delete(r.mu.offsets, addr)
				continue
			}
			if offset.isHealthy(maxOffset) {
				healthyOffsetCount++
			}
		}
		numClocks := len(r.mu.offsets)
		r.mu.Unlock()

		if numClocks > 0 && healthyOffsetCount <= numClocks/2 {
			return util.Errorf("fewer than half the known nodes are within the maximum offset of %s (%d of %d)", maxOffset, healthyOffsetCount, numClocks)
		}
		if log.V(1) {
			log.Infof("%d of %d nodes are within the maximum offset of %s", healthyOffsetCount, numClocks, maxOffset)
		}
	}

	return nil
}

func (r RemoteOffset) isHealthy(maxOffset time.Duration) bool {
	// Offset may be negative, but Uncertainty is always positive.
	absOffset := r.Offset
	if absOffset < 0 {
		absOffset = -absOffset
	}
	switch {
	case time.Duration(absOffset-r.Uncertainty)*time.Nanosecond > maxOffset:
		// The minimum possible true offset exceeds the maximum offset; definitely
		// unhealthy.
		return false

	case time.Duration(absOffset+r.Uncertainty)*time.Nanosecond < maxOffset:
		// The maximum possible true offset does not exceed the maximum offset;
		// definitely healthy.
		return true

	default:
		// The maximum offset is in the uncertainty window of the measured offset;
		// health is ambiguous. For now, we err on the side of not spuriously
		// killing nodes.
		if log.V(1) {
			log.Infof("uncertain remote offset %s for maximum offset %s, treating as healthy", r, maxOffset)
		}
		return true
	}
}

func (r RemoteOffset) isStale(ttl time.Duration, now time.Time) bool {
	return r.measuredAt().Add(ttl).Before(now)
}

// Registry returns a registry with the metrics tracked by this server, which can be used to
// access its stats or be added to another registry.
func (r *RemoteClockMonitor) Registry() *metric.Registry {
	return r.registry
}
