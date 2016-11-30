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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// Package hlc implements the Hybrid Logical Clock outlined in
// "Logical Physical Clocks and Consistent Snapshots in Globally
// Distributed Databases", available online at
// http://www.cse.buffalo.edu/tech-reports/2014-04.pdf.
package hlc

import (
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Clock is a hybrid logical clock. Objects of this
// type model causality while maintaining a relation
// to physical time. Roughly speaking, timestamps
// consist of the largest wall clock time among all
// events, and a logical clock that ticks whenever
// an event happens in the future of the local physical
// clock.
// The data structure is thread safe and thus can safely
// be shared by multiple goroutines.
//
// See NewClock for details.
type Clock struct {
	physicalClock func() int64

	// The maximal offset of the HLC's wall time from the underlying physical
	// clock. A well-chosen value is large enough to ignore a reasonable amount
	// of clock skew but will prevent ill-configured nodes from dramatically
	// skewing the wall time of the clock into the future.
	//
	// RPC heartbeats compare detected clock skews against this value to protect
	// data consistency.
	//
	// TODO(tamird): make this dynamic in the distant future.
	maxOffset time.Duration

	mu struct {
		syncutil.Mutex
		timestamp Timestamp

		// monotonicityErrorsCount indicate how often this clock was
		// observed to jump backwards.
		monotonicityErrorsCount int32
		// lastPhysicalTime reports the last measured physical time. This
		// is used to detect clock jumps.
		lastPhysicalTime int64
	}
}

// ManualClock is a convenience type to facilitate
// creating a hybrid logical clock whose physical clock
// is manually controlled. ManualClock is thread safe.
type ManualClock struct {
	nanos int64
}

// NewManualClock returns a new instance, initialized with
// specified timestamp.
func NewManualClock(nanos int64) *ManualClock {
	if nanos == 0 {
		panic("zero clock is forbidden")
	}
	return &ManualClock{nanos: nanos}
}

// UnixNano returns the underlying manual clock's timestamp.
func (m *ManualClock) UnixNano() int64 {
	return atomic.LoadInt64(&m.nanos)
}

// Increment atomically increments the manual clock's timestamp.
func (m *ManualClock) Increment(incr int64) {
	atomic.AddInt64(&m.nanos, incr)
}

// Set atomically sets the manual clock's timestamp.
func (m *ManualClock) Set(nanos int64) {
	atomic.StoreInt64(&m.nanos, nanos)
}

// UnixNano returns the local machine's physical nanosecond
// unix epoch timestamp as a convenience to create a HLC via
// c := hlc.NewClock(hlc.UnixNano).
func UnixNano() int64 {
	return timeutil.Now().UnixNano()
}

// NewClock creates a new hybrid logical clock associated
// with the given physical clock, initializing both wall time
// and logical time with zero.
//
// The physical clock is typically given by the wall time
// of the local machine in unix epoch nanoseconds, using
// hlc.UnixNano. This is not a requirement.
func NewClock(physicalClock func() int64, maxOffset time.Duration) *Clock {
	return &Clock{
		physicalClock: physicalClock,
		maxOffset:     maxOffset,
	}
}

// MaxOffset returns the maximal clock offset to any node in the cluster.
//
// A value of 0 means offset checking is disabled.
func (c *Clock) MaxOffset() time.Duration {
	return c.maxOffset
}

// getPhysicalClockLocked returns the current physical clock and checks for
// time jumps.
func (c *Clock) getPhysicalClockLocked() int64 {
	physicalTime := c.physicalClock()

	if c.mu.lastPhysicalTime != 0 {
		interval := time.Duration(c.mu.lastPhysicalTime - physicalTime)
		if 10*interval > c.maxOffset {
			c.mu.monotonicityErrorsCount++
			log.Warningf(context.TODO(), "backward time jump detected (%s)", interval)
		}
	}

	c.mu.lastPhysicalTime = physicalTime
	return physicalTime
}

// Now returns a timestamp associated with an event from
// the local machine that may be sent to other members
// of the distributed network. This is the counterpart
// of Update, which is passed a timestamp received from
// another member of the distributed network.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	if physicalTime := c.getPhysicalClockLocked(); c.mu.timestamp.WallTime >= physicalTime {
		// The wall time is ahead, so the logical clock ticks.
		c.mu.timestamp.Logical++
	} else {
		// Use the physical clock, and reset the logical one.
		c.mu.timestamp.WallTime = physicalTime
		c.mu.timestamp.Logical = 0
	}
	return c.mu.timestamp
}

// CheckOffset checks the timestamp against the clock and returns an error if
// the timestamp is ahead of the clock by more than the clock's max offset and
// the clock's max offset is not zero.
func (c *Clock) CheckOffset(ts Timestamp) error {
	c.mu.Lock()
	physicalTime := c.getPhysicalClockLocked()
	c.mu.Unlock()
	return c.checkOffset(ts, physicalTime)
}

func (c *Clock) checkOffset(ts Timestamp, physicalTime int64) error {
	if c.maxOffset == 0 {
		return nil
	}
	if offset := ts.WallTime - physicalTime; offset > c.maxOffset.Nanoseconds() {
		return errors.Errorf("offset %s exceeds maximum %s", time.Duration(offset), c.maxOffset)
	}
	return nil
}

// Update takes a hybrid timestamp, usually originating from
// an event received from another member of a distributed
// system. The clock is updated and the hybrid timestamp
// associated to the receipt of the event returned.
// An error may only occur if offset checking is active and
// the remote timestamp was rejected due to clock offset,
// in which case the timestamp of the clock will not have been
// altered.
// To timestamp events of local origin, use Now instead.
func (c *Clock) Update(ts Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	physicalTime := c.getPhysicalClockLocked()

	if physicalTime > c.mu.timestamp.WallTime && physicalTime > ts.WallTime {
		// Our physical clock is ahead of both wall times. It is used
		// as the new wall time and the logical clock is reset.
		c.mu.timestamp.WallTime = physicalTime
		c.mu.timestamp.Logical = 0
		return c.mu.timestamp
	}

	// In the remaining cases, our physical clock plays no role
	// as it is behind the local and remote wall times. Instead,
	// the logical clock comes into play.
	if ts.WallTime > c.mu.timestamp.WallTime {
		if err := c.checkOffset(ts, physicalTime); err != nil {
			log.Warningf(context.TODO(), "%s: updating anyway", err)
		}
		// The remote clock is ahead of ours, and we update
		// our own logical clock with theirs.
		c.mu.timestamp.WallTime = ts.WallTime
		c.mu.timestamp.Logical = ts.Logical + 1
	} else if c.mu.timestamp.WallTime > ts.WallTime {
		// Our wall time is larger, so it remains but we tick
		// the logical clock.
		c.mu.timestamp.Logical++
	} else {
		// Both wall times are equal, and the larger logical
		// clock is used for the update.
		if ts.Logical > c.mu.timestamp.Logical {
			c.mu.timestamp.Logical = ts.Logical
		}
		c.mu.timestamp.Logical++
	}
	return c.mu.timestamp
}
