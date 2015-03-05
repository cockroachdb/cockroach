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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// Package hlc implements the Hybrid Logical Clock outlined in
// "Logical Physical Clocks and Consistent Snapshots in Globally
// Distributed Databases", available online at
// http://www.cse.buffalo.edu/tech-reports/2014-04.pdf.
package hlc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

// TODO(Tobias): Figure out if it would make sense to save some
// history of the physical clock and react if it jumps backwards
// repeatedly. This is expected during NTP updates, but may
// indicate a broken clock in some cases.

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
	// Clock contains a mutex used to lock the below
	// fields while methods operate on them.
	sync.Mutex
	state proto.Timestamp
	// MaxOffset specifies how far ahead of the physical
	// clock (and cluster time) the wall time can be.
	// See SetMaxOffset.
	maxOffset time.Duration
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
	return time.Now().UnixNano()
}

// NewClock creates a new hybrid logical clock associated
// with the given physical clock, initializing both wall time
// and logical time with zero.
//
// The physical clock is typically given by the wall time
// of the local machine in unix epoch nanoseconds, using
// hlc.UnixNano. This is not a requirement.
func NewClock(physicalClock func() int64) *Clock {
	return &Clock{
		physicalClock: physicalClock,
	}
}

// SetMaxOffset sets the maximal offset of the physical clock from the cluster.
// It is used to set the max offset a call to Update may cause and to ensure
// an upperbound on timestamp WallTime in transactions. A well-chosen value is
// large enough to ignore a reasonable amount of clock skew but will prevent
// ill-configured nodes from dramatically skewing the wall time of the clock
// into the future.
//
// A value of zero disables all safety features.
// The default value for a new instance is zero.
func (c *Clock) SetMaxOffset(delta time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.maxOffset = delta
}

// MaxOffset returns the maximal offset allowed.
// A value of 0 means offset checking is disabled.
// See SetMaxOffset for details.
func (c *Clock) MaxOffset() time.Duration {
	c.Lock()
	defer c.Unlock()
	return c.maxOffset
}

// Timestamp returns a copy of the clock's current timestamp,
// without performing a clock adjustment.
func (c *Clock) Timestamp() proto.Timestamp {
	c.Lock()
	defer c.Unlock()
	return c.timestamp()
}

// timestamp returns the state as a timestamp, without
// a lock on the clock's state, for internal usage.
func (c *Clock) timestamp() proto.Timestamp {
	return proto.Timestamp{
		WallTime: c.state.WallTime,
		Logical:  c.state.Logical,
	}
}

// Now returns a timestamp associated with an event from
// the local machine that may be sent to other members
// of the distributed network. This is the counterpart
// of Update, which is passed a timestamp received from
// another member of the distributed network.
func (c *Clock) Now() (result proto.Timestamp) {
	c.Lock()
	defer c.Unlock()
	defer func() {
		result = c.timestamp()
	}()

	physicalClock := c.physicalClock()
	if c.state.WallTime >= physicalClock {
		// The wall time is ahead, so the logical clock ticks.
		c.state.Logical++
	} else {
		// Use the physical clock, and reset the logical one.
		c.state.WallTime = physicalClock
		c.state.Logical = 0
	}
	return
}

// PhysicalNow returns the local wall time. It corresponds to the physicalClock
// provided at instantiation. For a timestamp value, use Now() instead.
func (c *Clock) PhysicalNow() int64 {
	c.Lock()
	defer c.Unlock()
	wallTime := c.physicalClock()
	return wallTime
}

// PhysicalTime returns a time.Time struct using the local wall time.
func (c *Clock) PhysicalTime() time.Time {
	physNow := c.PhysicalNow()
	return time.Unix(physNow/1E9, physNow%1E9)
}

// Update takes a hybrid timestamp, usually originating from
// an event received from another member of a distributed
// system. The clock is updated and the hybrid timestamp
// associated to the receipt of the event returned.
// An error may only occur if offset checking is active and
// the remote timestamp was rejected due to clock offset,
// in which case the state of the clock will not have been
// altered.
// To timestamp events of local origin, use Now instead.
func (c *Clock) Update(rt proto.Timestamp) (result proto.Timestamp, err error) {
	c.Lock()
	defer c.Unlock()
	defer func() {
		result = c.timestamp()
	}()
	physicalClock := c.physicalClock()

	if physicalClock > c.state.WallTime && physicalClock > rt.WallTime {
		// Our physical clock is ahead of both wall times. It is used
		// as the new wall time and the logical clock is reset.
		c.state.WallTime = physicalClock
		c.state.Logical = 0
		return
	}

	// In the remaining cases, our physical clock plays no role
	// as it is behind the local and remote wall times. Instead,
	// the logical clock comes into play.
	if rt.WallTime > c.state.WallTime {
		if c.maxOffset.Nanoseconds() > 0 &&
			rt.WallTime-physicalClock > c.maxOffset.Nanoseconds() {
			// The remote wall time is too far ahead to be trustworthy.
			err = util.Errorf("Remote wall time offsets from local physical clock: %d (%dns ahead)",
				rt.WallTime, rt.WallTime-physicalClock)
			return
		}
		// The remote clock is ahead of ours, and we update
		// our own logical clock with theirs.
		c.state.WallTime = rt.WallTime
		c.state.Logical = rt.Logical + 1
	} else if c.state.WallTime > rt.WallTime {
		// Our wall time is larger, so it remains but we tick
		// the logical clock.
		c.state.Logical++
	} else {
		// Both wall times are equal, and the larger logical
		// clock is used for the update.
		if rt.Logical > c.state.Logical {
			c.state.Logical = rt.Logical
		}
		c.state.Logical++
	}
	// The variable result will be updated via defer just
	// before the object is unlocked.
	return
}
