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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
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
	state roachpb.Timestamp
	// MaxOffset specifies how far ahead of the physical
	// clock (and cluster time) the wall time can be.
	// See SetMaxOffset.
	maxOffset time.Duration

	// monotonicityErrorsCount indicate how often this clock was
	// observed to jump backwards.
	monotonicityErrorsCount int32
	// lastPhysicalTime reports the last measured physical time. This
	// is used to detect clock jumps.
	lastPhysicalTime int64
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
	return timeutil.Now().UnixNano()
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
func (c *Clock) Timestamp() roachpb.Timestamp {
	c.Lock()
	defer c.Unlock()
	return c.timestamp()
}

// timestamp returns the state as a timestamp, without
// a lock on the clock's state, for internal usage.
func (c *Clock) timestamp() roachpb.Timestamp {
	return roachpb.Timestamp{
		WallTime: c.state.WallTime,
		Logical:  c.state.Logical,
	}
}

// getPhysicalClock returns the current physical clock and checks for
// time jumps.
func (c *Clock) getPhysicalClock() int64 {
	newTime := c.physicalClock()

	if c.lastPhysicalTime != 0 {
		interval := c.lastPhysicalTime - newTime
		if interval > int64(c.maxOffset/10) {
			c.monotonicityErrorsCount++
			log.Warningf("backward time jump detected (%f seconds)", float64(newTime-c.lastPhysicalTime)/1e9)
		}
	}

	c.lastPhysicalTime = newTime
	return newTime
}

// Now returns a timestamp associated with an event from
// the local machine that may be sent to other members
// of the distributed network. This is the counterpart
// of Update, which is passed a timestamp received from
// another member of the distributed network.
func (c *Clock) Now() roachpb.Timestamp {
	c.Lock()
	defer c.Unlock()

	physicalClock := c.getPhysicalClock()
	if c.state.WallTime >= physicalClock {
		// The wall time is ahead, so the logical clock ticks.
		c.state.Logical++
	} else {
		// Use the physical clock, and reset the logical one.
		c.state.WallTime = physicalClock
		c.state.Logical = 0
	}
	return c.timestamp()
}

// PhysicalNow returns the local wall time. It corresponds to the physicalClock
// provided at instantiation. For a timestamp value, use Now() instead.
func (c *Clock) PhysicalNow() int64 {
	c.Lock()
	defer c.Unlock()
	wallTime := c.getPhysicalClock()
	return wallTime
}

// PhysicalTime returns a time.Time struct using the local wall time.
func (c *Clock) PhysicalTime() time.Time {
	return time.Unix(0, c.PhysicalNow()).UTC()
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
func (c *Clock) Update(rt roachpb.Timestamp) roachpb.Timestamp {
	c.Lock()
	defer c.Unlock()
	physicalClock := c.getPhysicalClock()

	if physicalClock > c.state.WallTime && physicalClock > rt.WallTime {
		// Our physical clock is ahead of both wall times. It is used
		// as the new wall time and the logical clock is reset.
		c.state.WallTime = physicalClock
		c.state.Logical = 0
		return c.timestamp()
	}

	// In the remaining cases, our physical clock plays no role
	// as it is behind the local and remote wall times. Instead,
	// the logical clock comes into play.
	if rt.WallTime > c.state.WallTime {
		offset := time.Duration(rt.WallTime-physicalClock) * time.Nanosecond
		if c.maxOffset > 0 && offset > c.maxOffset {
			log.Warningf("remote wall time is too far ahead (%s) to be trustworthy - updating anyway", offset)
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
	return c.timestamp()
}
