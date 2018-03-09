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

// Package hlc implements the Hybrid Logical Clock outlined in
// "Logical Physical Clocks and Consistent Snapshots in Globally
// Distributed Databases", available online at
// http://www.cse.buffalo.edu/tech-reports/2014-04.pdf.
package hlc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	// The following constants are used if server.clock.persist_future_wall_time
	// is true

	// persistFutureWallTimeInterval is the interval used for persisting the
	// future wall time
	persistFutureWallTimeInterval = 400 * time.Millisecond
	// persistFutureWallTimeDelta is the delta used to compute the future wall
	// time. This should be greater than persistFutureWallTimeInterval
	persistFutureWallTimeDelta = int64(15 * time.Second)

	// maxSleepToEnsureMonotonicity is the max duration to sleep while waiting
	// for wall time to catch up to the persisted future wall time. The physical
	// clock could have jumped so the delta between physical time and persisted
	// future wall time is recomputed periodically
	maxSleepToEnsureMonotonicity = 10 * time.Second
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

		// maxForwardClockJump is the maximum jump allowed for the physical clock between
		// two successive clock reads. Setting this to 0 disables this validation.
		maxForwardClockJump time.Duration

		// isMonitoringClockJumps is a flag to ensure that only one jump monitoring
		// goroutine is running per clock
		isMonitoringClockJumps bool

		// futureWallTime is latest future wall time successfully persisted.
		// The wall time used by the HLC will always be lesser than this timestamp.
		// If this is set to 0, this validation is skipped
		futureWallTime int64

		// isPersistingFutureWallTime is a flag to ensure that only one persisting
		// goroutine is running per clock
		isPersistingFutureWallTime bool
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
// c := hlc.NewClock(hlc.UnixNano, ...).
func UnixNano() int64 {
	return timeutil.Now().UnixNano()
}

// NewClock creates a new hybrid logical clock associated with the given
// physical clock. The logical ts is initialized to zero.
//
// The physical clock is typically given by the wall time of the local machine
// in unix epoch nanoseconds, using hlc.UnixNano. This is not a requirement.
//
// A value of 0 for maxOffset means that clock skew checking, if performed on
// this clock by RemoteClockMonitor, is disabled.
func NewClock(physicalClock func() int64, maxOffset time.Duration) *Clock {
	return &Clock{
		physicalClock: physicalClock,
		maxOffset:     maxOffset,
	}
}

// StartMonitoringForwardClockJumps starts a goroutine to update the clock's
// maxForwardClockJump based on the values pushed in maxForwardClockJumpCh.
//
// This also keeps lastPhysicalTime up to date to avoid spurious jump errors.
// maxForwardClockJumpCh is a channel from which the tolerated physical clock
// forward jump threshold is read.
//
// A nil channel or a value of 0 for maxForwardClockJump disables checking clock
// jumps between two successive reads of the physical clock.
//
// This should only be called once per clock, and will return an error if called
// more than once
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever maxForwardClockJumpCh or a ticker tick is
// processed
func (c *Clock) StartMonitoringForwardClockJumps(
	maxForwardClockJumpCh <-chan time.Duration,
	tickerFn func(d time.Duration) *time.Ticker,
	tickCallback func(),
) error {
	alreadyMonitoring := c.setMonitoringClockJump()
	if alreadyMonitoring {
		return errors.New("clock jumps are already being monitored")
	}

	go func() {
		ticker := tickerFn(c.maxOffset)
		ticker.Stop()
		for {
			select {
			case maxForwardClockJump, ok := <-maxForwardClockJumpCh:
				ticker.Stop()
				if !ok {
					return
				}

				refreshLastUpdateTimeItvl := maxForwardClockJump / 2
				if refreshLastUpdateTimeItvl > 0 {
					ticker = tickerFn(refreshLastUpdateTimeItvl)
				}
				c.updateMaxForwardClockJump(maxForwardClockJump)
			case <-ticker.C:
				c.PhysicalNow()
			}

			if tickCallback != nil {
				tickCallback()
			}
		}
	}()
	return nil
}

// wallTimeToPersist returns the wall time offset by persistFutureWallTimeDelta
func (c *Clock) wallTimeToPersist() int64 {
	return c.Now().WallTime + persistFutureWallTimeDelta
}

// EnsureInitialWallTimeMonotonicity sleeps till the wall time reaches
// prevWallTime. prevWallTime > 0 implies we need to guarantee wall time
// monotonicity across server restarts. prevWallTime is the last successfully
// persisted timestamp greater then any wall time used by the server.
//
// persistWallTimeFn is used to persist the future wall time, and should return
// an error if the persist fails.
func (c *Clock) EnsureInitialWallTimeMonotonicity(
	ctx context.Context,
	prevWallTime int64,
	persistWallTimeFn func(int64) error,
	sleepFn func(d time.Duration),
) error {
	if prevWallTime == 0 {
		return nil
	}

	for {
		currentWallTime := c.Now().WallTime
		sleepTime := time.Duration(prevWallTime-currentWallTime) + 1
		if sleepTime <= 0 {
			break
		}
		if sleepTime > maxSleepToEnsureMonotonicity {
			sleepTime = maxSleepToEnsureMonotonicity
		}

		log.Infof(
			ctx,
			"Sleeping for %v to for wall time %v to catch up to %v to ensure monotonicity",
			sleepTime,
			currentWallTime,
			prevWallTime,
		)
		sleepFn(sleepTime)
	}

	if prevWallTime > 0 {
		// A non zero prevWallTime signifies that the feature to persist future
		// wall times is enabled. Persist a new future wall time to continue
		// guaranteeing monotonicity
		return c.updateFutureWallTime(persistWallTimeFn, c.wallTimeToPersist())
	}
	return nil
}

// StartPersistingFutureWallTime starts a goroutine to persist a future wall time.
// The values pushed in persistFutureWallTimeCh specify whether the future
// timestamp should be persisted.
//
// persistWallTimeFn is used to persist the future wall time, and should return
// an error if the persist fails
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever maxClockJumpCh or a ticker tick is processed
func (c *Clock) StartPersistingFutureWallTime(
	persistFutureWallTimeCh <-chan bool,
	persistWallTimeFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
	tickCallback func(),
) error {
	alreadyPersisting := c.setPersistingFutureWallTime()
	if alreadyPersisting {
		return errors.New("future wall time is already being persisted")
	}

	go func() {
		ticker := tickerFn(persistFutureWallTimeInterval)
		ticker.Stop()

		for {
			select {
			case persistFutureWallTime, ok := <-persistFutureWallTimeCh:
				ticker.Stop()
				if !ok {
					return
				}

				if persistFutureWallTime {
					ticker = tickerFn(persistFutureWallTimeInterval)
				} else {
					persistWallTime := int64(0)
					if err := c.updateFutureWallTime(persistWallTimeFn, persistWallTime); err != nil {
						log.Fatalf(
							context.Background(),
							"error persisting future wall time of %d: %v",
							persistWallTime,
							err,
						)
					}
				}

			case <-ticker.C:
				futureWallTime := c.wallTimeToPersist()
				if err := c.updateFutureWallTime(persistWallTimeFn, futureWallTime); err != nil {
					log.Fatalf(
						context.Background(),
						"error persisting future wall time of %d: %v",
						futureWallTime,
						err,
					)
				}
			}

			if tickCallback != nil {
				tickCallback()
			}
		}
	}()
	return nil
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
	newTime := c.physicalClock()

	if c.mu.lastPhysicalTime != 0 {
		interval := c.mu.lastPhysicalTime - newTime
		if interval > int64(c.maxOffset/10) {
			c.mu.monotonicityErrorsCount++
			log.Warningf(context.TODO(), "backward time jump detected (%f seconds)", float64(-interval)/1e9)
		}

		// Physical time should not cross the persisted future wall time (if futureWallTime is set)
		if c.mu.futureWallTime != 0 && newTime > c.mu.futureWallTime {
			log.Fatalf(
				context.TODO(),
				"Cluster setting server.clock.persist_future_wall_time does not allow physical time "+
					"%d to be greater than persisted wall time %d.",
				c.mu.lastPhysicalTime,
				c.mu.futureWallTime,
			)
		}

		if c.mu.maxForwardClockJump > 0 {
			if interval <= int64(-c.mu.maxForwardClockJump) {
				log.Fatalf(
					context.TODO(),
					"detected forward time jump of %f seconds is not allowed with tolerance of %f seconds",
					float64(-interval)/1e9,
					float64(c.mu.maxForwardClockJump)/1e9,
				)
			}
		}
	}

	c.mu.lastPhysicalTime = newTime
	return newTime
}

// Now returns a timestamp associated with an event from
// the local machine that may be sent to other members
// of the distributed network. This is the counterpart
// of Update, which is passed a timestamp received from
// another member of the distributed network.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	if physicalClock := c.getPhysicalClockLocked(); c.mu.timestamp.WallTime >= physicalClock {
		// The wall time is ahead, so the logical clock ticks.
		c.mu.timestamp.Logical++
	} else {
		// Use the physical clock, and reset the logical one.
		c.mu.timestamp.WallTime = physicalClock
		c.mu.timestamp.Logical = 0
	}
	return c.mu.timestamp
}

// PhysicalNow returns the local wall time. It corresponds to the physicalClock
// provided at instantiation. For a timestamp value, use Now() instead.
func (c *Clock) PhysicalNow() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getPhysicalClockLocked()
}

// PhysicalTime returns a time.Time struct using the local wall time.
func (c *Clock) PhysicalTime() time.Time {
	return timeutil.Unix(0, c.PhysicalNow())
}

// Update takes a hybrid timestamp, usually originating from an event
// received from another member of a distributed system. The clock is
// updated and the clock's updated hybrid timestamp is returned. If
// the remote timestamp exceeds the wall clock time by more than the
// maximum clock offset, the update is still processed, but a warning
// is logged. To receive an error response instead of forcing the
// update in case the remote timestamp is too far into the future, use
// UpdateAndCheckMaxOffset() instead.
func (c *Clock) Update(rt Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	updateT, err := c.updateLocked(rt, true)
	if err != nil {
		log.Warningf(context.TODO(), "%s - updating anyway", err)
	}
	return updateT
}

func (c *Clock) updateLocked(rt Timestamp, updateIfMaxOffsetExceeded bool) (Timestamp, error) {
	var err error
	physicalClock := c.getPhysicalClockLocked()

	if physicalClock > c.mu.timestamp.WallTime && physicalClock > rt.WallTime {
		// Our physical clock is ahead of both wall times. It is used
		// as the new wall time and the logical clock is reset.
		c.mu.timestamp.WallTime = physicalClock
		c.mu.timestamp.Logical = 0
		return c.mu.timestamp, nil
	}

	offset := time.Duration(rt.WallTime - physicalClock)
	if c.maxOffset > 0 && c.maxOffset != timeutil.ClocklessMaxOffset && offset > c.maxOffset {
		err = fmt.Errorf("remote wall time is too far ahead (%s) to be trustworthy", offset)
		if !updateIfMaxOffsetExceeded {
			return Timestamp{}, err
		}
	}

	// In the remaining cases, our physical clock plays no role
	// as it is behind the local or remote wall times. Instead,
	// the logical clock comes into play.
	if rt.WallTime > c.mu.timestamp.WallTime {
		// The remote clock is ahead of ours, and we update
		// our own logical clock with theirs.
		c.mu.timestamp.WallTime = rt.WallTime
		c.mu.timestamp.Logical = rt.Logical + 1
	} else if c.mu.timestamp.WallTime > rt.WallTime {
		// Our wall time is larger, so it remains but we tick
		// the logical clock.
		c.mu.timestamp.Logical++
	} else {
		// Both wall times are equal, and the larger logical
		// clock is used for the update.
		if rt.Logical > c.mu.timestamp.Logical {
			c.mu.timestamp.Logical = rt.Logical
		}
		c.mu.timestamp.Logical++
	}
	return c.mu.timestamp, err
}

// UpdateAndCheckMaxOffset is similar to Update, except it returns an
// error instead of logging a warning and updating the clock's
// timestamp, in the event that the supplied remote timestamp exceeds
// the wall clock time by more than the maximum clock offset.
func (c *Clock) UpdateAndCheckMaxOffset(rt Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updateLocked(rt, false)
}

// lastPhysicalTime returns the last physical time
func (c *Clock) lastPhysicalTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.lastPhysicalTime
}

// updateMaxForwardClockJump atomically sets maxForwardClockJump
func (c *Clock) updateMaxForwardClockJump(maxForwardClockJump time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.maxForwardClockJump = maxForwardClockJump
}

// setMonitoringClockJump atomically sets isMonitoringClockJumps to true and
// returns the old value. This is used to ensure that only one monitoring
// goroutine is launched
func (c *Clock) setMonitoringClockJump() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	isMonitoring := c.mu.isMonitoringClockJumps
	c.mu.isMonitoringClockJumps = true
	return isMonitoring
}

// updateFutureWallTime persists the future wall time and updates the in memory value
// if the persist succeeds
func (c *Clock) updateFutureWallTime(persistFn func(int64) error, futureWallTime int64) error {
	if err := persistFn(futureWallTime); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.futureWallTime = futureWallTime
	return nil
}

// futureWallTime returns the in memory value of future wall time
func (c *Clock) futureWallTime() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.futureWallTime
}

// setPersistingFutureWallTime atomically sets isMonitoringClockJumps to true
// and returns the old value. This is used to ensure that only one persisting
// goroutine is launched
func (c *Clock) setPersistingFutureWallTime() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	isPersisting := c.mu.isPersistingFutureWallTime
	c.mu.isPersistingFutureWallTime = true
	return isPersisting
}
