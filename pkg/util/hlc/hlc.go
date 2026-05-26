// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hlc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
type Clock struct {
	// wallClock is used to read the current clock.
	wallClock WallClock

	// maxOffset is the maximal clock skew between any two nodes in the cluster,
	// as promised by the operator. See MaxOffset().
	//
	// TODO(kv): make this dynamic in the distant future, see
	// https://github.com/cockroachdb/cockroach/issues/75564
	maxOffset time.Duration

	// toleratedOffset is the tolerated clock skew with other cluster nodes,
	// beyond which the node will self-terminate. See ToleratedOffset().
	toleratedOffset time.Duration

	// logger is used when forward or backwards jumps are
	// detected.
	logger Logger

	// lastPhysicalTime reports the last measured physical time. This
	// is used to detect clock jumps. The field is accessed atomically.
	// This field isn't part of the mutex below to prevent
	// a second mutex acquisition in Now()
	lastPhysicalTime int64

	// monotonicityErrorsCount indicate how often this clock was
	// observed to jump backwards. The field is accessed atomically.
	monotonicityErrorsCount int32

	// forwardClockJumpCheckEnabled specifies whether to panic on forward
	// clock jumps. If set to 1, then jumps will cause panic. If set to 0,
	// the check is disabled. The field is accessed atomically.
	forwardClockJumpCheckEnabled int32

	mu struct {
		syncutil.Mutex

		// timestamp is the current HLC time. The timestamp.WallTime field must
		// be updated atomically, even though it is protected by a mutex - this
		// enables a fast path for reading the wall time without grabbing the
		// lock.
		timestamp ClockTimestamp

		// isMonitoringForwardClockJumps is a flag to ensure that only one jump monitoring
		// goroutine is running per clock
		isMonitoringForwardClockJumps bool

		// WallTimeUpperBound is an upper bound to the HLC which has been
		// successfully persisted.
		// The wall time used by the HLC will always be lesser than this timestamp.
		// If the physical time is greater than this value, it will cause a panic
		// If this is set to 0, this validation is skipped
		wallTimeUpperBound int64
	}
}

// WallClock models a physical clock. This is a sub-interface of
// timeutil.TimeSource.
type WallClock interface {
	// Now returns the current time.
	Now() time.Time
}

// Logger is used by Clock when clock anamolies are detected.
type Logger interface {
	// Warningf is used when the Clock detects non-fatal
	// conditions.
	Warningf(ctx context.Context, format string, args ...interface{})
	// Fatalf is used when forwardClockJump detection is enabled
	// and the Clock detects a fatal forward clock jump.
	//
	// Production implementations of Fatalf should halt the
	// process in manner appropriate to the caller.
	Fatalf(ctx context.Context, format string, args ...interface{})
}

// panicFataler is a Logger that panics when Fatal is called.
type panicLogger struct{}

var PanicLogger Logger = &panicLogger{}

func (*panicLogger) Warningf(context.Context, string, ...interface{}) {}
func (*panicLogger) Fatalf(_ context.Context, format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// HybridManualClock is a convenience type to facilitate
// creating a hybrid logical clock whose physical clock
// ticks with the wall clock, but that can be moved arbitrarily
// into the future or paused.
//
// ManualClock implements WallClock, so it can be used with
// NewClock(NewHybridManualClock(),...).
//
// HybridManualClock is thread safe.
type HybridManualClock struct {
	mu struct {
		syncutil.RWMutex
		// nanos, if not 0, is the amount of time the clock was manually incremented
		// by; it is added to physicalClock.
		nanos int64
		// nanosAtPause records the timestamp of the physical clock when it gets
		// paused. 0 means that the clock is not paused.
		nanosAtPause int64
	}
}

// NewHybridManualClock returns a new instance, initialized with
// specified timestamp.
func NewHybridManualClock() *HybridManualClock {
	return &HybridManualClock{}
}

var _ WallClock = &HybridManualClock{}

// Now implements the WallClock interface.
func (m *HybridManualClock) Now() time.Time {
	return timeutil.Unix(0, m.UnixNano())
}

// UnixNano returns the underlying hybrid manual clock's timestamp.
func (m *HybridManualClock) UnixNano() int64 {
	m.mu.RLock()
	nanosAtPause := m.mu.nanosAtPause
	nanos := m.mu.nanos
	m.mu.RUnlock()
	if nanosAtPause > 0 {
		return nanos + nanosAtPause
	}
	return nanos + timeutil.Now().UnixNano()
}

// Increment increments the hybrid manual clock's timestamp.
func (m *HybridManualClock) Increment(nanos int64) {
	m.mu.Lock()
	m.mu.nanos += nanos
	m.mu.Unlock()
}

// Forward sets the wall time to the supplied timestamp if this moves the clock
// forward in time. Note that this takes an absolute timestamp (i.e. a wall
// clock timestamp), not a delta.
func (m *HybridManualClock) Forward(tsNanos int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := timeutil.Now().UnixNano()
	if tsNanos < now {
		return
	}
	aheadNanos := tsNanos - now
	if aheadNanos > m.mu.nanos {
		m.mu.nanos = aheadNanos
	}
}

// Pause pauses the hybrid manual clock; the passage of time no longer causes
// the clock to tick. Increment can still be used, though.
func (m *HybridManualClock) Pause() {
	m.mu.Lock()
	m.mu.nanosAtPause = timeutil.Now().UnixNano()
	m.mu.Unlock()
}

// Resume resumes the hybrid manual clock; the passage of time continues to
// cause clock to tick.
func (m *HybridManualClock) Resume() {
	m.mu.Lock()
	m.mu.nanosAtPause = 0
	m.mu.Unlock()
}

// NewClockWithSystemTimeSource creates a Clock that reads the system time. This
// is equivalent to NewClock(timeutil.SystemTimeSource, maxOffset, toleratedOffset).
func NewClockWithSystemTimeSource(maxOffset, toleratedOffset time.Duration, logger Logger) *Clock {
	return NewClock(timeutil.DefaultTimeSource{}, maxOffset, toleratedOffset, logger)
}

// NewClockForTesting creates a new Clock for tests that don't care about clock
// offsets, disabling offset checks. A nil wallClock uses the system clock source.
func NewClockForTesting(wallClock WallClock) *Clock {
	if wallClock == nil {
		wallClock = timeutil.DefaultTimeSource{}
	}
	return NewClock(wallClock, 0 /* maxOffset */, 0 /* toleratedOffset */, PanicLogger)
}

// NewClock returns a Clock configured to use a specified time source. Use
// NewClockWithSystemTimeSource to use the system clock.
//
// maxOffset specifies the max clock offset between cluster nodes, used for
// linearizability and lease guarantees. toleratedOffset specifies the tolerated
// clock offset between cluster nodes as measured by RPC heartbeats, terminating
// the node via RemoteClockMonitor if violated. A value of 0 will disable the
// corresponding check. See Clock.MaxOffset() and Clock.ToleratedOffset() for
// details.
func NewClock(wallClock WallClock, maxOffset, toleratedOffset time.Duration, logger Logger) *Clock {
	return &Clock{
		wallClock:       wallClock,
		maxOffset:       maxOffset,
		toleratedOffset: toleratedOffset,
		logger:          logger,
	}
}

// WallClock returns the c's time source.
func (c *Clock) WallClock() WallClock {
	return c.wallClock
}

// UnixNano returns the local machine's physical nanosecond
// unix epoch timestamp as a convenience to create a HLC via
// c := hlc.NewClockWithSystemTimeSource( ... /* maxOffset */).
func UnixNano() int64 {
	return timeutil.Now().UnixNano()
}

// toleratedForwardClockJump is the tolerated forward jump. Jumps greater
// than the returned value will cause if panic if forward clock jump check is
// enabled
func (c *Clock) toleratedForwardClockJump() time.Duration {
	return c.maxOffset / 2
}

// StartMonitoringForwardClockJumps starts a goroutine to update the clock's
// forwardClockJumpCheckEnabled based on the values pushed in
// forwardClockJumpCheckEnabledCh.
//
// This also keeps lastPhysicalTime up to date to avoid spurious jump errors.
//
// A nil channel or a value of false pushed in forwardClockJumpCheckEnabledCh
// disables checking clock jumps between two successive reads of the physical
// clock.
//
// This should only be called once per clock, and will return an error if called
// more than once
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever maxForwardClockJumpCh or a ticker tick is
// processed
func (c *Clock) StartMonitoringForwardClockJumps(
	ctx context.Context,
	forwardClockJumpCheckEnabledCh <-chan bool,
	tickerFn func(d time.Duration) *time.Ticker,
	tickCallback func(),
) error {
	alreadyMonitoring := c.setMonitoringClockJump()
	if alreadyMonitoring {
		return errors.New("clock jumps are already being monitored")
	}

	ctx, sp := tracing.ForkSpan(ctx, "clock monitor")
	go func() {
		defer sp.Finish()
		// Create a ticker object which can be used in selects.
		// This ticker is turned on / off based on forwardClockJumpCheckEnabledCh
		ticker := tickerFn(time.Hour)
		ticker.Stop()
		refreshPhysicalClockItvl := c.toleratedForwardClockJump() / 2
		for {
			select {
			case forwardClockJumpEnabled, ok := <-forwardClockJumpCheckEnabledCh:
				ticker.Stop()
				if !ok {
					return
				}
				if forwardClockJumpEnabled {
					// Forward jump check is enabled. Start the ticker
					ticker = tickerFn(refreshPhysicalClockItvl)

					// Fetch the clock once before we start enforcing forward
					// jumps. Otherwise the gap between the previous call to
					// Now() and the time of the first tick would look like a
					// forward jump.
					c.getPhysicalClockAndCheck(ctx)
				}
				c.setForwardJumpCheckEnabled(forwardClockJumpEnabled)
			case <-ticker.C:
				c.getPhysicalClockAndCheck(ctx)
			}

			if tickCallback != nil {
				tickCallback()
			}
		}
	}()

	return nil
}

// MaxOffset returns the maximal clock offset to any node in the cluster as
// specified by the operator. This is used by the cluster to safely hand off
// leases and enforce single-key linearizability.
//
// A known consequence of clocks drifting apart by more than MaxOffset is the
// possibility of stale reads. At an architectural level CockroachDB *should*
// still be serializable in this case, but this has not been conclusively
// verified and should be taken as conjecture.
func (c *Clock) MaxOffset() time.Duration {
	return c.maxOffset
}

// ToleratedOffset returns the tolerated clock offset with other nodes in the
// cluster before self-terminating, as measured via RPC heartbeats. A
// ToleratedOffset of zero disables this mechanism, i.e. behaves like an
// infinite tolerated offset.
//
// Typically, ToleratedOffset is a slightly smaller than MaxOffset (to avoid
// correctness issues should the actual offset regress further) but it can be
// configured to be more lenient, instead taking the risk of a correctness
// issues over a period of unavailability. The latter is appealing in particular
// when clocks are very tightly synchronized and thus the MaxOffset is
// configured to a very small value for performance, where RPC latency spikes
// may cause spurious restarts.
func (c *Clock) ToleratedOffset() time.Duration {
	return c.toleratedOffset
}

// getPhysicalClockAndCheck reads the physical time as nanos since epoch. It
// also checks for backwards and forwards jumps, as configured.
func (c *Clock) getPhysicalClockAndCheck(ctx context.Context) int64 {
	oldTime := atomic.LoadInt64(&c.lastPhysicalTime)
	newTime := c.wallClock.Now().UnixNano()
	lastPhysTime := oldTime
	// Try to update c.lastPhysicalTime. When multiple updaters race, we want the
	// highest clock reading to win, so keep retrying while we interleave with
	// updaters with lower clock readings; bail if we interleave with a higher
	// clock reading.
	for {
		if atomic.CompareAndSwapInt64(&c.lastPhysicalTime, lastPhysTime, newTime) {
			break
		}
		lastPhysTime = atomic.LoadInt64(&c.lastPhysicalTime)
		if lastPhysTime >= newTime {
			// Someone else updated to a later time than ours.
			break
		}
		// Someone else did an update to an earlier time than what we got in newTime.
		// So try one more time to update.
	}
	c.checkPhysicalClock(ctx, oldTime, newTime)
	return newTime
}

// checkPhysicalClock checks for time jumps.
// oldTime is the lastPhysicalTime before the call to get a new time.
// newTime is the result of the call to get a new time.
func (c *Clock) checkPhysicalClock(ctx context.Context, oldTime, newTime int64) {
	if oldTime == 0 {
		return
	}

	interval := oldTime - newTime
	if interval > int64(c.maxOffset/10) {
		atomic.AddInt32(&c.monotonicityErrorsCount, 1)
		c.logger.Warningf(ctx, "backward time jump detected (%f seconds)", float64(-interval)/1e9)
	}

	if atomic.LoadInt32(&c.forwardClockJumpCheckEnabled) != 0 {
		toleratedForwardClockJump := c.toleratedForwardClockJump()
		if int64(toleratedForwardClockJump) <= -interval {
			c.logger.Fatalf(
				ctx,
				"detected forward time jump of %f seconds is not allowed with tolerance of %f seconds",
				redact.Safe(float64(-interval)/1e9),
				redact.Safe(float64(toleratedForwardClockJump)/1e9),
			)
		}
	}
}

// Now returns a timestamp associated with an event from the local
// machine that may be sent to other members of the distributed network.
func (c *Clock) Now() Timestamp {
	return c.NowAsClockTimestamp().ToTimestamp()
}

// NowAsClockTimestamp is like Now, but returns a ClockTimestamp instead
// of a raw Timestamp.
//
// This is the counterpart of Update, which is passed a ClockTimestamp
// received from another member of the distributed network. As such,
// callers that intend to use the returned timestamp to update a peer's
// HLC clock should use this method.
func (c *Clock) NowAsClockTimestamp() ClockTimestamp {
	physicalClock := c.getPhysicalClockAndCheck(context.TODO())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.timestamp.WallTime >= physicalClock {
		// The wall time is ahead, so the logical clock ticks.
		c.mu.timestamp.Logical++
	} else {
		// Use the physical clock, and reset the logical one.
		atomic.StoreInt64(&c.mu.timestamp.WallTime, physicalClock)
		c.mu.timestamp.Logical = 0
	}

	c.enforceWallTimeWithinBoundLocked()
	return c.mu.timestamp
}

// enforceWallTimeWithinBoundLocked panics if the clock's wall time is greater
// than the upper bound. The caller of this function must be holding the lock.
func (c *Clock) enforceWallTimeWithinBoundLocked() {
	// WallTime should not cross the upper bound (if WallTimeUpperBound is set)
	if c.mu.wallTimeUpperBound != 0 && c.mu.timestamp.WallTime > c.mu.wallTimeUpperBound {
		c.logger.Fatalf(
			context.TODO(),
			"wall time %d is not allowed to be greater than upper bound of %d.",
			redact.Safe(c.mu.timestamp.WallTime),
			redact.Safe(c.mu.wallTimeUpperBound),
		)
	}
}

// PhysicalNow returns the local wall time.
//
// Note that, contrary to Now(), PhysicalNow does not take into consideration
// higher clock signals received through Update(). If you want to take them into
// consideration, use c.Now().GoTime().
func (c *Clock) PhysicalNow() int64 {
	return c.wallClock.Now().UnixNano()
}

// PhysicalTime returns a time.Time struct using the local wall time.
func (c *Clock) PhysicalTime() time.Time { return c.wallClock.Now() }

// Update takes a hybrid timestamp, usually originating from an event
// received from another member of a distributed system. The clock is
// updated to reflect the later of the two. The update does not check
// the maximum clock offset. To receive an error response instead of forcing the
// update in case the remote timestamp is too far into the future, use
// UpdateAndCheckMaxOffset() instead.
func (c *Clock) Update(rt ClockTimestamp) {

	// Fast path to avoid grabbing the mutex if the remote time is behind. This
	// requires c.mu.timestamp.WallTime to be written atomically, even though
	// the writer has to hold the mutex lock as well.
	if rt.WallTime < atomic.LoadInt64(&c.mu.timestamp.WallTime) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// There is nothing to do if the remote wall time is behind ours. We just keep ours.
	if rt.WallTime > c.mu.timestamp.WallTime {
		// The remote clock is ahead of ours, and we update
		// our own logical clock with theirs.
		atomic.StoreInt64(&c.mu.timestamp.WallTime, rt.WallTime)
		c.mu.timestamp.Logical = rt.Logical
	} else if rt.WallTime == c.mu.timestamp.WallTime {
		// Both wall times are equal, and the larger logical
		// clock is used for the update.
		if rt.Logical > c.mu.timestamp.Logical {
			c.mu.timestamp.Logical = rt.Logical
		}
	}

	c.enforceWallTimeWithinBoundLocked()
}

// NB: don't change the string here; this will cause cross-version issues
// since this singleton is used as a marker.
var errUntrustworthyRemoteWallTimeErr = errors.New("remote wall time is too far ahead to be trustworthy")

// IsUntrustworthyRemoteWallTimeError returns true if the error came resulted
// from a call to Clock.UpdateAndCheckMaxOffset due to the passed ClockTimestamp
// being too far in the future.
func IsUntrustworthyRemoteWallTimeError(err error) bool {
	return errors.Is(err, errUntrustworthyRemoteWallTimeErr)
}

// UpdateAndCheckMaxOffset is like Update, but also takes the wall time into account and
// returns an error in the event that the supplied remote timestamp exceeds
// the wall clock time by more than the maximum clock offset.
//
// If an error is returned, it will be detectable with
// IsUntrustworthyRemoteWallTimeError.
func (c *Clock) UpdateAndCheckMaxOffset(ctx context.Context, rt ClockTimestamp) error {
	physicalClock := c.getPhysicalClockAndCheck(ctx)

	offset := time.Duration(rt.WallTime - physicalClock)
	if c.maxOffset > 0 && offset > c.maxOffset {
		return errors.Mark(
			errors.Errorf("remote wall time is too far ahead (%s) to be trustworthy", offset),
			errUntrustworthyRemoteWallTimeErr,
		)
	}

	if physicalClock > rt.WallTime {
		c.Update(ClockTimestamp{WallTime: physicalClock})
	} else {
		c.Update(rt)
	}

	return nil
}

// setForwardJumpCheckEnabled atomically sets forwardClockJumpCheckEnabled
func (c *Clock) setForwardJumpCheckEnabled(forwardJumpCheckEnabled bool) {
	if forwardJumpCheckEnabled {
		atomic.StoreInt32(&c.forwardClockJumpCheckEnabled, 1)
	} else {
		atomic.StoreInt32(&c.forwardClockJumpCheckEnabled, 0)
	}
}

// setMonitoringClockJump atomically sets isMonitoringForwardClockJumps to true and
// returns the old value. This is used to ensure that only one monitoring
// goroutine is launched
func (c *Clock) setMonitoringClockJump() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	isMonitoring := c.mu.isMonitoringForwardClockJumps
	c.mu.isMonitoringForwardClockJumps = true
	return isMonitoring
}

// RefreshHLCUpperBound persists the HLC upper bound and updates the in memory
// value if the persist succeeds. delta is used to compute the upper bound.
func (c *Clock) RefreshHLCUpperBound(persistFn func(int64) error, delta int64) error {
	if delta < 0 {
		return errors.Errorf("HLC upper bound delta %d should be positive", delta)
	}
	return c.persistHLCUpperBound(persistFn, c.Now().WallTime+delta)
}

// ResetHLCUpperBound persists a value of 0 as the HLC upper bound which
// disables upper bound validation
func (c *Clock) ResetHLCUpperBound(persistFn func(int64) error) error {
	return c.persistHLCUpperBound(persistFn, 0 /* hlcUpperBound */)
}

// RefreshHLCUpperBound persists the HLC upper bound and updates the in memory
// value if the persist succeeds
func (c *Clock) persistHLCUpperBound(persistFn func(int64) error, hlcUpperBound int64) error {
	if err := persistFn(hlcUpperBound); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.wallTimeUpperBound = hlcUpperBound
	return nil
}

// WallTimeUpperBound returns the in memory value of upper bound to wall time
func (c *Clock) WallTimeUpperBound() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.wallTimeUpperBound
}

// SleepUntil sleeps until the HLC reaches or exceeds the given timestamp. This
// typically results in sleeping for the duration between the given timestamp's
// nanosecond WallTime and the Clock's current WallTime time, but may result in
// sleeping for longer or shorter, depending on the HLC clock's relation to its
// physical time source (it may lead it) and whether it advances more rapidly
// due to updates from other nodes.
//
// If the provided context is canceled, the method will return the cancellation
// error immediately. If an error is returned, no guarantee is made that the HLC
// will have reached the specified timestamp.
func (c *Clock) SleepUntil(ctx context.Context, t Timestamp) error {
	// Don't busy loop if the HLC clock is out ahead of the system's
	// physical clock.
	const minSleep = 25 * time.Microsecond
	// Refresh every second in case there was a clock jump.
	const maxSleep = 1 * time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := c.Now()
		if t.LessEq(now) {
			return nil
		}
		d := now.GoTime().Sub(t.GoTime())
		if d < minSleep {
			d = minSleep
		} else if d > maxSleep {
			d = maxSleep
		}
		// If we're going to sleep for at least 1ms, listen for context
		// cancellation. Otherwise, don't bother with the select and the
		// more expensive use of time.After.
		if d < 1*time.Millisecond {
			time.Sleep(d)
		} else {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// DecimalToHLC performs the conversion from an inputted DECIMAL datum for an
// AS OF SYSTEM TIME query to an HLC timestamp.
func DecimalToHLC(d *apd.Decimal) (Timestamp, error) {
	if d.Negative {
		return Timestamp{}, pgerror.Newf(pgcode.Syntax, "cannot be negative")
	}
	var integral, fractional apd.Decimal
	d.Modf(&integral, &fractional)
	timestamp, err := integral.Int64()
	if err != nil {
		return Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "converting timestamp to integer") // should never happen
	}
	if fractional.IsZero() {
		// there is no logical portion to this clock
		return Timestamp{WallTime: timestamp}, nil
	}

	var logical apd.Decimal
	multiplier := apd.New(1, 10)
	condition, err := apd.BaseContext.Mul(&logical, &fractional, multiplier)
	if err != nil {
		return Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "determining value of logical clock")
	}
	if _, err := condition.GoError(apd.DefaultTraps); err != nil {
		return Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "determining value of logical clock")
	}

	counter, err := logical.Int64()
	if err != nil {
		return Timestamp{}, pgerror.Newf(pgcode.Syntax, "logical part has too many digits")
	}
	if counter > 1<<31 {
		return Timestamp{}, pgerror.Newf(pgcode.Syntax, "logical clock too large: %d", counter)
	}
	return Timestamp{
		WallTime: timestamp,
		Logical:  int32(counter),
	}, nil
}

// ParseHLC parses a string representation of an `hlc.Timestamp`.
// This differs from hlc.ParseTimestamp in that it parses the decimal
// serialization of an hlc timestamp as opposed to the string serialization
// performed by hlc.Timestamp.String().
//
// This function is used to parse:
//
//	1580361670629466905.0000000001
//
// hlc.ParseTimestamp() would be used to parse:
//
//	1580361670.629466905,1
func ParseHLC(s string) (Timestamp, error) {
	dec, _, err := apd.NewFromString(s)
	if err != nil {
		return Timestamp{}, err
	}
	return DecimalToHLC(dec)
}
