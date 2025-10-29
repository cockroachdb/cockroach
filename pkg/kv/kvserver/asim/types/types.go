// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"fmt"
	"time"
)

// Tick represents a logical tick of the simulator. The first tick starts at
// Start and subsequent ticks are spaced Tick apart. The Count field
// identifies which tick in the sequence this instance represents.
//
// For example, if Start is t0, Tick is 10ms and Count is 3, the wall time for
// this Tick is t0+30ms.
//
// The String implementation deliberately keeps the representation compact to
// ease log readability while remaining sortable. It prints the sequence number
// prefixed with a # and, in brackets, the cumulative offset from Start. For
// the example above this is "#3[30ms]".
//
// Additional helper methods exist to convert between Tick and time.Time so that
// existing code can migrate incrementally.
type Tick struct {
	Start time.Time     // Wall-clock reference for tick zero.
	Tick  time.Duration // Duration between consecutive ticks.
	Count int           // Zero-based tick index.
}

// WallTime returns the wall-clock time corresponding to the logical tick.
func (t Tick) WallTime() time.Time {
	return t.Start.Add(time.Duration(t.Count) * t.Tick)
}

// String implements fmt.Stringer.
func (t Tick) String() string {
	return fmt.Sprintf("#%d[%s]", t.Count, time.Duration(t.Count)*t.Tick)
}

// Before reports whether the instant represented by t occurs before u.
func (t Tick) Before(u Tick) bool {
	return t.WallTime().Before(u.WallTime())
}

// After reports whether the instant represented by t occurs after u.
func (t Tick) After(u Tick) bool {
	return t.WallTime().After(u.WallTime())
}

// Add returns a new Tick advanced by n logical steps. It panics if n is
// negative and would produce a negative Count.
func (t Tick) Add(n int) Tick {
	newCount := t.Count + n
	if newCount < 0 {
		panic("asim.Tick.Add: resulting count would be negative")
	}
	return Tick{Start: t.Start, Tick: t.Tick, Count: newCount}
}

// AsDuration returns the cumulative duration from Start to this Tick.
func (t Tick) AsDuration() time.Duration {
	return time.Duration(t.Count) * t.Tick
}

// Sub returns t.WallTime()-u.WallTime(). It is equivalent to time.Time.Sub.
func (t Tick) Sub(u Tick) time.Duration {
	return t.WallTime().Sub(u.WallTime())
}

// FromWallTime creates a new Tick from a wall-clock time, using the same
// Start and Tick interval as the receiver. This is useful for converting
// time.Time values back to Ticks during incremental migration.
func (t Tick) FromWallTime(wallTime time.Time) Tick {
	elapsed := wallTime.Sub(t.Start)
	count := int(elapsed / t.Tick)
	return Tick{Start: t.Start, Tick: t.Tick, Count: count}
}
