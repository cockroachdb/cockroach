// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// testClock is an hlc.WallClock that simulates drift around some fixed offset
// from the given system clock.
type testClock struct {
	clock hlc.WallClock

	sign int
	// freq is the angular frequency in radians/nanosecond.
	freq        float64
	amplitude   time.Duration
	fixedOffset time.Duration
}

// NewTestClock returns a testClock.
//
// The maxOffset is the maximum offset of two clocks created with NewTestClock.
// The clock will return times in the range
//
//	[t-maxOffset/2, t+maxOffset/2]
//
// We currently use a sine wave to drift the clock over time. Note that this was
// chosen without much detailed research. The goal was to have something that
// drifted the clock over time in one direction to simulate a slow or fast clock
// and then move it in the opposite direction to simulate ntpd or chronyd
// correcting the clock.
//
// We may instead want something more like a saw-tooth pattern where the
// correction is rather abrupt.
func NewTestClock(maxOffset time.Duration, period time.Duration, rng *rand.Rand) *testClock {
	maxAmplitude := time.Duration(float64(maxOffset / 2))
	// We don't want all the clocks moving in lock step. We do two things:
	//
	// 1. Randomly swap the initial direction of drift, and
	//
	// 2. Add a small fixed offset from the center (reducing amplitude
	// appropriately), in the direction of the swap.
	sign := 1
	if rng.Intn(2) == 1 {
		sign = -1
	}
	jitter := 0.10 * rng.Float64()
	fixedOffset := time.Duration(float64(maxAmplitude) * jitter)
	amplitude := maxAmplitude - fixedOffset
	freq := float64(2.0*math.Pi) / float64(period)
	return &testClock{
		clock:       timeutil.DefaultTimeSource{},
		sign:        sign,
		freq:        freq,
		amplitude:   amplitude,
		fixedOffset: fixedOffset,
	}
}

var _ hlc.WallClock = (*testClock)(nil)

func (c *testClock) Now() time.Time {
	//
	// f(t) = t + sign*(amplitude*sin(freq*t) + offset)
	//
	// TODO(ssd): math.Sin on every call to Now() might be expensive. One
	// alternative might be simple linear growth up to some max which then
	// reverses direction until the offset is back to zero. Or, if we like the Sin
	// wave perhaps we can store of the current offset and then only update it
	// every so many calls.
	t := c.clock.Now()
	p := math.Sin(c.freq * float64(t.UnixNano()))
	adjust := float64(c.amplitude)*p + float64(c.fixedOffset)
	adjust = float64(c.sign) * adjust
	return t.Add(time.Duration(adjust))
}

func (c *testClock) String() string {
	return fmt.Sprintf("clock[offset=%s,amp=%s]", time.Duration(c.sign)*c.fixedOffset, c.amplitude)
}
