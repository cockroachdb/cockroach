// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tick

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var now = timeutil.Now

// TestingSetNow changes the clock used by the metric system. For use by
// testing to precisely control the clock.
//
// NB: metric.TestingSetNow calls into this, so it's preferable to use that
// function instead if you're working on things related to the metric package.
//
// NB: aggmetric.TestingSetNow also calls into this, so again, it's preferable
// to use that function instead if you're working on things related to the
// aggmetric package.
//
// TODO(obs): I know this is janky. It's temporary. An upcoming patch will
// merge this package, pkg/util/metric, and pkg/util/aggmetric, after which
// we can get rid of all this TestingSetNow chaining.
func TestingSetNow(f func() time.Time) func() {
	origNow := now
	now = f
	return func() {
		now = origNow
	}
}

// Periodic is an interface used to control periodic "ticking" behavior.
// "Ticking" is triggered via the Tick function, and can be any arbitrary
// function.
type Periodic interface {
	// NextTick returns the timestamp for the next scheduled tick.
	NextTick() time.Time
	Tick()
}

// MaybeTick will tick the provided Periodic, if we're past its
// NextTick() time.
func MaybeTick(m Periodic) {
	for m.NextTick().Before(now()) {
		m.Tick()
	}
}

// Ticker is used by metrics that are at heart cumulative, but wish to also
// maintain a windowed version to work around limitations of our internal
// timeseries database.
type Ticker struct {
	nextT        atomic.Value
	tickInterval time.Duration

	onTick func()
}

var _ Periodic = &Ticker{}

// NewTicker returns a new *Ticker instance.
func NewTicker(nextT time.Time, tickInterval time.Duration, onTick func()) *Ticker {
	t := &Ticker{
		tickInterval: tickInterval,
		onTick:       onTick,
	}
	t.nextT.Store(nextT)
	return t
}

// OnTick calls the onTick function provided at construction.
// It does not update the next tick timestamp, and therefore should be used with
// caution.
//
// Consider using MaybeTick or Tick instead. Sometimes callers want
// to do an initial tick immediately after construction, which is where this
// function becomes useful.
func (s *Ticker) OnTick() {
	s.onTick()
}

// NextTick returns the timestamp of the next scheduled tick for this Ticker.
func (s *Ticker) NextTick() time.Time {
	return s.nextT.Load().(time.Time)
}

// Tick updates the next tick timestamp to the next tickInterval, and invokes
// the onTick function provided at construction.
//
// NB: Generally, MaybeTick should be used instead to ensure that we don't tick
// before nextT. This function is only used when we want to tick regardless
func (s *Ticker) Tick() {
	s.nextT.Store(s.nextT.Load().(time.Time).Add(s.tickInterval))
	s.OnTick()
}
