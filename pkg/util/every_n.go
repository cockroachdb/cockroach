// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
)

type clockReading[T any] interface {
	comparable
	Sub(T) time.Duration
}

// EveryN provides a way to rate limit spammy events. It tracks how recently a
// given event has occurred so that it can determine whether it's worth
// handling again.
//
// The zero value for EveryN is usable and is equivalent to Every(0), meaning
// that all calls to ShouldProcess will return true.
//
// NOTE: If you specifically care about log messages, you should use the
// version of this in the log package, as it integrates with the verbosity
// flags.
type EveryN[T clockReading[T]] struct {
	// N is the minimum duration of time between log messages.
	N time.Duration

	syncutil.Mutex
	lastProcessed T
}

// Every is a convenience constructor for an EveryN object that allows a log
// message every n duration.
func Every(n time.Duration) EveryN[time.Time] {
	return EveryN[time.Time]{N: n}
}

// EveryMono is a convenience constructor for an EveryN object that allows a log
// message every n duration, expecting crtime.Mono as input.
func EveryMono(n time.Duration) EveryN[crtime.Mono] {
	return EveryN[crtime.Mono]{N: n}
}

// ShouldProcess returns whether it's been more than N time since the last event.
func (e *EveryN[T]) ShouldProcess(now T) bool {
	var shouldProcess bool
	var zero T
	e.Lock()
	if e.lastProcessed == zero || now.Sub(e.lastProcessed) >= e.N {
		shouldProcess = true
		e.lastProcessed = now
	}
	e.Unlock()
	return shouldProcess
}
