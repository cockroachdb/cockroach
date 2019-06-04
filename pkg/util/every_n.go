// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package util

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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
type EveryN struct {
	// N is the minimum duration of time between log messages.
	N time.Duration

	syncutil.Mutex
	lastProcessed time.Time
}

// Every is a convenience constructor for an EveryN object that allows a log
// message every n duration.
func Every(n time.Duration) EveryN {
	return EveryN{N: n}
}

// ShouldProcess returns whether it's been more than N time since the last event.
func (e *EveryN) ShouldProcess(now time.Time) bool {
	var shouldProcess bool
	e.Lock()
	if now.Sub(e.lastProcessed) >= e.N {
		shouldProcess = true
		e.lastProcessed = now
	}
	e.Unlock()
	return shouldProcess
}
