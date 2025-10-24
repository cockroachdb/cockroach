// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/crlib/crtime"
)

// EveryN provides a way to rate limit spammy log messages. It tracks how
// recently a given log message has been emitted so that it can determine
// whether it's worth logging again.
type EveryN struct {
	util.EveryN[crtime.Mono]
}

// Every is a convenience constructor for an EveryN object that allows a log
// message every n duration.
func Every(n time.Duration) EveryN {
	return EveryN{EveryN: util.EveryMono(n)}
}

// ShouldLog returns whether it's been more than N time since the last event.
func (e *EveryN) ShouldLog() bool {
	return e.shouldLog(crtime.NowMono())
}

func (e *EveryN) shouldLog(now crtime.Mono) bool {
	if VDepth(2 /* level */, 2 /* depth */) {
		// Always log when high verbosity is desired.
		return true
	}
	return e.ShouldProcess(now)
}
