// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TimeSource is used to interact with clocks and timers. Generally exposed for
// testing.
type TimeSource interface {
	Now() time.Time
	NewTimer() Timer
}

// Timer is an interface wrapping timeutil.Timer.
type Timer interface {
	Reset(duration time.Duration)
	Stop() bool
	Ch() <-chan time.Time
	MarkRead()
}

type defaultTimeSource struct{}

var _ TimeSource = defaultTimeSource{}

func (defaultTimeSource) Now() time.Time {
	return timeutil.Now()
}

func (d defaultTimeSource) NewTimer() Timer {
	return (*timer)(timeutil.NewTimer())
}

type timer timeutil.Timer

var _ Timer = (*timer)(nil)

func (t *timer) Reset(duration time.Duration) {
	(*timeutil.Timer)(t).Reset(duration)
}

func (t *timer) Stop() bool {
	return (*timeutil.Timer)(t).Stop()
}

func (t *timer) Ch() <-chan time.Time {
	return t.C
}

func (t *timer) MarkRead() {
	t.Read = true
}
