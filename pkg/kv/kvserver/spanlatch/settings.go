// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package concurrency provides a concurrency manager structure that
// encapsulates the details of concurrency control and contention handling for
// serializable key-value transactions.

package spanlatch

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// LongLatchHoldThreshold controls when we will log latch holds.
var LongLatchHoldThreshold = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.concurrency.long_latch_hold_duration",
	"the threshold for logging long latch holds",
	3*time.Second,
)

// SlowLatchRequestThreshold controls when we will log slow latch acquisition
// attempts. When a latch acquisition has been waiting for this duration, a
// warning is logged and the slow request metric is incremented.
var SlowLatchRequestThreshold = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"kv.concurrency.slow_latch_request_duration",
	"the threshold for logging slow latch acquisition attempts",
	5*time.Second,
	settings.DurationWithMinimum(10*time.Millisecond),
)
