// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient

import "time"

// TestInstrumentation is used by tests to listen for tenant controller events.
type TestInstrumentation interface {
	Event(now time.Time, typ TestEventType)
}

// TestEventType indicates the type of an event reported through
// TestInstrumentation.
type TestEventType int

const (
	// MainLoopStarted indicates that the main loop has finished initializing the
	// controller and is waiting for work.
	MainLoopStarted TestEventType = 1 + iota

	// TickProcessed indicates that the main loop completed the processing of a
	// tick.
	TickProcessed

	// LowTokensNotification indicates that the main loop handled a "low RU"
	// notification from the token bucket.
	LowTokensNotification

	// TokenBucketResponseProcessed indicates that we have processed a
	// (successful) request to the global token bucket.
	TokenBucketResponseProcessed

	// TokenBucketResponseError indicates that a request to the global token
	// bucket has failed.
	TokenBucketResponseError
)
