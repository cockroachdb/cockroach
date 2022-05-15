// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
	// TickProcessed indicates that the main loop completed the processing of a
	// tick.
	TickProcessed TestEventType = 1 + iota

	// LowRUNotification indicates that the main loop handled a "low RU"
	// notification from the token bucket.
	LowRUNotification

	// TokenBucketResponseProcessed indicates that we have processed a
	// (successful) request to the global token bucket.
	TokenBucketResponseProcessed

	// TokenBucketResponseError indicates that a request to the global token
	// bucket has failed.
	TokenBucketResponseError
)
