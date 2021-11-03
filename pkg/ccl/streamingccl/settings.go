// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// DefaultJobLivenessTrackingFrequency is the default frequency to check
// the liveness of a streaming replication producer job.
var DefaultJobLivenessTrackingFrequency = 1 * time.Minute

// TestingSetDefaultJobLivenessTrackingFrequency changes DefaultJobLivenessTrackingFrequency for tests.
// Returns function to restore the frequency to its original value.
func TestingSetDefaultJobLivenessTrackingFrequency(f time.Duration) func() {
	old := DefaultJobLivenessTrackingFrequency
	DefaultJobLivenessTrackingFrequency = f
	return func() { DefaultJobLivenessTrackingFrequency = old }
}

// MinProtectedTimestampUpdateAdvance specifies the minimum amount of time a
// stream replication's protected timestamp must advance for it to be updated.
var MinProtectedTimestampUpdateAdvance = 10 * time.Minute

// TestingMinProtectedTimestampUpdateAdvance changes MinProtectedTimestampUpdateAdvance for tests.
// Returns function to restore it to its original value.
func TestingMinProtectedTimestampUpdateAdvance(f time.Duration) func() {
	old := MinProtectedTimestampUpdateAdvance
	MinProtectedTimestampUpdateAdvance = f
	return func() { MinProtectedTimestampUpdateAdvance = old }
}

// StreamReplicationJobLivenessTimeout controls how long we wait for to kill
// an inactive producer job.
var StreamReplicationJobLivenessTimeout = settings.RegisterDurationSetting(
	"stream_replication.job_liveness_timeout",
	"controls how long we wait for to kill an inactive producer job",
	time.Minute,
)
