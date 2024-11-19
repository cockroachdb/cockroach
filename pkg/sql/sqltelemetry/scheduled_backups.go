// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// ScheduledBackupControlCounter is to be incremented every time a scheduled job
// control action is taken.
func ScheduledBackupControlCounter(desiredStatus string) telemetry.Counter {
	return telemetry.GetCounter("sql.backup.scheduled.job.control." + desiredStatus)
}
