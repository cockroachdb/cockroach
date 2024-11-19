// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

var (
	// RowLevelTTLCreated is incremented when a row level TTL table is created.
	RowLevelTTLCreated = telemetry.GetCounterOnce("sql.row_level_ttl.created")

	// RowLevelTTLDropped is incremented when a row level TTL has been dropped
	// from a table.
	RowLevelTTLDropped = telemetry.GetCounterOnce("sql.row_level_ttl.dropped")

	// RowLevelTTLExecuted is incremented when a row level TTL job has executed.
	RowLevelTTLExecuted = telemetry.GetCounterOnce("sql.row_level_ttl.job_executed")
)
