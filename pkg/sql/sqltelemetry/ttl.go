// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
