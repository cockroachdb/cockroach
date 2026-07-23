// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// IncrementPlpgsqlStmtCounter is to be incremented every time a new plpgsql stmt is
// used for a newly created function.
func IncrementPlpgsqlStmtCounter(stmtTag string) {
	telemetry.Inc(telemetry.GetCounter("sql.plpgsql." + stmtTag))
}
