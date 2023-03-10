// Copyright 2023 The Cockroach Authors.
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

// PlpgsqlStmtCounter is to be incremented every time a new plpgsql stmt is
// used for a newly created function.
// TODO: check the naming of the counter
func PlpgsqlStmtCounter(stmtTag string) telemetry.Counter {
	return telemetry.GetCounter("sql.plpgsql." + stmtTag)
}
