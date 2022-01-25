// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstats

import "time"

// TestingKnobs provides hooks and knobs for unit tests.
type TestingKnobs struct {
	// OnStmtStatsFlushFinished is a callback that is triggered when stmt stats
	// finishes flushing.
	OnStmtStatsFlushFinished func()

	// OnTxnStatsFlushFinished is a callback that is triggered when txn stats
	// finishes flushing.
	OnTxnStatsFlushFinished func()

	// StubTimeNow allows tests to override the timeutil.Now() function used
	// by the flush operation to calculate aggregated_ts timestamp.
	StubTimeNow func() time.Time

	// AOSTClause overrides the AS OF SYSTEM TIME clause in queries used in
	// persistedsqlstats.
	AOSTClause string
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

// GetAOSTClause returns the appropriate AS OF SYSTEM TIME clause to be
// used when reading from statements and transactions system tables.
func (knobs *TestingKnobs) GetAOSTClause() string {
	if knobs != nil {
		return knobs.AOSTClause
	}

	return "AS OF SYSTEM TIME follower_read_timestamp()"
}
