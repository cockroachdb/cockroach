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
	// OnStatsFlushFinished is a callback that is triggered when a single
	// statistics object is flushed.
	OnStatsFlushFinished func(error)

	// StubTimeNow allows tests to override the timeutil.Now() function used
	// by the flush operation to calculate aggregated_ts timestamp.
	StubTimeNow func() time.Time

	// AOSTClause overrides the AS OF SYSTEM TIME clause in queries used in
	// persistedsqlstats.
	AOSTClause string
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
