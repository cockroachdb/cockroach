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

	// OnCleanupStartForShard is a callback that is triggered when background
	// cleanup job starts to delete data from a shard from the system table.
	OnCleanupStartForShard func(shardIdx int, existingCountInShard, shardLimit int64)

	// StubTimeNow allows tests to override the timeutil.Now() function used
	// by the flush operation to calculate aggregated_ts timestamp.
	StubTimeNow func() time.Time

	// aostClause overrides the AS OF SYSTEM TIME clause in queries used in
	// persistedsqlstats.
	aostClause string

	// JobMonitorUpdateCheckInterval if non-zero indicates the frequency at
	// which the job monitor needs to check whether the schedule needs to be
	// updated.
	JobMonitorUpdateCheckInterval time.Duration

	// SkipZoneConfigBootstrap used for backup tests where we want to skip
	// the Zone Config TTL setup.
	SkipZoneConfigBootstrap bool
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

// GetAOSTClause returns the appropriate AS OF SYSTEM TIME clause to be
// used when reading from statements and transactions system tables.
func (knobs *TestingKnobs) GetAOSTClause() string {
	if knobs != nil {
		return knobs.aostClause
	}

	return "AS OF SYSTEM TIME follower_read_timestamp()"
}

// CreateTestingKnobs creates a testing knob in the unit tests.
//
// Note: SQL Stats’s read path uses follower read (AS OF SYSTEM TIME
// follower_read_timestamp()) to ensure that contention between reads and writes
// (SQL Stats flush / SQL Stats cleanup) is minimized.
//
// However, in a new cluster in unit tests, system tables are created using the
// migration framework. The migration framework goes through a list of
// registered migrations and creates the stats system tables. By using follower
// read, we shift the transaction read timestamp far enough to the past. This
// means it is possible in the unit tests, the read timestamp would be chosen to
// be before the creation of the stats table. This can cause 'descriptor not
// found' error when accessing the stats system table.
//
// Additionally, we don't want to completely remove the AOST clause in the unit
// test. Therefore, `AS OF SYSTEM TIME '-1us'` is a compromise used to get
// around the 'descriptor not found' error.
func CreateTestingKnobs() *TestingKnobs {
	return &TestingKnobs{
		aostClause: "AS OF SYSTEM TIME '-1us'",
	}
}
