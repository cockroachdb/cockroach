// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacacheutil

import "context"

// TestingKnobs provides hooks into the table metadata cache job
type TestingKnobs struct {
	// onJobResume is called when the job is ready
	OnJobReady func()
	// onJobStart is called when the job starts
	OnJobStart func()
	// onJobComplete is called when the job completes
	OnJobComplete func()
	// aostClause overrides the AS OF SYSTEM TIME clause in queries used in
	// table metadata update job.
	aostClause string
	// TableMetadataUpdater overrides the ITableMetadataUpdater used in
	// tableMetadataUpdateJobResumer.Resume
	TableMetadataUpdater ITableMetadataUpdater
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

// GetAOSTClause returns the appropriate AS OF SYSTEM TIME clause to be
// used when reading from tables in the job run
func (knobs *TestingKnobs) GetAOSTClause() string {
	if knobs != nil {
		return knobs.aostClause
	}

	return "AS OF SYSTEM TIME follower_read_timestamp()"
}

// CreateTestingKnobs creates a testing knob in the unit tests.
//
// Note: The table metadata update job uses follower read (AS OF SYSTEM TIME
// follower_read_timestamp()) to ensure that contention between reads and writes
// is minimized.
//
// However, in a new cluster in unit tests, system tables are created using the
// migration framework. The migration framework goes through a list of
// registered migrations and creates the stats system tables. By using follower
// read, we shift the transaction read timestamp far enough to the past. This
// means it is possible in the unit tests, the read timestamp would be chosen to
// be before the creation of the stats table. This can cause 'descriptor not
// found' error when accessing the system tables.
//
// Additionally, we don't want to completely remove the AOST clause in the unit
// test. Therefore, `AS OF SYSTEM TIME '-1us'` is a compromise used to get
// around the 'descriptor not found' error.
func CreateTestingKnobs() *TestingKnobs {
	return &TestingKnobs{
		aostClause: "AS OF SYSTEM TIME '-1us'",
	}
}

// NoopUpdater is an implementation of ITableMetadataUpdater that performs a noop when RunUpdater is called.
// This should only be used in tests when the updating of the table_metadata system table isn't necessary.
type NoopUpdater struct{}

func (nu *NoopUpdater) RunUpdater(_ctx context.Context) error {
	return nil
}

var _ ITableMetadataUpdater = &NoopUpdater{}
