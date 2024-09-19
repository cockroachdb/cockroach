// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacacheutil

// TestingKnobs provides hooks into the table metadata cache job
type TestingKnobs struct {
	// onJobResume is called when the job is ready
	OnJobReady func()
	// onJobStart is called when the job starts
	OnJobStart func()
	// onJobComplete is called when the job completes
	OnJobComplete func()
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
