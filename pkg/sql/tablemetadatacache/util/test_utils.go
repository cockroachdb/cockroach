// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
