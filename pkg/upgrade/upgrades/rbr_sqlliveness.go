// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

// useRbrSqllivenessDescriptor is a precondition for the
// CleanupRbtSqllivenessIndex version gate.
func useRbrSqllivenessDescriptor() error {
	// DO NOT SUBMIT(jeffswenson): implement the migration

	// Step 1: Wait for all non-regional sessions to expire. The non-regional
	// sessions have no representation in the Rbr index.

	// Step 2: Replace the RBT sqlliveness descriptor with the RBR sqlliveness
	// descriptor. At this point the RBT descriptor is valid for reads.
	// slstorage.Table dual writes regional sessions to the RBT and RBR
	// indexes.

	return nil
}
