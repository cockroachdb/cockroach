// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

// TestingTableLeasesAreDisabled returns true if table leases have been
// disabled.
func TestingTableLeasesAreDisabled() bool {
	return testDisableTableLeases
}

var testDisableTableLeases bool

// TestingDisableTableLeases disables table leases and returns
// a function that can be used to enable it.
func TestingDisableTableLeases() func() {
	testDisableTableLeases = true
	return func() {
		testDisableTableLeases = false
	}
}
