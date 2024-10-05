// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
