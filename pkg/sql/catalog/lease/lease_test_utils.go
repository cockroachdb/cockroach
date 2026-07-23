// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import "sync/atomic"

// TestingTableLeasesAreDisabled returns true if table leases have been
// disabled.
func TestingTableLeasesAreDisabled() bool {
	return testDisableTableLeases.Load()
}

var testDisableTableLeases atomic.Bool

// TestingDisableTableLeases disables table leases and returns
// a function that can be used to enable it.
func TestingDisableTableLeases() func() {
	testDisableTableLeases.Store(true)
	return func() {
		testDisableTableLeases.Store(false)
	}
}
