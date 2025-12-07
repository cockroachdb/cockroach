// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
//
// Note: For Mocha tests organized in suites, test names are reported as
// "suite => subsuite => ... => testcase" (e.g., "pool => with callbacks => removes client...").
// Use the full hierarchical path in the blocklist/ignorelist.
var nodePostgresBlockList = blocklist{}

var nodePostgresIgnoreList = blocklist{
	"events => emits acquire every time a client is acquired":             "flaky",
	"pool => with callbacks => removes client if it errors in background": "152728",
	"pool size of 1 => can only send 1 query at a time":                   "152728",
}
