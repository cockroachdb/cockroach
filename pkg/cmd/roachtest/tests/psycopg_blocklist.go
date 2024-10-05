// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

// These are lists of known psycopg test errors and failures.
// When the psycopg test suite is run, the results are compared to this list.
// Any passed test that is not on this list is reported as PASS - expected
// Any passed test that is on this list is reported as PASS - unexpected
// Any failed test that is on this list is reported as FAIL - expected
// Any failed test that is not on this list is reported as FAIL - unexpected
// Any test on this list that is not run is reported as FAIL - not run
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var psycopgBlockList = blocklist{}

var psycopgIgnoreList = blocklist{
	"tests.test_async.AsyncTests.test_flush_on_write":           "44709",
	"tests.test_green.GreenTestCase.test_flush_on_write":        "flakey",
	"tests.test_connection.TestConnectionInfo.test_backend_pid": "we return -1 for pg_backend_pid()",
}
