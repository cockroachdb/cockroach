// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

var psycopgBlocklists = blocklistsForVersion{
	{"v20.2", "psycopgBlockList20_2", psycopgBlockList20_2, "psycopgIgnoreList20_2", psycopgIgnoreList20_2},
	{"v21.1", "psycopgBlockList21_1", psycopgBlockList21_1, "psycopgIgnoreList21_1", psycopgIgnoreList21_1},
	{"v21.2", "psycopgBlockList21_2", psycopgBlockList21_2, "psycopgIgnoreList21_2", psycopgIgnoreList21_2},
	{"v22.1", "psycopgBlockList22_1", psycopgBlockList22_1, "psycopgIgnoreList22_1", psycopgIgnoreList22_1},
}

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
var psycopgBlockList22_1 = blocklist{
	// The following item can be removed once there is a new psycopg2 release.
	"tests.test_module.ExceptionsTestCase.test_9_6_diagnostics": "58035",
}

var psycopgBlockList21_2 = blocklist{
	"tests.test_async_keyword.CancelTests.test_async_cancel": "41335",
	// The following two items can be removed once there is a new psycopg2 release.
	"tests.test_connection.TestEncryptPassword.test_encrypt_server": "42519",
	"tests.test_module.ExceptionsTestCase.test_9_6_diagnostics":     "58035",
}

var psycopgBlockList21_1 = blocklist{
	"tests.test_async_keyword.CancelTests.test_async_cancel": "41335",
	// The following two items can be removed once there is a new psycopg2 release.
	"tests.test_module.ExceptionsTestCase.test_9_6_diagnostics": "58035",
}

var psycopgBlockList20_2 = blocklist{
	"tests.test_async_keyword.CancelTests.test_async_cancel": "41335",
}

var psycopgIgnoreList22_1 = psycopgIgnoreList21_2

var psycopgIgnoreList21_2 = psycopgIgnoreList21_1

var psycopgIgnoreList21_1 = psycopgIgnoreList20_2

var psycopgIgnoreList20_2 = blocklist{
	"tests.test_async.AsyncTests.test_flush_on_write":           "44709",
	"tests.test_green.GreenTestCase.test_flush_on_write":        "flakey",
	"tests.test_connection.TestConnectionInfo.test_backend_pid": "we return -1 for pg_backend_pid()",
}
