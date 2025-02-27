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
var psycopgBlockList = blocklist{
	// This test fails because "defaultdb.public.test551" already exists. An
	// earlier test creates the same table. However, our autocommit_before_ddl
	// behavior commits that transaction, whereas it was expected to roll back
	// when the test ended.
	"tests.test_connection.SignalTestCase.test_bug_551_returning": "142047",
	// These tests fail with "DECLARE CURSOR can only be used in transaction blocks".
	// This happens because of our autocommit_before_ddl behavior. They all
	// create a table, which closes the existing transaction, causing the
	// above error to occur when DECLARE CURSOR is used.
	"tests.test_cursor.NamedCursorTests.test_invalid_name":   "142047",
	"tests.test_cursor.NamedCursorTests.test_not_scrollable": "142047",
	// The `*executemany*` tests fail because they attempt to create a
	// table that already exists. This is due to our auto-commit behavior before DDL.
	// Each failing test creates the same table in a transaction, performs the test,
	// and then rolls back the transaction. However, we end up committing the first
	// `CREATE TABLE`, causing all subsequent tests that follow the same pattern to fail.
	"tests.test_fast_executemany.TestExecuteBatch.test_empty":           "142047",
	"tests.test_fast_executemany.TestExecuteBatch.test_many":            "142047",
	"tests.test_fast_executemany.TestExecuteBatch.test_one":             "142047",
	"tests.test_fast_executemany.TestExecuteBatch.test_pages":           "142047",
	"tests.test_fast_executemany.TestExecuteBatch.test_tuples":          "142047",
	"tests.test_fast_executemany.TestExecuteBatch.test_unicode":         "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_composed":       "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_dicts":          "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_empty":          "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_invalid_sql":    "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_many":           "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_one":            "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_pages":          "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_percent_escape": "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_returning":      "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_tuples":         "142047",
	"tests.test_fast_executemany.TestExecuteValues.test_unicode":        "142047",
	"tests.test_sql.SqlFormatTests.test_executemany":                    "142047",
}

var psycopgIgnoreList = blocklist{
	"tests.test_async.AsyncTests.test_flush_on_write":           "44709",
	"tests.test_green.GreenTestCase.test_flush_on_write":        "flakey",
	"tests.test_connection.TestConnectionInfo.test_backend_pid": "we return -1 for pg_backend_pid()",
}
