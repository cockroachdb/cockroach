// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

// These are lists of known activerecord test errors and failures.
// When the activerecord test suite is run, the results are compared to this list.
// Any passed test that is not on this list is reported as PASS - expected
// Any passed test that is on this list is reported as PASS - unexpected
// Any failed test that is on this list is reported as FAIL - expected
// Any failed test that is not on this list is reported as FAIL - unexpected
// Any test on this list that is not run is reported as FAIL - not run
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var activeRecordBlocklist = blocklist{}

var activeRecordIgnorelist = blocklist{
	"CockroachDB::PostgresqlIntervalTest#test_interval_type":               "flaky",
	"ConcurrentTransactionTest#test_transaction_isolation__read_committed": "flaky - https://github.com/cockroachdb/activerecord-cockroachdb-adapter/issues/237",
	"FixturesTest#test_create_fixtures":                                    "flaky - FK constraint violated sometimes when loading all fixture data",
	"IgnoreFixturesTest#test_ignores_books_fixtures":                       "flaky - FK constraint violated sometimes when loading all fixture data",
	"IgnoreFixturesTest#test_ignores_parrots_fixtures":                     "flaky - FK constraint violated sometimes when loading all fixture data",
	"PostgresqlIntervalTest#test_interval_type":                            "flaky",
}
