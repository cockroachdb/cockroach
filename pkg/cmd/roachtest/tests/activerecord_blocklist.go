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

var activeRecordBlocklists = blocklistsForVersion{
	{"v20.2", "activeRecordBlockList20_2", activeRecordBlockList20_2, "activeRecordIgnoreList20_2", activeRecordIgnoreList20_2},
	{"v21.1", "activeRecordBlockList21_1", activeRecordBlockList21_1, "activeRecordIgnoreList21_1", activeRecordIgnoreList21_1},
	{"v21.2", "activeRecordBlockList21_2", activeRecordBlockList21_2, "activeRecordIgnoreList21_2", activeRecordIgnoreList21_2},
}

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
var activeRecordIgnoreList21_2 = blocklist{
	"ConcurrentTransactionTest#test_transaction_isolation__read_committed": "flaky - https://github.com/cockroachdb/activerecord-cockroachdb-adapter/issues/237",
}

var activeRecordIgnoreList21_1 = blocklist{}

var activeRecordIgnoreList20_2 = blocklist{}
