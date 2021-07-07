// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

var pgxBlocklists = blocklistsForVersion{
	{"v20.2", "pgxBlocklist20_2", pgxBlocklist20_2, "pgxIgnorelist20_2", pgxIgnorelist20_2},
	{"v21.1", "pgxBlocklist21_1", pgxBlocklist21_1, "pgxIgnorelist21_1", pgxIgnorelist21_1},
	{"v21.2", "pgxBlocklist21_2", pgxBlocklist21_2, "pgxIgnorelist21_2", pgxIgnorelist21_2},
}

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var pgxBlocklist21_2 = blocklist{}

var pgxBlocklist21_1 = blocklist{}

var pgxBlocklist20_2 = blocklist{}

var pgxIgnorelist21_2 = pgxIgnorelist21_1

var pgxIgnorelist21_1 = pgxIgnorelist20_2

var pgxIgnorelist20_2 = blocklist{
	"v4.TestBeginIsoLevels":   "We don't support isolation levels",
	"v4.TestQueryEncodeError": "This test checks the exact error message",
}
