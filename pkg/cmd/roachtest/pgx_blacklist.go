// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

var pgxBlacklists = blacklistsForVersion{
	{"v4.1.2", "pgxBlacklist20_1", pgxBlackList20_1, "", nil},
}

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blacklist should be available
// in the test log.
var pgxBlackList20_1 = blacklist{
}
