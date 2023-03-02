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

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var pgxBlocklist = blocklist{}

var pgxIgnorelist = blocklist{
	"v5.TestBeginIsoLevels": "We don't support isolation levels",
	"v5.TestConnCopyFromFailServerSideMidwayAbortsWithoutWaiting": "https://github.com/cockroachdb/cockroach/issues/69291#issuecomment-906898940",
	"v5.TestQueryEncodeError":                                     "This test checks the exact error message",
}
