// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var pgxBlocklist = blocklist{
	"v5.TestBeginReadOnly": "142043",
}

var pgxIgnorelist = blocklist{
	"v5.TestBeginIsoLevels": "We don't support isolation levels",
	"v5.TestConnCopyFromFailServerSideMidwayAbortsWithoutWaiting": "https://github.com/cockroachdb/cockroach/issues/69291#issuecomment-906898940",
	"v5.TestQueryEncodeError":                                     "This test checks the exact error message",
}
