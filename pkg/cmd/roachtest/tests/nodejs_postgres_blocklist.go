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
	"connection timeout => releases newly connected clients if the queued already timed out":                       "flaky",
	"events => emits acquire every time a client is acquired":                                                      "flaky",
	"idle timeout => keeps old behavior when allowExitOnIdle option is not set":                                    "flaky",
	"lifetime timeout => can remove expired clients and recreate them":                                             "flaky",
	"lifetime timeout => connection lifetime should expire and remove the client after the client is done working": "flaky",
	"pool ending => ends with clients":                                                                             "flaky",
	"pool error handling => pool with lots of errors => continues to work and provide new clients":                 "flaky",
	"pool => with callbacks => removes client if it errors in background":                                          "152728",
	"pool => with callbacks => works totally unconfigured":                                                         "flaky",
	"pool size of 1 => can only send 1 query at a time":                                                            "152728",
}
