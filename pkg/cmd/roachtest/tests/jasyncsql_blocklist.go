// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

var jasyncsqlBlocklists = blocklistsForVersion{
	{"v22.1", "jasyncsqlBlocklist22_1", jasyncBlocklist22_1, "jasyncsqlIgnoreList22_1", jasyncsqlIgnoreList22_1},
}

var jasyncBlocklist22_1 = blocklist{}

var jasyncsqlIgnoreList22_1 = blocklist{
	"com.github.aysnc.sql.db.integration.PreparedStatementSpec":                  "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.NumericSpec":                            "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.TransactionSpec":                        "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.pool.SingleThreadedAsyncObjectPoolSpec": "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.pool.NextGenConnectionPoolSpec":         "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.pool.ActorAsyncObjectPoolSpec":          "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.LoginSpec":                              "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.ArrayTypesSpec":                         "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.PostgreSQLSSLConnectionSpec":            "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.ListenNotifySpec":                       "expected fail - checks error message",
	"com.github.aysnc.sql.db.integration.PostgreSQLConnectionSpec":               "expected fail - checks error message",
}
