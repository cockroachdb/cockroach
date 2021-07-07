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

var sqlAlchemyBlocklists = blocklistsForVersion{
	{"v20.2", "sqlAlchemyBlocklist20_2", sqlAlchemyBlocklist20_2, "sqlAlchemyIgnoreList20_2", sqlAlchemyIgnoreList20_2},
	{"v21.1", "sqlAlchemyBlocklist21_1", sqlAlchemyBlocklist21_1, "sqlAlchemyIgnoreList21_1", sqlAlchemyIgnoreList21_1},
	{"v21.2", "sqlAlchemyBlocklist21_2", sqlAlchemyBlocklist21_2, "sqlAlchemyIgnoreList21_2", sqlAlchemyIgnoreList21_2},
}

var sqlAlchemyBlocklist21_2 = blocklist{}

var sqlAlchemyBlocklist21_1 = blocklist{}

var sqlAlchemyBlocklist20_2 = blocklist{}

var sqlAlchemyIgnoreList21_2 = sqlAlchemyIgnoreList21_1

var sqlAlchemyIgnoreList21_1 = sqlAlchemyIgnoreList20_2

var sqlAlchemyIgnoreList20_2 = blocklist{}
