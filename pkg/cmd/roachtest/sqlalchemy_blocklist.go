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

var sqlAlchemyBlocklists = blocklistsForVersion{
	{"v2.1", "sqlAlchemyBlocklist", sqlAlchemyBlocklist, "sqlAlchemyIgnoreList", sqlAlchemyIgnoreList},
	{"v19.1", "sqlAlchemyBlocklist", sqlAlchemyBlocklist, "sqlAlchemyIgnoreList", sqlAlchemyIgnoreList},
	{"v19.2", "sqlAlchemyBlocklist", sqlAlchemyBlocklist, "sqlAlchemyIgnoreList", sqlAlchemyIgnoreList},
	{"v20.1", "sqlAlchemyBlocklist20_1", sqlAlchemyBlocklist20_1, "sqlAlchemyIgnoreList20_1", sqlAlchemyIgnoreList20_1},
	{"v20.2", "sqlAlchemyBlocklist20_2", sqlAlchemyBlocklist20_2, "sqlAlchemyIgnoreList20_2", sqlAlchemyIgnoreList20_2},
}

var sqlAlchemyBlocklist20_2 = blocklist{}

var sqlAlchemyBlocklist20_1 = blocklist{
	"test/dialect/test_suite.py::ExpandingBoundInTest_cockroachdb+psycopg2_9_5_0::test_null_in_empty_set_is_false": "41596",
}

var sqlAlchemyBlocklist = blocklist{
	"test/dialect/test_suite.py::ExpandingBoundInTest_cockroachdb+psycopg2_9_5_0::test_null_in_empty_set_is_false": "41596",
}

var sqlAlchemyIgnoreList20_2 = sqlAlchemyIgnoreList

var sqlAlchemyIgnoreList20_1 = sqlAlchemyIgnoreList

var sqlAlchemyIgnoreList = blocklist{
	"test/dialect/test_suite.py::ComponentReflectionTest_cockroachdb+psycopg2_9_5_0::test_deprecated_get_primary_keys": "test has a bug and is getting removed",
}
