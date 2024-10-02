// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

var sqlAlchemyBlocklist = blocklist{}

var sqlAlchemyIgnoreList = blocklist{
	`test/test_suite_sqlalchemy.py::IsolationLevelTest_cockroachdb+psycopg2_9_5_0::test_default_isolation_level`: "roachtest configures the default isolation level different to what the text expects",
}
