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

var sqlAlchemyBlocklist = blocklist{}

var sqlAlchemyIgnoreList = blocklist{
	`test/test_suite_sqlalchemy.py::IsolationLevelTest_cockroachdb+psycopg2_9_5_0::test_default_isolation_level`: "roachtest configures the default isolation level different to what the text expects",
}
