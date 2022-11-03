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

var liquibaseBlocklists = blocklistsForVersion{
	{"v20.2", "liquibaseBlocklist20_2", liquibaseBlocklist20_2, "liquibaseIgnorelist20_2", liquibaseIgnorelist20_2},
	{"v21.1", "liquibaseBlocklist21_1", liquibaseBlocklist21_1, "liquibaseIgnorelist21_1", liquibaseIgnorelist21_1},
	{"v21.2", "liquibaseBlocklist21_2", liquibaseBlocklist21_2, "liquibaseIgnorelist21_2", liquibaseIgnorelist21_2},
	{"v22.1", "liquibaseBlocklist22_1", liquibaseBlocklist22_1, "liquibaseIgnorelist21_2", liquibaseIgnorelist22_1},
	{"v22.2", "liquibaseBlocklist22_2", liquibaseBlocklist22_2, "liquibaseIgnorelist21_2", liquibaseIgnorelist22_2},
	{"v23.1", "liquibaseBlocklist23_1", liquibaseBlocklist23_1, "liquibaseIgnorelist23_1", liquibaseIgnorelist23_1},
}

var liquibaseBlocklist23_1 = liquibaseBlocklist22_2

var liquibaseBlocklist22_2 = liquibaseBlocklist22_1

var liquibaseBlocklist22_1 = blocklist{
	"liquibase.harness.change.ChangeObjectTests.apply addCheckConstraint against cockroachdb 20.2":  "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply createPackage against cockroachdb 20.2":       "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply dropCheckConstraint against cockroachdb 20.2": "unknown",
}

var liquibaseBlocklist21_2 = blocklist{
	"liquibase.harness.change.ChangeObjectTests.apply addDefaultValueSequenceNext against cockroachdb 20.2; verify generated SQL and DB snapshot": "",
}

var liquibaseBlocklist21_1 = liquibaseBlocklist20_2

var liquibaseBlocklist20_2 = blocklist{}

var liquibaseIgnorelist23_1 = liquibaseIgnorelist22_2

var liquibaseIgnorelist22_2 = liquibaseIgnorelist22_1

var liquibaseIgnorelist22_1 = liquibaseIgnorelist21_2

var liquibaseIgnorelist21_2 = liquibaseIgnorelist21_1

var liquibaseIgnorelist21_1 = liquibaseIgnorelist20_2

var liquibaseIgnorelist20_2 = blocklist{}
