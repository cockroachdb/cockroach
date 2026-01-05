// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

var liquibaseBlocklist = blocklist{}

var liquibaseIgnorelist = blocklist{
	`ChangeObjectTests.apply addCheckConstraint against cockroachdb 24.1`:  "requires enterprise version of liquibase",
	`ChangeObjectTests.apply createPackage against cockroachdb 24.1`:       "requires enterprise version of liquibase",
	`ChangeObjectTests.apply dropCheckConstraint against cockroachdb 24.1`: "requires enterprise version of liquibase",
}
