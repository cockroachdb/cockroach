// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

var liquibaseBlocklist = blocklist{
	"liquibase.harness.change.ChangeObjectTests.apply addAutoIncrement against cockroachdb 20.2":            "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply addCheckConstraint against cockroachdb 20.2":          "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply addDefaultValueSequenceNext against cockroachdb 20.2": "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply alterSequence against cockroachdb 20.2":               "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply createPackage against cockroachdb 20.2":               "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply createSequence against cockroachdb 20.2":              "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply dropCheckConstraint against cockroachdb 20.2":         "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply dropSequence against cockroachdb 20.2":                "unknown",
	"liquibase.harness.change.ChangeObjectTests.apply renameSequence against cockroachdb 20.2":              "unknown",
}

var liquibaseIgnorelist = blocklist{}
