// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var hibernateSpatialBlockList = blocklist{}

var hibernateBlockList = blocklist{}

var hibernateSpatialIgnoreList = blocklist{
	"org.hibernate.serialization.SessionFactorySerializationTest.testUnNamedSessionFactorySerialization": "flaky",
	"org.hibernate.serialization.SessionFactorySerializationTest.testNamedSessionFactorySerialization":   "flaky",
	"org.hibernate.test.batch.BatchTest.testBatchInsertUpdate":                                           "flaky",
}

var hibernateIgnoreList = blocklist{
	"org.hibernate.orm.test.batch.BatchTest.testBatchInsertUpdate":                                       "flaky",
	"org.hibernate.orm.test.bulkid.OracleInlineMutationStrategyIdTest.testDeleteFromEngineer":            "flaky",
	"org.hibernate.serialization.SessionFactorySerializationTest.testUnNamedSessionFactorySerialization": "flaky",
	"org.hibernate.serialization.SessionFactorySerializationTest.testNamedSessionFactorySerialization":   "flaky",
	"org.hibernate.test.batch.BatchTest.testBatchInsertUpdate":                                           "flaky",
}
