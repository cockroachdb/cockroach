// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"org.hibernate.serialization.SessionFactorySerializationTest.testUnNamedSessionFactorySerialization": "flaky",
	"org.hibernate.serialization.SessionFactorySerializationTest.testNamedSessionFactorySerialization":   "flaky",
	"org.hibernate.test.batch.BatchTest.testBatchInsertUpdate":                                           "flaky",
}
