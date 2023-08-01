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

var hibernateBlockList = blocklist{
	"org.hibernate.jpa.test.graphs.FetchGraphTest.testCollectionEntityGraph":                                                "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testConfiguration":                                          "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testDefaultPar":                                             "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testDefaultParForPersistence_1_0":                           "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testExcludeHbmPar":                                          "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testExplodedPar":                                            "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testExtendedEntityManager":                                  "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testExternalJar":                                            "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testListeners":                                              "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testListenersDefaultPar":                                    "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testORMFileOnMainAndExplicitJars":                           "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testRelativeJarReferences":                                  "unknown",
	"org.hibernate.jpa.test.packaging.PackagedEntityManagerTest.testSpacePar":                                               "unknown",
	"org.hibernate.jpa.test.packaging.ScannerTest.testCustomScanner":                                                        "unknown",
	"org.hibernate.test.bytecode.enhancement.lazy.proxy.inlinedirtychecking.DirtyCheckPrivateUnMappedCollectionTest.testIt": "unknown",
	"org.hibernate.test.hql.BulkManipulationTest.testUpdateWithSubquery":                                                    "unknown",
}

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
