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

var pgjdbcBlacklists = blacklistsForVersion{
	{"v2.0", "pgjdbcBlackList2_0", pgjdbcBlackList2_0, "", nil},
	{"v2.1", "pgjdbcBlackList2_1", pgjdbcBlackList2_1, "", nil},
	{"v2.2", "pgjdbcBlackList19_1", pgjdbcBlackList19_1, "", nil},
	{"v19.1", "pgjdbcBlackList19_1", pgjdbcBlackList19_1, "", nil},
	{"v19.2", "pgjdbcBlackList19_2", pgjdbcBlackList19_2, "", nil},
}

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blacklist should be available
// in the test log.
var pgjdbcBlackList19_2 = blacklist{
	"org.postgresql.test.jdbc2.ClientEncodingTest.setEncodingAscii[allowEncodingChanges=true]":                                                                                 "unknown",
	"org.postgresql.test.jdbc4.ClientInfoTest.testExplicitSetAppNameNotificationIsParsed":                                                                                      "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]": "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                 "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                 "unknown",
}

var pgjdbcBlackList19_1 = blacklist{
	"org.postgresql.test.jdbc2.ClientEncodingTest.setEncodingAscii[allowEncodingChanges=true]":                                                                                 "unknown",
	"org.postgresql.test.jdbc4.ClientInfoTest.testExplicitSetAppNameNotificationIsParsed":                                                                                      "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]": "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                 "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                 "unknown",
}

var pgjdbcBlackList2_1 = blacklist{
	"org.postgresql.test.jdbc2.ClientEncodingTest.setEncodingAscii[allowEncodingChanges=true]":                                                                                 "unknown",
	"org.postgresql.test.jdbc4.ClientInfoTest.testExplicitSetAppNameNotificationIsParsed":                                                                                      "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]": "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                 "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                 "unknown",
}

var pgjdbcBlackList2_0 = blacklist{
	"org.postgresql.test.jdbc2.ClientEncodingTest.setEncodingAscii[allowEncodingChanges=true]":                                                                                 "unknown",
	"org.postgresql.test.jdbc4.ClientInfoTest.testExplicitSetAppNameNotificationIsParsed":                                                                                      "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                    "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = FORCE, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                   "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]": "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                 "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = -infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp with time zone, klass = class java.time.OffsetDateTime]":  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.LocalDateTime]":                  "unknown",
	"org.postgresql.test.jdbc42.GetObject310InfinityTests.test[binary = REGULAR, expr = infinity, pgType = timestamp, klass = class java.time.OffsetDateTime]":                 "unknown",
}
