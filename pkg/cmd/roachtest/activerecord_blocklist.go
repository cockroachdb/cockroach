// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

var activeRecordBlocklists = blocklistsForVersion{
	{"v20.1", "activeRecordBlockList20_1", activeRecordBlockList20_1, "activeRecordIgnoreList20_1", activeRecordIgnoreList20_1},
	{"v20.2", "activeRecordBlockList20_2", activeRecordBlockList20_2, "activeRecordIgnoreList20_2", activeRecordIgnoreList20_2},
}

// These are lists of known activerecord test errors and failures.
// When the activerecord test suite is run, the results are compared to this list.
// Any passed test that is not on this list is reported as PASS - expected
// Any passed test that is on this list is reported as PASS - unexpected
// Any failed test that is on this list is reported as FAIL - expected
// Any failed test that is not on this list is reported as FAIL - unexpected
// Any test on this list that is not run is reported as FAIL - not run
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var activeRecordBlockList20_2 = blocklist{}

var activeRecordBlockList20_1 = blocklist{
	"ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_partial_index":          "9683",
	"ActiveRecord::Migration::CompatibilityTest#test_migration_does_remove_unnamed_index": "9683",
	"PostgresqlActiveSchemaTest#test_add_index":                                           "9683",
	"PostgresqlEnumTest#test_assigning_enum_to_nil":                                       "24873",
	"PostgresqlEnumTest#test_column":                                                      "24873",
	"PostgresqlEnumTest#test_enum_defaults":                                               "24873",
	"PostgresqlEnumTest#test_enum_mapping":                                                "24873",
	"PostgresqlEnumTest#test_enum_type_cast":                                              "24873",
	"PostgresqlEnumTest#test_invalid_enum_update":                                         "24873",
	"PostgresqlEnumTest#test_no_oid_warning":                                              "24873",
	"PostgresqlUUIDTest#test_add_column_with_default_array":                               "55320",
}

var activeRecordIgnoreList20_2 = blocklist{
	"FixturesTest#test_create_fixtures": "flaky - FK constraint violated sometimes when loading all fixture data",
}

var activeRecordIgnoreList20_1 = blocklist{
	"FixturesTest#test_create_fixtures": "flaky - FK constraint violated sometimes when loading all fixture data",
}
