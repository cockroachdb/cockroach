// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

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

var activeRecordBlocklist = blocklist{
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_string_to_date`:                                               "unknown",
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_type_with_array`:                                              "unknown",
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_type_with_symbol`:                                             "unknown",
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_type_with_symbol_using_datetime`:                              "unknown",
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_type_with_symbol_using_datetime_with_timestamptz_as_default`:  "unknown",
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_type_with_symbol_using_timestamp_with_timestamptz_as_default`: "unknown",
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_type_with_symbol_with_timestamptz`:                            "unknown",
	`ActiveRecord::CockroachDB::Migration::PGChangeSchemaTest#test_change_type_with_symbol_with_timestamptz_as_default`:                 "unknown",
	`CompatibilityTest4_2#test_datetime_doesnt_set_precision_on_change_column`:                                                          "unknown",
	`CompatibilityTest4_2#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest5_0#test_datetime_doesnt_set_precision_on_change_column`:                                                          "unknown",
	`CompatibilityTest5_0#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest5_1#test_datetime_doesnt_set_precision_on_change_column`:                                                          "unknown",
	`CompatibilityTest5_1#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest5_2#test_datetime_doesnt_set_precision_on_change_column`:                                                          "unknown",
	`CompatibilityTest5_2#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest6_0#test_datetime_doesnt_set_precision_on_change_column`:                                                          "unknown",
	`CompatibilityTest6_0#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest6_1#test_datetime_doesnt_set_precision_on_change_column`:                                                          "unknown",
	`CompatibilityTest6_1#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest7_0#test_datetime_sets_precision_6_on_change_column`:                                                              "unknown",
	`CompatibilityTest7_0#test_options_are_not_validated`:                                                                               "unknown",
	`PostGISTest#test_point_to_json`:                                                                                                    "unknown",
}

var activeRecordIgnoreList = blocklist{
	`ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_translate_no_connection_exception_to_not_established`: "pg_terminate_backend not implemented",
}
