// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	`ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_pk_and_sequence_for`:                                                  "unknown",
	`ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_pk_and_sequence_for_with_collision_pg_class_oid`:                      "unknown",
	`ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_pk_and_sequence_for_with_non_standard_primary_key`:                    "unknown",
	`ActiveRecord::PostgresqlTransactionNestedTest#test_SerializationFailure_inside_nested_SavepointTransaction_is_recoverable`:         "unknown",
	`ActiveRecord::PostgresqlTransactionNestedTest#test_deadlock_inside_nested_SavepointTransaction_is_recoverable`:                     "unknown",
	`CockroachDB::ConnectionAdapters::PostgreSQLAdapterTest#test_database_exists_returns_false_when_the_database_does_not_exist`:        "unknown",
	`CockroachDB::ConnectionAdapters::TypeTest#test_type_can_be_used_with_various_db`:                                                   "unknown",
	`CompatibilityTest4_2#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest5_0#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest5_1#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest5_2#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest6_0#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest6_1#test_options_are_not_validated`:                                                                               "unknown",
	`CompatibilityTest7_0#test_datetime_sets_precision_6_on_change_table`:                                                               "unknown",
	`CompatibilityTest7_0#test_datetime_sets_precision_6_on_create_table`:                                                               "unknown",
	`CompatibilityTest7_0#test_options_are_not_validated`:                                                                               "unknown",
	`PostGISTest#test_point_to_json`: "unknown",
	`TimestampTest#test_saving_an_unchanged_record_with_a_non_mutating_before_update_callback_does_not_update_its_timestamp`: "unknown",
}

var activeRecordIgnoreList = blocklist{}
