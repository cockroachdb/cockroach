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
	`PostGISTest#test_point_to_json`: "unknown",
}

var activeRecordIgnoreList = blocklist{
	`ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_translate_no_connection_exception_to_not_established`:                                        "pg_terminate_backend not implemented",
	`BasicsTest#test_default_values_are_deeply_dupped`:                                                                                                         "flaky",
	`TestAutosaveAssociationOnAHasAndBelongsToManyAssociation#test_should_not_save_and_return_false_if_a_callback_cancelled_saving_in_either_create_or_update`: "flaky",
	`TestAutosaveAssociationOnAHasAndBelongsToManyAssociation#test_should_not_update_children_when_parent_creation_with_no_reason`:                             "flaky",
	`TestAutosaveAssociationOnAHasAndBelongsToManyAssociation#test_should_update_children_when_autosave_is_true_and_parent_is_new_but_child_is_not`:            "flaky",
	`CockroachDB::FixturesTest#test_create_fixtures`:                                                                                                           "flaky",
}
