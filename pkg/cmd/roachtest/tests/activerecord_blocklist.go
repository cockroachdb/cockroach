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
	`ActiveRecord::AdapterTestWithoutTransaction#test_create_with_query_cache`:                                                                                 "affected by autocommit_before_ddl",
	`ActiveRecord::CockroachDBStructureDumpTest#test_structure_dump`:                                                                                           "flaky",
	`ActiveRecord::ConnectionAdapters::ConnectionPoolThreadTest#test_checkout_fairness`:                                                                        "flaky",
	`ActiveRecord::ConnectionAdapters::ConnectionPoolThreadTest#test_checkout_fairness_by_group`:                                                               "flaky",
	`ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_translate_no_connection_exception_to_not_established`:                                        "pg_terminate_backend not implemented",
	`ActiveRecord::Encryption::EncryptableRecordTest#test_by_default,_it's_case_sensitive`:                                                                     "flaky",
	`ActiveRecord::Encryption::EncryptableRecordTest#test_forced_encoding_for_deterministic_attributes_will_replace_invalid_characters`:                        "flaky",
	`AssociationCallbacksTest#test_has_many_callbacks_for_destroy_on_parent`:                                                                                   "flaky",
	`BasicsTest#test_default_values_are_deeply_dupped`:                                                                                                         "flaky",
	`CascadedEagerLoadingTest#test_eager_association_loading_with_cascaded_three_levels_by_ping_pong`:                                                          "flaky",
	`CockroachDB::AdapterForeignKeyTest#test_foreign_key_violations_are_translated_to_specific_exception_with_validate_false`:                                  "flaky",
	`CockroachDB::AdapterForeignKeyTest#test_foreign_key_violations_on_delete_are_translated_to_specific_exception`:                                            "flaky",
	`CockroachDB::AdapterForeignKeyTest#test_foreign_key_violations_on_insert_are_translated_to_specific_exception`:                                            "flaky",
	`CockroachDB::FixturesTest#test_create_fixtures`:                                                                                                           "flaky",
	`FixtureWithSetModelClassPrevailsOverNamingConventionTest#test_model_class_in_fixture_file_is_respected`:                                                   "flaky",
	`InheritanceTest#test_eager_load_belongs_to_primary_key_quoting`:                                                                                           "flaky",
	`InheritanceTest#test_eager_load_belongs_to_something_inherited`:                                                                                           "flaky",
	`PostgresqlArrayTest#test_uniqueness_validation`:                                                                                                           "affected by autocommit_before_ddl",
	`PostgresqlEnumTest#test_schema_dump_renamed_enum`:                                                                                                         "affected by autocommit_before_ddl",
	`PostgresqlEnumTest#test_schema_dump_renamed_enum_with_to_option`:                                                                                          "affected by autocommit_before_ddl",
	`PostgresqlInvertibleMigrationTest#test_migrate_revert_create_enum`:                                                                                        "affected by autocommit_before_ddl",
	`PostgresqlInvertibleMigrationTest#test_migrate_revert_drop_enum`:                                                                                          "affected by autocommit_before_ddl",
	`PostgresqlInvertibleMigrationTest#test_migrate_revert_rename_enum_value`:                                                                                  "affected by autocommit_before_ddl",
	`TestAutosaveAssociationOnAHasAndBelongsToManyAssociation#test_should_not_save_and_return_false_if_a_callback_cancelled_saving_in_either_create_or_update`: "flaky",
	`TestAutosaveAssociationOnAHasAndBelongsToManyAssociation#test_should_not_update_children_when_parent_creation_with_no_reason`:                             "flaky",
	`TestAutosaveAssociationOnAHasAndBelongsToManyAssociation#test_should_update_children_when_autosave_is_true_and_parent_is_new_but_child_is_not`:            "flaky",
	`TestAutosaveAssociationOnAHasManyAssociation#test_should_not_save_and_return_false_if_a_callback_cancelled_saving_in_either_create_or_update`:             "flaky",
	`TestAutosaveAssociationOnAHasManyAssociation#test_should_not_update_children_when_parent_creation_with_no_reason`:                                         "flaky",
	`TestAutosaveAssociationOnAHasManyAssociation#test_should_update_children_when_autosave_is_true_and_parent_is_new_but_child_is_not`:                        "flaky",
}
