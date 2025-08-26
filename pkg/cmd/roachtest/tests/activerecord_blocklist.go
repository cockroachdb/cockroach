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

var activeRecordIgnoreList = blocklist{
	`ActiveRecord::CockroachDBStructureDumpTest#test_structure_dump`:                                                                                           "flaky",
	`ActiveRecord::ConnectionAdapters::ConnectionPoolThreadTest#test_checkout_fairness_by_group`:                                                               "flaky",
	`ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_translate_no_connection_exception_to_not_established`:                                        "pg_terminate_backend not implemented",
	`ActiveRecord::Encryption::EncryptableRecordTest#test_by_default,_it's_case_sensitive`:                                                                     "flaky",
	`ActiveRecord::Encryption::EncryptableRecordTest#test_forced_encoding_for_deterministic_attributes_will_replace_invalid_characters`:                        "flaky",
	`ActiveRecord::Encryption::UniquenessValidationsTest#test_uniqueness_validations_work_when_using_old_encryption_schemes`:                                   "flaky",
	`AssociationCallbacksTest#test_has_many_callbacks_for_destroy_on_parent`:                                                                                   "flaky",
	`BasicsTest#test_default_values_are_deeply_dupped`:                                                                                                         "flaky",
	`CockroachDB::FixturesTest#test_create_fixtures`:                                                                                                           "flaky",
	`FixtureWithSetModelClassPrevailsOverNamingConventionTest#test_model_class_in_fixture_file_is_respected`:                                                   "flaky",
	`HasManyAssociationsTest#test_collection_association_with_private_kernel_method`:                                                                           "flaky",
	`HasManyAssociationsTest#test_delete_all_association_with_primary_key_deletes_correct_records`:                                                             "flaky",
	`HasManyAssociationsTest#test_dependence`:                                                                                                                  "flaky",
	`HasManyAssociationsTest#test_dependence_on_account`:                                                                                                       "flaky",
	`HasManyAssociationsTest#test_dependent_association_respects_optional_conditions_on_delete`:                                                                "flaky",
	`HasManyAssociationsTest#test_dependent_association_respects_optional_hash_conditions_on_delete`:                                                           "flaky",
	`HasManyAssociationsTest#test_dependent_association_respects_optional_sanitized_conditions_on_delete`:                                                      "flaky",
	`HasManyAssociationsTest#test_depends_and_nullify`:                                                                                                         "flaky",
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
