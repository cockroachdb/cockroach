// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

// These are lists of known rust-sqlx test errors and failures.
// When the rust-sqlx test suite is run, the results are compared to this list.
// Any failed test that is on this list is reported as FAIL - expected.
// Any failed test that is not on this list is reported as FAIL - unexpected.
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var rustSqlxBlocklist = blocklist{
	`copy_can_work_with_failed_transactions`:              "test is #[ignore]d in sqlx source",
	`it_can_abort_copy_in`:                                "default int size: expressions return INT8 instead of INT4",
	`it_can_copy_in`:                                      "default int size: expressions return INT8 instead of INT4",
	`it_can_copy_out`:                                     "default int size: expressions return INT8 instead of INT4",
	`it_can_inspect_constraint_errors`:                    "missing fixture: relation \"products\" does not exist",
	`it_can_inspect_errors`:                               "error position field not populated by CockroachDB",
	`it_can_nest_map`:                                     "default int size: expressions return INT8 instead of INT4",
	`it_can_prepare_then_execute`:                         "missing fixture: relation \"tweet\" does not exist",
	`it_can_query_scalar`:                                 "default int size: expressions return INT8 instead of INT4",
	`it_can_recover_from_copy_in_empty_query`:             "default int size: expressions return INT8 instead of INT4",
	`it_can_recover_from_copy_in_invalid_params`:          "default int size: expressions return INT8 instead of INT4",
	`it_can_recover_from_copy_in_syntax_error`:            "default int size: expressions return INT8 instead of INT4",
	`it_can_recover_from_copy_in_to_missing_table`:        "default int size: expressions return INT8 instead of INT4",
	`it_can_select_void`:                                  "unsupported function: pg_notify()",
	`it_can_work_with_failed_transactions`:                "default int size: expressions return INT8 instead of INT4",
	`it_closes_statements_when_not_persistent_issue_3850`: "prepared statement lifecycle differs from PostgreSQL",
	`it_connects`:                                         "default int size: expressions return INT8 instead of INT4",
	`it_describes_composite`:                              "missing fixture: type \"inventory_item\" does not exist",
	`it_describes_enum`:                                   "missing fixture: type \"status\" does not exist",
	`it_describes_simple`:                                 "missing fixture: relation \"tweet\" does not exist",
	`it_fails_with_begin_failed`:                          "missing fixture: relation \"tweet\" does not exist",
	`it_fails_with_check_violation`:                       "constraint violation error classification differs from PostgreSQL",
	`it_fails_with_foreign_key_violation`:                 "constraint violation error classification differs from PostgreSQL",
	`it_fails_with_not_null_violation`:                    "constraint violation error classification differs from PostgreSQL",
	`it_fails_with_unique_violation`:                      "missing fixture: relation \"tweet\" does not exist",
	`it_maths`:                                            "default int size: expressions return INT8 instead of INT4",
	`it_resolves_custom_types_in_anonymous_records`:       "error message text differs from PostgreSQL",
	`it_supports_domain_types_in_composite_domain_types`:  "unsupported PL/pgSQL syntax: DO $$ IF NOT EXISTS ... $$",
	`pool_smoke_test`:                                     "test is #[ignore]d in sqlx source",
	`test_advisory_locks`:                                 "unsupported function: pg_advisory_lock()",
	`test_describe_outer_join_nullable`:                   "missing fixture: relation \"tweet\" does not exist",
	`test_error_handling_with_deferred_constraints`:       "unsupported feature: DEFERRABLE constraints",
	`test_invalid_query`:                                  "default int size: expressions return INT8 instead of INT4",
	`test_listener_cleanup`:                               "unsupported feature: LISTEN/NOTIFY",
	`test_listener_try_recv_buffered`:                     "unsupported feature: LISTEN/NOTIFY",
	`test_multi_read_write`:                               "unique_rowid() produces different values than PostgreSQL SERIAL",
	`test_pg_copy_chunked`:                                "missing fixture: relation \"products\" does not exist",
	`test_pg_listener_implements_acquire`:                 "unsupported feature: LISTEN/NOTIFY",
	`test_prepared_decode_type_num_tuple`:                 "default int size: expressions return INT8 instead of INT4",
	`test_prepared_type_f32`:                              "REAL (float4) round-trip precision differs from PostgreSQL",
	`test_prepared_type_i16`:                              "default int size: expressions return INT8 instead of INT2",
	`test_prepared_type_i32`:                              "default int size: expressions return INT8 instead of INT4",
	`test_prepared_type_int4range`:                        "unsupported type: INT4RANGE (unknown oid 3904)",
	`test_prepared_type_ipnet`:                            "unsupported: CIDR IS NOT DISTINCT FROM",
	`test_prepared_type_ipnetwork`:                        "unsupported: CIDR IS NOT DISTINCT FROM",
	`test_prepared_type_mac_address`:                      "unsupported type: MACADDR",
	`test_prepared_type_mac_address_vec`:                  "unsupported type: MACADDR[]",
	`test_prepared_type_money`:                            "unsupported type: MONEY",
	`test_prepared_type_money_vec`:                        "unsupported type: MONEY[]",
	`test_prepared_type_numrange_bigdecimal`:              "unsupported type: NUMRANGE (unknown oid 3906)",
	`test_prepared_type_numrange_decimal`:                 "unsupported type: NUMRANGE (unknown oid 3906)",
	`test_select_expression`:                              "default int size: expressions return INT8 instead of INT4",
	`test_unprepared_type_i16`:                            "default int size: expressions return INT8 instead of INT2",
	`test_unprepared_type_i32`:                            "default int size: expressions return INT8 instead of INT4",
	`test_unprepared_type_int4range`:                      "unsupported type: INT4RANGE",
	`test_unprepared_type_ipnet`:                          "unsupported: CIDR IS NOT DISTINCT FROM",
	`test_unprepared_type_ipnetwork`:                      "unsupported: CIDR IS NOT DISTINCT FROM",
	`test_unprepared_type_mac_address`:                    "unsupported type: MACADDR",
	`test_unprepared_type_mac_address_vec`:                "unsupported type: MACADDR[]",
	`test_unprepared_type_numrange_bigdecimal`:            "unsupported type: NUMRANGE",
	`test_unprepared_type_numrange_decimal`:               "unsupported type: NUMRANGE",
}

var rustSqlxIgnoreList = blocklist{}
