// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

// These are lists of known asyncpg test errors and failures.
// When the asyncpg test suite is run, the results are compared to this list.
// Any failed test that is on this list is reported as FAIL - expected.
// Any failed test that is not on this list is reported as FAIL - unexpected.
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var asyncpgBlocklist = blocklist{
	"test_cache_invalidation.TestCacheInvalidation.test_prepare_cache_invalidation_in_pool":               "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_prepare_cache_invalidation_in_transaction":        "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_prepare_cache_invalidation_silent":                "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_prepared_type_cache_invalidation":                 "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_in_cancelled_transaction": "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_in_pool":                  "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_in_transaction":           "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_on_change_attr":           "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_on_drop_type_attr":        "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	"test_codecs.TestCodecs.test_array_with_custom_json_text_codec":                                       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/23468",
	"test_codecs.TestCodecs.test_composites_in_arrays":                                                    "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"test_codecs.TestCodecs.test_enum":                                                                    "query decorrelation - https://github.com/cockroachdb/cockroach/issues/80169",
	"test_codecs.TestCodecs.test_enum_and_range":                                                          "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27791",
	"test_codecs.TestCodecs.test_enum_function_return":                                                    "unsupported feature - https://github.com/cockroachdb/cockroach/issues/17511",
	"test_codecs.TestCodecs.test_enum_in_array":                                                           "query decorrelation - https://github.com/cockroachdb/cockroach/issues/80169",
	"test_codecs.TestCodecs.test_enum_in_composite":                                                       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"test_codecs.TestCodecs.test_invalid_input":                                                           "default int size discrepancy",
	"test_codecs.TestCodecs.test_relacl_array_type":                                                       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/40283",
	"test_codecs.TestCodecs.test_timetz_encoding":                                                         "multiple active portals unsupported - https://github.com/cockroachdb/cockroach/issues/40195",
	"test_codecs.TestCodecs.test_unhandled_type_fallback":                                                 "unsupported feature - https://github.com/cockroachdb/cockroach/issues/54516",
	"test_codecs.TestCodecs.test_unknown_type_text_fallback":                                              "unsupported feature - https://github.com/cockroachdb/cockroach/issues/54516",
	"test_codecs.TestCodecs.test_void":                                                                    "unknown",
	"test_connect.TestSettings.test_get_settings_01":                                                      "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_basics":                                                  "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_cancellation_explicit":                                   "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_cancellation_on_sink_error":                              "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_cancellation_while_waiting_for_data":                     "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_timeout_1":                                               "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_timeout_2":                                               "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_to_path":                                                 "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_to_path_like":                                            "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_to_sink":                                                 "unknown",
	"test_copy.TestCopyFrom.test_copy_from_query_with_args":                                               "unknown",
	"test_copy.TestCopyFrom.test_copy_from_table_basics":                                                  "unknown",
	"test_copy.TestCopyFrom.test_copy_from_table_large_rows":                                              "unknown",
	"test_copy.TestCopyTo.test_copy_records_to_table_1":                                                   "unknown",
	"test_copy.TestCopyTo.test_copy_records_to_table_async":                                               "unknown",
	"test_copy.TestCopyTo.test_copy_to_table_basics":                                                      "unknown",
	"test_cursor.TestCursor.test_cursor_02":                                                               "unknown",
	"test_cursor.TestCursor.test_cursor_04":                                                               "unknown",
	"test_cursor.TestIterableCursor.test_cursor_iterable_02":                                              "unknown",
	"test_exceptions.TestExceptions.test_exceptions_str":                                                  "unknown",
	"test_exceptions.TestExceptions.test_exceptions_unpacking":                                            "unknown",
	"test_execute.TestExecuteMany.test_executemany_client_failure_in_transaction":                         "unknown",
	"test_execute.TestExecuteMany.test_executemany_client_server_failure_conflict":                        "unknown",
	"test_execute.TestExecuteMany.test_executemany_prepare":                                               "unknown",
	"test_execute.TestExecuteMany.test_executemany_server_failure":                                        "unknown",
	"test_execute.TestExecuteMany.test_executemany_server_failure_after_writes":                           "unknown",
	"test_execute.TestExecuteMany.test_executemany_server_failure_during_writes":                          "unknown",
	"test_execute.TestExecuteMany.test_executemany_timeout":                                               "unknown",
	"test_execute.TestExecuteMany.test_executemany_timeout_flow_control":                                  "unknown",
	"test_execute.TestExecuteScript.test_execute_script_3":                                                "unknown",
	"test_execute.TestExecuteScript.test_execute_script_check_transactionality":                           "unknown",
	"test_introspection.TestIntrospection.test_introspection_no_stmt_cache_01":                            "unknown",
	"test_introspection.TestIntrospection.test_introspection_no_stmt_cache_02":                            "unknown",
	"test_introspection.TestIntrospection.test_introspection_no_stmt_cache_03":                            "unknown",
	"test_introspection.TestIntrospection.test_introspection_on_large_db":                                 "unknown",
	"test_introspection.TestIntrospection.test_introspection_retries_after_cache_bust":                    "unknown",
	"test_introspection.TestIntrospection.test_introspection_sticks_for_ps":                               "unknown",
	"test_listeners.TestListeners.test_listen_01":                                                         "unknown",
	"test_listeners.TestListeners.test_listen_02":                                                         "unknown",
	"test_listeners.TestListeners.test_listen_notletters":                                                 "unknown",
	"test_listeners.TestLogListeners.test_log_listener_01":                                                "unknown",
	"test_listeners.TestLogListeners.test_log_listener_02":                                                "unknown",
	"test_listeners.TestLogListeners.test_log_listener_03":                                                "unknown",
	"test_pool.TestPool.test_pool_remote_close":                                                           "unknown",
	"test_prepare.TestPrepare.test_prepare_08_big_result":                                                 "unknown",
	"test_prepare.TestPrepare.test_prepare_09_raise_error":                                                "unknown",
	"test_prepare.TestPrepare.test_prepare_14_explain":                                                    "unknown",
	"test_prepare.TestPrepare.test_prepare_16_command_result":                                             "unknown",
	"test_prepare.TestPrepare.test_prepare_19_concurrent_calls":                                           "unknown",
	"test_prepare.TestPrepare.test_prepare_22_empty":                                                      "unknown",
	"test_prepare.TestPrepare.test_prepare_28_max_args":                                                   "unknown",
	"test_prepare.TestPrepare.test_prepare_31_pgbouncer_note":                                             "unknown",
	"test_prepare.TestPrepare.test_prepare_statement_invalid":                                             "unknown",
	"test_record.TestRecord.test_record_subclass_01":                                                      "unknown",
	"test_record.TestRecord.test_record_subclass_02":                                                      "unknown",
	"test_record.TestRecord.test_record_subclass_03":                                                      "unknown",
	"test_record.TestRecord.test_record_subclass_04":                                                      "unknown",
	"test_utils.TestUtils.test_mogrify_multiple":                                                          "unknown",
	"test_utils.TestUtils.test_mogrify_simple":                                                            "unknown",
}

var asyncpgIgnoreList = blocklist{}
