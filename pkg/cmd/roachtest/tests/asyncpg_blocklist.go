// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	`test_cache_invalidation.TestCacheInvalidation.test_prepare_cache_invalidation_in_pool`:               "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_prepare_cache_invalidation_in_transaction`:        "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_prepare_cache_invalidation_silent`:                "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_prepared_type_cache_invalidation`:                 "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_in_cancelled_transaction`: "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_in_pool`:                  "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_in_transaction`:           "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_on_change_attr`:           "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_cache_invalidation.TestCacheInvalidation.test_type_cache_invalidation_on_drop_type_attr`:        "experimental support - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_codecs.TestCodecs.test_array_with_custom_json_text_codec`:                                       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/23468",
	`test_codecs.TestCodecs.test_composites_in_arrays`:                                                    "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	`test_codecs.TestCodecs.test_enum_and_range`:                                                          "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27791",
	`test_codecs.TestCodecs.test_enum_in_composite`:                                                       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/91779",
	`test_codecs.TestCodecs.test_invalid_input`:                                                           "default int size discrepancy",
	`test_codecs.TestCodecs.test_relacl_array_type`:                                                       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/40283",
	`test_codecs.TestCodecs.test_unhandled_type_fallback`:                                                 "unsupported feature - https://github.com/cockroachdb/cockroach/issues/54516",
	`test_codecs.TestCodecs.test_unknown_type_text_fallback`:                                              "unsupported feature - https://github.com/cockroachdb/cockroach/issues/54516",
	`test_codecs.TestCodecs.test_void`:                                                                    "unknown",
	`test_connect.TestSettings.test_get_settings_01`:                                                      "unknown",
	`test_copy.TestCopyFrom.test_copy_from_table_basics`:                                                  "no support for COPY TO - https://github.com/cockroachdb/cockroach/issues/85571",
	`test_cursor.TestCursor.test_cursor_02`:                                                               "unknown",
	`test_cursor.TestCursor.test_cursor_04`:                                                               "unknown",
	`test_cursor.TestIterableCursor.test_cursor_iterable_02`:                                              "unsupported feature - https://github.com/cockroachdb/cockroach/issues/77099",
	`test_exceptions.TestExceptions.test_exceptions_str`:                                                  "unknown",
	`test_exceptions.TestExceptions.test_exceptions_unpacking`:                                            "unknown",
	`test_execute.TestExecuteMany.test_executemany_client_failure_in_transaction`:                         "unknown",
	`test_execute.TestExecuteScript.test_execute_script_3`:                                                "unknown",
	`test_execute.TestExecuteScript.test_execute_script_check_transactionality`:                           "unknown",
	`test_introspection.TestIntrospection.test_introspection_no_stmt_cache_01`:                            "hstore - https://github.com/cockroachdb/cockroach/issues/41284",
	`test_introspection.TestIntrospection.test_introspection_no_stmt_cache_02`:                            "hstore - https://github.com/cockroachdb/cockroach/issues/41284",
	`test_introspection.TestIntrospection.test_introspection_on_large_db`:                                 "unsupported feature - https://github.com/cockroachdb/cockroach/issues/22456",
	`test_introspection.TestIntrospection.test_introspection_retries_after_cache_bust`:                    "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	`test_introspection.TestIntrospection.test_introspection_sticks_for_ps`:                               "unknown type: pg_catalog.json",
	`test_listeners.TestListeners.test_listen_01`:                                                         "LISTEN - https://github.com/cockroachdb/cockroach/issues/41522",
	`test_listeners.TestListeners.test_listen_02`:                                                         "LISTEN - https://github.com/cockroachdb/cockroach/issues/41522",
	`test_listeners.TestListeners.test_listen_notletters`:                                                 "LISTEN - https://github.com/cockroachdb/cockroach/issues/41522",
	`test_listeners.TestLogListeners.test_log_listener_01`:                                                "unsupported feature - https://github.com/cockroachdb/cockroach/issues/17511",
	`test_listeners.TestLogListeners.test_log_listener_02`:                                                "unsupported feature - https://github.com/cockroachdb/cockroach/issues/17511",
	`test_pool.TestPool.test_pool_remote_close`:                                                           "unsupported pg_terminate_backend() function",
	`test_prepare.TestPrepare.test_prepare_14_explain`:                                                    "unknown",
	`test_prepare.TestPrepare.test_prepare_16_command_result`:                                             "unknown",
	`test_prepare.TestPrepare.test_prepare_19_concurrent_calls`:                                           "unknown",
	`test_prepare.TestPrepare.test_prepare_28_max_args`:                                                   "unknown",
	`test_prepare.TestPrepare.test_prepare_statement_invalid`:                                             "experimental feature - https://github.com/cockroachdb/cockroach/issues/49329",
	`test_timeout.TestTimeout.test_timeout_06`:                                                            "unknown",
	// The test_transaction* tests fail when attempting to check if we are in an
	// active transaction. This occurs due to the autocommit_before_ddl behavior.
	// The transaction they have open creates a table right after opening, which
	// causes the transaction to close.
	"test_transaction.TestTransaction.test_transaction_nested":  "142048",
	"test_transaction.TestTransaction.test_transaction_regular": "142048",
	`test_utils.TestUtils.test_mogrify_simple`:                  "multi-dim arrays - https://github.com/cockroachdb/cockroach/issues/32552",
}

var asyncpgIgnoreList = blocklist{
	`test_pool.TestPool.test_pool_01`:                                   "can't parse output",
	`test_copy.TestCopyFrom.test_copy_from_query_basics`:                "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyFrom.test_copy_from_query_cancellation_explicit`: "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyFrom.test_copy_from_query_timeout_1`:             "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyFrom.test_copy_from_query_to_sink`:               "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyFrom.test_copy_from_table_large_rows`:            "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_records_to_table_1`:                 "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_records_to_table_no_binary_codec`:   "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_to_table_basics`:                    "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_to_table_fail_in_source_1`:          "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_to_table_fail_in_source_2`:          "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_to_table_from_bytes_like`:           "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_to_table_from_file_path`:            "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_to_table_large_rows`:                "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_copy.TestCopyTo.test_copy_to_table_timeout`:                   "flaky; see #119291 and https://github.com/MagicStack/asyncpg/issues/240",
	`test_listeners.TestListeners.test_dangling_listener_warns`:         "flaky",
}
