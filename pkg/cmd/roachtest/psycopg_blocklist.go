// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

var psycopgBlocklists = blocklistsForVersion{
	{"v19.2", "psycopgBlockList19_2", psycopgBlockList19_2, "psycopgIgnoreList19_2", psycopgIgnoreList19_2},
	{"v20.1", "psycopgBlockList20_1", psycopgBlockList20_1, "psycopgIgnoreList20_1", psycopgIgnoreList20_1},
	{"v20.2", "psycopgBlockList20_2", psycopgBlockList20_2, "psycopgIgnoreList20_2", psycopgIgnoreList20_2},
}

// These are lists of known psycopg test errors and failures.
// When the psycopg test suite is run, the results are compared to this list.
// Any passed test that is not on this list is reported as PASS - expected
// Any passed test that is on this list is reported as PASS - unexpected
// Any failed test that is on this list is reported as FAIL - expected
// Any failed test that is not on this list is reported as FAIL - unexpected
// Any test on this list that is not run is reported as FAIL - not run
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var psycopgBlockList20_2 = blocklist{
	"tests.test_async_keyword.CancelTests.test_async_cancel": "41335",
}

var psycopgBlockList20_1 = blocklist{
	"tests.test_async_keyword.CancelTests.test_async_cancel": "41335",
}

var psycopgBlockList19_2 = blocklist{
	"tests.test_async.AsyncTests.test_async_after_async":                                                     "5807",
	"tests.test_async.AsyncTests.test_async_callproc":                                                        "5807",
	"tests.test_async.AsyncTests.test_async_connection_error_message":                                        "5807",
	"tests.test_async.AsyncTests.test_async_cursor_gone":                                                     "5807",
	"tests.test_async.AsyncTests.test_async_dont_read_all":                                                   "5807",
	"tests.test_async.AsyncTests.test_async_executemany":                                                     "5807",
	"tests.test_async.AsyncTests.test_async_fetch_wrong_cursor":                                              "5807",
	"tests.test_async.AsyncTests.test_async_iter":                                                            "5807",
	"tests.test_async.AsyncTests.test_async_named_cursor":                                                    "5807",
	"tests.test_async.AsyncTests.test_async_scroll":                                                          "5807",
	"tests.test_async.AsyncTests.test_async_select":                                                          "5807",
	"tests.test_async.AsyncTests.test_async_subclass":                                                        "5807",
	"tests.test_async.AsyncTests.test_close":                                                                 "unknown",
	"tests.test_async.AsyncTests.test_commit_while_async":                                                    "5807",
	"tests.test_async.AsyncTests.test_connection_setup":                                                      "5807",
	"tests.test_async.AsyncTests.test_copy_no_hang":                                                          "5807",
	"tests.test_async.AsyncTests.test_copy_while_async":                                                      "5807",
	"tests.test_async.AsyncTests.test_error":                                                                 "5807",
	"tests.test_async.AsyncTests.test_error_two_cursors":                                                     "5807",
	"tests.test_async.AsyncTests.test_fetch_after_async":                                                     "5807",
	"tests.test_async.AsyncTests.test_lobject_while_async":                                                   "5807",
	"tests.test_async.AsyncTests.test_non_block_after_notification":                                          "5807",
	"tests.test_async.AsyncTests.test_notices":                                                               "5807",
	"tests.test_async.AsyncTests.test_notify":                                                                "5807",
	"tests.test_async.AsyncTests.test_poll_conn_for_notification":                                            "5807",
	"tests.test_async.AsyncTests.test_poll_noop":                                                             "5807",
	"tests.test_async.AsyncTests.test_reset_while_async":                                                     "5807",
	"tests.test_async.AsyncTests.test_rollback_while_async":                                                  "5807",
	"tests.test_async.AsyncTests.test_scroll":                                                                "5807",
	"tests.test_async.AsyncTests.test_set_parameters_while_async":                                            "5807",
	"tests.test_async.AsyncTests.test_stop_on_first_error":                                                   "5807",
	"tests.test_async.AsyncTests.test_sync_poll":                                                             "5807",
	"tests.test_async_keyword.AsyncTests.test_async_connection_error_message":                                "5807",
	"tests.test_async_keyword.AsyncTests.test_async_subclass":                                                "5807",
	"tests.test_async_keyword.AsyncTests.test_connection_setup":                                              "5807",
	"tests.test_async_keyword.CancelTests.test_async_cancel":                                                 "5807",
	"tests.test_async_keyword.CancelTests.test_async_connection_cancel":                                      "5807",
	"tests.test_dates.DatetimeTests.test_adapt_negative_timedelta":                                           "35807",
	"tests.test_dates.DatetimeTests.test_adapt_timedelta":                                                    "35807",
	"tests.test_dates.DatetimeTests.test_time_24":                                                            "36118",
	"tests.test_dates.DatetimeTests.test_type_roundtrip_timetz":                                              "26097",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorRealWithNamedCursorFetchAll":       "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorRealWithNamedCursorFetchMany":      "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorRealWithNamedCursorFetchManyNoarg": "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorRealWithNamedCursorFetchOne":       "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorRealWithNamedCursorIter":           "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorRealWithNamedCursorIterRowNumber":  "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorRealWithNamedCursorNotGreedy":      "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorWithPlainCursorRealFetchAll":       "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorWithPlainCursorRealFetchMany":      "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorWithPlainCursorRealFetchManyNoarg": "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorWithPlainCursorRealFetchOne":       "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorWithPlainCursorRealIter":           "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testDictCursorWithPlainCursorRealIterRowNumber":  "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testPickleRealDictRow":                           "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.testRealMeansReal":                               "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.test_copy":                                       "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.test_iter_methods_2":                             "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.test_mod":                                        "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.test_order":                                      "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.test_order_iter":                                 "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorRealTests.test_pop":                                        "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictConnCursorArgs":                              "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithNamedCursorFetchAll":               "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithNamedCursorFetchMany":              "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithNamedCursorFetchManyNoarg":         "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithNamedCursorFetchOne":               "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithNamedCursorIter":                   "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithNamedCursorIterRowNumber":          "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithNamedCursorNotGreedy":              "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithPlainCursorFetchAll":               "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithPlainCursorFetchMany":              "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithPlainCursorFetchManyNoarg":         "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithPlainCursorFetchOne":               "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithPlainCursorIter":                   "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testDictCursorWithPlainCursorIterRowNumber":          "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testPickleDictRow":                                   "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.testUpdateRow":                                       "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.test_copy":                                           "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.test_iter_methods_2":                                 "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.test_order":                                          "5807",
	"tests.test_extras_dictcursor.ExtrasDictCursorTests.test_order_iter":                                     "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_bad_col_names":                                   "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_cache":                                           "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_cursor_args":                                     "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_executemany":                                     "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_fetchall":                                        "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_fetchmany":                                       "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_fetchmany_noarg":                                 "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_fetchone":                                        "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_iter":                                            "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_max_cache":                                       "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_minimal_generation":                              "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_named":                                           "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_named_fetchall":                                  "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_named_fetchmany":                                 "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_named_fetchone":                                  "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_named_rownumber":                                 "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_no_result_no_surprise":                           "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_not_greedy":                                      "5807",
	"tests.test_extras_dictcursor.NamedTupleCursorTest.test_record_updated":                                  "5807",
	"tests.test_types_basic.TypesBasicTests.testEmptyArray":                                                  "23299",
}

var psycopgIgnoreList20_2 = psycopgIgnoreList20_1

var psycopgIgnoreList20_1 = psycopgIgnoreList19_2

var psycopgIgnoreList19_2 = blocklist{
	"tests.test_async.AsyncTests.test_flush_on_write":           "44709",
	"tests.test_green.GreenTestCase.test_flush_on_write":        "flakey",
	"tests.test_connection.TestConnectionInfo.test_backend_pid": "we return -1 for pg_backend_pid()",
}
