// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

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
var psycopgBlockList = blocklist{}

var psycopgIgnoreList = blocklist{
	`tests.pool.test_pool.test_connect_check_timeout`:                                                "requires insecure mode",
	`tests.pool.test_pool.test_reconnect`:                                                            "requires insecure mode",
	`tests.pool.test_pool.test_reconnect_after_grow_failed`:                                          "requires insecure mode",
	`tests.pool.test_pool.test_reconnect_failure[False]`:                                             "requires insecure mode",
	`tests.pool.test_pool.test_refill_on_check`:                                                      "requires insecure mode",
	`tests.pool.test_pool.test_shrink`:                                                               "flaky; see #146895",
	`tests.pool.test_pool.test_stats_connect`:                                                        "requires insecure mode",
	`tests.pool.test_pool_async.test_connect_check_timeout[asyncio]`:                                 "requires insecure mode",
	`tests.pool.test_pool_async.test_reconnect[asyncio]`:                                             "requires insecure mode",
	`tests.pool.test_pool_async.test_reconnect_after_grow_failed[asyncio]`:                           "requires insecure mode",
	`tests.pool.test_pool_async.test_reconnect_failure[asyncio-False]`:                               "requires insecure mode",
	`tests.pool.test_pool_async.test_reconnect_failure[asyncio-True]`:                                "requires insecure mode",
	`tests.pool.test_pool_async.test_refill_on_check[asyncio]`:                                       "requires insecure mode",
	`tests.pool.test_pool_async.test_shrink[asyncio]`:                                                "see https://github.com/cockroachdb/cockroach/issues/144532",
	`tests.pool.test_pool_async.test_stats_connect[asyncio]`:                                         "requires insecure mode",
	`tests.pool.test_pool_common.test_queue[NullConnectionPool]`:                                     "test is timing dependent, see https://github.com/cockroachdb/cockroach/issues/143491",
	`tests.pool.test_pool_common.test_queue_timeout_override`:                                        "see https://github.com/cockroachdb/cockroach/issues/144532",
	`tests.pool.test_pool_common.test_setup_no_timeout[ConnectionPool]`:                              "requires insecure mode",
	`tests.pool.test_pool_common.test_setup_no_timeout[NullConnectionPool]`:                          "requires insecure mode",
	`tests.pool.test_pool_common.test_stats_usage[NullConnectionPool]`:                               "test is timing dependent, see https://github.com/cockroachdb/cockroach/issues/143491",
	`tests.pool.test_pool_common_async.test_queue[asyncio-AsyncNullConnectionPool]`:                  "requires insecure mode",
	`tests.pool.test_pool_common_async.test_queue_timeout[asyncio-AsyncNullConnectionPool]`:          "see https://github.com/cockroachdb/cockroach/issues/144532",
	`tests.pool.test_pool_common_async.test_queue_timeout_override[asyncio-AsyncNullConnectionPool]`: "see https://github.com/cockroachdb/cockroach/issues/144532",
	`tests.pool.test_pool_common_async.test_setup_no_timeout[asyncio-AsyncConnectionPool]`:           "requires insecure mode",
	`tests.pool.test_pool_common_async.test_setup_no_timeout[asyncio-AsyncNullConnectionPool]`:       "requires insecure mode",
	`tests.pool.test_pool_common_async.test_stats_measures[asyncio-AsyncNullConnectionPool]`:         "see https://github.com/cockroachdb/cockroach/issues/144532",
	`tests.pool.test_pool_common_async.test_stats_usage[asyncio-AsyncNullConnectionPool]`:            "requires insecure mode",
	`tests.pool.test_pool_null.test_stats_connect`:                                                   "requires insecure mode",
	`tests.pool.test_pool_null_async.test_stats_connect[asyncio]`:                                    "requires insecure mode",
	`tests.test_connection.test_cancel_safe_error`:                                                   "requires insecure mode",
	`tests.test_connection_async.test_cancel_safe_error[asyncio]`:                                    "requires insecure mode",
	`tests.test_generators.test_connect_operationalerror_pgconn`:                                     "requires insecure mode",
	"tests.test_connection.TestConnectionInfo.test_backend_pid":                                      "we return -1 for pg_backend_pid()",
}
