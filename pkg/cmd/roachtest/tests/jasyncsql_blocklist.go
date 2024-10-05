// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

var jasyncSqlBlocklist = blocklist{
	`com.github.aysnc.sql.db.integration.ArrayTypesSpec.connection should correctly parse the array type`:                                                           "unknown",
	`com.github.aysnc.sql.db.integration.ArrayTypesSpec.connection should correctly send arrays using prepared statements`:                                          "unknown",
	`com.github.aysnc.sql.db.integration.ListenNotifySpec.connection should be able to receive a notification from a pg_notify call`:                                "unknown",
	`com.github.aysnc.sql.db.integration.ListenNotifySpec.connection should be able to receive a notification if listening`:                                         "unknown",
	`com.github.aysnc.sql.db.integration.ListenNotifySpec.connection should be able to receive notify ,out payload`:                                                 "unknown",
	`com.github.aysnc.sql.db.integration.ListenNotifySpec.connection should not receive any notification if not registered to the correct channel`:                  "unknown",
	`com.github.aysnc.sql.db.integration.ListenNotifySpec.connection should not receive notification if listener was removed`:                                       "unknown",
	`com.github.aysnc.sql.db.integration.ListenNotifySpec.connection should not receive notifications if cleared the collection`:                                    "unknown",
	`com.github.aysnc.sql.db.integration.LoginSpec.handler should fail login using , an invalid credential exception`:                                               "unknown",
	`com.github.aysnc.sql.db.integration.LoginSpec.handler should login using MD5 authentication`:                                                                   "unknown",
	`com.github.aysnc.sql.db.integration.LoginSpec.handler should login using SCRAM-SHA-256 authentication`:                                                         "unknown",
	`com.github.aysnc.sql.db.integration.LoginSpec.handler should login using cleartext authentication`:                                                             "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLConnectionSpec.handler should return correct application_name`:                                                   "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLConnectionSpec.handler should select rows in the database`:                                                       "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLConnectionSpec.handler should select rows that has duplicate column names`:                                       "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLConnectionSpec.handler should transaction and flatmap example`:                                                   "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLSSLConnectionSpec.ssl handler should connect to the database in ssl verifying CA`:                                "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLSSLConnectionSpec.ssl handler should connect to the database in ssl verifying CA and hostname`:                   "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLSSLConnectionSpec.ssl handler should connect to the database in ssl without verifying CA`:                        "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLSSLConnectionSpec.ssl handler should connect with a local client cert`:                                           "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLSSLConnectionSpec.ssl handler should throws exception when CA verification fails`:                                "unknown",
	`com.github.aysnc.sql.db.integration.PostgreSQLSSLConnectionSpec.ssl handler should throws exception when hostname verification fails`:                          "unknown",
	`com.github.aysnc.sql.db.integration.PreparedStatementSpec.prepared statements should deallocates prepared statements`:                                          "unknown",
	`com.github.aysnc.sql.db.integration.PreparedStatementSpec.prepared statements should support handling JSON type`:                                               "unknown",
	`com.github.aysnc.sql.db.integration.TransactionSpec.transactions should commit simple inserts`:                                                                 "unknown",
	`com.github.aysnc.sql.db.integration.TransactionSpec.transactions should commit simple inserts, prepared statements`:                                            "unknown",
	`com.github.aysnc.sql.db.integration.TransactionSpec.transactions should rollback to savepoint`:                                                                 "unknown",
	`com.github.aysnc.sql.db.integration.pool.ActorAsyncObjectPoolSpec.pool should enqueue an action if the pool is full`:                                           "unknown",
	`com.github.aysnc.sql.db.integration.pool.ActorAsyncObjectPoolSpec.pool should give me a valid object when I ask for one`:                                       "unknown",
	`com.github.aysnc.sql.db.integration.pool.ActorAsyncObjectPoolSpec.pool should it should remove aged out connections once the time limit has been reached`:      "unknown",
	`com.github.aysnc.sql.db.integration.pool.ActorAsyncObjectPoolSpec.pool should it should remove idle connections once the time limit has been reached`:          "unknown",
	`com.github.aysnc.sql.db.integration.pool.ConnectionPoolSpec.pool should give you a connection for prepared statements`:                                         "unknown",
	`com.github.aysnc.sql.db.integration.pool.ConnectionPoolSpec.pool should give you a connection when sending statements`:                                         "unknown",
	`com.github.aysnc.sql.db.integration.pool.NextGenConnectionPoolSpec.pool should give you a connection for prepared statements`:                                  "unknown",
	`com.github.aysnc.sql.db.integration.pool.NextGenConnectionPoolSpec.pool should give you a connection when sending statements`:                                  "unknown",
	`com.github.aysnc.sql.db.integration.pool.SingleThreadedAsyncObjectPoolSpec.pool should enqueue an action if the pool is full`:                                  "unknown",
	`com.github.aysnc.sql.db.integration.pool.SingleThreadedAsyncObjectPoolSpec.pool should give me a valid object when I ask for one`:                              "unknown",
	`com.github.aysnc.sql.db.integration.pool.SingleThreadedAsyncObjectPoolSpec.pool should it should remove idle connections once the time limit has been reached`: "unknown",
	`com.github.aysnc.sql.db.integration.pool.SuspendingPoolSpec.SuspendingConnection pool simple send prepared statement`:                                          "unknown",
	`com.github.aysnc.sql.db.integration.pool.SuspendingPoolSpec.SuspendingConnection pool simple send query`:                                                       "unknown",
	`com.github.aysnc.sql.db.integration.pool.SuspendingPoolSpec.transactions should commit simple inserts , prepared statements`:                                   "unknown",
	`com.github.aysnc.sql.db.integration.pool.SuspendingPoolSpec.transactions with pool should commit simple inserts , prepared statements`:                         "unknown",
}

var jasyncsqlIgnoreList = blocklist{}
