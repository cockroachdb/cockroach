// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

type queryCounter struct {
	query                           string
	expectError                     bool
	txnBeginCount                   int64
	selectCount                     int64
	selectExecutedCount             int64
	distSQLSelectCount              int64
	updateCount                     int64
	insertCount                     int64
	deleteCount                     int64
	ddlCount                        int64
	callSPCount                     int64
	callSPExecutedCount             int64
	miscCount                       int64
	miscExecutedCount               int64
	copyCount                       int64
	failureCount                    int64
	txnCommitCount                  int64
	txnRollbackCount                int64
	txnAbortCount                   int64
	savepointCount                  int64
	restartSavepointCount           int64
	releaseRestartSavepointCount    int64
	rollbackToRestartSavepointCount int64
	statementTimeoutCount           int64
	transactionTimeoutCount         int64
}

func TestQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			// Disable SELECT called for delete orphaned leases to keep
			// query stats stable.
			DisableDeleteOrphanedLeases: true,
		},
	}
	srv, sqlDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	if _, err := sqlDB.Exec("CREATE PROCEDURE p_select() LANGUAGE SQL AS 'SELECT 1'"); err != nil {
		t.Fatal(err)
	}

	var testcases = []queryCounter{
		// The counts are deltas for each query.
		{query: "SET DISTSQL = 'off'", miscCount: 1, miscExecutedCount: 1},
		{query: "BEGIN; END", txnBeginCount: 1, txnCommitCount: 1},
		{query: "SELECT 1", selectCount: 1, selectExecutedCount: 1, txnCommitCount: 1},
		{query: "CREATE DATABASE mt", ddlCount: 1},
		{query: "CREATE TABLE mt.n (num INTEGER PRIMARY KEY)", ddlCount: 1},
		{query: "INSERT INTO mt.n VALUES (3)", insertCount: 1},
		// Test failure (uniqueness violation).
		{query: "INSERT INTO mt.n VALUES (3)", failureCount: 1, insertCount: 1, expectError: true},
		// Test failure (planning error).
		{
			query:        "INSERT INTO nonexistent VALUES (3)",
			failureCount: 1, insertCount: 1, expectError: true,
		},
		{query: "UPDATE mt.n SET num = num + 1", updateCount: 1},
		{query: "DELETE FROM mt.n", deleteCount: 1},
		{query: "ALTER TABLE mt.n ADD COLUMN num2 INTEGER", ddlCount: 1},
		{query: "EXPLAIN SELECT * FROM mt.n", miscCount: 1, miscExecutedCount: 1},
		{
			query:         "BEGIN; UPDATE mt.n SET num = num + 1; END",
			txnBeginCount: 1, updateCount: 1, txnCommitCount: 1,
		},
		{
			query:       "SELECT * FROM mt.n; SELECT * FROM mt.n; SELECT * FROM mt.n",
			selectCount: 3, selectExecutedCount: 3,
		},
		{query: "SET DISTSQL = 'on'", miscCount: 1, miscExecutedCount: 1},
		{
			query:       "SELECT * FROM mt.n",
			selectCount: 1, selectExecutedCount: 1, distSQLSelectCount: 1,
		},
		{query: "SET DISTSQL = 'off'", miscCount: 1, miscExecutedCount: 1},
		{query: "DROP TABLE mt.n", ddlCount: 1},
		{query: "SET database = system", miscCount: 1, miscExecutedCount: 1},
		{query: "SELECT 3", selectCount: 1, selectExecutedCount: 1},
		{query: "CREATE TABLE mt.n (num INTEGER PRIMARY KEY)", ddlCount: 1},
		{query: "UPDATE mt.n SET num = num + 1", updateCount: 1},
		{query: "COPY mt.n(num) FROM STDIN", copyCount: 1, expectError: true},
		{
			query:       "BEGIN; SET LOCAL statement_timeout = '100ms'; SELECT pg_sleep(10)",
			expectError: true, txnBeginCount: 1, selectCount: 1, miscCount: 1,
			miscExecutedCount: 1, failureCount: 1, statementTimeoutCount: 1,
			txnRollbackCount: 1, txnAbortCount: 1,
		},
		{
			query:       "BEGIN; SET LOCAL transaction_timeout = '100ms'; SELECT pg_sleep(10)",
			expectError: true, txnBeginCount: 1, selectCount: 1, miscCount: 1,
			miscExecutedCount: 1, failureCount: 1, transactionTimeoutCount: 1,
			txnRollbackCount: 1, txnAbortCount: 1,
		},
		{query: "CALL p_select()", callSPCount: 1, callSPExecutedCount: 1},
	}

	accum := initializeQueryCounter(s)

	for _, tc := range testcases {
		t.Run(tc.query, func(t *testing.T) {
			if _, err := sqlDB.Exec(tc.query); err != nil && !tc.expectError {
				t.Fatalf("unexpected error executing '%s': %s'", tc.query, err)
			}
			// If the query included a BEGIN statement and failed, we need to abort the transaction
			// to set up for the next test.
			if tc.txnBeginCount > 0 && tc.expectError {
				if _, err := sqlDB.Exec("ABORT"); err != nil {
					t.Fatalf("unexpected error when attempting to abort opened txn: %s'", err)
				}
			}

			// Force metric snapshot refresh.
			if err := srv.WriteSummaries(); err != nil {
				t.Fatal(err)
			}

			var err error
			if accum.txnBeginCount, err = checkCounterDelta(s, sql.MetaTxnBeginStarted, accum.txnBeginCount, tc.txnBeginCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.distSQLSelectCount, err = checkCounterDelta(s, sql.MetaDistSQLSelect, accum.distSQLSelectCount, tc.distSQLSelectCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.txnRollbackCount, err = checkCounterDelta(s, sql.MetaTxnRollbackStarted, accum.txnRollbackCount, tc.txnRollbackCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.txnAbortCount, err = checkCounterDelta(s, sql.MetaTxnAbort, accum.txnAbortCount, tc.txnAbortCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.selectCount, err = checkCounterDelta(s, sql.MetaSelectStarted, accum.selectCount, tc.selectCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.selectExecutedCount, err = checkCounterDelta(s, sql.MetaSelectExecuted, accum.selectExecutedCount, tc.selectExecutedCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.updateCount, err = checkCounterDelta(s, sql.MetaUpdateStarted, accum.updateCount, tc.updateCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.insertCount, err = checkCounterDelta(s, sql.MetaInsertStarted, accum.insertCount, tc.insertCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.deleteCount, err = checkCounterDelta(s, sql.MetaDeleteStarted, accum.deleteCount, tc.deleteCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.ddlCount, err = checkCounterDelta(s, sql.MetaDdlStarted, accum.ddlCount, tc.ddlCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.callSPCount, err = checkCounterDelta(s, sql.MetaCallStoredProcStarted, accum.callSPCount, tc.callSPCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.callSPExecutedCount, err = checkCounterDelta(s, sql.MetaCallStoredProcExecuted, accum.callSPExecutedCount, tc.callSPExecutedCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.miscCount, err = checkCounterDelta(s, sql.MetaMiscStarted, accum.miscCount, tc.miscCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.miscExecutedCount, err = checkCounterDelta(s, sql.MetaMiscExecuted, accum.miscExecutedCount, tc.miscExecutedCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.copyCount, err = checkCounterDelta(s, sql.MetaCopyStarted, accum.copyCount, tc.copyCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.failureCount, err = checkCounterDelta(s, sql.MetaFailure, accum.failureCount, tc.failureCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.statementTimeoutCount, err = checkCounterDelta(s, sql.MetaStatementTimeout, accum.statementTimeoutCount, tc.statementTimeoutCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.transactionTimeoutCount, err = checkCounterDelta(s, sql.MetaTransactionTimeout, accum.transactionTimeoutCount, tc.transactionTimeoutCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
		})
	}
}

func TestStatementMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	runner := sqlutils.MakeSQLRunner(sqlDB)
	s := srv.ApplicationLayer()

	testCases := []struct {
		query            string
		rowsRead         int64
		indexRowsWritten int64
		// If true, then skip metamorphic testing for this query.
		skipMetamorphic bool
		// If true, then this is a deletion case, in which case indexBytesWritten
		// is expected to be zero.
		isDelete bool
	}{
		// Read-only statements.
		{query: `SELECT * FROM db.t WHERE x = 1`, rowsRead: 1},
		{query: `SELECT * FROM db.t WHERE x <= 2`, rowsRead: 2},
		{query: `SELECT * FROM db.t WHERE a @> ARRAY['apple']`, rowsRead: 4},
		{
			query:    `SELECT * FROM db.t@t_a_idx t1, db.t t2 WHERE t1.a @> t2.a`,
			rowsRead: 12,
			// Skip this variation under metamorphic testing because the inverted
			// joiner batch size can affect number of rows read.
			skipMetamorphic: true,
		},
		{query: `SELECT * FROM db.t ORDER BY v <-> '[2, 2]' LIMIT 1`, rowsRead: 3},

		// Mutation statements.
		{query: `INSERT INTO db.t VALUES (4, 40, ARRAY['orange', 'cherry'], '[7, 8]')`, indexRowsWritten: 5},
		{
			query:            `INSERT INTO db.t VALUES (5, 50, ARRAY['orange', 'cherry']), (6, 60, NULL)`,
			indexRowsWritten: 6,
		},
		// Expect to read 2 rows - one in secondary index, one in primary index.
		// Expect to write 3 rows - update primary index, delete and insert in
		// "y" secondary index.
		{query: `UPDATE db.t SET y = 100 WHERE y = 40`, rowsRead: 2, indexRowsWritten: 3},
		// Expect to read 4 rows - two in secondary index, two in primary index.
		// Expect to write 5 rows - update primary index, delete and insert two
		// rows in "y" secondary index.
		{query: `UPDATE db.t SET y = 100 WHERE y >= 10 AND y <= 20`, rowsRead: 4, indexRowsWritten: 6},
		{
			query:            `UPSERT INTO db.t VALUES (6, 60, ARRAY['date'], '[100, 110]')`,
			rowsRead:         1,
			indexRowsWritten: 3,
		},
		{
			query:            `INSERT INTO db.t VALUES (6, 600) ON CONFLICT (x) DO UPDATE SET a = ARRAY['strawberry']`,
			rowsRead:         1,
			indexRowsWritten: 3,
		},
		{query: `DELETE FROM db.t WHERE x = 2`, rowsRead: 1, indexRowsWritten: 4, isDelete: true},
		{
			query:            `DELETE FROM db.t WHERE a @> ARRAY['apple']`,
			rowsRead:         4,
			indexRowsWritten: 11,
			isDelete:         true,
		},
		// CREATE TABLE does not count rows or bytes written.
		{query: `CREATE TABLE db.t2 (x, y) AS SELECT 1, 10 UNION SELECT 2, 20`},
	}

	for _, vectorized := range []string{"off", "on"} {
		t.Run(fmt.Sprintf("vectorize = %s", vectorized), func(t *testing.T) {
			runner.Exec(t, "SET vectorize = $1", vectorized)
			runner.Exec(t, `DROP DATABASE IF EXISTS db`)
			runner.Exec(t, `CREATE DATABASE db`)
			runner.Exec(t, `CREATE TABLE db.t (x INT PRIMARY KEY, y INT, a STRING[], v VECTOR(2))`)
			runner.Exec(t, `CREATE INDEX ON db.t (y)`)
			runner.Exec(t, `CREATE INVERTED INDEX ON db.t (a)`)
			runner.Exec(t, `CREATE VECTOR INDEX ON db.t (v)`)
			runner.Exec(t, `INSERT INTO db.t VALUES (1, 10, ARRAY['apple', 'orange'], '[1, 2]')`)
			runner.Exec(t, `INSERT INTO db.t VALUES (2, 20, ARRAY['banana'], '[3, 4]')`)
			runner.Exec(t, `INSERT INTO db.t VALUES (3, 30, ARRAY['apple', 'pear', 'mango'], '[5, 6]')`)

			for _, tc := range testCases {
				t.Run(tc.query, func(t *testing.T) {
					if tc.skipMetamorphic && metamorphic.IsMetamorphicBuild() {
						return
					}

					// Get initial values of the counters.
					lastRowsRead := s.MustGetSQLCounter(sql.MetaStatementRowsRead.Name)
					lastBytesRead := s.MustGetSQLCounter(sql.MetaStatementBytesRead.Name)
					lastRowsWritten := s.MustGetSQLCounter(sql.MetaStatementIndexRowsWritten.Name)
					lastBytesWritten := s.MustGetSQLCounter(sql.MetaStatementIndexBytesWritten.Name)

					runner.Exec(t, tc.query)

					// Test the rows read metric.
					_, err := checkCounterDelta(
						s, sql.MetaStatementRowsRead, lastRowsRead, tc.rowsRead)
					require.NoError(t, err)

					// If there were rows read, then expect bytes read to have
					// increased. Don't check for a specific value, since there are
					// too many factors that can change this.
					if tc.rowsRead > 0 {
						currBytesRead := s.MustGetSQLCounter(sql.MetaStatementBytesRead.Name)
						require.Greater(t, currBytesRead, lastBytesRead)
					}

					// Test the rows written metric.
					_, err = checkCounterDelta(
						s, sql.MetaStatementIndexRowsWritten, lastRowsWritten, tc.indexRowsWritten)
					require.NoError(t, err)

					// If there were index rows written, then expect bytes written
					// to have increased (or not in the delete case). Don't check
					// for a specific value, since there are too many factors that
					// can change this.
					if tc.indexRowsWritten > 0 {
						currBytesWritten := s.MustGetSQLCounter(sql.MetaStatementIndexBytesWritten.Name)
						if !tc.isDelete {
							require.Greater(t, currBytesWritten, lastBytesWritten)
						} else {
							require.Equal(t, currBytesWritten, lastBytesWritten)
						}
					}
				})
			}
		})
	}
}

func TestAbortCountConflictingWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "retry loop", func(t *testing.T, retry bool) {
		var cmdFilters tests.CommandFilters
		params := base.TestServerArgs{}
		params.Knobs.Store = &kvserver.StoreTestingKnobs{
			EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
				TestingEvalFilter: cmdFilters.RunFilters,
			},
		}
		s, sqlDB, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		accum := initializeQueryCounter(s)

		// The test injects a retry error after the interceptors, so we need to
		// disable write buffers for the request to make it to the server.
		if _, err := sqlDB.Exec("SET kv_transaction_buffered_writes_enabled = false"); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec("CREATE DATABASE db"); err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec("CREATE TABLE db.t (k TEXT PRIMARY KEY, v TEXT)"); err != nil {
			t.Fatal(err)
		}

		// Inject errors on the INSERT below.
		restarted := false
		cmdFilters.AppendFilter(func(args kvserverbase.FilterArgs) *kvpb.Error {
			switch req := args.Req.(type) {
			// SQL INSERT generates ConditionalPuts for unique indexes (such as the PK).
			case *kvpb.ConditionalPutRequest:
				if bytes.Contains(req.Value.RawBytes, []byte("marker")) && !restarted {
					restarted = true
					return kvpb.NewErrorWithTxn(
						kvpb.NewTransactionAbortedError(
							kvpb.ABORT_REASON_ABORTED_RECORD_FOUND), args.Hdr.Txn)
				}
			}
			return nil
		}, false)

		txn, err := sqlDB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if retry {
			if _, err := txn.Exec("SAVEPOINT cockroach_restart"); err != nil {
				t.Fatal(err)
			}
		}
		// Run a batch of statements to move the txn out of the AutoRetry state,
		// otherwise the INSERT below would be automatically retried.
		if _, err := txn.Exec("SELECT 1"); err != nil {
			t.Fatal(err)
		}

		_, err = txn.Exec("INSERT INTO db.t VALUES ('key', 'marker')")
		expErr := "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)"
		if !testutils.IsError(err, regexp.QuoteMeta(expErr)) {
			t.Fatalf("expected %s, got: %v", expErr, err)
		}

		var expRestart, expRollback, expCommit, expAbort int64
		if retry {
			if _, err := txn.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
				t.Fatal(err)
			}
			if _, err := txn.Exec("RELEASE SAVEPOINT cockroach_restart"); err != nil {
				t.Fatal(err)
			}
			if err = txn.Commit(); err != nil {
				t.Fatal(err)
			}

			expRestart = 1
			expCommit = 1
		} else {
			if err = txn.Rollback(); err != nil {
				t.Fatal(err)
			}

			expRollback = 1
			expAbort = 1
		}

		if _, err := checkCounterDelta(s, sql.MetaTxnBeginStarted, accum.txnBeginCount, 1); err != nil {
			t.Error(err)
		}
		if _, err := checkCounterDelta(s, sql.MetaInsertStarted, accum.insertCount, 1); err != nil {
			t.Error(err)
		}
		if _, err := checkCounterDelta(s, sql.MetaRestartSavepointStarted, accum.restartSavepointCount, expRestart); err != nil {
			t.Error(err)
		}
		if _, err := checkCounterDelta(s, sql.MetaRollbackToRestartSavepointStarted, accum.rollbackToRestartSavepointCount, expRestart); err != nil {
			t.Error(err)
		}
		if _, err := checkCounterDelta(s, sql.MetaReleaseRestartSavepointStarted, accum.releaseRestartSavepointCount, expRestart); err != nil {
			t.Error(err)
		}
		if _, err := checkCounterDelta(s, sql.MetaTxnRollbackStarted, accum.txnRollbackCount, expRollback); err != nil {
			t.Error(err)
		}
		if _, err := checkCounterDelta(s, sql.MetaTxnCommitStarted, accum.txnCommitCount, expCommit); err != nil {
			t.Error(err)
		}
		if _, err := checkCounterDelta(s, sql.MetaTxnAbort, accum.txnAbortCount, expAbort); err != nil {
			t.Error(err)
		}
	})
}

// TestErrorDuringTransaction tests that the transaction abort count goes up when a query
// results in an error during a txn.
func TestAbortCountErrorDuringTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	accum := initializeQueryCounter(s)

	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Query("SELECT * FROM i_do.not_exist"); err == nil {
		t.Fatal("Expected an error but didn't get one")
	}

	if _, err := checkCounterDelta(s, sql.MetaTxnBeginStarted, accum.txnBeginCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaSelectStarted, accum.selectCount, 1); err != nil {
		t.Error(err)
	}

	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := checkCounterDelta(s, sql.MetaTxnAbort, accum.txnAbortCount, 1); err != nil {
		t.Error(err)
	}
}

func TestSavepointMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	accum := initializeQueryCounter(s)

	// Normal-case use of all three savepoint statements.
	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("ROLLBACK TRANSACTION TO SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("RELEASE SAVEPOINT cockroach_restart"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := checkCounterDelta(s, sql.MetaRestartSavepointStarted, accum.restartSavepointCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaRestartSavepointStarted, accum.releaseRestartSavepointCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaRestartSavepointStarted, accum.rollbackToRestartSavepointCount, 1); err != nil {
		t.Error(err)
	}

	// Unsupported savepoints go in a different counter.
	txn, err = sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("SAVEPOINT blah"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaSavepointStarted, accum.savepointCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaTxnRollbackStarted, accum.txnRollbackCount, 1); err != nil {
		t.Error(err)
	}

	// Custom restart savepoint names are recognized.
	txn, err = sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("SET force_savepoint_restart = true"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("SAVEPOINT blah"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaRestartSavepointStarted, accum.restartSavepointCount, 2); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaTxnRollbackStarted, accum.txnRollbackCount, 2); err != nil {
		t.Error(err)
	}
}

func TestMemMetricsCorrectlyRegistered(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	r := metric.NewRegistry()
	mm := sql.MakeMemMetrics("test", base.DefaultHistogramWindowInterval())
	r.AddMetricStruct(mm)

	expectedMetrics := []string{
		"sql.mem.test.max",
		"sql.mem.test.current",
		"sql.mem.test.txn.max",
		"sql.mem.test.txn.current",
		"sql.mem.test.session.max",
		"sql.mem.test.session.current",
		"sql.mem.test.session.prepared.max",
		"sql.mem.test.session.prepared.current",
	}
	var registered []string
	r.Each(func(name string, val interface{}) {
		registered = append(registered, name)
	})
	require.ElementsMatch(t, expectedMetrics, registered)
}
