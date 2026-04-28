// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestTxnModeSmoketest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithExternalProcessMode(134857),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(conn)

	// Configure low latency replication settings
	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	// Create source and destination databases
	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	for _, db := range [](*sqlutils.SQLRunner){sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE parent (id INT PRIMARY KEY)")
		db.Exec(t, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)")
		// TODO(jeffswenson): add fk support to lock derivation then uncomment this.
		// db.Exec(t, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))")
	}

	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLES (parent, child) ON $1 INTO TABLES (parent, child) WITH MODE = 'transactional'",
		sourceURL.String(),
	).Scan(&jobID)

	// Insert parent and children. Lock derivation must order inserts so that
	// the parent row is written before the child rows.
	sourceDB.Exec(t, "INSERT INTO parent (id) VALUES (1); INSERT INTO child (id, parent_id) VALUES (1, 1), (2, 1), (3, 1)")

	now := s.Clock().Now()
	ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

	destDB.CheckQueryResults(t, "SELECT * FROM child ORDER BY id", [][]string{
		{"1", "1"},
		{"2", "1"},
		{"3", "1"},
	})

	// Delete children and parent. Lock derivation must order deletes so that
	// child rows are removed before the parent row.
	sourceDB.Exec(t, "DELETE FROM child WHERE parent_id = 1; DELETE FROM parent WHERE id = 1")

	now = s.Clock().Now()
	ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

	destDB.CheckQueryResults(t, "SELECT * FROM parent ORDER BY id", [][]string{})
	destDB.CheckQueryResults(t, "SELECT * FROM child ORDER BY id", [][]string{})
}

func TestTxnModeUniqueConstraintUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithExternalProcessMode(134857),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(conn)

	// Configure low latency replication settings
	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	// Create source and destination databases
	runner.Exec(t, "CREATE DATABASE source_db")
	runner.Exec(t, "CREATE DATABASE dest_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	// Create table with UUID primary key and unique int column
	for _, db := range [](*sqlutils.SQLRunner){sourceDB, destDB} {
		db.Exec(t, "CREATE TABLE test_table (uuid UUID PRIMARY KEY, unique_value INT UNIQUE)")
	}

	// Get connection URL for source database
	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	// Create logical replication stream with transactional mode
	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLES (source_db.test_table) ON $1 INTO TABLES (dest_db.test_table) WITH MODE = 'transactional'",
		sourceURL.String(),
	).Scan(&jobID)

	// Insert initial rows after starting replication
	sourceDB.Exec(t, "INSERT INTO test_table (uuid, unique_value) VALUES (gen_random_uuid(), 1337)")

	// Update the UUID (primary key) of the row with unique_value = 1337
	// This tests that lock synthesis correctly orders the delete and insert operations
	now := s.Clock().Now()
	sourceDB.Exec(t, "UPDATE test_table SET uuid = gen_random_uuid() WHERE unique_value = 1337")

	ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

	// Verify the update was replicated (row with unique_value = 1337 still exists with new UUID)
	destDB.CheckQueryResults(t, "SELECT unique_value FROM test_table ORDER BY unique_value", [][]string{
		{"1337"},
	})

	// Verify we can still query by unique_value
	var count int
	destDB.QueryRow(t, "SELECT count(*) FROM test_table WHERE unique_value = 1337").Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 row with unique_value = 1337, got %d", count)
	}
}

// TestTxnModeLWW exercises every combination of incoming replication action
// (insert, update, delete) against every possible local state at the
// destination (nothing, tombstone losing lww, tombstone winning lww, value
// losing lww, value winning lww). The table uses (tc, cluster, version) where
// tc encodes the test case, cluster records who wrote the row, and version
// describes the row's state.
//
// The test is written against the desired LWW semantics. It is expected to
// fail with transactional LDR until tombstone handling is fully implemented.
func TestTxnModeLWW(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithExternalProcessMode(134857),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(conn)

	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	// execPhase runs a list of SQL statements either individually or grouped
	// in a single transaction, depending on the useTxn flag.
	execPhase := func(t *testing.T, db *sqlutils.SQLRunner, useTxn bool, stmts []string) {
		if useTxn {
			db.Exec(t, "BEGIN")
			for _, stmt := range stmts {
				db.Exec(t, stmt)
			}
			db.Exec(t, "COMMIT")
		} else {
			for _, stmt := range stmts {
				db.Exec(t, stmt)
			}
		}
	}

	tests := []struct {
		name   string
		useTxn bool
	}{
		{name: "individual_statements", useTxn: false},
		{name: "transactional", useTxn: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dbName := fmt.Sprintf("%s_db", tc.name)
			sourceDBName := fmt.Sprintf("source_%s", dbName)
			destDBName := fmt.Sprintf("dest_%s", dbName)
			runner.Exec(t, fmt.Sprintf("CREATE DATABASE %s", sourceDBName))
			runner.Exec(t, fmt.Sprintf("CREATE DATABASE %s", destDBName))

			sourceDB := sqlutils.MakeSQLRunner(
				s.SQLConn(t, serverutils.DBName(sourceDBName)))
			destDB := sqlutils.MakeSQLRunner(
				s.SQLConn(t, serverutils.DBName(destDBName)))

			for _, db := range []*sqlutils.SQLRunner{sourceDB, destDB} {
				db.Exec(t,
					"CREATE TABLE lww (tc STRING PRIMARY KEY, cluster STRING, version STRING)")
			}

			// Phase 0: Insert source rows that will later be updated or
			// deleted. These are written before the cursor so the initial
			// inserts are not replicated.
			execPhase(t, sourceDB, tc.useTxn, []string{
				"INSERT INTO lww VALUES ('upd_nothing',   'source', 'pre-update')",
				"INSERT INTO lww VALUES ('upd_tomb_lose', 'source', 'pre-update')",
				"INSERT INTO lww VALUES ('upd_tomb_win',  'source', 'pre-update')",
				"INSERT INTO lww VALUES ('upd_val_lose',  'source', 'pre-update')",
				"INSERT INTO lww VALUES ('upd_val_win',   'source', 'pre-update')",
				"INSERT INTO lww VALUES ('del_nothing',   'source', 'pre-delete')",
				"INSERT INTO lww VALUES ('del_tomb_lose', 'source', 'pre-delete')",
				"INSERT INTO lww VALUES ('del_tomb_win',  'source', 'pre-delete')",
				"INSERT INTO lww VALUES ('del_val_lose',  'source', 'pre-delete')",
				"INSERT INTO lww VALUES ('del_val_win',   'source', 'pre-delete')",
			})

			// Capture cursor timestamp. All changes after this point will be
			// replicated when the LDR stream starts.
			cursorTS := s.Clock().Now()

			// Phase 2a: Insert destination data that will lose LWW. This
			// includes values that will be overwritten and seeds for
			// tombstones.
			execPhase(t, destDB, tc.useTxn, []string{
				"INSERT INTO lww VALUES ('ins_val_lose',  'dest', 'losing-value')",
				"INSERT INTO lww VALUES ('upd_val_lose',  'dest', 'losing-value')",
				"INSERT INTO lww VALUES ('del_val_lose',  'dest', 'losing-value')",
				"INSERT INTO lww VALUES ('ins_tomb_lose', 'dest', 'tombstone-seed')",
				"INSERT INTO lww VALUES ('upd_tomb_lose', 'dest', 'tombstone-seed')",
				"INSERT INTO lww VALUES ('del_tomb_lose', 'dest', 'tombstone-seed')",
			})

			// Phase 2b: Delete the tombstone seeds to create losing
			// tombstones.
			execPhase(t, destDB, tc.useTxn, []string{
				"DELETE FROM lww WHERE tc = 'ins_tomb_lose'",
				"DELETE FROM lww WHERE tc = 'upd_tomb_lose'",
				"DELETE FROM lww WHERE tc = 'del_tomb_lose'",
			})

			// Phase 3: Source writes that will be replicated. Inserts,
			// updates to pre-existing rows, and deletes of pre-existing
			// rows.
			execPhase(t, sourceDB, tc.useTxn, []string{
				"INSERT INTO lww VALUES ('ins_nothing',   'source', 'inserted')",
				"INSERT INTO lww VALUES ('ins_tomb_lose', 'source', 'inserted')",
				"INSERT INTO lww VALUES ('ins_tomb_win',  'source', 'inserted')",
				"INSERT INTO lww VALUES ('ins_val_lose',  'source', 'inserted')",
				"INSERT INTO lww VALUES ('ins_val_win',   'source', 'inserted')",
				"UPDATE lww SET cluster = 'source', version = 'updated' WHERE tc = 'upd_nothing'",
				"UPDATE lww SET cluster = 'source', version = 'updated' WHERE tc = 'upd_tomb_lose'",
				"UPDATE lww SET cluster = 'source', version = 'updated' WHERE tc = 'upd_tomb_win'",
				"UPDATE lww SET cluster = 'source', version = 'updated' WHERE tc = 'upd_val_lose'",
				"UPDATE lww SET cluster = 'source', version = 'updated' WHERE tc = 'upd_val_win'",
				"DELETE FROM lww WHERE tc = 'del_nothing'",
				"DELETE FROM lww WHERE tc = 'del_tomb_lose'",
				"DELETE FROM lww WHERE tc = 'del_tomb_win'",
				"DELETE FROM lww WHERE tc = 'del_val_lose'",
				"DELETE FROM lww WHERE tc = 'del_val_win'",
			})

			// Phase 4a: Insert destination data that will win LWW. These
			// have the newest timestamps so they survive conflict
			// resolution.
			execPhase(t, destDB, tc.useTxn, []string{
				"INSERT INTO lww VALUES ('ins_val_win',  'dest', 'winning-value')",
				"INSERT INTO lww VALUES ('upd_val_win',  'dest', 'winning-value')",
				"INSERT INTO lww VALUES ('del_val_win',  'dest', 'winning-value')",
				"INSERT INTO lww VALUES ('ins_tomb_win', 'dest', 'tombstone-seed')",
				"INSERT INTO lww VALUES ('upd_tomb_win', 'dest', 'tombstone-seed')",
				"INSERT INTO lww VALUES ('del_tomb_win', 'dest', 'tombstone-seed')",
			})

			// Phase 4b: Delete the tombstone seeds to create winning
			// tombstones.
			execPhase(t, destDB, tc.useTxn, []string{
				"DELETE FROM lww WHERE tc = 'ins_tomb_win'",
				"DELETE FROM lww WHERE tc = 'upd_tomb_win'",
				"DELETE FROM lww WHERE tc = 'del_tomb_win'",
			})

			// Start transactional LDR with cursor set before all the
			// write traffic.
			sourceURL := replicationtestutils.GetExternalConnectionURI(
				t, s, s, serverutils.DBName(sourceDBName))

			var jobID jobspb.JobID
			destDB.QueryRow(t,
				"CREATE LOGICAL REPLICATION STREAM FROM TABLE lww ON $1 INTO TABLE lww WITH MODE = 'transactional', CURSOR = $2",
				sourceURL.String(),
				cursorTS.AsOfSystemTime(),
			).Scan(&jobID)

			now := s.Clock().Now()
			ldrtestutils.WaitUntilReplicatedTime(t, now, destDB, jobID)

			// Assert the final state. Rows where the source wins or there
			// is no conflict appear with source data. Rows where the
			// destination wins appear with destination data. Rows deleted
			// by a winning action are absent.
			//
			// Absent rows:
			//   del_nothing   - delete against nothing produces no row
			//   del_tomb_lose - source delete wins over older tombstone
			//   del_tomb_win  - source delete loses to winning tombstone
			//   del_val_lose  - source delete wins over older value
			//   ins_tomb_win  - source insert loses to winning tombstone
			//   upd_tomb_win  - source update loses to winning tombstone
			destDB.CheckQueryResults(t,
				"SELECT * FROM lww ORDER BY tc",
				[][]string{
					{"del_val_win", "dest", "winning-value"},
					{"ins_nothing", "source", "inserted"},
					{"ins_tomb_lose", "source", "inserted"},
					{"ins_val_lose", "source", "inserted"},
					{"ins_val_win", "dest", "winning-value"},
					{"upd_nothing", "source", "updated"},
					{"upd_tomb_lose", "source", "updated"},
					{"upd_val_lose", "source", "updated"},
					{"upd_val_win", "dest", "winning-value"},
				},
			)
		})
	}
}

func TestTxnModeCreateLogicallyReplicated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithExternalProcessMode(134857),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(conn)

	// Configure low latency replication settings
	sysRunner := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	ldrtestutils.ApplyLowLatencyReplicationSettings(t, sysRunner, runner)

	// Create source database
	runner.Exec(t, "CREATE DATABASE source_db")

	sourceDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("source_db")))

	// Create a table in source with some initial data
	sourceDB.Exec(t, "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL)")
	sourceDB.Exec(t, "INSERT INTO orders VALUES (1, 100, 50.00)")

	// Create destination database
	runner.Exec(t, "CREATE DATABASE dest_db")
	destDB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("dest_db")))

	// Get connection URL for source database
	sourceURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("source_db"))

	// Create logically replicated table with transactional mode and unidirectional replication
	var jobID jobspb.JobID
	destDB.QueryRow(t,
		"CREATE LOGICALLY REPLICATED TABLE orders FROM TABLE orders ON $1 WITH MODE = 'transactional', UNIDIRECTIONAL",
		sourceURL.String(),
	).Scan(&jobID)

	// Check job status
	var status string
	destDB.QueryRow(t, "SELECT status FROM [SHOW JOB $1]", jobID).Scan(&status)
	t.Logf("Job %d status: %s", jobID, status)

	// Insert more data in a transaction after replication starts
	sourceDB.Exec(t, `
		BEGIN;
		INSERT INTO orders VALUES (2, 100, 75.00);
		INSERT INTO orders VALUES (3, 101, 100.00);
		COMMIT;
	`)

	// Wait for replication to catch up
	ldrtestutils.WaitUntilReplicatedTime(t, s.Clock().Now(), destDB, jobID)

	// Verify all data was replicated including initial scan and transactional inserts
	destDB.CheckQueryResults(t, "SELECT * FROM orders ORDER BY id", [][]string{
		{"1", "100", "50.00"},
		{"2", "100", "75.00"},
		{"3", "101", "100.00"},
	})

	// Test that a multi-statement transaction is replicated atomically
	sourceDB.Exec(t, `
		BEGIN;
		UPDATE orders SET amount = amount + 10 WHERE customer_id = 100;
		INSERT INTO orders VALUES (4, 102, 200.00);
		COMMIT;
	`)

	ldrtestutils.WaitUntilReplicatedTime(t, s.Clock().Now(), destDB, jobID)

	// Verify the transaction was applied atomically
	destDB.CheckQueryResults(t, "SELECT * FROM orders ORDER BY id", [][]string{
		{"1", "100", "60.00"},
		{"2", "100", "85.00"},
		{"3", "101", "100.00"},
		{"4", "102", "200.00"},
	})
}
