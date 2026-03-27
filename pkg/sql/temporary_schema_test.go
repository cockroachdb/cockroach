// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupSchemaObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `
SET experimental_enable_temp_tables=true;
SET serial_normalization='sql_sequence';
CREATE TEMP TABLE a (a SERIAL, c INT);`,
	)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `
ALTER TABLE a ADD COLUMN b SERIAL;
CREATE TEMP SEQUENCE a_sequence;
CREATE TEMP VIEW a_view AS SELECT a FROM a;`,
	)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		`CREATE TABLE perm_table (a int DEFAULT nextval('a_sequence'), b int)`)
	require.NoError(t, err)
	// Unlock perm_table so that cleanup can ALTER it to remove the DEFAULT
	// expression referencing the temp sequence; see #167117.
	_, err = conn.ExecContext(ctx, `ALTER TABLE perm_table SET (schema_locked=false)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `INSERT INTO perm_table VALUES (DEFAULT, 1)`)
	require.NoError(t, err)

	namesToID, tempSchemaNames := constructNameToIDMapping(ctx, t, conn)
	require.Equal(t, len(tempSchemaNames), 1, "unexpected number of temp schemas")
	tempSchemaName := tempSchemaNames[0]
	require.NotEqual(t, "", tempSchemaName)

	tempNames := []string{
		"a",
		"a_view",
		"a_sequence",
		"a_a_seq",
		"a_b_seq",
	}
	selectableTempNames := []string{"a", "a_view"}
	for _, name := range append(tempNames, tempSchemaName) {
		require.Contains(t, namesToID, name)
	}
	for _, name := range selectableTempNames {
		// Check tables are accessible.
		var rows *gosql.Rows
		rows, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s", tempSchemaName, name))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
	}
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	ief := execCfg.InternalDB
	require.NoError(t, ief.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		// Add a hack to not wait for one version on the descriptors.
		defer txn.Descriptors().ReleaseAll(ctx)
		defaultDB, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, namesToID["defaultdb"])
		if err != nil {
			return err
		}
		tempSchema, err := txn.Descriptors().ByName(txn.KV()).Get().Schema(ctx, defaultDB, tempSchemaName)
		if err != nil {
			return err
		}
		return cleanupTempSchemaObjects(
			ctx,
			txn,
			txn.Descriptors(),
			defaultDB,
			tempSchema,
		)
	}))

	ensureTemporaryObjectsAreDeleted(ctx, t, conn, tempSchemaName, tempNames)

	// Check perm_table performs correctly, and has the right schema.
	var rows *gosql.Rows
	rows, err = db.Query("SELECT * FROM perm_table")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	var colDefault gosql.NullString
	err = db.QueryRow(
		`SELECT column_default FROM information_schema.columns
		WHERE table_name = 'perm_table' and column_name = 'a'`,
	).Scan(&colDefault)
	require.NoError(t, err)
	assert.False(t, colDefault.Valid)
}

func TestTemporaryObjectCleaner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	numNodes := 3
	ch := make(chan time.Time)
	finishedCh := make(chan struct{})
	knobs := base.TestingKnobs{
		SQLExecutor: &ExecutorTestingKnobs{
			DisableTempObjectsCleanupOnSessionExit: true,
			TempObjectsCleanupCh:                   ch,
			OnTempObjectsCleanupDone: func() {
				finishedCh <- struct{}{}
			},
		},
	}
	settings := cluster.MakeTestingClusterSettings()
	TempObjectWaitInterval.Override(context.Background(), &settings.SV, time.Microsecond)
	tc := serverutils.StartCluster(
		t,
		numNodes,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				UseDatabase: "defaultdb",
				Knobs:       knobs,
				Settings:    settings,
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	{
		// Create another empty database to ensure that cleanup still works in the
		// presence of databases without temp objects. Regression test for #55086.
		db := tc.ServerConn(0)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE DATABASE d`)
	}

	// Start and close two temporary schemas.
	for _, dbID := range []int{0, 1} {
		db := tc.ServerConn(dbID)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET experimental_enable_temp_tables=true`)
		sqlDB.Exec(t, `CREATE TEMP TABLE t (x INT)`)
		// Close the client connection. Normally the temporary data would immediately
		// be cleaned up on session exit, but this is disabled via the
		// DisableTempObjectsCleanupOnSessionExit testing knob.
		require.NoError(t, db.Close())
	}

	// Sanity check: there should still be all temporary schemas present from above.
	// dbConn 2 should have a db connection living longer, which we don't delete.
	db := tc.ServerConn(2)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET experimental_enable_temp_tables=true`)
	sqlDB.Exec(t, `CREATE TEMP TABLE t (x INT); INSERT INTO t VALUES (1)`)
	tempSchemaQuery := `SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`
	var tempSchemaCount int
	sqlDB.QueryRow(t, tempSchemaQuery).Scan(&tempSchemaCount)
	require.Equal(t, tempSchemaCount, 3)

	// Verify that the asynchronous cleanup job kicks in and removes the temporary
	// data.
	testutils.SucceedsSoon(t, func() error {
		// Now force a cleanup run (by default, it is every 30mins).
		// Send this to every node, in case one is not the leaseholder.
		// This needs to be sent on each run, in case the lease master
		// has not been decided.
		for i := 0; i < numNodes; i++ {
			ch <- timeutil.Now()
		}
		// Block until all nodes have responded.
		// This prevents the stress tests running into #28033, where
		// ListSessions races with the QueryRow.
		for i := 0; i < numNodes; i++ {
			<-finishedCh
		}
		sqlDB.QueryRow(t, tempSchemaQuery).Scan(&tempSchemaCount)
		if tempSchemaCount != 1 {
			return errors.Errorf("expected 1 temp schemas, found %d", tempSchemaCount)
		}
		return nil
	})
	var tRowCount int
	sqlDB.QueryRow(t, "SELECT count(*) FROM t").Scan(&tRowCount)
	require.Equal(t, 1, tRowCount)
	require.NoError(t, db.Close())
}

// TestTemporarySchemaDropDatabase tests having a temporary schema on one session
// whilst dropping a database on another session will have the database drop
// succeed.
func TestTemporarySchemaDropDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	numNodes := 3
	tc := serverutils.StartCluster(
		t,
		numNodes,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				UseDatabase: "defaultdb",
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	// Create a database to drop that has a temporary table inside.
	{
		db := tc.ServerConn(0)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE DATABASE drop_me`)
		sqlDB.Exec(t, `USE drop_me`)
		sqlDB.Exec(t, `SET experimental_enable_temp_tables=true`)
		sqlDB.Exec(t, `CREATE TEMP TABLE t (x INT)`)
	}

	// On another session, only leave the schema behind.
	{
		db := tc.ServerConn(1)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `USE drop_me`)
		sqlDB.Exec(t, `SET experimental_enable_temp_tables=true`)
		sqlDB.Exec(t, `CREATE TEMP TABLE t2 (x INT)`)
		sqlDB.Exec(t, `DROP TABLE t2`)
	}

	// On another session, drop the database.
	{
		db := tc.ServerConn(2)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `DROP DATABASE drop_me CASCADE`)

		testutils.SucceedsSoon(t, func() error {
			var tempObjectCount int
			sqlDB.QueryRow(
				t,
				`SELECT count(1) FROM system.namespace WHERE name LIKE 'pg_temp%' OR name IN ('t', 't2')`,
			).Scan(&tempObjectCount)
			if tempObjectCount == 0 {
				return nil
			}
			return errors.AssertionFailedf("expected count 0, got %d", tempObjectCount)
		})
	}
}

// ensureTemporaryObjectsAreDeleted ensures all the tempNames have been deleted.
// This can take a longer amount of time if the job takes time.
func ensureTemporaryObjectsAreDeleted(
	ctx context.Context, t *testing.T, conn *gosql.Conn, schemaName string, tempNames []string,
) {
	for _, name := range tempNames {
		_, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s", schemaName, name))
		if err != nil {
			if !strings.Contains(err.Error(), fmt.Sprintf(`relation "%s.%s" does not exist`, schemaName, name)) {
				t.Fatal(errors.Errorf("expected %s.%s error to resolve relation not existing", schemaName, name))
			}
		}
	}
}

// constructNameToIDMapping constructs and returns a mapping of names to IDs for
// all objects in system.namespace along with the temp schemas.
func constructNameToIDMapping(
	ctx context.Context, t *testing.T, conn *gosql.Conn,
) (map[string]descpb.ID, []string) {
	rows, err := conn.QueryContext(ctx, `SELECT id, name FROM system.namespace`)
	require.NoError(t, err)

	namesToID := make(map[string]descpb.ID)
	tempSchemaNames := make([]string, 0)
	for rows.Next() {
		var id int64
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)

		namesToID[name] = descpb.ID(id)
		if strings.HasPrefix(name, catconstants.PgTempSchemaName) {
			tempSchemaNames = append(tempSchemaNames, name)
		}
	}
	return namesToID, tempSchemaNames
}

// TestTemporaryObjectCleanupRetriesWithPoisonedTransaction tests that the
// cleanup process doesn't retry when encountering a poisoned transaction error.
func TestTemporaryObjectCleanupRetriesWithPoisonedTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		name               string
		errorInjectionFunc func(attemptCount *int) error
		expectedAttempts   int
		expectError        bool
		errorSubstring     string
	}{
		{
			name: "poisoned transaction stops retry",
			errorInjectionFunc: func(attemptCount *int) error {
				*attemptCount++
				return &kvpb.TxnAlreadyEncounteredErrorError{}
			},
			expectedAttempts: 1,
			expectError:      true,
			errorSubstring:   "txn already encountered an error",
		},
		{
			name: "regular error retries and succeeds",
			errorInjectionFunc: func(attemptCount *int) error {
				*attemptCount++
				// Fail on first attempt, succeed on second
				if *attemptCount == 1 {
					return errors.New("some retryable error")
				}
				return nil
			},
			expectedAttempts: 2,
			expectError:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var attemptCount int

			testingKnobs := ExecutorTestingKnobs{
				TempObjectCleanupErrorInjection: func() error {
					return tc.errorInjectionFunc(&attemptCount)
				},
			}

			s := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			execCfg := s.ExecutorConfig().(ExecutorConfig)

			// Create a temporary object cleaner with our testing knobs
			cleaner := NewTemporaryObjectCleaner(
				execCfg.Settings,
				execCfg.InternalDB,
				execCfg.Codec,
				metric.NewRegistry(),
				execCfg.SQLStatusServer,
				func(context.Context, hlc.ClockTimestamp) (bool, error) { return true, nil },
				testingKnobs,
				nil,
			)

			// Run cleanup and verify behavior
			err := cleaner.doTemporaryObjectCleanup(ctx, nil)
			if tc.expectError {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.errorSubstring)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedAttempts, attemptCount,
				"expected %d attempts, got %d", tc.expectedAttempts, attemptCount)
		})
	}
}

// TestBatchedTempObjectCleanup verifies that session-close cleanup works
// correctly when the number of temp objects exceeds the batch size, requiring
// multiple batched transactions. It tests both batch size 5 (multi-object
// batches) and batch size 1 (each object in its own transaction).
func TestBatchedTempObjectCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numTables = 17
	testCases := []struct {
		name      string
		batchSize int64
	}{
		{"batch_size_5", 5},
		{"batch_size_1", 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			TempObjectCleanupBatchSize.Override(ctx, &settings.SV, tc.batchSize)

			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				Settings: settings,
			})
			defer s.Stopper().Stop(ctx)

			// Open a separate connection for temp table creation. Closing this
			// connection triggers cleanupSessionTempObjects on session exit,
			// exercising the multi-batch path through dropTempSchemaObjectsInBatches.
			tempDB := s.ApplicationLayer().SQLConn(t)
			conn, err := tempDB.Conn(ctx)
			require.NoError(t, err)

			_, err = conn.ExecContext(ctx, `SET experimental_enable_temp_tables=true`)
			require.NoError(t, err)

			for i := 0; i < numTables; i++ {
				_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE t%d (x INT)", i))
				require.NoError(t, err)
			}

			// Verify temp schema exists before cleanup.
			var tempSchemaCount int
			err = db.QueryRow(
				`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
			).Scan(&tempSchemaCount)
			require.NoError(t, err)
			require.Equal(t, 1, tempSchemaCount)

			// Close the connection to trigger session-exit cleanup via
			// cleanupSessionTempObjects -> dropTempSchemaObjectsInBatches.
			require.NoError(t, conn.Close())
			require.NoError(t, tempDB.Close())

			// Verify all temp schemas are cleaned up. Phase 3 of
			// cleanupSessionTempObjects only removes schema entries after all
			// objects are dropped in Phase 2, so 0 schemas implies all objects
			// were successfully dropped across batches.
			testutils.SucceedsSoon(t, func() error {
				var count int
				if err := db.QueryRow(
					`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
				).Scan(&count); err != nil {
					return err
				}
				if count != 0 {
					return errors.Errorf("expected 0 temp schemas, found %d", count)
				}
				return nil
			})

			// Verify server is healthy after batched cleanup.
			var result int
			err = db.QueryRow("SELECT 1").Scan(&result)
			require.NoError(t, err)
			require.Equal(t, 1, result)
		})
	}
}

// TestBatchedTempCleanupWithMixedObjectTypes verifies that batching respects
// dependency ordering (views -> tables -> sequences) and handles the
// cleanupTempSequenceDeps preHook correctly across batch boundaries.
func TestBatchedTempCleanupWithMixedObjectTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	TempObjectCleanupBatchSize.Override(ctx, &settings.SV, 3)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
	})
	defer s.Stopper().Stop(ctx)

	tempDB := s.ApplicationLayer().SQLConn(t)
	conn, err := tempDB.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET experimental_enable_temp_tables=true`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SET serial_normalization='sql_sequence'`)
	require.NoError(t, err)

	// Create 4 temp tables (requires 2 batches with batch size 3).
	// t0 uses a SERIAL column, which creates an owned sequence (t0_x_seq).
	// Owned sequences are excluded from the explicit drop lists and must be
	// dropped via CASCADE when their owner table is dropped.
	_, err = conn.ExecContext(ctx, `CREATE TEMP TABLE t0 (x SERIAL)`)
	require.NoError(t, err)
	for i := 1; i < 4; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE t%d (x INT)", i))
		require.NoError(t, err)
	}

	// Create 4 temp views referencing the tables (requires 2 batches).
	for i := 0; i < 4; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TEMP VIEW v%d AS SELECT x FROM t%d", i, i,
		))
		require.NoError(t, err)
	}

	// Create 2 unowned temp sequences (1 batch).
	_, err = conn.ExecContext(ctx, `CREATE TEMP SEQUENCE s0`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TEMP SEQUENCE s1`)
	require.NoError(t, err)

	// Create a permanent table with a default expression referencing a temp
	// sequence. This exercises the cleanupTempSequenceDeps path. The table
	// must be unlocked so cleanup can ALTER it to remove the DEFAULT; see
	// #167117 for the underlying schema_locked interaction.
	_, err = conn.ExecContext(ctx,
		`CREATE TABLE perm_table (a INT DEFAULT nextval('s0'), b INT)`)
	require.NoError(t, err)
	// Unlock perm_table so that cleanup can ALTER it to remove the DEFAULT
	// expression referencing the temp sequence; see #167117.
	_, err = conn.ExecContext(ctx, `ALTER TABLE perm_table SET (schema_locked=false)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `INSERT INTO perm_table VALUES (DEFAULT, 1)`)
	require.NoError(t, err)

	// Close the connection to trigger session-exit cleanup.
	require.NoError(t, conn.Close())
	require.NoError(t, tempDB.Close())

	// Verify all temp schemas are cleaned up.
	testutils.SucceedsSoon(t, func() error {
		var count int
		if err := db.QueryRow(
			`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
		).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.Errorf("expected 0 temp schemas, found %d", count)
		}
		return nil
	})

	// Verify permanent table is intact and queryable.
	var rows *gosql.Rows
	rows, err = db.Query("SELECT * FROM perm_table")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	// Verify the default expression on the permanent table was cleared
	// (the temp sequence it referenced was dropped by the preHook).
	var colDefault gosql.NullString
	err = db.QueryRow(
		`SELECT column_default FROM information_schema.columns
		WHERE table_name = 'perm_table' and column_name = 'a'`,
	).Scan(&colDefault)
	require.NoError(t, err)
	assert.False(t, colDefault.Valid)
}

// TestBatchedTempCleanupCascadeAcrossBatches verifies that CASCADE in one
// batch does not break subsequent batches when it implicitly drops objects
// scheduled for later batches. For example, with batch size 1:
//
//	DROP VIEW v1 CASCADE  → also drops v2 (which depends on v1)
//	DROP VIEW v2 CASCADE  → v2 already gone, must not error
//
// The DROP uses IF EXISTS to handle this scenario.
func TestBatchedTempCleanupCascadeAcrossBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	// Batch size 1 forces each object into its own batch, maximizing the
	// chance of cross-batch CASCADE conflicts.
	TempObjectCleanupBatchSize.Override(ctx, &settings.SV, 1)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings: settings,
	})
	defer s.Stopper().Stop(ctx)

	// Use a separate pool so closing it terminates the pgwire session.
	tempDB := s.ApplicationLayer().SQLConn(t)
	conn, err := tempDB.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET experimental_enable_temp_tables=true`)
	require.NoError(t, err)

	// Create a chain of views: v3 → v2 → v1 → base_t.
	// DROP VIEW v1 CASCADE will also drop v2 and v3.
	_, err = conn.ExecContext(ctx, `CREATE TEMP TABLE base_t (x INT)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TEMP VIEW v1 AS SELECT x FROM base_t`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TEMP VIEW v2 AS SELECT x FROM v1`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TEMP VIEW v3 AS SELECT x FROM v2`)
	require.NoError(t, err)

	// Close the session to trigger cleanup.
	require.NoError(t, conn.Close())
	require.NoError(t, tempDB.Close())

	// Verify everything was cleaned up despite cross-batch CASCADE.
	verifyDB := s.ApplicationLayer().SQLConn(t)
	defer verifyDB.Close()
	testutils.SucceedsSoon(t, func() error {
		var count int
		if err := verifyDB.QueryRow(
			`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
		).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.Errorf("expected 0 temp schemas, found %d", count)
		}
		return nil
	})
}

// TestBatchedTempCleanupViaBackgroundCleaner verifies that the background
// TemporaryObjectCleaner correctly cleans up batched objects when session-close
// cleanup is disabled.
func TestBatchedTempCleanupViaBackgroundCleaner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ch := make(chan time.Time)
	finishedCh := make(chan struct{})
	knobs := base.TestingKnobs{
		SQLExecutor: &ExecutorTestingKnobs{
			DisableTempObjectsCleanupOnSessionExit: true,
			TempObjectsCleanupCh:                   ch,
			OnTempObjectsCleanupDone: func() {
				finishedCh <- struct{}{}
			},
		},
	}
	settings := cluster.MakeTestingClusterSettings()
	TempObjectWaitInterval.Override(ctx, &settings.SV, time.Microsecond)
	TempObjectCleanupBatchSize.Override(ctx, &settings.SV, 5)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs:    knobs,
		Settings: settings,
	})
	defer s.Stopper().Stop(ctx)

	// Create temp tables on a separate connection.
	tempDB := s.ApplicationLayer().SQLConn(t)
	conn, err := tempDB.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET experimental_enable_temp_tables=true`)
	require.NoError(t, err)

	const numTables = 17
	for i := 0; i < numTables; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE t%d (x INT)", i))
		require.NoError(t, err)
	}
	require.NoError(t, conn.Close())
	require.NoError(t, tempDB.Close())

	// Verify temp schemas still exist (cleanup was disabled on session exit).
	verifyDB := s.ApplicationLayer().SQLConn(t)
	defer verifyDB.Close()

	var tempSchemaCount int
	err = verifyDB.QueryRow(
		`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
	).Scan(&tempSchemaCount)
	require.NoError(t, err)
	require.Equal(t, 1, tempSchemaCount, "temp schema should still exist after session close")

	// Trigger background cleaner and wait for it to clean up all temp objects.
	testutils.SucceedsSoon(t, func() error {
		ch <- timeutil.Now()
		<-finishedCh
		var count int
		if err := verifyDB.QueryRow(
			`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
		).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.Errorf("expected 0 temp schemas, found %d", count)
		}
		return nil
	})
}

// TestBatchedTempCleanupPartialFailureRecovery verifies that if cleanup is
// partially completed (some objects already dropped), the background cleaner
// can recover and clean up the remaining objects and schema.
func TestBatchedTempCleanupPartialFailureRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ch := make(chan time.Time)
	finishedCh := make(chan struct{})
	knobs := base.TestingKnobs{
		SQLExecutor: &ExecutorTestingKnobs{
			DisableTempObjectsCleanupOnSessionExit: true,
			TempObjectsCleanupCh:                   ch,
			OnTempObjectsCleanupDone: func() {
				finishedCh <- struct{}{}
			},
		},
	}
	settings := cluster.MakeTestingClusterSettings()
	TempObjectWaitInterval.Override(ctx, &settings.SV, time.Microsecond)
	TempObjectCleanupBatchSize.Override(ctx, &settings.SV, 5)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs:    knobs,
		Settings: settings,
	})
	defer s.Stopper().Stop(ctx)

	// Create 15 temp tables on a separate connection.
	tempDB := s.ApplicationLayer().SQLConn(t)
	conn, err := tempDB.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET experimental_enable_temp_tables=true`)
	require.NoError(t, err)

	const numTables = 15
	for i := 0; i < numTables; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE t%d (x INT)", i))
		require.NoError(t, err)
	}
	require.NoError(t, conn.Close())
	require.NoError(t, tempDB.Close())

	// Get the temp schema name for cross-session table references.
	verifyDB := s.ApplicationLayer().SQLConn(t)
	defer verifyDB.Close()

	var tempSchemaName string
	err = verifyDB.QueryRow(
		`SELECT name FROM system.namespace WHERE name LIKE 'pg_temp%'`,
	).Scan(&tempSchemaName)
	require.NoError(t, err)

	// Simulate partial cleanup by manually dropping 5 of the 15 temp tables.
	// This mimics the state after an interrupted batched cleanup (e.g., server
	// crash after some batches completed).
	for i := 0; i < 5; i++ {
		_, err = verifyDB.Exec(fmt.Sprintf(
			`DROP TABLE defaultdb."%s".t%d`, tempSchemaName, i,
		))
		require.NoError(t, err)
	}

	// Verify temp schema still exists with remaining objects.
	var tempSchemaCount int
	err = verifyDB.QueryRow(
		`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
	).Scan(&tempSchemaCount)
	require.NoError(t, err)
	require.Equal(t, 1, tempSchemaCount, "temp schema should still exist")

	// Trigger background cleaner to recover the remaining objects.
	// Phase 1 should discover only the 10 remaining tables, and Phase 2/3
	// should clean them up along with the schema.
	testutils.SucceedsSoon(t, func() error {
		ch <- timeutil.Now()
		<-finishedCh
		var count int
		if err := verifyDB.QueryRow(
			`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
		).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.Errorf("expected 0 temp schemas, found %d", count)
		}
		return nil
	})
}

// TestBatchedTempCleanupMultiDatabase verifies that batched cleanup works
// correctly when a session has temp objects across multiple databases.
// This exercises Phase 1 collecting multiple tempSchemaInfo entries,
// Phase 2 dropping objects per-database, and Phase 3 deleting multiple
// schema namespace entries in a single KV batch.
func TestBatchedTempCleanupMultiDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	TempObjectCleanupBatchSize.Override(ctx, &settings.SV, 3)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
	})
	defer s.Stopper().Stop(ctx)

	// Create a second database.
	_, err := db.Exec(`CREATE DATABASE db2`)
	require.NoError(t, err)

	// Create temp objects in both databases on the same connection. This
	// produces one temp schema per database, both owned by the same session.
	tempDB := s.ApplicationLayer().SQLConn(t)
	conn, err := tempDB.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET experimental_enable_temp_tables=true`)
	require.NoError(t, err)

	// Create 4 temp tables in defaultdb.
	for i := 0; i < 4; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE t%d (x INT)", i))
		require.NoError(t, err)
	}

	// Switch to db2 and create 4 temp tables there.
	_, err = conn.ExecContext(ctx, `USE db2`)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE t%d (x INT)", i))
		require.NoError(t, err)
	}

	// Verify two temp schemas exist (one per database).
	var tempSchemaCount int
	err = db.QueryRow(
		`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
	).Scan(&tempSchemaCount)
	require.NoError(t, err)
	require.Equal(t, 2, tempSchemaCount)

	// Close the connection to trigger session-exit cleanup.
	require.NoError(t, conn.Close())
	require.NoError(t, tempDB.Close())

	// Verify all temp schemas across both databases are cleaned up.
	testutils.SucceedsSoon(t, func() error {
		var count int
		if err := db.QueryRow(
			`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
		).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.Errorf("expected 0 temp schemas, found %d", count)
		}
		return nil
	})
}

// TestBatchedTempCleanupIdempotent verifies that calling
// cleanupSessionTempObjects twice for the same session is safe — the second
// call should find no temp schemas and return early. This validates the
// schemas = nil reset logic in Phase 1 and the len(schemas) == 0 early
// return.
func TestBatchedTempCleanupIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	TempObjectCleanupBatchSize.Override(ctx, &settings.SV, 5)

	// Disable session-exit cleanup so we can call cleanupSessionTempObjects
	// directly without racing against automatic cleanup.
	knobs := base.TestingKnobs{
		SQLExecutor: &ExecutorTestingKnobs{
			DisableTempObjectsCleanupOnSessionExit: true,
		},
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs:    knobs,
	})
	defer s.Stopper().Stop(ctx)

	tempDB := s.ApplicationLayer().SQLConn(t)
	conn, err := tempDB.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET experimental_enable_temp_tables=true`)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMP TABLE t%d (x INT)", i))
		require.NoError(t, err)
	}

	// Get the temp schema name to extract the session ID.
	var tempSchemaName string
	err = db.QueryRow(
		`SELECT name FROM system.namespace WHERE name LIKE 'pg_temp%'`,
	).Scan(&tempSchemaName)
	require.NoError(t, err)

	// Parse the session ID from the temp schema name.
	isTempSchema, sessionID, err := temporarySchemaSessionID(tempSchemaName)
	require.NoError(t, err)
	require.True(t, isTempSchema)

	// Close the connection. Session-exit cleanup is disabled, so the temp
	// objects remain.
	require.NoError(t, conn.Close())
	require.NoError(t, tempDB.Close())

	// Verify temp schema still exists.
	var tempSchemaCount int
	err = db.QueryRow(
		`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
	).Scan(&tempSchemaCount)
	require.NoError(t, err)
	require.Equal(t, 1, tempSchemaCount, "temp schema should still exist")

	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// First call: should drop all 10 tables and remove the schema entry.
	err = cleanupSessionTempObjects(ctx, execCfg.InternalDB, execCfg.Settings, sessionID)
	require.NoError(t, err)

	// Verify everything is cleaned up.
	var count int
	err = db.QueryRow(
		`SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`,
	).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "expected 0 temp schemas after first cleanup")

	// Second call: should be a no-op (Phase 1 finds no schemas, returns
	// early at the len(schemas) == 0 check).
	err = cleanupSessionTempObjects(ctx, execCfg.InternalDB, execCfg.Settings, sessionID)
	require.NoError(t, err)
}
