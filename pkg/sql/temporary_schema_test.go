// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupSchemaObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `
SET experimental_enable_temp_tables=true;
SET serial_normalization='sql_sequence';
CREATE TEMP TABLE a (a SERIAL, c INT);
ALTER TABLE a ADD COLUMN b SERIAL;
CREATE TEMP SEQUENCE a_sequence;
CREATE TEMP VIEW a_view AS SELECT a FROM a;
CREATE TABLE perm_table (a int DEFAULT nextval('a_sequence'), b int);
INSERT INTO perm_table VALUES (DEFAULT, 1);
`)
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, `SELECT id, name FROM system.namespace`)
	require.NoError(t, err)

	namesToID := make(map[string]sqlbase.ID)
	var schemaName string
	for rows.Next() {
		var id int64
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)

		namesToID[name] = sqlbase.ID(id)
		if strings.HasPrefix(name, sessiondata.PgTempSchemaName) {
			schemaName = name
		}
	}

	require.NotEqual(t, "", schemaName)

	tempNames := []string{
		"a",
		"a_view",
		"a_sequence",
		"a_a_seq",
		"a_b_seq",
	}
	selectableTempNames := []string{"a", "a_view"}
	for _, name := range append(tempNames, schemaName) {
		require.Contains(t, namesToID, name)
	}
	for _, name := range selectableTempNames {
		// Check tables are accessible.
		_, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s", schemaName, name))
		require.NoError(t, err)
	}

	require.NoError(
		t,
		kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			execCfg := s.ExecutorConfig().(ExecutorConfig)
			err = cleanupSchemaObjects(
				ctx,
				execCfg.Settings,
				txn,
				execCfg.Codec,
				s.InternalExecutor().(*InternalExecutor),
				namesToID["defaultdb"],
				schemaName,
			)
			require.NoError(t, err)
			return nil
		}),
	)

	for _, name := range selectableTempNames {
		// Ensure all the entries for the given temporary structures are gone.
		// This can take a longer amount of time if the job takes time / lease doesn't expire in time.
		testutils.SucceedsSoon(t, func() error {
			_, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s", schemaName, name))
			if err != nil {
				if !strings.Contains(err.Error(), fmt.Sprintf(`relation "%s.%s" does not exist`, schemaName, name)) {
					return errors.Errorf("expected %s.%s error to resolve relation not existing", schemaName, name)
				}
				return nil //nolint:returnerrcheck
			}
			return errors.Errorf("expected %s.%s to be deleted", schemaName, name)
		})
	}

	// Check perm_table performs correctly, and has the right schema.
	_, err = db.Query("SELECT * FROM perm_table")
	require.NoError(t, err)

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
	tc := serverutils.StartTestCluster(
		t,
		numNodes,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				UseDatabase: "defaultdb",
				Knobs:       knobs,
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

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

	numNodes := 3
	tc := serverutils.StartTestCluster(
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

		var tempObjectCount int
		sqlDB.QueryRow(
			t,
			`SELECT count(1) FROM system.namespace WHERE name LIKE 'pg_temp%' OR name IN ('t', 't2')`,
		).Scan(&tempObjectCount)
		assert.Equal(t, 0, tempObjectCount)
	}
}
