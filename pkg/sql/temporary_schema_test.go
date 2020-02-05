// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
SET experimental_serial_normalization='sql_sequence';
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
		kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			err = sql.TestingCleanupSchemaObjects(
				ctx,
				s.ExecutorConfig().(sql.ExecutorConfig).Settings,
				func(
					ctx context.Context, _ string, _ *client.Txn, query string, _ ...interface{},
				) (int, error) {
					_, err := conn.ExecContext(ctx, query)
					return 0, err
				},
				txn,
				namesToID["defaultdb"],
				schemaName,
			)
			require.NoError(t, err)
			return nil
		}),
	)

	for _, name := range selectableTempNames {
		// Ensure all the entries for the given temporary structures are gone.
		_, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s", schemaName, name))
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf(`relation "%s.%s" does not exist`, schemaName, name))
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
