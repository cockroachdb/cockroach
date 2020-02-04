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
CREATE TEMP TABLE a (a int);
CREATE TEMP VIEW a_view AS SELECT a FROM a;
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

	require.Contains(t, namesToID, "a")
	require.Contains(t, namesToID, "a_view")
	require.NotEqual(t, "", schemaName)

	// Check tables are accessible.
	_, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.a", schemaName))
	require.NoError(t, err)

	_, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.a_view", schemaName))
	require.NoError(t, err)

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

	// Ensure all the entries for the given temporary structures are gone.
	_, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.a", schemaName))
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf(`relation "%s.a" does not exist`, schemaName))

	_, err = conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.a_view", schemaName))
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf(`relation "%s.a_view" does not exist`, schemaName))
}
