// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestShowTransferState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tenant, mainDB := serverutils.StartTenant(t, s, tests.CreateTestTenantParams(serverutils.TestTenantID()))
	defer tenant.Stopper().Stop(ctx)
	defer mainDB.Close()

	_, err := mainDB.Exec("CREATE USER testuser WITH PASSWORD 'hunter2'")
	require.NoError(t, err)
	_, err = mainDB.Exec("SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true")
	require.NoError(t, err)

	t.Run("without_transfer_key", func(t *testing.T) {
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestShowTransferState-without_transfer_key",
			url.UserPassword(security.TestUser, "hunter2"),
		)
		defer cleanup()

		conn, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		defer conn.Close()

		rows, err := conn.Query("SHOW TRANSFER STATE")
		require.NoError(t, err, "show transfer state failed")
		defer rows.Close()

		resultColumns, err := rows.Columns()
		require.NoError(t, err)

		require.Equal(t, []string{
			"error",
			"session_state_base64",
			"session_revival_token_base64",
		}, resultColumns)

		var errVal, sessionState, sessionRevivalToken gosql.NullString

		rows.Next()
		err = rows.Scan(&errVal, &sessionState, &sessionRevivalToken)
		require.NoError(t, err, "unexpected error while reading transfer state")

		require.False(t, errVal.Valid)
		require.True(t, sessionState.Valid)
		require.True(t, sessionRevivalToken.Valid)
	})

	var state, token string
	t.Run("with_transfer_key", func(t *testing.T) {
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestShowTransferState-with_transfer_key",
			url.UserPassword(security.TestUser, "hunter2"),
		)
		defer cleanup()

		q := pgURL.Query()
		q.Add("application_name", "carl")
		pgURL.RawQuery = q.Encode()
		conn, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		defer conn.Close()

		// Add a prepared statement to make sure SHOW TRANSFER STATE handles it.
		// Since lib/pq doesn't tell us the name of the prepared statement, we won't
		// be able to test that we can use it after deserializing the session, but
		// there are other tests for that.
		stmt, err := conn.Prepare("SELECT 1 WHERE 1 = 1")
		require.NoError(t, err)
		defer stmt.Close()

		rows, err := conn.Query(`SHOW TRANSFER STATE WITH 'foobar'`)
		require.NoError(t, err, "show transfer state failed")
		defer rows.Close()

		resultColumns, err := rows.Columns()
		require.NoError(t, err)

		require.Equal(t, []string{
			"error",
			"session_state_base64",
			"session_revival_token_base64",
			"transfer_key",
		}, resultColumns)

		var key string
		var errVal, sessionState, sessionRevivalToken gosql.NullString

		rows.Next()
		err = rows.Scan(&errVal, &sessionState, &sessionRevivalToken, &key)
		require.NoError(t, err, "unexpected error while reading transfer state")

		require.Equal(t, "foobar", key)
		require.False(t, errVal.Valid)
		require.True(t, sessionState.Valid)
		require.True(t, sessionRevivalToken.Valid)
		state = sessionState.String
		token = sessionRevivalToken.String
	})

	t.Run("successful_transfer", func(t *testing.T) {
		pgURL, cleanup := sqlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestShowTransferState-successful_transfer",
			url.User(security.TestUser), // Do not use a password here.
		)
		defer cleanup()

		q := pgURL.Query()
		q.Add("application_name", "someotherapp")
		q.Add("crdb:session_revival_token_base64", token)
		pgURL.RawQuery = q.Encode()
		conn, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		defer conn.Close()

		var appName string
		err = conn.QueryRow("SHOW application_name").Scan(&appName)
		require.NoError(t, err)
		require.Equal(t, "someotherapp", appName)

		var b bool
		err = conn.QueryRow(
			"SELECT crdb_internal.deserialize_session(decode($1, 'base64'))",
			state,
		).Scan(&b)
		require.NoError(t, err)
		require.True(t, b)

		err = conn.QueryRow("SHOW application_name").Scan(&appName)
		require.NoError(t, err)
		require.Equal(t, "carl", appName)
	})

	// Errors should be displayed as a SQL value.
	t.Run("errors", func(t *testing.T) {
		t.Run("root_user", func(t *testing.T) {
			var key string
			var errVal, sessionState, sessionRevivalToken gosql.NullString
			err := mainDB.QueryRow(`SHOW TRANSFER STATE WITH 'bar'`).Scan(&errVal, &sessionState, &sessionRevivalToken, &key)
			require.NoError(t, err)

			require.True(t, errVal.Valid)
			require.Equal(t, "cannot create token for root user", errVal.String)
			require.False(t, sessionState.Valid)
			require.False(t, sessionRevivalToken.Valid)
		})

		t.Run("transaction", func(t *testing.T) {
			pgURL, cleanup := sqlutils.PGUrl(
				t,
				tenant.SQLAddr(),
				"TestShowTransferState-errors-transaction",
				url.UserPassword(security.TestUser, "hunter2"),
			)
			defer cleanup()

			conn, err := gosql.Open("postgres", pgURL.String())
			require.NoError(t, err)
			defer conn.Close()

			var errVal, sessionState, sessionRevivalToken gosql.NullString
			err = crdb.ExecuteTx(ctx, conn, nil /* txopts */, func(tx *gosql.Tx) error {
				return tx.QueryRow("SHOW TRANSFER STATE").Scan(&errVal, &sessionState, &sessionRevivalToken)
			})
			require.NoError(t, err)

			require.True(t, errVal.Valid)
			require.Equal(t, "cannot serialize a session which is inside a transaction", errVal.String)
			require.False(t, sessionState.Valid)
			require.False(t, sessionRevivalToken.Valid)
		})

		t.Run("temp_tables", func(t *testing.T) {
			pgURL, cleanup := sqlutils.PGUrl(
				t,
				tenant.SQLAddr(),
				"TestShowTransferState-errors-temp_tables",
				url.UserPassword(security.TestUser, "hunter2"),
			)
			defer cleanup()

			q := pgURL.Query()
			q.Add("experimental_enable_temp_tables", "true")
			pgURL.RawQuery = q.Encode()
			conn, err := gosql.Open("postgres", pgURL.String())
			require.NoError(t, err)
			defer conn.Close()

			_, err = conn.Exec("CREATE TEMP TABLE temp_tbl()")
			require.NoError(t, err)

			var errVal, sessionState, sessionRevivalToken gosql.NullString
			err = conn.QueryRow("SHOW TRANSFER STATE").Scan(&errVal, &sessionState, &sessionRevivalToken)
			require.NoError(t, err)

			require.True(t, errVal.Valid)
			require.Equal(t, "cannot serialize session with temporary schemas", errVal.String)
			require.False(t, sessionState.Valid)
			require.False(t, sessionRevivalToken.Valid)
		})
	})
}
