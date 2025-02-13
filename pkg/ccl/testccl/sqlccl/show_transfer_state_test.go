// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlccl

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestShowTransferState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, mainDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)
	tenant, tenantDB := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
	})
	defer tenant.AppStopper().Stop(ctx)

	_, err := tenantDB.Exec("CREATE USER testuser WITH PASSWORD 'hunter2'")
	require.NoError(t, err)
	_, err = mainDB.Exec("ALTER TENANT ALL SET CLUSTER SETTING server.user_login.session_revival_token.enabled = true")
	require.NoError(t, err)
	_, err = tenantDB.Exec("CREATE TYPE typ AS ENUM ('foo', 'bar')")
	require.NoError(t, err)
	_, err = tenantDB.Exec("CREATE TABLE tab (a INT4, b typ)")
	require.NoError(t, err)
	_, err = tenantDB.Exec("INSERT INTO tab VALUES (1, 'foo')")
	require.NoError(t, err)
	_, err = tenantDB.Exec("GRANT SELECT ON tab TO testuser")
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		// Waiting for the cluster setting to propagate to the tenant.
		var enabled bool
		if err := tenantDB.QueryRow(
			`SHOW CLUSTER SETTING server.user_login.session_revival_token.enabled`,
		).Scan(&enabled); err != nil {
			return err
		}
		if !enabled {
			return errors.New("cluster setting not yet propagated")
		}
		return nil
	})

	testUserConn := tenant.SQLConn(t, serverutils.User(username.TestUser))

	t.Run("without_transfer_key", func(t *testing.T) {
		conn := testUserConn
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

		require.Falsef(t, errVal.Valid, "expected null error, got %s", errVal.String)
		require.True(t, sessionState.Valid)
		require.True(t, sessionRevivalToken.Valid)
	})

	var state, token string
	t.Run("with_transfer_key", func(t *testing.T) {
		pgURL, cleanup := pgurlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestShowTransferState-with_transfer_key",
			url.UserPassword(username.TestUser, "hunter2"),
		)
		defer cleanup()

		q := pgURL.Query()
		q.Add("application_name", "carl")
		pgURL.RawQuery = q.Encode()
		conn, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()

		// Add a prepared statement to make sure SHOW TRANSFER STATE handles it.
		_, err = conn.Prepare(ctx, "prepared_stmt_const", "SELECT $1::INT4, 'foo'::typ WHERE 1 = 1")
		require.NoError(t, err)
		_, err = conn.Prepare(ctx, "prepared_stmt_aost", "SELECT a, b FROM tab AS OF SYSTEM TIME '-1us'")
		require.NoError(t, err)

		var intResult int
		var enumResult string
		err = conn.QueryRow(ctx, "prepared_stmt_const", 1).Scan(&intResult, &enumResult)
		require.NoError(t, err)
		require.Equal(t, 1, intResult)
		require.Equal(t, "foo", enumResult)
		err = conn.QueryRow(ctx, "prepared_stmt_aost").Scan(&intResult, &enumResult)
		require.NoError(t, err)
		require.Equal(t, 1, intResult)
		require.Equal(t, "foo", enumResult)

		rows, err := conn.Query(ctx, `SHOW TRANSFER STATE WITH 'foobar'`, pgx.QueryExecModeSimpleProtocol)
		require.NoError(t, err, "show transfer state failed")
		defer rows.Close()

		fieldDescriptions := rows.FieldDescriptions()
		var resultColumns []string
		for _, f := range fieldDescriptions {
			resultColumns = append(resultColumns, f.Name)
		}

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
		pgURL, cleanup := pgurlutils.PGUrl(
			t,
			tenant.SQLAddr(),
			"TestShowTransferState-successful_transfer",
			url.User(username.TestUser), // Do not use a password here.
		)
		defer cleanup()

		q := pgURL.Query()
		q.Add("application_name", "someotherapp")
		q.Add("crdb:session_revival_token_base64", token)
		pgURL.RawQuery = q.Encode()
		conn, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()

		var appName string
		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&appName)
		require.NoError(t, err)
		require.Equal(t, "someotherapp", appName)

		var b bool
		err = conn.QueryRow(
			ctx,
			"SELECT crdb_internal.deserialize_session(decode($1, 'base64'))",
			state,
		).Scan(&b)
		require.NoError(t, err)
		require.True(t, b)

		err = conn.QueryRow(ctx, "SHOW application_name").Scan(&appName)
		require.NoError(t, err)
		require.Equal(t, "carl", appName)

		// Confirm that the prepared statement can be used after deserializing the
		// session.
		result := conn.PgConn().ExecPrepared(
			ctx,
			"prepared_stmt_const",
			[][]byte{{0, 0, 0, 2}}, // binary representation of 2
			[]int16{1},             // paramFormats - 1 means binary
			[]int16{1, 1},          // resultFormats - 1 means binary
		).Read()
		require.NoError(t, result.Err)
		require.Equal(t, [][][]byte{{
			{0, 0, 0, 2}, {0x66, 0x6f, 0x6f}, // binary representation of 2, 'foo'
		}}, result.Rows)
		result = conn.PgConn().ExecPrepared(
			ctx,
			"prepared_stmt_aost",
			[][]byte{},    // paramValues
			[]int16{},     // paramFormats
			[]int16{1, 1}, // resultFormats - 1 means binary
		).Read()
		require.NoError(t, result.Err)
		require.Equal(t, [][][]byte{{
			{0, 0, 0, 1}, {0x66, 0x6f, 0x6f}, // binary representation of 1, 'foo'
		}}, result.Rows)
	})

	// Errors should be displayed as a SQL value.
	t.Run("errors", func(t *testing.T) {
		t.Run("root_user", func(t *testing.T) {
			var key string
			var errVal, sessionState, sessionRevivalToken gosql.NullString

			err := tenantDB.QueryRow(`SHOW TRANSFER STATE WITH 'bar'`).Scan(&errVal, &sessionState, &sessionRevivalToken, &key)
			require.NoError(t, err)
			require.True(t, errVal.Valid)
			require.Equal(t, "cannot create token for root user", errVal.String)
			require.False(t, sessionState.Valid)
			require.False(t, sessionRevivalToken.Valid)
		})

		t.Run("transaction", func(t *testing.T) {
			conn := testUserConn

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
			pgURL, cleanup := pgurlutils.PGUrl(
				t,
				tenant.SQLAddr(),
				"TestShowTransferState-errors-temp_tables",
				url.UserPassword(username.TestUser, "hunter2"),
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
