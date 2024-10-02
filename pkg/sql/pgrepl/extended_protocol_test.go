// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// TestExtendedProtocolDisabled ensures the extended protocol is disabled
// during replication mode.
func TestExtendedProtocolDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER testuser LOGIN REPLICATION`)

	pgURL, cleanup := s.PGUrl(
		t, serverutils.CertsDirPrefix("pgrepl_extended_protocol_test"), serverutils.User(username.TestUser),
	)
	defer cleanup()

	cfg, err := pgconn.ParseConfig(pgURL.String())
	require.NoError(t, err)
	cfg.RuntimeParams["replication"] = "database"
	ctx := context.Background()

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	fe := conn.Frontend()

	for _, tc := range []struct {
		desc string
		msg  []pgproto3.FrontendMessage
	}{
		{desc: "parse", msg: []pgproto3.FrontendMessage{&pgproto3.Parse{Name: "a", Query: "SELECT 1"}}},
		{desc: "bind", msg: []pgproto3.FrontendMessage{&pgproto3.Bind{}}},
		{desc: "parse and bind", msg: []pgproto3.FrontendMessage{
			&pgproto3.Parse{Name: "a", Query: "SELECT 1"},
			&pgproto3.Bind{},
		}},
		{desc: "describe", msg: []pgproto3.FrontendMessage{&pgproto3.Describe{Name: "a"}}},
		{desc: "exec", msg: []pgproto3.FrontendMessage{&pgproto3.Execute{Portal: "a"}}},
		{desc: "close", msg: []pgproto3.FrontendMessage{&pgproto3.Close{}}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for _, msg := range tc.msg {
				fe.Send(msg)
			}
			fe.Send(&pgproto3.Sync{})
			err := fe.Flush()
			require.NoError(t, err)
			var pgErr *pgconn.PgError
			done := false
			for !done {
				recv, err := fe.Receive()
				require.NoError(t, err)
				switch recv := recv.(type) {
				case *pgproto3.ReadyForQuery:
					done = true
				case *pgproto3.ErrorResponse:
					// Ensure we do not have multiple errors.
					require.Nil(t, pgErr)
					pgErr = pgconn.ErrorResponseToPgError(recv)
				default:
					t.Errorf("received unexpected message %#v", recv)
				}
			}
			require.NotNil(t, pgErr)
			require.Equal(t, pgcode.ProtocolViolation.String(), pgErr.Code)
			require.Contains(t, pgErr.Message, "extended query protocol not supported in a replication connection")

			// Ensure we can use the connection using the simple protocol.
			rows := conn.Exec(ctx, "SELECT 1")
			_, err = rows.ReadAll()
			require.NoError(t, err)
			require.NoError(t, rows.Close())
		})
	}
}
