// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgrepl_test

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestConnect(t *testing.T) {
	params, _ := tests.CreateTestServerParams()

	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER testuser`)

	for _, tc := range []struct {
		replicationMode    string
		expectedSessionVar string
		expectError        bool
	}{
		{replicationMode: "", expectedSessionVar: "off"},
		{replicationMode: "0", expectedSessionVar: "off"},
		{replicationMode: "1", expectedSessionVar: "on"},
		{replicationMode: "false", expectedSessionVar: "off"},
		{replicationMode: "true", expectedSessionVar: "on"},
		{replicationMode: "database", expectedSessionVar: "database"},
		{replicationMode: "asdf", expectError: true},
	} {
		t.Run(tc.replicationMode, func(t *testing.T) {
			pgURL, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), "pgrepl_conn_test", url.User(username.TestUser))
			defer cleanup()

			cfg, err := pgx.ParseConfig(pgURL.String())
			require.NoError(t, err)
			if tc.replicationMode != "" {
				cfg.RuntimeParams["replication"] = tc.replicationMode
			}

			ctx := context.Background()
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if tc.expectError {
				require.Error(t, err)
				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr))
				require.Equal(t, pgcode.InvalidParameterValue.String(), pgErr.Code)
				return
			}
			require.NoError(t, err)
			var val string
			require.NoError(t, conn.QueryRow(ctx, "SELECT current_setting('replication')").Scan(&val))
			require.Equal(t, tc.expectedSessionVar, val)
		})
	}
}
