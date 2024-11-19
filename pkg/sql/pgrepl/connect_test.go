// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepl_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestReplicationConnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE ROLE replicationsystempriv`)
	sqlDB.Exec(t, `GRANT SYSTEM REPLICATION TO replicationsystempriv`)

	for _, tc := range []struct {
		replicationMode    string
		expectedSessionVar string
		useRoot            bool
		hasAdmin           bool
		hasPrivilege       bool
		hasSystemGrant     bool
		expectErrorCode    pgcode.Code
	}{
		{replicationMode: "", hasPrivilege: true, expectedSessionVar: "off"},
		{replicationMode: "0", hasPrivilege: true, expectedSessionVar: "off"},
		{replicationMode: "1", hasPrivilege: true, expectedSessionVar: "on"},
		{replicationMode: "false", hasPrivilege: true, expectedSessionVar: "off"},
		{replicationMode: "true", hasPrivilege: true, expectedSessionVar: "on"},
		{replicationMode: "database", hasPrivilege: true, expectedSessionVar: "database"},
		{replicationMode: "asdf", hasPrivilege: true, expectErrorCode: pgcode.InvalidParameterValue},

		{replicationMode: "true", hasSystemGrant: true, expectedSessionVar: "on"},
		{replicationMode: "database", hasSystemGrant: true, expectedSessionVar: "database"},

		{replicationMode: "true", hasAdmin: true, expectedSessionVar: "on"},
		{replicationMode: "database", hasAdmin: true, expectedSessionVar: "database"},

		{replicationMode: "true", useRoot: true, expectedSessionVar: "on"},
		{replicationMode: "database", useRoot: true, expectedSessionVar: "database"},

		{replicationMode: "", expectedSessionVar: "off"},
		{replicationMode: "false", expectedSessionVar: "off"},
		{replicationMode: "true", expectErrorCode: pgcode.InsufficientPrivilege},
		{replicationMode: "database", expectErrorCode: pgcode.InsufficientPrivilege},
		{replicationMode: "asdf", expectErrorCode: pgcode.InvalidParameterValue},
	} {
		t.Run(fmt.Sprintf("hasPrivilege=%t/useRoot=%t/hasSystemGrant=%t/hasAdmin=%t/replicationMode=%s", tc.hasPrivilege, tc.useRoot, tc.hasSystemGrant, tc.hasAdmin, tc.replicationMode), func(t *testing.T) {
			sqlDB.Exec(t, `DROP USER IF EXISTS testuser`)
			sqlDB.Exec(t, `CREATE USER testuser LOGIN`)

			if tc.hasPrivilege {
				sqlDB.Exec(t, `ALTER USER testuser REPLICATION`)
			} else {
				sqlDB.Exec(t, `ALTER USER testuser NOREPLICATION`)
			}

			if tc.hasSystemGrant {
				sqlDB.Exec(t, `GRANT replicationsystempriv TO testuser`)
			}
			if tc.hasAdmin {
				sqlDB.Exec(t, `GRANT ADMIN TO testuser`)
			}

			u := username.TestUser
			if tc.useRoot {
				u = username.RootUser
			}

			pgURL, cleanup := s.PGUrl(t, serverutils.CertsDirPrefix("pgrepl_conn_test"), serverutils.User(u))
			defer cleanup()

			cfg, err := pgx.ParseConfig(pgURL.String())
			require.NoError(t, err)
			if tc.replicationMode != "" {
				cfg.RuntimeParams["replication"] = tc.replicationMode
			}

			ctx := context.Background()
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if tc.expectErrorCode != (pgcode.Code{}) {
				require.Error(t, err)
				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr))
				require.Equal(t, tc.expectErrorCode.String(), pgErr.Code)
				return
			}
			require.NoError(t, err)
			var val string
			require.NoError(t, conn.QueryRow(ctx, "SELECT current_setting('replication')", pgx.QueryExecModeSimpleProtocol).Scan(&val))
			require.Equal(t, tc.expectedSessionVar, val)
		})
	}
}
