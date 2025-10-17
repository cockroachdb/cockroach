// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAdminShowExternalConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	srv := s.ApplicationLayer()

	adminDB := sqlutils.MakeSQLRunner(db)
	const password = "correcthorsebatterystaple"
	adminDB.Exec(t, "CREATE USER testuser1 WITH PASSWORD $1", password)
	adminDB.Exec(t, "GRANT SYSTEM EXTERNALCONNECTION TO testuser1")

	nonAdminDB := sqlutils.MakeSQLRunner(srv.SQLConn(
		t, serverutils.UserPassword("testuser1", password), serverutils.ClientCerts(false),
	))
	nonAdminDB.Exec(t, "CREATE EXTERNAL CONNECTION foo AS 'userfile:///'")

	t.Run("non-admin user cannot see other external connections", func(t *testing.T) {
		_, err := db.Exec("CREATE USER testuser2 WITH PASSWORD $1", password)
		require.NoError(t, err)

		nonAdminDB2 := sqlutils.MakeSQLRunner(srv.SQLConn(
			t, serverutils.UserPassword("testuser2", password), serverutils.ClientCerts(false),
		))

		rows := nonAdminDB2.QueryStr(t, "SHOW EXTERNAL CONNECTIONS")
		require.Empty(t, rows, "expected no rows for non-admin user without privileges")
	})

	t.Run("admin user can see all external connections", func(t *testing.T) {
		rows := adminDB.QueryStr(t, "SHOW EXTERNAL CONNECTIONS")
		require.Len(t, rows, 1, "expected one row for admin user")
	})
}
