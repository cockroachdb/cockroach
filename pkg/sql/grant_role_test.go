// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestNoOpGrantRole tests that if we GRANT membership of a role to a user who
// is already a member of that role will not cause any schema change to the
// `system.role_members` table.
func TestNoOpGrantRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := createTestServerParamsAllowTenants()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, "CREATE USER developer WITH CREATEDB")
	tdb.Exec(t, "CREATE USER roach WITH PASSWORD NULL")

	// Grant membership of role `developer` to user `roach`.
	// Assert that user `roach` is now a member of role `developer` by peeking the `system.role_members` table.
	tdb.Exec(t, "GRANT developer TO roach")
	num := 0
	tdb.QueryRow(t, "SELECT count(1) FROM system.role_members WHERE role = 'developer' AND member = 'roach'").Scan(&num)
	require.Equal(t, 1, num)
	roleMembersTableDesc := desctestutils.TestingGetTableDescriptor(kvDB, s.Codec(), "system", "public", "role_members")
	roleMembersTableVersion := roleMembersTableDesc.GetVersion()

	// Repeat the statement and assert that no schema change was performed to table `system.role_members`
	// by checking the table's version remains the same.
	tdb.Exec(t, "GRANT developer TO roach")
	tdb.QueryRow(t, "SELECT count(1) FROM system.role_members WHERE role = 'developer' AND member = 'roach'").Scan(&num)
	require.Equal(t, 1, num)
	roleMembersTableDesc = desctestutils.TestingGetTableDescriptor(kvDB, s.Codec(), "system", "public", "role_members")
	require.Equal(t, roleMembersTableVersion, roleMembersTableDesc.GetVersion())
}
