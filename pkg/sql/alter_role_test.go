// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestNoOpAlterRole tests that if we alter a role with an option that this role already
// has, no schema change will happen to the `system.role_options` table.
func TestNoOpAlterRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	//ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Create a role `roach` and alter it with a bunch of options.
	tdb.Exec(t, "CREATE USER roach")
	options := []string{"CREATEDB", "CREATEROLE", "NOLOGIN", "CONTROLJOB", "CONTROLCHANGEFEED", "CREATELOGIN",
		"VIEWACTIVITY", "CANCELQUERY", "MODIFYCLUSTERSETTING", "NOSQLLOGIN", "VIEWACTIVITYREDACTED", "VIEWCLUSTERSETTING"}
	tdb.Exec(t, fmt.Sprintf("ALTER ROLE roach WITH %s", strings.Join(options, " ")))

	// Assert that those many (`len(options)`) rows exists in `system.role_options` table as a
	// result of the `alter role` stmt above.
	num := 0
	tdb.QueryRow(t, "SELECT count(1) FROM system.role_options WHERE username = 'roach'").Scan(&num)
	require.Equal(t, len(options), num)
	roleOptionsTableDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec,
		"system", "public", "role_options")
	roleOptionsTableVersion := roleOptionsTableDesc.GetVersion()

	// Alter the same role `roach` with the same options and assert that the `system.role_options` table should not
	// undergo a schema change by checking this table's version reamins unchanged.
	tdb.Exec(t, fmt.Sprintf("ALTER ROLE roach WITH %s", strings.Join(options, " ")))
	tdb.QueryRow(t, "SELECT count(1) FROM system.role_options WHERE username = 'roach'").Scan(&num)
	require.Equal(t, len(options), num)
	roleOptionsTableDesc = desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec,
		"system", "public", "role_options")
	require.Equal(t, roleOptionsTableVersion, roleOptionsTableDesc.GetVersion())
}

// TestNoOpAlterRoleSet tests that if an `alter role ... set/reset ...` statement will
// not cause any change to the session variable(s) (e.g. reset the default value for a
// session variable for a role that's never set to anything other than the default, or,
// set a session variable's default value to something that's equal to its current
// default value), then no schema change will happen to the `system.database_role_settings`
// table.
func TestNoOpAlterRoleSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	//ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Create a role `roach` and set default values of a few session variables for `roach`.
	tdb.Exec(t, "CREATE user roach")
	tdb.Exec(t, "alter role roach set timezone = 'America/New_York'")
	tdb.Exec(t, "ALTER ROLE roach SET use_declarative_schema_changer = 'off'")
	tdb.Exec(t, "ALTER ROLE roach SET statement_timeout = '10s'")

	// Assert a row is created for user `roach` in `system.database_role_settings` as a result of the
	// above `ALTER ROLE ... SET ...` commands.
	num := 0
	tdb.QueryRow(t, "SELECT count(1) FROM system.database_role_settings WHERE role_name = 'roach'").Scan(&num)
	require.Equal(t, 1, num)
	databaseRoleSettingsTableDesc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec,
		"system", "public", "database_role_settings")
	databaseRoleSettingsTableVersion := databaseRoleSettingsTableDesc.GetVersion()

	// Issue a few `ALTER ROLE ... SET/RESET` statements that are not supposed to alter
	// `system.database_role_settings` table and assert that the table's version remains unchanged.
	tdb.Exec(t, "ALTER ROLE roach SET timezone = 'America/New_York'")
	tdb.Exec(t, "ALTER ROLE roach SET use_declarative_schema_changer = 'off'")
	tdb.Exec(t, "ALTER ROLE roach SET statement_timeout = '10s'")
	tdb.Exec(t, "ALTER ROLE roach RESET search_path")
	tdb.Exec(t, "ALTER ROLE roach RESET tracing")
	tdb.QueryRow(t, "SELECT count(1) FROM system.database_role_settings WHERE role_name = 'roach'").Scan(&num)
	require.Equal(t, 1, num)
	databaseRoleSettingsTableDesc = desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec,
		"system", "public", "database_role_settings")
	require.Equal(t, databaseRoleSettingsTableVersion, databaseRoleSettingsTableDesc.GetVersion())
}
