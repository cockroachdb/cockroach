// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"strconv"
	"strings"
	"testing"
)

func BenchmarkCreateRole(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("CreateRole", []RoundTripBenchTestCase{
		{
			Name:  "create role with no options",
			Stmt:  "CREATE ROLE rolea",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "create role with 1 option",
			Stmt:  "CREATE ROLE rolea LOGIN",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "create role with 2 options",
			Stmt:  "CREATE ROLE rolea LOGIN CREATEROLE",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "create role with 3 options",
			Stmt:  "CREATE ROLE rolea LOGIN CREATEROLE VALID UNTIL '2021-01-01'",
			Reset: "DROP ROLE rolea",
		},
	})
}

func BenchmarkAlterRole(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("AlterRole", []RoundTripBenchTestCase{
		{
			Name:  "alter role with 1 option",
			Setup: "CREATE ROLE rolea",
			Stmt:  "ALTER ROLE rolea CREATEROLE",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "alter role with 2 options",
			Setup: "CREATE ROLE rolea",
			Stmt:  "ALTER ROLE rolea CREATEROLE LOGIN",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "alter role with 3 options",
			Setup: "CREATE ROLE rolea",
			Stmt:  "ALTER ROLE rolea CREATEROLE LOGIN PASSWORD '123'",
			Reset: "DROP ROLE rolea",
		},
	})
}

// BenchmarkUseManyRoles is useful for testing the performance of the
// role membership cache.
func BenchmarkUseManyRoles(b *testing.B) { reg.Run(b) }
func init() {

	createFunc := `
CREATE OR REPLACE FUNCTION query_table_with_roles(num_roles INT)
RETURNS VOID AS $$
DECLARE
    counter INT := 0;
BEGIN
    WHILE counter < num_roles LOOP
        SELECT set_config('role', 'role' || counter::TEXT, false);
        SELECT a FROM tab;
        counter := counter + 1;
    END LOOP;
    SELECT set_config('role', 'testuser', false);
END;
$$ LANGUAGE plpgsql;`

	reg.Register("UseManyRoles", []RoundTripBenchTestCase{
		{
			Name: "use 2 roles",
			SetupEx: []string{
				"CREATE ROLE parent_role",
				createNRoles(2),
				grantRoleToNRoles("parent_role", 2),
				"CREATE TABLE tab (a INT)",
				"GRANT SELECT ON tab TO parent_role",
				"INSERT INTO tab VALUES (1)",
				createFunc,
			},
			Stmt: "SELECT query_table_with_roles(2)",
			ResetEx: []string{
				"RESET ROLE",
				dropNRoles(2),
				"DROP ROLE parent_role",
			},
		},
		{
			Name: "use 50 roles",
			SetupEx: []string{
				"CREATE ROLE parent_role",
				createNRoles(50),
				grantRoleToNRoles("parent_role", 50),
				"CREATE TABLE tab (a INT)",
				"GRANT SELECT ON tab TO parent_role",
				"INSERT INTO tab VALUES (1)",
				createFunc,
			},
			Stmt: "SELECT query_table_with_roles(50)",
			ResetEx: []string{
				dropNRoles(50),
				"DROP ROLE parent_role",
			},
		},
	})
}

func createNRoles(n int) string {
	roles := make([]string, n)
	for i := 0; i < n; i++ {
		roles[i] = "CREATE ROLE role" + strconv.Itoa(i)
	}
	return strings.Join(roles, "; ")
}

func dropNRoles(n int) string {
	roles := make([]string, n)
	for i := 0; i < n; i++ {
		roles[i] = "DROP ROLE role" + strconv.Itoa(i)
	}
	return strings.Join(roles, "; ")
}

func grantRoleToNRoles(role string, n int) string {
	roles := make([]string, n)
	for i := 0; i < n; i++ {
		roles[i] = "role" + strconv.Itoa(i)
	}
	return "GRANT " + role + " TO " + strings.Join(roles, ", ")
}
