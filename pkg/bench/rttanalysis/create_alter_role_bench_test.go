// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
)

var createRoleCases = []RoundTripBenchTestCase{
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
}

// benchmark-ci: benchtime=20x
func BenchmarkCreateRole(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, createRoleCases, defaultCC)
}

func TestBenchmarkExpectation_CreateRole(t *testing.T) {
	runExpectation(t, "CreateRole", createRoleCases, defaultCC)
}

var alterRoleCases = []RoundTripBenchTestCase{
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
}

// benchmark-ci: benchtime=20x
func BenchmarkAlterRole(b *testing.B) {
	runCPUMemBenchmark(bShim{b}, alterRoleCases, defaultCC)
}

func TestBenchmarkExpectation_AlterRole(t *testing.T) {
	runExpectation(t, "AlterRole", alterRoleCases, defaultCC)
}

const useManyRolesCreateFunc = `
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

var useManyRolesCases = []RoundTripBenchTestCase{
	{
		Name: "use 2 roles",
		SetupEx: []string{
			"CREATE ROLE parent_role",
			createNRoles(2),
			grantRoleToNRoles("parent_role", 2),
			"CREATE TABLE tab (a INT)",
			"GRANT SELECT ON tab TO parent_role",
			"INSERT INTO tab VALUES (1)",
			useManyRolesCreateFunc,
		},
		Stmt: "SELECT query_table_with_roles(2)",
		ResetEx: []string{
			"RESET ROLE",
			dropNRoles(2),
			"DROP ROLE parent_role",
		},
	},
	{
		Name: "use 25 roles",
		SetupEx: []string{
			"CREATE ROLE parent_role",
			createNRoles(25),
			grantRoleToNRoles("parent_role", 25),
			"CREATE TABLE tab (a INT)",
			"GRANT SELECT ON tab TO parent_role",
			"INSERT INTO tab VALUES (1)",
			useManyRolesCreateFunc,
		},
		Stmt: "SELECT query_table_with_roles(25)",
		ResetEx: []string{
			dropNRoles(25),
			"DROP ROLE parent_role",
		},
	},
}

func BenchmarkUseManyRoles(b *testing.B) {
	skip.UnderShort(b, "skipping long benchmark")
	runCPUMemBenchmark(bShim{b}, useManyRolesCases, defaultCC)
}

func TestBenchmarkExpectation_UseManyRoles(t *testing.T) {
	runExpectation(t, "UseManyRoles", useManyRolesCases, defaultCC)
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
