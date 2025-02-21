// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Use the output from crdb_internal.show_create_all_tables() to recreate the
// tables and perform another crdb_internal.show_create_all_tables() to ensure
// that the output is the same after recreating the tables.
func TestRecreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `CREATE DATABASE test;`)
	sqlRunner.Exec(t, `USE test;`)
	sqlRunner.Exec(t, `CREATE TABLE foo(x INT primary key);`)
	sqlRunner.Exec(t, `CREATE TABLE bar(x INT, y INT, z STRING, FAMILY f1(x, y, z))`)
	sqlRunner.Exec(t, `
		CREATE TABLE tab (
			a STRING COLLATE en,
			b STRING COLLATE en_US,
			c STRING COLLATE en_US DEFAULT ('c' COLLATE en_US),
			d STRING COLLATE en_u_ks_level1 DEFAULT ('d'::STRING COLLATE en_u_ks_level1),
			e STRING COLLATE en_US AS (a COLLATE en_US) STORED,
			f STRING COLLATE en_US ON UPDATE ('f' COLLATE en_US))`)

	var recreateTablesStmts []string
	for _, r := range sqlRunner.QueryStr(t, "SELECT crdb_internal.show_create_all_tables('test')") {
		recreateTablesStmts = append(recreateTablesStmts, r[0])
	}

	// Use the recreateTablesStmt to recreate the tables, perform another
	// show_create_all_tables and compare that the output is the same.
	sqlRunner.Exec(t, `DROP DATABASE test;`)
	sqlRunner.Exec(t, `CREATE DATABASE test;`)

	for _, stmt := range recreateTablesStmts {
		sqlRunner.Exec(t, stmt)
	}

	var recreateTablesStmts2 []string
	for _, r := range sqlRunner.QueryStr(t, "SELECT crdb_internal.show_create_all_tables('test')") {
		recreateTablesStmts2 = append(recreateTablesStmts2, r[0])
	}

	require.ElementsMatch(t, recreateTablesStmts, recreateTablesStmts2)
}
