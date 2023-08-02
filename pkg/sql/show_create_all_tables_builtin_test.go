// Copyright 2021 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Use the output from crdb_internal.show_create_all_tables() to recreate the
// tables and perform another crdb_internal.show_create_all_tables() to ensure
// that the output is the same after recreating the tables.
func TestRecreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `CREATE DATABASE test;`)
	sqlRunner.Exec(t, `USE test;`)
	sqlRunner.Exec(t, `CREATE TABLE foo(x INT primary key);`)
	sqlRunner.Exec(t, `CREATE TABLE bar(x INT, y INT, z STRING, FAMILY f1(x, y, z))`)

	row := sqlRunner.QueryRow(t, "SELECT crdb_internal.show_create_all_tables('test')")
	var recreateTablesStmt string
	row.Scan(&recreateTablesStmt)

	// Use the recreateTablesStmt to recreate the tables, perform another
	// show_create_all_tables and compare that the output is the same.
	sqlRunner.Exec(t, `DROP DATABASE test;`)
	sqlRunner.Exec(t, `CREATE DATABASE test;`)
	sqlRunner.Exec(t, recreateTablesStmt)

	row = sqlRunner.QueryRow(t, "SELECT crdb_internal.show_create_all_tables('test')")
	var recreateTablesStmt2 string
	row.Scan(&recreateTablesStmt2)

	if recreateTablesStmt != recreateTablesStmt2 {
		t.Fatalf("got: %s\nexpected: %s", recreateTablesStmt2, recreateTablesStmt)
	}
}
