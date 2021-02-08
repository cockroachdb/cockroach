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

	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Use the output from crdb_internal.show_create_all_tables() to recreate the
// tables and perform another crdb_internal.show_create_all_tables() to ensure
// that the output is the same after recreating the tables.
func TestRecreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
		CREATE DATABASE test;
		USE test;
		CREATE TABLE foo(x INT primary key);
		CREATE TABLE bar(x INT, y INT, z STRING, FAMILY f1(x, y, z))
	`); err != nil {
		t.Fatal(err)
	}

	row := sqlDB.QueryRow("SELECT crdb_internal.show_create_all_tables('test')")
	var recreateTablesStmt string
	if err := row.Scan(&recreateTablesStmt); err != nil {
		t.Fatal(err)
	}

	// Use the recreateTablesStmt to recreate the tables, perform another
	// show_create_all_tables and compare that the output is the same.
	if _, err := sqlDB.Exec(`
		DROP DATABASE test; 
		CREATE DATABASE test;
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(recreateTablesStmt); err != nil {
		t.Fatal(err)
	}

	row = sqlDB.QueryRow("SELECT crdb_internal.show_create_all_tables('test')")
	var recreateTablesStmt2 string
	if err := row.Scan(&recreateTablesStmt2); err != nil {
		t.Fatal(err)
	}

	if recreateTablesStmt != recreateTablesStmt2 {
		t.Fatalf("got: %s\nexpected: %s", recreateTablesStmt2, recreateTablesStmt)
	}
}
