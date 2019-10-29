// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func setDb(t *testing.T, db *gosql.DB, name string) {
	if _, err := db.Exec("USE " + name); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRandomSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("CREATE DATABASE test; CREATE DATABASE test2"); err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		tab := sqlbase.RandCreateTable(rng, "table", i)
		setDb(t, db, "test")
		_, err := db.Exec(tab.String())
		if err != nil {
			t.Fatal(tab, err)
		}

		var tabName, tabStmt, secondTabStmt string
		if err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s",
			tab.Table.String())).Scan(&tabName, &tabStmt); err != nil {
			t.Fatal(err)
		}

		if tabName != tab.Table.String() {
			t.Fatalf("found table name %s, expected %s", tabName, tab.Table.String())
		}

		// Reparse the show create table statement that's stored in the database.
		parsed, err := parser.ParseOne(tabStmt)
		if err != nil {
			t.Fatalf("error parsing show create table: %s", err)
		}

		setDb(t, db, "test2")
		// Now run the SHOW CREATE TABLE statement we found on a new db and verify
		// that both tables are the same.

		_, err = db.Exec(parsed.AST.String())
		if err != nil {
			t.Fatal(parsed.AST, err)
		}

		if err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s",
			tabName)).Scan(&tabName, &secondTabStmt); err != nil {
			t.Fatal(err)
		}

		if tabName != tab.Table.String() {
			t.Fatalf("found table name %s, expected %s", tabName, tab.Table.String())
		}
		// Reparse the show create table statement that's stored in the database.
		secondParsed, err := parser.ParseOne(secondTabStmt)
		if err != nil {
			t.Fatalf("error parsing show create table: %s", err)
		}
		if parsed.AST.String() != secondParsed.AST.String() {
			t.Fatalf("for input statement\n%s\nfound first output\n%q\nbut second output\n%q",
				tab.String(), parsed.AST.String(), secondParsed.AST.String())
		}
		if tabStmt != secondTabStmt {
			t.Fatalf("for input statement\n%s\nfound first output\n%q\nbut second output\n%q",
				tab.String(), tabStmt, secondTabStmt)
		}
	}
}
