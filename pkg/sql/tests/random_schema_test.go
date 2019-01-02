// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tests_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
		tab := sqlbase.RandCreateTable(rng, i)
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
		stmtAst, err := parser.ParseOne(tabStmt)
		if err != nil {
			t.Fatalf("error parsing show create table: %s", err)
		}

		setDb(t, db, "test2")
		// Now run the SHOW CREATE TABLE statement we found on a new db and verify
		// that both tables are the same.

		_, err = db.Exec(stmtAst.String())
		if err != nil {
			t.Fatal(stmtAst, err)
		}

		if err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s",
			tabName)).Scan(&tabName, &secondTabStmt); err != nil {
			t.Fatal(err)
		}

		if tabName != tab.Table.String() {
			t.Fatalf("found table name %s, expected %s", tabName, tab.Table.String())
		}
		// Reparse the show create table statement that's stored in the database.
		var secondStmtAst tree.Statement
		secondStmtAst, err = parser.ParseOne(secondTabStmt)
		if err != nil {
			t.Fatalf("error parsing show create table: %s", err)
		}
		if stmtAst.String() != secondStmtAst.String() {
			t.Fatalf("for input statement\n%s\nfound first output\n%q\nbut second output\n%q",
				tab.String(), stmtAst.String(), secondStmtAst.String())
		}
		if tabStmt != secondTabStmt {
			t.Fatalf("for input statement\n%s\nfound first output\n%q\nbut second output\n%q",
				tab.String(), tabStmt, secondTabStmt)
		}
	}
}
