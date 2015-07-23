// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver_test

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var isError = testutils.IsError

// TODO(mberhault): should have one insecure test as well.
func setup(t *testing.T) (*server.TestServer, *sql.DB) {
	s := server.StartTestServer(nil)
	db, err := sql.Open("cockroach", "https://root@"+s.ServingAddr()+"?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}
	return s, db
}

func cleanup(s *server.TestServer, db *sql.DB) {
	_ = db.Close()
	s.Stop()
}

func readAll(t *testing.T, rows *sql.Rows) [][]string {
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var results [][]string
	results = append(results, cols)

	for rows.Next() {
		strs := make([]string, len(cols))
		vals := make([]interface{}, len(cols))
		for i := range vals {
			vals[i] = &strs[i]
		}
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		results = append(results, strs)
	}

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return results
}

func TestCreateDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	if _, err := db.Exec(`CREATE DATABASE foo`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE DATABASE foo`); !isError(err, "database .* already exists") {
		t.Fatalf("expected failure, but found success")
	}
	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS foo`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE DATABASE ""`); !isError(err, "empty database name") {
		t.Fatal(err)
	}
}

func TestShowDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	names := []string{"a", "b", "c", "d", "e", "f", "g", "i"}
	for i, name := range names {
		if _, err := db.Exec("CREATE DATABASE " + name); err != nil {
			t.Fatal(err)
		}

		rows, err := db.Query("SHOW DATABASES")
		if err != nil {
			t.Fatal(err)
		}
		var databases []string
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			databases = append(databases, n)
		}

		expectedDatabases := names[:i+1]
		if !reflect.DeepEqual(expectedDatabases, databases) {
			t.Fatalf("expected %+v, but got %+v", expectedDatabases, databases)
		}
	}
}

func TestCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}

	const cols = "(id INT PRIMARY KEY)"
	if _, err := db.Exec("CREATE TABLE users " + cols); !isError(err, "no database specified") {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t.users " + cols); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t.users " + cols); !isError(err, "table .* already exists") {
		t.Fatal(err)
	}

	if _, err := db.Exec("SET DATABASE = t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE users " + cols); !isError(err, "table .* already exists") {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS users " + cols); err != nil {
		t.Fatal(err)
	}
}

func TestShowTables(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}

	names := []string{"a", "b", "c", "d", "e", "f", "g", "i"}
	for i, name := range names {
		if _, err := db.Exec("CREATE TABLE t." + name + " (id INT PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}

		rows, err := db.Query("SHOW TABLES FROM t")
		if err != nil {
			t.Fatal(err)
		}
		var tables []string
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			tables = append(tables, n)
		}

		expectedTables := names[:i+1]
		if !reflect.DeepEqual(expectedTables, tables) {
			t.Fatalf("expected %+v, but got %+v", expectedTables, tables)
		}
	}

	if _, err := db.Query("SHOW TABLES"); !isError(err, "no database specified") {
		t.Fatal(err)
	}
	if _, err := db.Exec("SET DATABASE = t"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShowColumns(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	const schema = `
CREATE TABLE t.users (
  id    INT PRIMARY KEY,
  name  VARCHAR NOT NULL,
  title VARCHAR
)`

	if _, err := db.Query("SHOW COLUMNS FROM t.users"); !isError(err, "database .* does not exist") {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SHOW COLUMNS FROM t.users"); !isError(err, "table .* does not exist") {
		t.Fatal(err)
	}
	if _, err := db.Query("SHOW COLUMNS FROM users"); !isError(err, "no database specified") {
		t.Fatal(err)
	}

	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SHOW COLUMNS FROM t.users")
	if err != nil {
		t.Fatal(err)
	}
	results := readAll(t, rows)
	expectedResults := [][]string{
		{"Field", "Type", "Null"},
		{"id", "INT", "true"},
		{"name", "CHAR", "false"},
		{"title", "CHAR", "true"},
	}
	if !reflect.DeepEqual(expectedResults, results) {
		t.Fatalf("expected %s, but got %s", expectedResults, results)
	}
}

func TestShowIndex(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	const schema = `
CREATE TABLE t.users (
  id    INT PRIMARY KEY,
  name  VARCHAR NOT NULL,
  CONSTRAINT foo INDEX (name),
  CONSTRAINT bar UNIQUE (id, name)
)`

	if _, err := db.Query("SHOW INDEX FROM t.users"); !isError(err, "database .* does not exist") {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("SHOW INDEX FROM t.users"); !isError(err, "table .* does not exist") {
		t.Fatal(err)
	}
	if _, err := db.Query("SHOW INDEX FROM users"); !isError(err, "no database specified") {
		t.Fatal(err)
	}

	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SHOW INDEX FROM t.users")
	if err != nil {
		t.Fatal(err)
	}
	results := readAll(t, rows)
	expectedResults := [][]string{
		{"Table", "Name", "Unique", "Seq", "Column"},
		{"users", "primary", "true", "1", "id"},
		{"users", "foo", "false", "1", "name"},
		{"users", "bar", "true", "1", "id"},
		{"users", "bar", "true", "2", "name"},
	}
	if !reflect.DeepEqual(expectedResults, results) {
		t.Fatalf("expected %s, but got %s", expectedResults, results)
	}
}

func TestInsertSelect(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	schema := `
CREATE TABLE t.kv (
  k CHAR PRIMARY KEY,
  v CHAR
)`

	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t.kv VALUES ('a', 'b')`); !isError(err, "table .* does not exist") {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.Query(`SELECT * FROM t.kv`); err != nil {
		t.Fatal(err)
	} else if results := readAll(t, rows); len(results) > 1 {
		t.Fatalf("non-empty result set from empty table: %s", results)
	}
	if _, err := db.Exec(`INSERT INTO t.kv VALUES ('a')`); !isError(err, "invalid values for columns") {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t.kv (v) VALUES ('a')`); !isError(err, "missing .* primary key column") {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t.kv (k,v) VALUES ('a', 'b'), ('c', 'd')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t.kv VALUES ('e', 'f')`); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM t.kv")
	if err != nil {
		t.Fatal(err)
	}
	results := readAll(t, rows)
	expectedResults := [][]string{
		{"k", "v"},
		{"a", "b"},
		{"c", "d"},
		{"e", "f"},
	}
	if !reflect.DeepEqual(expectedResults, results) {
		t.Fatalf("expected %s, but got %s", expectedResults, results)
	}

	// TODO(pmattis): We need much more testing of WHERE clauses. Need to think
	// through the whole testing story in general.
	rows, err = db.Query("SELECT * FROM t.kv WHERE k IN ('a', 'c')")
	if err != nil {
		t.Fatal(err)
	}
	results = readAll(t, rows)
	expectedResults = [][]string{
		{"k", "v"},
		{"a", "b"},
		{"c", "d"},
	}
	if !reflect.DeepEqual(expectedResults, results) {
		t.Fatalf("expected %s, but got %s", expectedResults, results)
	}
}

func TestSelectExpr(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	schema := `
CREATE TABLE t.kv (
  a INT PRIMARY KEY,
  b INT,
  c INT
)`

	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t.kv VALUES (1, 2, 3)`); err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		expr     string
		expected [][]string
	}{
		{`*`, [][]string{
			{"a", "b", "c"},
			{"1", "2", "3"},
		}},
		{`*,*`, [][]string{
			{"a", "b", "c", "a", "b", "c"},
			{"1", "2", "3", "1", "2", "3"},
		}},
		{`a,a,a,a`, [][]string{
			{"a", "a", "a", "a"},
			{"1", "1", "1", "1"},
		}},
		{`a,c`, [][]string{
			{"a", "c"},
			{"1", "3"},
		}},
		{`a+b+c`, [][]string{
			{"a+b+c"},
			{"6"},
		}},
		{`a+b AS foo`, [][]string{
			{"foo"},
			{"3"},
		}},
	}
	for _, d := range testData {
		rows, err := db.Query(fmt.Sprintf("SELECT %s FROM t.kv", d.expr))
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		results := readAll(t, rows)
		if !reflect.DeepEqual(d.expected, results) {
			t.Fatalf("%s: expected %s, but got %s", d.expr, d.expected, results)
		}
	}
}

func TestSelectNoTable(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	testData := []struct {
		expr     string
		expected [][]string
	}{
		{`1`, [][]string{
			{"1"},
			{"1"},
		}},
		{`1+1 AS two, 2+2 AS four`, [][]string{
			{"two", "four"},
			{"2", "4"},
		}},
	}
	for _, d := range testData {
		rows, err := db.Query(fmt.Sprintf("SELECT %s", d.expr))
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		results := readAll(t, rows)
		if !reflect.DeepEqual(d.expected, results) {
			t.Fatalf("%s: expected %s, but got %s", d.expr, d.expected, results)
		}
	}
}

func TestInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Start test server in insecure mode.
	s := &server.TestServer{}
	s.Ctx = server.NewTestContext()
	s.Ctx.Insecure = true
	if err := s.Start(); err != nil {
		t.Fatalf("Could not start server: %v", err)
	}
	t.Logf("Test server listening on %s: %s", s.Ctx.RequestScheme(), s.ServingAddr())
	defer s.Stop()

	// We can't attempt a connection through HTTPS since the client just retries forever.
	// DB connection using plain HTTP.
	db, err := sql.Open("cockroach", "http://root@"+s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
}
