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

package driver

import (
	"database/sql"
	"reflect"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

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

func isError(err error, re string) bool {
	if err == nil {
		return false
	}
	matched, merr := regexp.MatchString(re, err.Error())
	if merr != nil {
		return false
	}
	return matched
}

func TestCreateDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup(t)
	defer cleanup(s, db)

	if _, err := db.Exec("CREATE DATABASE foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE foo"); !isError(err, "database .* already exists") {
		t.Fatalf("expected failure, but found success")
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS foo"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE ``"); !isError(err, "empty database name") {
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
		_ = rows.Close()

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

	if _, err := db.Exec("USE t"); err != nil {
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
		_ = rows.Close()

		expectedTables := names[:i+1]
		if !reflect.DeepEqual(expectedTables, tables) {
			t.Fatalf("expected %+v, but got %+v", expectedTables, tables)
		}
	}

	if _, err := db.Query("SHOW TABLES"); !isError(err, "no database specified") {
		t.Fatal(err)
	}
	if _, err := db.Exec("USE t"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
}

// TODO(pmattis)
// func TestShowColumnsFromTable(t *testing.T) {
// }
