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
// Author: Marc Berhault (marc@cockroachlabs.com)

package driver_test

import (
	"database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var (
	rootUser = security.RootUser
	testUser = server.TestUser
)

// setupMultiuser creates a testserver and two clients, one for "root",
// the other for "testuser".
func setupMultiuser(t *testing.T) (*server.TestServer, *sql.DB, *sql.DB) {
	s := server.StartTestServer(nil)
	dbRoot, err := sql.Open("cockroach", "https://"+rootUser+"@"+s.ServingAddr()+"?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}
	dbTest, err := sql.Open("cockroach", "https://"+testUser+"@"+s.ServingAddr()+"?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}
	return s, dbRoot, dbTest
}

func cleanupMultiuser(s *server.TestServer, dbRoot, dbTest *sql.DB) {
	_ = dbRoot.Close()
	_ = dbTest.Close()
	s.Stop()
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

func TestDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, dbRoot, dbTest := setupMultiuser(t)
	defer cleanupMultiuser(s, dbRoot, dbTest)

	// All statements must succeed with dbRoot.
	// Statements with success==true must succeed with dbTest.
	// They are evaluated in order, with dbTest first followed by dbRoot.
	testCases := []struct {
		query   string
		success bool
	}{
		/* Database-level permissions */
		{`CREATE DATABASE foo`, false},

		{`SHOW DATABASES`, true},
		{`SET DATABASE = foo`, true},

		{`CREATE TABLE tbl (id INT PRIMARY KEY)`, false},

		{`SHOW TABLES`, true},
		{`SHOW GRANTS ON DATABASE foo`, true},

		{`GRANT ALL ON DATABASE foo TO bar`, false},
		{`REVOKE ALL ON DATABASE foo FROM bar`, false},
	}

	for _, tc := range testCases {
		if _, err := dbTest.Exec(tc.query); (err == nil) != tc.success {
			t.Fatalf("statement %q using testuser has err=%s, expected success=%t", tc.query, err, tc.success)
		}
		if _, err := dbRoot.Exec(tc.query); err != nil {
			t.Fatalf("statement %q using root failed: %v", tc.query, err)
		}
	}
}

func TestReadPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, dbRoot, dbTest := setupMultiuser(t)
	defer cleanupMultiuser(s, dbRoot, dbTest)

	if _, err := dbRoot.Exec(`CREATE DATABASE foo`); err != nil {
		t.Fatal(err)
	}
	if _, err := dbRoot.Exec(`GRANT READ ON DATABASE foo TO testuser`); err != nil {
		t.Fatal(err)
	}

	// All statements must succeed with dbRoot.
	// Statements with success==true must succeed with dbTest.
	// They are evaluated in order, with dbTest first followed by dbRoot.
	testCases := []struct {
		query   string
		success bool
	}{
		{`SHOW DATABASES`, true},
		{`SET DATABASE = foo`, true},

		{`CREATE TABLE tbl (id INT PRIMARY KEY)`, false},

		{`SHOW TABLES`, true},
		{`SHOW GRANTS ON DATABASE foo`, true},

		{`GRANT ALL ON DATABASE foo TO bar`, false},
		{`REVOKE ALL ON DATABASE foo FROM bar`, false},
	}

	for _, tc := range testCases {
		if _, err := dbTest.Exec(tc.query); (err == nil) != tc.success {
			t.Fatalf("statement %q using testuser has err=%s, expected success=%t", tc.query, err, tc.success)
		}
	}
}

func TestWritePrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, dbRoot, dbTest := setupMultiuser(t)
	defer cleanupMultiuser(s, dbRoot, dbTest)

	if _, err := dbRoot.Exec(`CREATE DATABASE foo`); err != nil {
		t.Fatal(err)
	}
	if _, err := dbRoot.Exec(`GRANT WRITE ON DATABASE foo TO testuser`); err != nil {
		t.Fatal(err)
	}

	// Statements with success==true must succeed with dbTest.
	testCases := []struct {
		query   string
		success bool
	}{
		{`SHOW DATABASES`, true},
		{`SET DATABASE = foo`, true},

		{`CREATE TABLE tbl (id INT PRIMARY KEY)`, true},

		{`SHOW TABLES`, true},
		{`SHOW GRANTS ON DATABASE foo`, true},

		{`GRANT ALL ON DATABASE foo TO bar`, true},
		{`REVOKE ALL ON DATABASE foo FROM bar`, true},
	}

	for _, tc := range testCases {
		if _, err := dbTest.Exec(tc.query); (err == nil) != tc.success {
			t.Fatalf("statement %q using testuser has err=%s, expected success=%t", tc.query, err, tc.success)
		}
	}
}
