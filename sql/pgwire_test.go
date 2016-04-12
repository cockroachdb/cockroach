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
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/pq"
)

func trivialQuery(pgURL url.URL) error {
	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		return err
	}
	defer db.Close()
	{
		_, err := db.Exec("SELECT 1")
		return err
	}
}

func TestPGWire(t *testing.T) {
	defer leaktest.AfterTest(t)()

	certPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedTestUserCert)
	keyPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedTestUserKey)

	tempDir, err := ioutil.TempDir("", "TestPGWire")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			// Not Fatal() because we might already be panicking.
			t.Error(err)
		}
	}()

	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	tempCertPath := securitytest.RestrictedCopy(t, certPath, tempDir, "cert")
	tempKeyPath := securitytest.RestrictedCopy(t, keyPath, tempDir, "key")

	for _, insecure := range [...]bool{true, false} {
		ctx, _ := createTestServerContext()
		ctx.Insecure = insecure
		s := setupTestServerWithContext(t, ctx)

		host, port, err := net.SplitHostPort(s.ServingAddr())
		if err != nil {
			t.Fatal(err)
		}

		basePgUrl := url.URL{
			Scheme: "postgres",
			Host:   net.JoinHostPort(host, port),
		}
		if err := trivialQuery(basePgUrl); err != nil {
			if insecure {
				if err != pq.ErrSSLNotSupported {
					t.Error(err)
				}
			} else {
				if !testutils.IsError(err, "no client certificates in request") {
					t.Error(err)
				}
			}
		}

		{
			disablePgUrl := basePgUrl
			disablePgUrl.RawQuery = "sslmode=disable"
			err := trivialQuery(disablePgUrl)
			if insecure {
				if err != nil {
					t.Error(err)
				}
			} else {
				if !testutils.IsError(err, pgwire.ErrSSLRequired) {
					t.Error(err)
				}
			}
		}

		{
			requirePgUrlNoCert := basePgUrl
			requirePgUrlNoCert.RawQuery = "sslmode=require"
			err := trivialQuery(requirePgUrlNoCert)
			if insecure {
				if err != pq.ErrSSLNotSupported {
					t.Error(err)
				}
			} else {
				if !testutils.IsError(err, "no client certificates in request") {
					t.Error(err)
				}
			}
		}

		{
			for _, optUser := range []string{server.TestUser, security.RootUser} {
				requirePgUrlWithCert := basePgUrl
				requirePgUrlWithCert.User = url.User(optUser)
				requirePgUrlWithCert.RawQuery = fmt.Sprintf("sslmode=require&sslcert=%s&sslkey=%s",
					url.QueryEscape(tempCertPath),
					url.QueryEscape(tempKeyPath),
				)
				err := trivialQuery(requirePgUrlWithCert)
				if insecure {
					if err != pq.ErrSSLNotSupported {
						t.Error(err)
					}
				} else {
					if optUser == server.TestUser {
						if err != nil {
							t.Error(err)
						}
					} else {
						if !testutils.IsError(err, `requested user is \w+, but certificate is for \w+`) {
							t.Error(err)
						}
					}
				}
			}
		}

		cleanupTestServer(s)
	}
}

func TestPGWireDBName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestPGWireDBName")
	pgURL.Path = "foo"
	defer cleanupFn()
	{
		db, err := sql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.Exec(`CREATE DATABASE foo`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`CREATE TABLE bar (i INT PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO bar VALUES ($1)`, 1); err != nil {
		t.Fatal(err)
	}
}

func TestPGPrepareFail(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestPGPrepareFail")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testFailures := map[string]string{
		"SELECT $1 = $1":                                "pq: unsupported comparison operator: <parameter> = <parameter>",
		"SELECT $1 > 0 AND NOT $1":                      "pq: incompatible NOT argument type: int",
		"SELECT $1":                                     "pq: could not determine data type of parameter $1",
		"SELECT $1 + $1":                                "pq: unsupported binary operator: <parameter> + <parameter>",
		"SELECT now() + $1":                             "pq: unsupported binary operator: <timestamp> + <parameter>",
		"SELECT CASE $1 WHEN 1 THEN 1 END":              "pq: could not determine data type of parameter $1",
		"SELECT CASE WHEN TRUE THEN $1 END":             "pq: could not determine data type of parameter $1",
		"SELECT CASE WHEN TRUE THEN $1 ELSE $2 END":     "pq: could not determine data type of parameter $2",
		"SELECT CASE WHEN TRUE THEN 1 ELSE $1 END":      "pq: could not determine data type of parameter $1",
		"UPDATE d.t SET d = CASE WHEN TRUE THEN $1 END": "pq: could not determine data type of parameter $1",
		"CREATE TABLE $1 (id INT)":                      "pq: syntax error at or near \"1\"\nCREATE TABLE $1 (id INT)\n             ^\n",
		"UPDATE d.t SET s = i + $1":                     "pq: value type int doesn't match type STRING of column \"s\"",
		"SELECT $0 > 0":                                 "pq: there is no parameter $0",
		"SELECT $2 > 0":                                 "pq: could not determine data type of parameter $1",
	}

	if _, err := db.Exec(`CREATE DATABASE d; CREATE TABLE d.t (i INT, s STRING, d INT)`); err != nil {
		t.Fatal(err)
	}

	for query, reason := range testFailures {
		if stmt, err := db.Prepare(query); err == nil {
			t.Errorf("expected error: %s", query)
			if err := stmt.Close(); err != nil {
				t.Fatal(err)
			}
		} else if err.Error() != reason {
			t.Errorf("%s: unexpected error: %s", query, err)
		}
	}
}

type preparedQueryTest struct {
	params  []interface{}
	results [][]interface{}
	others  int
	error   string
}

func (p preparedQueryTest) Params(v ...interface{}) preparedQueryTest {
	p.params = v
	return p
}

func (p preparedQueryTest) Results(v ...interface{}) preparedQueryTest {
	p.results = append(p.results, v)
	return p
}

func (p preparedQueryTest) Others(o int) preparedQueryTest {
	p.others = o
	return p
}

func (p preparedQueryTest) Error(err string) preparedQueryTest {
	p.error = err
	return p
}

func TestPGPreparedQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var base preparedQueryTest

	queryTests := map[string][]preparedQueryTest{
		"SELECT $1 > 0": {
			base.Params(1).Results(true),
			base.Params("1").Results(true),
			base.Params(1.1).Error(`pq: param $1: strconv.ParseInt: parsing "1.1": invalid syntax`).Results(true),
			base.Params("1.0").Error(`pq: param $1: strconv.ParseInt: parsing "1.0": invalid syntax`),
			base.Params(true).Error(`pq: param $1: strconv.ParseInt: parsing "true": invalid syntax`),
		},
		"SELECT TRUE AND $1": {
			base.Params(true).Results(true),
			base.Params(false).Results(false),
			base.Params(1).Results(true),
			base.Params("").Error(`pq: param $1: strconv.ParseBool: parsing "": invalid syntax`),
			// Make sure we can run another after a failure.
			base.Params(true).Results(true),
		},
		"SELECT $1::bool": {
			base.Params(true).Results(true),
			base.Params("true").Results(true),
			base.Params("false").Results(false),
			base.Params("1").Results(true),
			base.Params(2).Error(`pq: strconv.ParseBool: parsing "2": invalid syntax`),
			base.Params(3.1).Error(`pq: strconv.ParseBool: parsing "3.1": invalid syntax`),
			base.Params("").Error(`pq: strconv.ParseBool: parsing "": invalid syntax`),
		},
		"SELECT $1::int > $2::float": {
			base.Params("2", 1).Results(true),
			base.Params(1, "2").Results(false),
			base.Params("2", "1.0").Results(true),
			base.Params("2.0", "1").Error(`pq: strconv.ParseInt: parsing "2.0": invalid syntax`),
			base.Params(2.1, 1).Error(`pq: strconv.ParseInt: parsing "2.1": invalid syntax`),
		},
		"SELECT GREATEST($1, 0, $2), $2": {
			base.Params(1, -1).Results(1, -1),
			base.Params(-1, 10).Results(10, 10),
			base.Params("-2", "-1").Results(0, -1),
			base.Params(1, 2.1).Error(`pq: param $2: strconv.ParseInt: parsing "2.1": invalid syntax`),
		},
		"SELECT $1::int, $1::float": {
			base.Params("1").Results(1, 1.0),
		},
		"SELECT 3 + $1, $1 + $2": {
			base.Params("1", "2").Results(4, 3),
			base.Params(3, "4").Results(6, 7),
			base.Params(0, "a").Error(`pq: param $2: strconv.ParseInt: parsing "a": invalid syntax`),
		},
		// Check for QualifiedName resolution.
		"SELECT COUNT(*)": {
			base.Results(1),
		},
		"SELECT CASE WHEN $1 THEN 1-$3 WHEN $2 THEN 1+$3 END": {
			base.Params(true, false, 2).Results(-1),
			base.Params(false, true, 3).Results(4),
			base.Params(false, false, 2).Results(sql.NullBool{}),
		},
		"SELECT CASE 1 WHEN $1 THEN $2 ELSE 2 END": {
			base.Params(1, 3).Results(3),
			base.Params(2, 3).Results(2),
			base.Params(true, 0).Error(`pq: param $1: strconv.ParseInt: parsing "true": invalid syntax`),
		},
		"SHOW database": {
			base.Results(""),
		},
		"SELECT descriptor FROM system.descriptor WHERE descriptor != $1 LIMIT 1": {
			base.Params([]byte("abc")).Results([]byte("\x12\x16\n\x06system\x10\x01\x1a\n\n\b\n\x04root\x100")),
		},
		"SHOW COLUMNS FROM system.users": {
			base.
				Results("username", "STRING", false, sql.NullBool{}).
				Results("hashedPassword", "BYTES", true, sql.NullBool{}),
		},
		"SHOW DATABASES": {
			base.Results("d").Results("system"),
		},
		"SHOW GRANTS ON system.users": {
			base.Results("users", security.RootUser, "DELETE,GRANT,INSERT,SELECT,UPDATE"),
		},
		"SHOW INDEXES FROM system.users": {
			base.Results("users", "primary", true, 1, "username", "ASC", false),
		},
		"SHOW TABLES FROM system": {
			base.Results("descriptor").Others(7),
		},
		"SHOW TIME ZONE": {
			base.Results("UTC"),
		},
		"SELECT (SELECT 1+$1)": {
			base.Params(1).Results(2),
		},
		"SELECT CASE WHEN $1 THEN $2 ELSE 3 END": {
			base.Params(true, 2).Results(2),
			base.Params(false, 2).Results(3),
		},
		"SELECT $1::timestamp, $2::date": {
			base.Params("2001-01-02 03:04:05", "2006-07-08").Results(
				time.Date(2001, 1, 2, 3, 4, 5, 0, time.FixedZone("", 0)),
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"SELECT $1::date, $2::timestamp": {
			base.Params(
				time.Date(2006, 7, 8, 0, 0, 0, 9, time.FixedZone("", 0)),
				time.Date(2001, 1, 2, 3, 4, 5, 6, time.FixedZone("", 0)),
			).Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
				time.Date(2001, 1, 2, 3, 4, 5, 6, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.ts VALUES($1, $2) RETURNING *": {
			base.Params("2001-01-02 03:04:05", "2006-07-08").Results(
				time.Date(2001, 1, 2, 3, 4, 5, 0, time.FixedZone("", 0)),
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.ts VALUES(CURRENT_TIMESTAMP(), $1) RETURNING b": {
			base.Params("2006-07-08").Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.ts VALUES(STATEMENT_TIMESTAMP(), $1) RETURNING b": {
			base.Params("2006-07-08").Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.ts (a) VALUES ($1) RETURNING a": {
			base.Params(
				time.Date(2006, 7, 8, 0, 0, 0, 123, time.FixedZone("", 0)),
			).Results(
				time.Date(2006, 7, 8, 0, 0, 0, 123, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.T VALUES ($1) RETURNING 1": {
			base.Params(1).Results(1),
			base.Params(nil).Results(1),
		},
		"INSERT INTO d.T VALUES ($1) RETURNING $1": {
			base.Params(1).Results(1),
			base.Params(3).Results(3),
		},
		"INSERT INTO d.T VALUES ($1) RETURNING $1, 1 + $1": {
			base.Params(1).Results(1, 2),
			base.Params(3).Results(3, 4),
		},
		"SELECT a FROM d.T WHERE a = $1 AND (SELECT a >= $2 FROM d.T WHERE a = $1)": {
			base.Params(10, 5).Results(10),
		},
		"SELECT * FROM (VALUES (1), (2), (3), (4)) AS foo (a) LIMIT $1 OFFSET $2": {
			base.Params(1, 0).Results(1),
			base.Params(1, 1).Results(2),
			base.Params(1, 2).Results(3),
		},
	}

	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestPGPreparedQuery")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	runTests := func(query string, tests []preparedQueryTest, queryFunc func(...interface{}) (*sql.Rows, error)) {
		for _, test := range tests {
			rows, err := queryFunc(test.params...)
			if err != nil {
				if test.error == "" {
					t.Errorf("%s: %v: unexpected error: %s", query, test.params, err)
				} else if err.Error() != test.error {
					t.Errorf("%s: %v: expected error: %s, got %s", query, test.params, test.error, err)
				}
				continue
			}
			defer rows.Close()

			if test.error != "" {
				t.Errorf("expected error: %s: %v", query, test.params)
				continue
			}

			for _, expected := range test.results {
				if !rows.Next() {
					t.Errorf("expected row: %s: %v", query, test.params)
					continue
				}
				dst := make([]interface{}, len(expected))
				for i, d := range expected {
					dst[i] = reflect.New(reflect.TypeOf(d)).Interface()
				}
				if err := rows.Scan(dst...); err != nil {
					t.Error(err)
				}
				for i, d := range dst {
					dst[i] = reflect.Indirect(reflect.ValueOf(d)).Interface()
				}
				if !reflect.DeepEqual(dst, expected) {
					t.Errorf("%s: %v: expected %v, got %v", query, test.params, expected, dst)
				}
			}
			for rows.Next() {
				if test.others > 0 {
					test.others--
					continue
				}
				cols, err := rows.Columns()
				if err != nil {
					t.Errorf("%s: %s", query, err)
					continue
				}
				// Unexpected line. Get and print out the details.
				dst := make([]interface{}, len(cols))
				for i := range dst {
					dst[i] = new(interface{})
				}
				if err := rows.Scan(dst...); err != nil {
					t.Errorf("%s: %s", query, err)
					continue
				}
				b, err := json.Marshal(dst)
				if err != nil {
					t.Errorf("%s: %s", query, err)
					continue
				}
				t.Errorf("%s: unexpected row: %s", query, b)
			}
			if test.others > 0 {
				t.Errorf("%s: expected %d more rows", query, test.others)
				continue
			}
		}
	}

	initStmt := `CREATE DATABASE d; CREATE TABLE d.t (a INT); INSERT INTO d.t VALUES (10),(11); CREATE TABLE d.ts (a TIMESTAMP, b DATE);`
	if _, err := db.Exec(initStmt); err != nil {
		t.Fatal(err)
	}

	for query, tests := range queryTests {
		runTests(query, tests, func(args ...interface{}) (*sql.Rows, error) {
			return db.Query(query, args...)
		})
	}

	for query, tests := range queryTests {
		if stmt, err := db.Prepare(query); err != nil {
			t.Errorf("%s: prepare error: %s", query, err)
		} else {
			func() {
				defer stmt.Close()

				runTests(query, tests, stmt.Query)
			}()
		}
	}
}

type preparedExecTest struct {
	params       []interface{}
	rowsAffected int64
	error        string
}

func (p preparedExecTest) Params(v ...interface{}) preparedExecTest {
	p.params = v
	return p
}

func (p preparedExecTest) RowsAffected(rowsAffected int64) preparedExecTest {
	p.rowsAffected = rowsAffected
	return p
}

func (p preparedExecTest) Error(err string) preparedExecTest {
	p.error = err
	return p
}

func TestPGPreparedExec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var base preparedExecTest
	execTests := []struct {
		query string
		tests []preparedExecTest
	}{
		{
			"CREATE DATABASE d",
			[]preparedExecTest{
				base,
			},
		},
		{
			"CREATE TABLE d.t (i INT, s STRING, d INT)",
			[]preparedExecTest{
				base,
				base.Error(`pq: table "t" already exists`),
			},
		},
		{
			"INSERT INTO d.t VALUES ($1, $2, $3)",
			[]preparedExecTest{
				base.Params(1, "one", 2).RowsAffected(1),
				base.Params("two", 2, 2).Error(`pq: param $1: strconv.ParseInt: parsing "two": invalid syntax`),
			},
		},
		{
			"UPDATE d.t SET s = $1, i = i + $2, d = 1 + $3 WHERE i = $4",
			[]preparedExecTest{
				base.Params(4, 3, 2, 1).RowsAffected(1),
			},
		},
		{
			"DELETE FROM d.t WHERE s = $1 and i = $2 and d = 2 + $3",
			[]preparedExecTest{
				base.Params(1, 2, 3).RowsAffected(0),
			},
		},
		{
			"INSERT INTO d.t VALUES ($1), ($2)",
			[]preparedExecTest{
				base.Params(1, 2).RowsAffected(2),
			},
		},
		{
			"UPDATE d.t SET i = CASE WHEN $1 THEN i-$3 WHEN $2 THEN i+$3 END",
			[]preparedExecTest{
				base.Params(true, true, 3).RowsAffected(3),
			},
		},
		{
			"UPDATE d.t SET i = CASE i WHEN $1 THEN i-$3 WHEN $2 THEN i+$3 END",
			[]preparedExecTest{
				base.Params(1, 2, 3).RowsAffected(3),
			},
		},
		{
			"DROP TABLE d.t",
			[]preparedExecTest{
				base,
				base.Error(`pq: table "t" does not exist`),
			},
		},
		{
			"DROP DATABASE d",
			[]preparedExecTest{
				base,
			},
		},
	}

	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestPGPreparedExec")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	runTests := func(query string, tests []preparedExecTest, execFunc func(...interface{}) (sql.Result, error)) {
		for _, test := range tests {
			if result, err := execFunc(test.params...); err != nil {
				if test.error == "" {
					t.Errorf("%s: %v: unexpected error: %s", query, test.params, err)
				} else if err.Error() != test.error {
					t.Errorf("%s: %v: expected error: %s, got %s", query, test.params, test.error, err)
				}
			} else {
				if rowsAffected, err := result.RowsAffected(); err != nil {
					t.Errorf("%s: %v: unexpected error: %s", query, test.params, err)
				} else if rowsAffected != test.rowsAffected {
					t.Errorf("%s: %v: expected %v, got %v", query, test.params, test.rowsAffected, rowsAffected)
				}
			}
		}
	}

	for _, execTest := range execTests {
		runTests(execTest.query, execTest.tests, func(args ...interface{}) (sql.Result, error) {
			return db.Exec(execTest.query, args...)
		})
	}

	for _, execTest := range execTests {
		if stmt, err := db.Prepare(execTest.query); err != nil {
			t.Errorf("%s: prepare error: %s", execTest.query, err)
		} else {
			func() {
				defer stmt.Close()

				runTests(execTest.query, execTest.tests, stmt.Exec)
			}()
		}
	}
}

// Names should be qualified automatically during Prepare when a database name
// was given in the connection string.
func TestPGPrepareNameQual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestPGPrepareNameQual")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS testing`); err != nil {
		t.Fatal(err)
	}

	pgURL.Path = "/testing"
	db2, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	statements := []string{
		`CREATE TABLE IF NOT EXISTS f (v INT)`,
		`INSERT INTO f VALUES (42)`,
		`SELECT * FROM f`,
		`DELETE FROM f WHERE v = 42`,
		`DROP TABLE IF EXISTS f`,
	}

	for _, stmtString := range statements {
		if _, err = db2.Exec(stmtString); err != nil {
			t.Fatal(err)
		}

		stmt, err := db2.Prepare(stmtString)
		if err != nil {
			t.Fatal(err)
		}

		if _, err = stmt.Exec(); err != nil {
			t.Fatal(err)
		}
	}
}

// A DDL should return "CommandComplete", not "EmptyQuery" Response.
func TestCmdCompleteVsEmptyStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestCmdCompleteVsEmptyStatements")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// cockroachdb/pq handles the empty query response by returning a nil driver.Result.
	// Unfortunately sql.Exec wraps that, nil or not, in a sql.Result which doesn't
	// expose the underlying driver.Result.
	// sql.Result does however have methods which attempt to dereference the underlying
	// driver.Result and can thus be used to determine if it is nil.
	// TODO(dt): This would be prettier and generate better failures with testify/assert's helpers.

	// Result of a DDL (command complete) yields a non-nil underlying driver result.
	nonempty, err := db.Exec(`CREATE DATABASE IF NOT EXISTS testing`)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = nonempty.RowsAffected() // should not panic if cockroachdb/pq returned a non-nil result.

	empty, err := db.Exec(" ; ; ;")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = recover()
	}()
	_, _ = empty.RowsAffected() // should panic if cockroachdb/pq returned a nil result as expected.
	t.Fatal("should not get here -- empty result from empty query should panic first")
}

// Unfortunately cockroachdb/pq doesn't expose returned command tags directly, but we can test
// the methods where it depends on their values (Begin, Commit, RowsAffected for INSERTs).
func TestPGCommandTags(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestPGCommandTags")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS testing`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE testing.tags (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	// Begin will error if the returned tag is not BEGIN.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Commit also checks the correct tag is returned.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO testing.tags VALUES (4, 1)"); err != nil {
		t.Fatal(err)
	}
	// Rollback also checks the correct tag is returned.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// An error will abort the server's transaction.
	if _, err := tx.Exec("INSERT INTO testing.tags VALUES (4, 1), (4, 1)"); err == nil {
		t.Fatal("expected an error on duplicate k")
	}
	// Rollback, even of an aborted txn, should also return the correct tag.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// cockroachdb/pq has a special-case for INSERT (due to oids), so test insert and update statements.
	res, err := db.Exec("INSERT INTO testing.tags VALUES (1, 1), (2, 2)")
	if err != nil {
		t.Fatal(err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 2 {
		t.Fatal("unexpected number of rows affected:", affected)
	}

	res, err = db.Exec("INSERT INTO testing.tags VALUES (3, 3)")
	if err != nil {
		t.Fatal(err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatal("unexpected number of rows affected:", affected)
	}

	res, err = db.Exec("UPDATE testing.tags SET v = 3")
	if err != nil {
		t.Fatal(err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 3 {
		t.Fatal("unexpected number of rows affected:", affected)
	}
}

// checkSQLNetworkMetrics returns the server's pgwire bytesIn/bytesOut and an
// error if the bytesIn/bytesOut don't satisfy the given minimums and maximums.
func checkSQLNetworkMetrics(s *server.TestServer, minBytesIn, minBytesOut, maxBytesIn, maxBytesOut int64) (int64, int64, error) {
	if err := s.WriteSummaries(); err != nil {
		return -1, -1, err
	}

	bytesIn := s.MustGetSQLNetworkCounter("bytesin")
	bytesOut := s.MustGetSQLNetworkCounter("bytesout")
	if a, min := bytesIn, minBytesIn; a < min {
		return bytesIn, bytesOut, util.Errorf("bytesin %d < expected min %d", a, min)
	}
	if a, min := bytesOut, minBytesOut; a < min {
		return bytesIn, bytesOut, util.Errorf("bytesout %d < expected min %d", a, min)
	}
	if a, max := bytesIn, maxBytesIn; a > max {
		return bytesIn, bytesOut, util.Errorf("bytesin %d > expected max %d", a, max)
	}
	if a, max := bytesOut, maxBytesOut; a > max {
		return bytesIn, bytesOut, util.Errorf("bytesout %d > expected max %d", a, max)
	}
	return bytesIn, bytesOut, nil
}

func TestSQLNetworkMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := server.StartTestServer(t)
	defer s.Stop()

	// Setup pgwire client.
	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestSQLNetworkMetrics")
	defer cleanupFn()

	const minbytes = 20

	// Make sure we're starting at 0.
	if _, _, err := checkSQLNetworkMetrics(s, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}

	// A single query should give us some I/O.
	if err := trivialQuery(pgURL); err != nil {
		t.Fatal(err)
	}
	bytesIn, bytesOut, err := checkSQLNetworkMetrics(s, minbytes, minbytes, 300, 300)
	if err != nil {
		t.Fatal(err)
	}
	if err := trivialQuery(pgURL); err != nil {
		t.Fatal(err)
	}

	// A second query should give us more I/O.
	_, _, err = checkSQLNetworkMetrics(s, bytesIn+minbytes, bytesOut+minbytes, 300, 300)
	if err != nil {
		t.Fatal(err)
	}

	// Verify connection counter.
	expectConns := func(n int) {
		util.SucceedsSoon(t, func() error {
			if conns := s.MustGetSQLNetworkCounter("conns"); conns != int64(n) {
				return util.Errorf("connections %d != expected %d", conns, n)
			}
			return nil
		})
	}

	var conns [10]*sql.DB
	for i := range conns {
		var err error
		if conns[i], err = sql.Open("postgres", pgURL.String()); err != nil {
			t.Fatal(err)
		}
		defer conns[i].Close()

		rows, err := conns[i].Query("SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		expectConns(i + 1)
	}

	for i := len(conns) - 1; i >= 0; i-- {
		conns[i].Close()
		expectConns(i)
	}
}

func TestPrepareSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "TestPrepareSyntax")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const strTest = `SELECT """test"""`

	if _, err := db.Exec(`SET SYNTAX = traditional`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Prepare(strTest); err == nil {
		t.Fatal("expected error")
	}

	if _, err := db.Exec(`SET SYNTAX = modern`); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare(strTest)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	var v string
	if err := stmt.QueryRow().Scan(&v); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if v != "test" {
		t.Fatalf("unexpected result: %q", v)
	}
}

func TestPGWireOverUnixSocket(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We need a temp directory in which we'll create the
	// unix socket ".s.PGSQL.<port>".
	// We hard-code "/tmp" as the directory as the osx default can cause
	// the socket filename length to exceed 104 characters, triggering an error.
	tempDir, err := ioutil.TempDir("/tmp", "cockroach-unix")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	socketFile := filepath.Join(tempDir, ".s.PGSQL.123456")

	ctx, _ := createTestServerContext()
	ctx.Insecure = true
	ctx.SocketFile = socketFile
	s := setupTestServerWithContext(t, ctx)
	defer s.Stop()

	// We can't pass socket paths as url.Host to libpq, use ?host=/... instead.
	options := url.Values{
		"host": []string{tempDir},
	}
	pgURL := url.URL{
		Scheme:   "postgres",
		Host:     ":123456",
		RawQuery: options.Encode(),
	}
	t.Logf("PGURL: %s", pgURL.String())
	if err := trivialQuery(pgURL); err != nil {
		t.Fatal(err)
	}
}
