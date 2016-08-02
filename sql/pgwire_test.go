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
	gosql "database/sql"
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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/pq"
	"github.com/pkg/errors"
)

func trivialQuery(pgURL url.URL) error {
	db, err := gosql.Open("postgres", pgURL.String())
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
		params, _ := createTestServerParams()
		params.Insecure = insecure
		s, _, _ := serverutils.StartServer(t, params)

		host, port, err := net.SplitHostPort(s.ServingAddr())
		if err != nil {
			t.Fatal(err)
		}

		pgBaseURL := url.URL{
			Scheme: "postgres",
			Host:   net.JoinHostPort(host, port),
		}
		if err := trivialQuery(pgBaseURL); err != nil {
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
			pgDisableURL := pgBaseURL
			pgDisableURL.RawQuery = "sslmode=disable"
			err := trivialQuery(pgDisableURL)
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
			pgNoCertRequireURL := pgBaseURL
			pgNoCertRequireURL.RawQuery = "sslmode=require"
			err := trivialQuery(pgNoCertRequireURL)
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
				pgWithCertRequireURL := pgBaseURL
				pgWithCertRequireURL.User = url.User(optUser)
				pgWithCertRequireURL.RawQuery = fmt.Sprintf("sslmode=require&sslcert=%s&sslkey=%s",
					url.QueryEscape(tempCertPath),
					url.QueryEscape(tempKeyPath),
				)
				err := trivialQuery(pgWithCertRequireURL)
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

		s.Stopper().Stop()
	}
}

// TestPGWireDrainClient makes sure the server refuses new connections when
// it's in draining mode.
func TestPGWireDrainClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	params.Insecure = true
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	host, port, err := net.SplitHostPort(s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}

	pgBaseURL := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, port),
		RawQuery: "sslmode=disable",
	}

	on := []serverpb.DrainMode{serverpb.DrainMode_CLIENT}

	if now, err := s.(*server.TestServer).Drain(on); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(on, now) {
		t.Fatalf("expected drain modes %v, got %v", on, now)
	}
	if err := trivialQuery(pgBaseURL); !testutils.IsError(err, pgwire.ErrDraining) {
		t.Fatal(err)
	}
	if now := s.(*server.TestServer).Undrain(
		[]serverpb.DrainMode{serverpb.DrainMode_CLIENT}); len(now) != 0 {
		t.Fatalf("unexpected active drain modes: %v", now)
	}
	if err := trivialQuery(pgBaseURL); err != nil {
		t.Fatal(err)
	}
}

func TestPGWireDBName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestPGWireDBName")
	pgURL.Path = "foo"
	defer cleanupFn()
	{
		db, err := gosql.Open("postgres", pgURL.String())
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
	db, err := gosql.Open("postgres", pgURL.String())
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

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestPGPrepareFail")
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testFailures := map[string]string{
		"SELECT $1 = $1":                            "pq: could not determine data type of placeholder $1",
		"SELECT $1":                                 "pq: could not determine data type of placeholder $1",
		"SELECT $1 + $1":                            "pq: could not determine data type of placeholder $1",
		"SELECT CASE WHEN TRUE THEN $1 END":         "pq: could not determine data type of placeholder $1",
		"SELECT CASE WHEN TRUE THEN $1 ELSE $2 END": "pq: could not determine data type of placeholder $1",
		"SELECT $1 > 0 AND NOT $1":                  "pq: incompatible NOT argument type: int",
		"CREATE TABLE $1 (id INT)":                  "pq: syntax error at or near \"1\"\nCREATE TABLE $1 (id INT)\n             ^\n",
		"UPDATE d.t SET s = i + $1":                 "pq: unsupported binary operator: <int> + <placeholder> (desired <string>)",
		"SELECT $0 > 0":                             "pq: invalid placeholder name: $0",
		"SELECT $2 > 0":                             "pq: could not determine data type of placeholder $1",
		"SELECT 3 + CASE (4) WHEN 4 THEN $1 END":    "pq: could not determine data type of placeholder $1",
		"SELECT ($1 + $1) + CURRENT_DATE()":         "pq: could not determine data type of placeholder $1",
		"SELECT $1 + $2, $2::FLOAT":                 "pq: could not determine data type of placeholder $1",
		"SELECT ($1 + 2) + ($1 + 2.5::FLOAT)":       "pq: unsupported binary operator: <int> + <float>",
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
			t.Errorf(`%s: got: %q, expected: %q`, query, err, reason)
		}
	}
}

type preparedQueryTest struct {
	qargs   []interface{}
	results [][]interface{}
	others  int
	error   string
}

func (p preparedQueryTest) SetArgs(v ...interface{}) preparedQueryTest {
	p.qargs = v
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
	var baseTest preparedQueryTest

	queryTests := map[string][]preparedQueryTest{
		"SELECT $1 > 0": {
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs("1").Results(true),
			baseTest.SetArgs(1.1).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "1.1": invalid syntax`).Results(true),
			baseTest.SetArgs("1.0").Error(`pq: error in argument for $1: strconv.ParseInt: parsing "1.0": invalid syntax`),
			baseTest.SetArgs(true).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "true": invalid syntax`),
		},
		"SELECT ($1) > 0": {
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs(-1).Results(false),
		},
		"SELECT ((($1))) > 0": {
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs(-1).Results(false),
		},
		"SELECT TRUE AND $1": {
			baseTest.SetArgs(true).Results(true),
			baseTest.SetArgs(false).Results(false),
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs("").Error(`pq: error in argument for $1: strconv.ParseBool: parsing "": invalid syntax`),
			// Make sure we can run another after a failure.
			baseTest.SetArgs(true).Results(true),
		},
		"SELECT $1::bool": {
			baseTest.SetArgs(true).Results(true),
			baseTest.SetArgs("true").Results(true),
			baseTest.SetArgs("false").Results(false),
			baseTest.SetArgs("1").Results(true),
			baseTest.SetArgs(2).Error(`pq: error in argument for $1: strconv.ParseBool: parsing "2": invalid syntax`),
			baseTest.SetArgs(3.1).Error(`pq: error in argument for $1: strconv.ParseBool: parsing "3.1": invalid syntax`),
			baseTest.SetArgs("").Error(`pq: error in argument for $1: strconv.ParseBool: parsing "": invalid syntax`),
		},
		"SELECT $1::int > $2::float": {
			baseTest.SetArgs(2, 1).Results(true),
			baseTest.SetArgs("2", 1).Results(true),
			baseTest.SetArgs(1, "2").Results(false),
			baseTest.SetArgs("2", "1.0").Results(true),
			baseTest.SetArgs("2.0", "1").Error(`pq: error in argument for $1: strconv.ParseInt: parsing "2.0": invalid syntax`),
			baseTest.SetArgs(2.1, 1).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "2.1": invalid syntax`),
		},
		"SELECT GREATEST($1, 0, $2), $2": {
			baseTest.SetArgs(1, -1).Results(1, -1),
			baseTest.SetArgs(-1, 10).Results(10, 10),
			baseTest.SetArgs("-2", "-1").Results(0, -1),
			baseTest.SetArgs(1, 2.1).Error(`pq: error in argument for $2: strconv.ParseInt: parsing "2.1": invalid syntax`),
		},
		"SELECT $1::int, $1::float": {
			baseTest.SetArgs(1).Results(1, 1.0),
			baseTest.SetArgs("1").Results(1, 1.0),
		},
		"SELECT 3 + $1, $1 + $2": {
			baseTest.SetArgs("1", "2").Results(4, 3),
			baseTest.SetArgs(3, "4").Results(6, 7),
			baseTest.SetArgs(0, "a").Error(`pq: error in argument for $2: strconv.ParseInt: parsing "a": invalid syntax`),
		},
		// Check for QualifiedName resolution.
		"SELECT COUNT(*)": {
			baseTest.Results(1),
		},
		"SELECT CASE WHEN $1 THEN 1-$3 WHEN $2 THEN 1+$3 END": {
			baseTest.SetArgs(true, false, 2).Results(-1),
			baseTest.SetArgs(false, true, 3).Results(4),
			baseTest.SetArgs(false, false, 2).Results(gosql.NullBool{}),
		},
		"SELECT CASE 1 WHEN $1 THEN $2 ELSE 2 END": {
			baseTest.SetArgs(1, 3).Results(3),
			baseTest.SetArgs(2, 3).Results(2),
			baseTest.SetArgs(true, 0).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "true": invalid syntax`),
		},
		"SHOW DATABASE": {
			baseTest.Results(""),
		},
		"SELECT descriptor FROM system.descriptor WHERE descriptor != $1 LIMIT 1": {
			baseTest.SetArgs([]byte("abc")).Results([]byte("\x12\x16\n\x06system\x10\x01\x1a\n\n\b\n\x04root\x100")),
		},
		"SHOW COLUMNS FROM system.users": {
			baseTest.
				Results("username", "STRING", false, gosql.NullBool{}).
				Results("hashedPassword", "BYTES", true, gosql.NullBool{}),
		},
		"SHOW DATABASES": {
			baseTest.Results("information_schema").Results("d").Results("system"),
		},
		"SHOW GRANTS ON system.users": {
			baseTest.Results("users", security.RootUser, "DELETE,GRANT,INSERT,SELECT,UPDATE"),
		},
		"SHOW INDEXES FROM system.users": {
			baseTest.Results("users", "primary", true, 1, "username", "ASC", false),
		},
		"SHOW TABLES FROM system": {
			baseTest.Results("descriptor").Others(7),
		},
		"SHOW TIME ZONE": {
			baseTest.Results("UTC"),
		},
		"SELECT (SELECT 1+$1)": {
			baseTest.SetArgs(1).Results(2),
		},
		"SELECT CASE WHEN $1 THEN $2 ELSE 3 END": {
			baseTest.SetArgs(true, 2).Results(2),
			baseTest.SetArgs(false, 2).Results(3),
		},
		"SELECT CASE WHEN TRUE THEN 1 ELSE $1 END": {
			baseTest.SetArgs(2).Results(1),
		},
		"SELECT CASE $1 WHEN 1 THEN 1 END": {
			baseTest.SetArgs(1).Results(1),
			baseTest.SetArgs(2).Results(gosql.NullInt64{}),
		},
		"SELECT $1::timestamp, $2::date": {
			baseTest.SetArgs("2001-01-02 03:04:05", "2006-07-08").Results(
				time.Date(2001, 1, 2, 3, 4, 5, 0, time.FixedZone("", 0)),
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"SELECT $1::date, $2::timestamp": {
			baseTest.SetArgs(
				time.Date(2006, 7, 8, 0, 0, 0, 9, time.FixedZone("", 0)),
				time.Date(2001, 1, 2, 3, 4, 5, 6000, time.FixedZone("", 0)),
			).Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
				time.Date(2001, 1, 2, 3, 4, 5, 6000, time.FixedZone("", 0)),
			),
		},
		"SELECT (CASE a WHEN 10 THEN 'one' WHEN 11 THEN (CASE 'en' WHEN 'en' THEN $1 END) END) AS ret FROM d.T ORDER BY ret DESC LIMIT 2": {
			baseTest.SetArgs("hello").Results("one").Results("hello"),
		},
		"INSERT INTO d.ts VALUES($1, $2) RETURNING *": {
			baseTest.SetArgs("2001-01-02 03:04:05", "2006-07-08").Results(
				time.Date(2001, 1, 2, 3, 4, 5, 0, time.FixedZone("", 0)),
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.ts VALUES(CURRENT_TIMESTAMP(), $1) RETURNING b": {
			baseTest.SetArgs("2006-07-08").Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.ts VALUES(STATEMENT_TIMESTAMP(), $1) RETURNING b": {
			baseTest.SetArgs("2006-07-08").Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.ts (a) VALUES ($1) RETURNING a": {
			baseTest.SetArgs(
				time.Date(2006, 7, 8, 0, 0, 0, 123000, time.FixedZone("", 0)),
			).Results(
				time.Date(2006, 7, 8, 0, 0, 0, 123000, time.FixedZone("", 0)),
			),
		},
		"INSERT INTO d.T VALUES ($1) RETURNING 1": {
			baseTest.SetArgs(1).Results(1),
			baseTest.SetArgs(nil).Results(1),
		},
		"INSERT INTO d.T VALUES ($1::INT) RETURNING 1": {
			baseTest.SetArgs(1).Results(1),
		},
		"INSERT INTO d.T VALUES ($1) RETURNING $1": {
			baseTest.SetArgs(1).Results(1),
			baseTest.SetArgs(3).Results(3),
		},
		"INSERT INTO d.T VALUES ($1) RETURNING $1, 1 + $1": {
			baseTest.SetArgs(1).Results(1, 2),
			baseTest.SetArgs(3).Results(3, 4),
		},
		"INSERT INTO d.T VALUES (GREATEST(42, $1)) RETURNING a": {
			baseTest.SetArgs(40).Results(42),
			baseTest.SetArgs(45).Results(45),
		},
		"SELECT a FROM d.T WHERE a = $1 AND (SELECT a >= $2 FROM d.T WHERE a = $1)": {
			baseTest.SetArgs(10, 5).Results(10),
		},
		"SELECT * FROM (VALUES (1), (2), (3), (4)) AS foo (a) LIMIT $1 OFFSET $2": {
			baseTest.SetArgs(1, 0).Results(1),
			baseTest.SetArgs(1, 1).Results(2),
			baseTest.SetArgs(1, 2).Results(3),
		},
		"SELECT 3 + CASE (4) WHEN 4 THEN $1 ELSE 42 END": {
			baseTest.SetArgs(12).Results(15),
			baseTest.SetArgs(-12).Results(-9),
		},
		"SELECT DATE '2001-01-02' + ($1 + $1)": {
			baseTest.SetArgs(12).Results("2001-01-26T00:00:00Z"),
		},
		"SELECT TO_HEX(~(~$1))": {
			baseTest.SetArgs(12).Results("c"),
		},
		"SELECT $1::INT": {
			baseTest.SetArgs(12).Results(12),
		},
		"SELECT ANNOTATE_TYPE($1, int)": {
			baseTest.SetArgs(12).Results(12),
		},
		"SELECT $1 + $2, ANNOTATE_TYPE($2, float)": {
			baseTest.SetArgs(12, 23).Results(35, 23),
		},
		"INSERT INTO d.T VALUES ($1 + 1) RETURNING a": {
			baseTest.SetArgs(1).Results(2),
			baseTest.SetArgs(11).Results(12),
		},
		"INSERT INTO d.T VALUES (-$1) RETURNING a": {
			baseTest.SetArgs(1).Results(-1),
			baseTest.SetArgs(-999).Results(999),
		},
		"INSERT INTO d.two (a, b) VALUES (~$1, $1 + $2) RETURNING a, b": {
			baseTest.SetArgs(5, 6).Results(-6, 11),
		},
		"INSERT INTO d.str (s) VALUES (LEFT($1, 3)) RETURNING s": {
			baseTest.SetArgs("abcdef").Results("abc"),
			baseTest.SetArgs("123456").Results("123"),
		},
		"INSERT INTO d.str (b) VALUES (COALESCE($1, 'strLit')) RETURNING b": {
			baseTest.SetArgs(nil).Results("strLit"),
			baseTest.SetArgs("123456").Results("123456"),
		},
		"INSERT INTO d.intStr VALUES ($1, 'hello ' || $1::TEXT) RETURNING *": {
			baseTest.SetArgs(123).Results(123, "hello 123"),
		},
	}

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestPGPreparedQuery")
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	runTests := func(query string, tests []preparedQueryTest, queryFunc func(...interface{}) (*gosql.Rows, error)) {
		for _, test := range tests {
			if testing.Verbose() || log.V(1) {
				log.Infof(context.Background(), "query: %s", query)
			}
			rows, err := queryFunc(test.qargs...)
			if err != nil {
				if test.error == "" {
					t.Errorf("%s: %v: unexpected error: %s", query, test.qargs, err)
				} else if err.Error() != test.error {
					t.Errorf("%s: %v: expected error: %s, got %s", query, test.qargs, test.error, err)
				}
				continue
			}
			defer rows.Close()

			if test.error != "" {
				t.Errorf("expected error: %s: %v", query, test.qargs)
				continue
			}

			for _, expected := range test.results {
				if !rows.Next() {
					t.Errorf("expected row: %s: %v", query, test.qargs)
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
					t.Errorf("%s: %v: expected %v, got %v", query, test.qargs, expected, dst)
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

	initStmt := `
CREATE DATABASE d;
CREATE TABLE d.t (a INT);
INSERT INTO d.t VALUES (10),(11);
CREATE TABLE d.ts (a TIMESTAMP, b DATE);
CREATE TABLE d.two (a INT, b INT);
CREATE TABLE d.intStr (a INT, s STRING);
CREATE TABLE d.str (s STRING, b BYTES);`
	if _, err := db.Exec(initStmt); err != nil {
		t.Fatal(err)
	}

	for query, tests := range queryTests {
		runTests(query, tests, func(args ...interface{}) (*gosql.Rows, error) {
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
	qargs        []interface{}
	rowsAffected int64
	error        string
}

func (p preparedExecTest) SetArgs(v ...interface{}) preparedExecTest {
	p.qargs = v
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
	var baseTest preparedExecTest
	execTests := []struct {
		query string
		tests []preparedExecTest
	}{
		{
			"CREATE DATABASE d",
			[]preparedExecTest{
				baseTest,
			},
		},
		{
			"CREATE TABLE d.t (i INT, s STRING, d INT)",
			[]preparedExecTest{
				baseTest,
				baseTest.Error(`pq: table "t" already exists`),
			},
		},
		{
			"INSERT INTO d.t VALUES ($1, $2, $3)",
			[]preparedExecTest{
				baseTest.SetArgs(1, "one", 2).RowsAffected(1),
				baseTest.SetArgs("two", 2, 2).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "two": invalid syntax`),
			},
		},
		{
			"UPDATE d.t SET s = $1, i = i + $2, d = 1 + $3 WHERE i = $4",
			[]preparedExecTest{
				baseTest.SetArgs(4, 3, 2, 1).RowsAffected(1),
			},
		},
		{
			"UPDATE d.t SET i = $1 WHERE (i, s) = ($2, $3)",
			[]preparedExecTest{
				baseTest.SetArgs(8, 4, "4").RowsAffected(1),
			},
		},
		{
			"DELETE FROM d.t WHERE s = $1 and i = $2 and d = 2 + $3",
			[]preparedExecTest{
				baseTest.SetArgs(1, 2, 3).RowsAffected(0),
			},
		},
		{
			"INSERT INTO d.t VALUES ($1), ($2)",
			[]preparedExecTest{
				baseTest.SetArgs(1, 2).RowsAffected(2),
			},
		},
		{
			"INSERT INTO d.t VALUES ($1), ($2) RETURNING $3 + 1",
			[]preparedExecTest{
				baseTest.SetArgs(3, 4, 5).RowsAffected(2),
			},
		},
		{
			"UPDATE d.t SET i = CASE WHEN $1 THEN i-$3 WHEN $2 THEN i+$3 END",
			[]preparedExecTest{
				baseTest.SetArgs(true, true, 3).RowsAffected(5),
			},
		},
		{
			"UPDATE d.t SET i = CASE i WHEN $1 THEN i-$3 WHEN $2 THEN i+$3 END",
			[]preparedExecTest{
				baseTest.SetArgs(1, 2, 3).RowsAffected(5),
			},
		},
		{
			"UPDATE d.t SET d = CASE WHEN TRUE THEN $1 END",
			[]preparedExecTest{
				baseTest.SetArgs(2).RowsAffected(5),
			},
		},
		{
			"DELETE FROM d.t RETURNING $1+1",
			[]preparedExecTest{
				baseTest.SetArgs(1).RowsAffected(5),
			},
		},
		{
			"DROP TABLE d.t",
			[]preparedExecTest{
				baseTest,
				baseTest.Error(`pq: table "d.t" does not exist`),
			},
		},
		{
			"CREATE TABLE d.types (i int, f float, s string, b bytes, d date, m timestamp, z timestamp with time zone, n interval, o bool, e decimal)",
			[]preparedExecTest{
				baseTest,
			},
		},
		{
			"INSERT INTO d.types VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
			[]preparedExecTest{
				baseTest.RowsAffected(1).SetArgs(
					int64(0),
					float64(0),
					"",
					[]byte{},
					time.Time{}, // date
					time.Time{}, // timestamp
					time.Time{}, // timestamptz
					time.Hour.String(),
					true,
					"0.0", // decimal
				),
			},
		},
		{
			"DROP DATABASE d",
			[]preparedExecTest{
				baseTest,
			},
		},
	}

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestPGPreparedExec")
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	runTests := func(query string, tests []preparedExecTest, execFunc func(...interface{}) (gosql.Result, error)) {
		for _, test := range tests {
			if testing.Verbose() || log.V(1) {
				log.Infof(context.Background(), "exec: %s", query)
			}
			if result, err := execFunc(test.qargs...); err != nil {
				if test.error == "" {
					t.Errorf("%s: %v: unexpected error: %s", query, test.qargs, err)
				} else if err.Error() != test.error {
					t.Errorf("%s: %v: expected error: %s, got %s", query, test.qargs, test.error, err)
				}
			} else {
				if rowsAffected, err := result.RowsAffected(); err != nil {
					t.Errorf("%s: %v: unexpected error: %s", query, test.qargs, err)
				} else if rowsAffected != test.rowsAffected {
					t.Errorf("%s: %v: expected %v, got %v", query, test.qargs, test.rowsAffected, rowsAffected)
				}
			}
		}
	}

	for _, execTest := range execTests {
		runTests(execTest.query, execTest.tests, func(args ...interface{}) (gosql.Result, error) {
			return db.Exec(execTest.query, args...)
		})
	}

	for _, execTest := range execTests {
		if testing.Verbose() || log.V(1) {
			log.Infof(context.Background(), "prepare: %s", execTest.query)
		}
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestPGPrepareNameQual")
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS testing`); err != nil {
		t.Fatal(err)
	}

	pgURL.Path = "/testing"
	db2, err := gosql.Open("postgres", pgURL.String())
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser,
		"TestCmdCompleteVsEmptyStatements")
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// cockroachdb/pq handles the empty query response by returning a nil driver.Result.
	// Unfortunately gosql.Exec wraps that, nil or not, in a gosql.Result which doesn't
	// expose the underlying driver.Result.
	// gosql.Result does however have methods which attempt to dereference the underlying
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestPGCommandTags")
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
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
func checkSQLNetworkMetrics(
	s serverutils.TestServerInterface,
	minBytesIn, minBytesOut, maxBytesIn, maxBytesOut int64,
) (int64, int64, error) {
	if err := s.WriteSummaries(); err != nil {
		return -1, -1, err
	}

	bytesIn := s.MustGetSQLNetworkCounter("bytesin")
	bytesOut := s.MustGetSQLNetworkCounter("bytesout")
	if a, min := bytesIn, minBytesIn; a < min {
		return bytesIn, bytesOut, errors.Errorf("bytesin %d < expected min %d", a, min)
	}
	if a, min := bytesOut, minBytesOut; a < min {
		return bytesIn, bytesOut, errors.Errorf("bytesout %d < expected min %d", a, min)
	}
	if a, max := bytesIn, maxBytesIn; a > max {
		return bytesIn, bytesOut, errors.Errorf("bytesin %d > expected max %d", a, max)
	}
	if a, max := bytesOut, maxBytesOut; a > max {
		return bytesIn, bytesOut, errors.Errorf("bytesout %d > expected max %d", a, max)
	}
	return bytesIn, bytesOut, nil
}

func TestSQLNetworkMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	// Setup pgwire client.
	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser,
		"TestSQLNetworkMetrics")
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
				return errors.Errorf("connections %d != expected %d", conns, n)
			}
			return nil
		})
	}

	var conns [10]*gosql.DB
	for i := range conns {
		var err error
		if conns[i], err = gosql.Open("postgres", pgURL.String()); err != nil {
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

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestPrepareSyntax")
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
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

	params, _ := createTestServerParams()
	params.Insecure = true
	params.SocketFile = socketFile
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

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
