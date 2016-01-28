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
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func trivialQuery(pgUrl url.URL) error {
	db, err := sql.Open("postgres", pgUrl.String())
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
	defer leaktest.AfterTest(t)

	certUser := server.TestUser
	certPath := security.ClientCertPath(security.EmbeddedCertsDir, certUser)
	keyPath := security.ClientKeyPath(security.EmbeddedCertsDir, certUser)

	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	tempCertPath, tempCertCleanup := securitytest.RestrictedCopy(t, certPath, os.TempDir(), "TestPGWire_cert")
	defer tempCertCleanup()
	tempKeyPath, tempKeyCleanup := securitytest.RestrictedCopy(t, keyPath, os.TempDir(), "TestPGWire_key")
	defer tempKeyCleanup()

	for _, insecure := range [...]bool{true, false} {
		ctx := server.NewTestContext()
		ctx.Insecure = insecure
		s := setupTestServerWithContext(t, ctx)

		host, port, err := net.SplitHostPort(s.PGAddr())
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
			for _, optUser := range []string{certUser, security.RootUser} {
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
					if optUser == certUser {
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

type preparedTest struct {
	params []interface{}
	error  string
	result []interface{}
}

func (p preparedTest) Params(v ...interface{}) preparedTest {
	p.params = v
	return p
}

func (p preparedTest) Error(err string) preparedTest {
	p.error = err
	return p
}

func (p preparedTest) Results(v ...interface{}) preparedTest {
	p.result = v
	return p
}

func TestPGPrepared(t *testing.T) {
	defer leaktest.AfterTest(t)
	var base preparedTest
	queryTests := map[string][]preparedTest{
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
		// TODO(mjibson): test date/time types
	}

	s := server.StartTestServer(t)
	defer s.Stop()

	pgUrl, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, os.TempDir(), "TestPGPrepared")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgUrl.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for query, tests := range queryTests {
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Errorf("prepare error: %s: %s", query, err)
			continue
		}

		type result struct {
			rows *sql.Rows
			err  error
		}

		for _, test := range tests {
			var results []result
			{
				rows, err := db.Query(query, test.params...)
				results = append(results, result{rows: rows, err: err})
			}
			{
				rows, err := stmt.Query(test.params...)
				results = append(results, result{rows: rows, err: err})
			}
			for _, res := range results {
				rows, err := res.rows, res.err
				if err != nil {
					if test.error == "" {
						t.Errorf("%s: %#v: unexpected error: %s", query, test.params, err)
					}
					if test.error != err.Error() {
						t.Errorf("%s: %#v: expected error: %s, got %s", query, test.params, test.error, err)
					}
					continue
				}
				if test.error != "" && err == nil {
					t.Errorf("expected error: %s: %#v", query, test.params)
				}
				dst := make([]interface{}, len(test.result))
				for i, d := range test.result {
					dst[i] = reflect.New(reflect.TypeOf(d)).Interface()
				}
				if !rows.Next() {
					t.Errorf("expected row: %s: %#v", query, test.params)
				}
				if err := rows.Scan(dst...); err != nil {
					t.Error(err)
				}
				rows.Close()
				for i, d := range dst {
					v := reflect.Indirect(reflect.ValueOf(d)).Interface()
					dst[i] = v
				}
				if !reflect.DeepEqual(dst, test.result) {
					t.Errorf("%s: %#v: expected %v, got %v", query, test.params, test.result, dst)
				}
			}
		}
		if err := stmt.Close(); err != nil {
			t.Error(err)
		}
	}

	testFailures := map[string]string{
		"SELECT $1 = $1":           "pq: unsupported comparison operator: <valarg> = <valarg>",
		"SELECT $1 > 0 AND NOT $1": "pq: incompatible NOT argument type: int",
		"SELECT $1":                "pq: unsupported result type: valarg",
		"SELECT $1 + $1":           "pq: unsupported binary operator: <valarg> + <valarg>",
		"SELECT now() + $1":        "pq: unsupported binary operator: <timestamp> + <valarg>",
	}

	for query, reason := range testFailures {
		stmt, err := db.Prepare(query)
		if err == nil {
			t.Errorf("expected error: %s", query)
			stmt.Close()
			continue
		}
		if err.Error() != reason {
			t.Errorf("unexpected error: %s: %s", query, err)
		}
	}
}

// A DDL should return "CommandComplete", not "EmptyQuery" Response.
func TestCmdCompleteVsEmptyStatements(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	pgUrl, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, os.TempDir(), "TestCmdCompleteVsEmptyStatements")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgUrl.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// lib/pq handles the empty query response by returning a nil driver.Result.
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
	_, _ = nonempty.RowsAffected() // should not panic if lib/pq returned a non-nil result.

	empty, err := db.Exec(" ")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = recover()
	}()
	_, _ = empty.RowsAffected() // should panic if lib/pq returned a nil result as expected.
	t.Fatal("should not get here -- empty result from empty query should panic first")
	// TODO(dt): clean this up with testify/assert and add tests for less trivial empty queries.
}
