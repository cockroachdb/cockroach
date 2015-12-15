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
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/testutils"
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
	tempCertPath, tempCertCleanup := tempRestrictedCopy(t, os.TempDir(), certPath, "TestPGWire_cert")
	defer tempCertCleanup()
	tempKeyPath, tempKeyCleanup := tempRestrictedCopy(t, os.TempDir(), keyPath, "TestPGWire_key")
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
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, "no client certificates in request") {
					t.Fatal(err)
				}
			}
		}

		{
			disablePgUrl := basePgUrl
			disablePgUrl.RawQuery = "sslmode=disable"
			err := trivialQuery(disablePgUrl)
			if insecure {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, pgwire.ErrSSLRequired) {
					t.Fatal(err)
				}
			}
		}

		{
			requirePgUrlNoCert := basePgUrl
			requirePgUrlNoCert.RawQuery = "sslmode=require"
			err := trivialQuery(requirePgUrlNoCert)
			if insecure {
				if err != pq.ErrSSLNotSupported {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, "no client certificates in request") {
					t.Fatal(err)
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
						t.Fatal(err)
					}
				} else {
					if optUser == certUser {
						if err != nil {
							t.Fatal(err)
						}
					} else {
						if !testutils.IsError(err, `requested user is \w+, but certificate is for \w+`) {
							t.Fatal(err)
						}
					}
				}
			}
		}

		cleanupTestServer(s)
	}
}

func TestPGPrepared(t *testing.T) {
	defer leaktest.AfterTest(t)
	queryTests := map[string][]struct {
		params []interface{}
		error  string
		result []interface{}
	}{
		"SELECT $1 > 0": {
			{
				[]interface{}{1},
				"",
				[]interface{}{true},
			},
			{
				[]interface{}{"1"},
				"",
				[]interface{}{true},
			},
			{
				[]interface{}{1.1},
				"pq: unknown int value: 1.1",
				[]interface{}{true},
			},
			{
				[]interface{}{"1.0"},
				"pq: unknown int value: 1.0",
				nil,
			},
			{
				[]interface{}{true},
				"pq: unknown int value: true",
				nil,
			},
		},
		"SELECT TRUE AND $1": {
			{
				[]interface{}{true},
				"",
				[]interface{}{true},
			},
			{
				[]interface{}{false},
				"",
				[]interface{}{false},
			},
			{
				[]interface{}{1},
				"pq: unknown bool value: 1",
				nil,
			},
			{
				[]interface{}{""},
				"pq: unknown bool value: ",
				nil,
			},
			// Make sure we can run another after a failure.
			{
				[]interface{}{true},
				"",
				[]interface{}{true},
			},
		},
		"SELECT $1::bool": {
			{
				[]interface{}{true},
				"",
				[]interface{}{true},
			},
			{
				[]interface{}{"true"},
				"",
				[]interface{}{true},
			},
			{
				[]interface{}{"false"},
				"",
				[]interface{}{false},
			},
			{
				[]interface{}{"1"},
				"pq: unknown bool value: 1",
				nil,
			},
			{
				[]interface{}{2},
				"pq: unknown bool value: 2",
				nil,
			},
			{
				[]interface{}{3.1},
				"pq: unknown bool value: 3.1",
				nil,
			},
			{
				[]interface{}{""},
				"pq: unknown bool value: ",
				nil,
			},
		},
		"SELECT $1::int > $2::float": {
			{
				[]interface{}{"2", 1},
				"",
				[]interface{}{true},
			},
			{
				[]interface{}{1, "2"},
				"",
				[]interface{}{false},
			},
			{
				[]interface{}{"2", "1.0"},
				"",
				[]interface{}{true},
			},
			{
				[]interface{}{"2.0", "1"},
				"pq: unknown int value: 2.0",
				nil,
			},
			{
				[]interface{}{2.1, 1},
				"pq: unknown int value: 2.1",
				nil,
			},
		},
		"SELECT GREATEST($1, 0, $2), $2": {
			{
				[]interface{}{1, -1},
				"",
				[]interface{}{1, -1},
			},
			{
				[]interface{}{-1, 10},
				"",
				[]interface{}{10, 10},
			},
			{
				[]interface{}{"-2", "-1"},
				"",
				[]interface{}{0, -1},
			},
			{
				[]interface{}{1, 2.1},
				"pq: unknown int value: 2.1",
				nil,
			},
		},
		"SELECT $1::int, $1::float": {
			{
				[]interface{}{"1"},
				"",
				[]interface{}{1, 1.0},
			},
		},
	}

	defer leaktest.AfterTest(t)

	ctx := server.NewTestContext()
	ctx.Insecure = true
	s := setupTestServerWithContext(t, ctx)
	defer cleanupTestServer(s)

	host, port, err := net.SplitHostPort(s.PGAddr())
	if err != nil {
		t.Fatal(err)
	}
	pgUrl := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, port),
		RawQuery: "sslmode=disable",
	}

	db, err := sql.Open("postgres", pgUrl.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	{
		stmt, err := db.Prepare("SELECT $1::int")
		if err != nil {
			t.Fatal(err)
		}
		stmt.Close()
	}

	for query, tests := range queryTests {
		fmt.Println("\nPREPARE", query)
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Fatalf("prepare error: %s: %s", query, err)
		}
		for _, test := range tests {
			fmt.Printf("\nBIND %#v\n", test.params)
			rows, err := stmt.Query(test.params...)
			if err != nil {
				if test.error == "" {
					t.Fatalf("%s: %#v: unexpected error: %s", query, test.params, err)
				}
				if test.error != err.Error() {
					t.Fatalf("%s: %#v: expected error: %s, got %s", query, test.params, test.error, err)
				}
				continue
			}
			if test.error != "" && err == nil {
				t.Fatalf("expected error: %s: %#v", query, test.params)
			}
			dst := make([]interface{}, len(test.result))
			for i, d := range test.result {
				dst[i] = reflect.New(reflect.TypeOf(d)).Interface()
			}
			if !rows.Next() {
				t.Fatalf("expected row: %s: %#v", query, test.params)
			}
			if err := rows.Scan(dst...); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			for i, d := range dst {
				v := reflect.Indirect(reflect.ValueOf(d)).Interface()
				dst[i] = v
			}
			if !reflect.DeepEqual(dst, test.result) {
				t.Fatalf("%s: %#v: expected %v, got %v", query, test.params, test.result, dst)
			}
		}
		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
	}

	testFailures := map[string]string{
		"SELECT $1 = $1":             "pq: unsupported comparison operator: <valarg> = <valarg>",
		"SELECT $1 > 0 AND $1 > 0.0": "pq: parameter $1 has multiple types: float, int",
		"SELECT $1":                  "pq: arg $1 not found",
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
