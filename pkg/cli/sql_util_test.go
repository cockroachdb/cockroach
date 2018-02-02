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

package cli

import (
	"bytes"
	"database/sql/driver"
	"net/url"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestConnRecover(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := cliTestParams{t: t}
	c := newCLITest(p)
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	// Sanity check to establish baseline.
	rows, err := conn.Query(`SELECT 1`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Check that Query detects a connection close.
	defer simulateServerRestart(&c, p, conn)()

	_, err = conn.Query(`SELECT 1`, nil)
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("conn.Query(): expected bad conn, got %v", err)
	}
	// Check that Query recovers from a connection close by re-connecting.
	rows, err = conn.Query(`SELECT 1`, nil)
	if err != nil {
		t.Fatalf("conn.Query(): expected no error after reconnect, got %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Check that Exec detects a connection close.
	defer simulateServerRestart(&c, p, conn)()

	if err := conn.Exec(`SELECT 1`, nil); err == nil || err != driver.ErrBadConn {
		t.Fatalf("conn.Exec(): expected bad conn, got %v", err)
	}
	// Check that Exec recovers from a connection close by re-connecting.
	if err := conn.Exec(`SELECT 1`, nil); err != nil {
		t.Fatalf("conn.Exec(): expected no error after reconnect, got %v", err)
	}
}

// simulateServerRestart restarts the test server and reconfigures the connection
// to use the new test server's port number. This is necessary because the port
// number is selected randomly.
func simulateServerRestart(c *cliTest, p cliTestParams, conn *sqlConn) func() {
	c.restartServer(p)
	url2, cleanup2 := sqlutils.PGUrl(c.t, c.ServingAddr(), c.t.Name(), url.User(security.RootUser))
	conn.url = url2.String()
	return cleanup2
}

func TestRunQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	setCLIDefaultsForTests()

	var b bytes.Buffer

	// Non-query statement.
	if err := runQueryAndFormatResults(conn, &b, makeQuery(`SET DATABASE=system`)); err != nil {
		t.Fatal(err)
	}

	expected := `
SET
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Use system database for sample query/output as they are fairly fixed.
	cols, rows, err := runQuery(conn, makeQuery(`SHOW COLUMNS FROM system.public.namespace`), false)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := []string{"Field", "Type", "Null", "Default", "Indices"}
	if !reflect.DeepEqual(expectedCols, cols) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedCols, cols)
	}

	expectedRows := [][]string{
		{`parentID`, `INT`, `false`, `NULL`, `{\"primary\"}`},
		{`name`, `STRING`, `false`, `NULL`, `{\"primary\"}`},
		{`id`, `INT`, `true`, `NULL`, `{}`},
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedRows, rows)
	}

	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SHOW COLUMNS FROM system.public.namespace`)); err != nil {
		t.Fatal(err)
	}

	expected = `
+----------+--------+-------+---------+-------------+
|  Field   |  Type  | Null  | Default |   Indices   |
+----------+--------+-------+---------+-------------+
| parentID | INT    | false | NULL    | {"primary"} |
| name     | STRING | false | NULL    | {"primary"} |
| id       | INT    | true  | NULL    | {}          |
+----------+--------+-------+---------+-------------+
(3 rows)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test placeholders.
	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SELECT * FROM system.public.namespace WHERE name=$1`, "descriptor")); err != nil {
		t.Fatal(err)
	}

	expected = `
+----------+------------+----+
| parentID |    name    | id |
+----------+------------+----+
|        1 | descriptor |  3 |
+----------+------------+----+
(1 row)
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test multiple results.
	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SELECT 1; SELECT 2, 3; SELECT 'hello'`)); err != nil {
		t.Fatal(err)
	}

	expected = `
+---+
| 1 |
+---+
| 1 |
+---+
(1 row)
+---+---+
| 2 | 3 |
+---+---+
| 2 | 3 |
+---+---+
(1 row)
+---------+
| 'hello' |
+---------+
| hello   |
+---------+
(1 row)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()
}
