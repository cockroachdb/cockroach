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
// Author: Marc Berhault (peter@cockroachlabs.com)

package cli

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestRunQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	url, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestRunQuery")
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	// Use a buffer as the io.Writer.
	var b bytes.Buffer

	// Non-query statement.
	if err := runQueryAndFormatResults(conn, &b, makeQuery(`SET DATABASE=system`), true); err != nil {
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
	cols, rows, _, err := runQuery(conn, makeQuery(`SHOW COLUMNS FROM system.namespace`), false)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := []string{"Field", "Type", "Null", "Default"}
	if !reflect.DeepEqual(expectedCols, cols) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedCols, cols)
	}

	expectedRows := [][]string{
		{`parentID`, `INT`, `false`, `NULL`},
		{`name`, `STRING`, `false`, `NULL`},
		{`id`, `INT`, `true`, `NULL`},
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedRows, rows)
	}

	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SHOW COLUMNS FROM system.namespace`), true); err != nil {
		t.Fatal(err)
	}

	expected = `
+----------+--------+-------+---------+
|  Field   |  Type  | Null  | Default |
+----------+--------+-------+---------+
| parentID | INT    | false | NULL    |
| name     | STRING | false | NULL    |
| id       | INT    | true  | NULL    |
+----------+--------+-------+---------+
(3 rows)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test placeholders.
	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SELECT * FROM system.namespace WHERE name=$1`, "descriptor"), true); err != nil {
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
		makeQuery(`SELECT 1; SELECT 2, 3; SELECT 'hello'`), true); err != nil {
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
