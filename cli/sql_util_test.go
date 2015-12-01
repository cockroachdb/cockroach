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
// Author: Marc Berhault (peter@cockroachlabs.com)

package cli

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func makeTestDBClient(t *testing.T, s *server.TestServer) *sql.DB {
	db, err := sql.Open("cockroach", fmt.Sprintf("https://%s@%s?certs=%s",
		security.RootUser,
		s.ServingAddr(),
		security.EmbeddedCertsDir))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestRunQuery(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(nil)
	db := makeTestDBClient(t, s)

	defer db.Close()
	defer s.Stop()

	// Use a buffer as the io.Writer.
	var b bytes.Buffer

	// Non-query statement.
	if err := runPrettyQuery(db, &b, `SET DATABASE=system`); err != nil {
		t.Fatal(err)
	}

	expected := `
OK
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Use system database for sample query/output as they are fairly fixed.
	cols, rows, err := runQuery(db, `SHOW COLUMNS FROM system.namespace`)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := []string{"Field", "Type", "Null", "Default"}
	if !reflect.DeepEqual(expectedCols, cols) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedCols, cols)
	}

	expectedRows := [][]string{
		{`parentID`, `INT`, `true`, `NULL`},
		{`name`, `STRING`, `true`, `NULL`},
		{`id`, `INT`, `true`, `NULL`},
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedRows, rows)
	}

	if err := runPrettyQuery(db, &b, `SHOW COLUMNS FROM system.namespace`); err != nil {
		t.Fatal(err)
	}

	expected = `
+----------+--------+------+---------+
|  Field   |  Type  | Null | Default |
+----------+--------+------+---------+
| parentID | INT    | true | NULL    |
| name     | STRING | true | NULL    |
| id       | INT    | true | NULL    |
+----------+--------+------+---------+
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test placeholders.
	if err := runPrettyQuery(db, &b, `SELECT * FROM system.namespace WHERE name=$1`, "descriptor"); err != nil {
		t.Fatal(err)
	}

	expected = `
+----------+------------+----+
| parentID |    name    | id |
+----------+------------+----+
|        1 | descriptor |  3 |
+----------+------------+----+
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test custom formatting.
	newFormat := func(val interface{}) string {
		return fmt.Sprintf("--> %#v <--", val)
	}

	if err := runPrettyQueryWithFormat(db, &b, fmtMap{"name": newFormat},
		`SELECT * FROM system.namespace WHERE name=$1`, "descriptor"); err != nil {
		t.Fatal(err)
	}

	expected = `
+----------+----------------------+----+
| parentID |         name         | id |
+----------+----------------------+----+
|        1 | --> "descriptor" <-- |  3 |
+----------+----------------------+----+
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()
}
