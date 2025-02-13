// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec_test

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var testExecCtx = clisqlexec.Context{
	TableDisplayFormat: clisqlexec.TableDisplayTable,
}

func makeSQLConn(url string) clisqlclient.Conn {
	var sqlConnCtx clisqlclient.Context
	return sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url)
}

func runQueryAndFormatResults(
	conn clisqlclient.Conn, w io.Writer, fn clisqlclient.QueryFn,
) (err error) {
	return testExecCtx.RunQueryAndFormatResults(
		context.Background(),
		conn, w, io.Discard, io.Discard, fn)
}

func TestRunQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := cli.NewCLITest(cli.TestCLIParams{T: t})
	defer c.Cleanup()

	url, cleanup := pgurlutils.PGUrl(t, c.Server.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var b bytes.Buffer

	// Non-query statement.
	if err := runQueryAndFormatResults(conn, &b,
		clisqlclient.MakeQuery(`SET DATABASE=system`)); err != nil {
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
	cols, rows, err := testExecCtx.RunQuery(
		context.Background(),
		conn,
		clisqlclient.MakeQuery(`SHOW COLUMNS FROM system.namespace`),
		false, /* showMoreChars */
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := []string{
		"column_name",
		"data_type",
		"is_nullable",
		"column_default",
		"generation_expression",
		"indices",
		"is_hidden",
	}
	if !reflect.DeepEqual(expectedCols, cols) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedCols, cols)
	}

	expectedRows := [][]string{
		{`parentID`, `INT8`, `f`, `NULL`, ``, `{primary}`, `f`},
		{`parentSchemaID`, `INT8`, `f`, `NULL`, ``, `{primary}`, `f`},
		{`name`, `STRING`, `f`, `NULL`, ``, `{primary}`, `f`},
		{`id`, `INT8`, `t`, `NULL`, ``, `{primary}`, `f`},
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedRows, rows)
	}

	if err := runQueryAndFormatResults(conn, &b,
		clisqlclient.MakeQuery(`SHOW COLUMNS FROM system.namespace`)); err != nil {
		t.Fatal(err)
	}

	expected = `
   column_name   | data_type | is_nullable | column_default | generation_expression |  indices  | is_hidden
-----------------+-----------+-------------+----------------+-----------------------+-----------+------------
  parentID       | INT8      |      f      | NULL           |                       | {primary} |     f
  parentSchemaID | INT8      |      f      | NULL           |                       | {primary} |     f
  name           | STRING    |      f      | NULL           |                       | {primary} |     f
  id             | INT8      |      t      | NULL           |                       | {primary} |     f
(4 rows)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test placeholders.
	if err := runQueryAndFormatResults(conn, &b,
		clisqlclient.MakeQuery(`SELECT * FROM system.namespace WHERE name=$1`, "descriptor")); err != nil {
		t.Fatal(err)
	}

	expected = `
  parentID | parentSchemaID |    name    | id
-----------+----------------+------------+-----
         1 |             29 | descriptor |  3
(1 row)
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test multiple results.
	if err := runQueryAndFormatResults(conn, &b,
		clisqlclient.MakeQuery(`SELECT 1 AS "1"; SELECT 2 AS "2", 3 AS "3"; SELECT 'hello' AS "'hello'"`)); err != nil {
		t.Fatal(err)
	}

	expected = `
  1
-----
  1
(1 row)
  2 | 3
----+----
  2 | 3
(1 row)
  'hello'
-----------
  hello
(1 row)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()
}

func TestUtfName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := cli.NewCLITest(cli.TestCLIParams{T: t})
	defer c.Cleanup()

	url, cleanup := pgurlutils.PGUrl(t, c.Server.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var b bytes.Buffer

	if err := runQueryAndFormatResults(conn, &b,
		clisqlclient.MakeQuery(`CREATE DATABASE test_utf;
CREATE TABLE test_utf.żółw (id INT PRIMARY KEY, value INT);
ALTER TABLE test_utf.żółw ADD CONSTRAINT żó UNIQUE (value)`)); err != nil {
		t.Fatal(err)
	}

	b.Reset()
	if err := runQueryAndFormatResults(conn, &b,
		clisqlclient.MakeQuery(`SELECT table_name FROM [SHOW TABLES FROM test_utf];`)); err != nil {
		t.Fatal(err)
	}
	expected := `
  table_name
--------------
  żółw
(1 row)
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Errorf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	if err := runQueryAndFormatResults(conn, &b,
		clisqlclient.MakeQuery(`SELECT table_name, constraint_name FROM [SHOW CONSTRAINTS FROM test_utf.żółw] ORDER BY 1,2;`)); err != nil {
		t.Fatal(err)
	}
	expected = `
  table_name | constraint_name
-------------+------------------
  żółw       | żó
  żółw       | żółw_pkey
(2 rows)
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Errorf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()
}
