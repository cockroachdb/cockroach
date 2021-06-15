// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnRecover(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := TestCLIParams{T: t}
	c := NewCLITest(p)
	defer c.Cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
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

	// When the server restarts, the next Query() attempt may encounter a
	// TCP reset error before the SQL driver realizes there is a problem
	// and starts delivering ErrBadConn. We don't know the timing of
	// this however.
	testutils.SucceedsSoon(t, func() error {
		if sqlRows, err := conn.Query(`SELECT 1`, nil); !errors.Is(err, driver.ErrBadConn) {
			return fmt.Errorf("expected ErrBadConn, got %v", err)
		} else if err == nil {
			if closeErr := sqlRows.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		return nil
	})

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

	// Ditto from Query().
	testutils.SucceedsSoon(t, func() error {
		if err := conn.Exec(`SELECT 1`, nil); !errors.Is(err, driver.ErrBadConn) {
			return fmt.Errorf("expected ErrBadConn, got %v", err)
		}
		return nil
	})

	// Check that Exec recovers from a connection close by re-connecting.
	if err := conn.Exec(`SELECT 1`, nil); err != nil {
		t.Fatalf("conn.Exec(): expected no error after reconnect, got %v", err)
	}
}

// simulateServerRestart restarts the test server and reconfigures the connection
// to use the new test server's port number. This is necessary because the port
// number is selected randomly.
func simulateServerRestart(c *TestCLI, p TestCLIParams, conn *sqlConn) func() {
	c.restartServer(p)
	url2, cleanup2 := sqlutils.PGUrl(c.t, c.ServingSQLAddr(), c.t.Name(), url.User(security.RootUser))
	conn.url = url2.String()
	return cleanup2
}

func TestRunQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
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
	cols, rows, err := runQuery(conn, makeQuery(`SHOW COLUMNS FROM system.namespace`), false)
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
		{`parentID`, `INT8`, `false`, `NULL`, ``, `{primary}`, `false`},
		{`parentSchemaID`, `INT8`, `false`, `NULL`, ``, `{primary}`, `false`},
		{`name`, `STRING`, `false`, `NULL`, ``, `{primary}`, `false`},
		{`id`, `INT8`, `true`, `NULL`, ``, `{primary}`, `false`},
	}
	if !reflect.DeepEqual(expectedRows, rows) {
		t.Fatalf("expected:\n%v\ngot:\n%v", expectedRows, rows)
	}

	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SHOW COLUMNS FROM system.namespace`)); err != nil {
		t.Fatal(err)
	}

	expected = `
   column_name   | data_type | is_nullable | column_default | generation_expression |  indices  | is_hidden
-----------------+-----------+-------------+----------------+-----------------------+-----------+------------
  parentID       | INT8      |    false    | NULL           |                       | {primary} |   false
  parentSchemaID | INT8      |    false    | NULL           |                       | {primary} |   false
  name           | STRING    |    false    | NULL           |                       | {primary} |   false
  id             | INT8      |    true     | NULL           |                       | {primary} |   false
(4 rows)
`

	if a, e := b.String(), expected[1:]; a != e {
		t.Fatalf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	// Test placeholders.
	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SELECT * FROM system.namespace WHERE name=$1`, "descriptor")); err != nil {
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
		makeQuery(`SELECT 1 AS "1"; SELECT 2 AS "2", 3 AS "3"; SELECT 'hello' AS "'hello'"`)); err != nil {
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

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	setCLIDefaultsForTests()

	var b bytes.Buffer

	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`CREATE DATABASE test_utf;
CREATE TABLE test_utf.żółw (id INT PRIMARY KEY, value INT);
ALTER TABLE test_utf.żółw ADD CONSTRAINT żó UNIQUE (value)`)); err != nil {
		t.Fatal(err)
	}

	b.Reset()
	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SHOW TABLES FROM test_utf;`)); err != nil {
		t.Fatal(err)
	}
	expected := `
  schema_name | table_name | type  | owner | estimated_row_count | locality
--------------+------------+-------+-------+---------------------+-----------
  public      | żółw       | table | root  |                NULL | NULL
(1 row)
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Errorf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()

	if err := runQueryAndFormatResults(conn, &b,
		makeQuery(`SHOW CONSTRAINTS FROM test_utf.żółw;`)); err != nil {
		t.Fatal(err)
	}
	expected = `
  table_name | constraint_name | constraint_type |       details        | validated
-------------+-----------------+-----------------+----------------------+------------
  żółw       | primary         | PRIMARY KEY     | PRIMARY KEY (id ASC) |   true
  żółw       | żó              | UNIQUE          | UNIQUE (value ASC)   |   true
(2 rows)
`
	if a, e := b.String(), expected[1:]; a != e {
		t.Errorf("expected output:\n%s\ngot:\n%s", e, a)
	}
	b.Reset()
}

func TestTransactionRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := TestCLIParams{T: t}
	c := NewCLITest(p)
	defer c.Cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	var tries int
	err := conn.ExecTxn(func(conn *sqlConn) error {
		tries++
		if tries > 2 {
			return nil
		}

		// Prevent automatic server-side retries.
		rows, err := conn.Query(`SELECT now()`, nil)
		if err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}

		// Force a client-side retry.
		rows, err = conn.Query(`SELECT crdb_internal.force_retry('1h')`, nil)
		if err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
	if tries <= 2 {
		t.Fatalf("expected transaction to require at least two tries, but it only required %d", tries)
	}
}

func TestParseBool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testcases := []struct {
		input     string
		expect    bool
		expectErr bool
	}{
		{"true", true, false},
		{"on", true, false},
		{"yes", true, false},
		{"1", true, false},
		{" TrUe	", true, false},

		{"false", false, false},
		{"off", false, false},
		{"no", false, false},
		{"0", false, false},
		{"	FaLsE ", false, false},

		{"", false, true},
		{"foo", false, true},
	}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			b, err := parseBool(tc.input)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expect, b)
			}
		})
	}
}
