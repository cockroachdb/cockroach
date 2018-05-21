// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var testEvalCtx = &tree.EvalContext{
	SessionData:   &sessiondata.SessionData{Location: time.UTC},
	StmtTimestamp: timeutil.Unix(100000000, 0),
}

func writeMysqldumpTestdata(t *testing.T, dest string, args ...string) {
	if err := os.MkdirAll(filepath.Dir(dest), 0777); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	writer := bufio.NewWriter(out)

	baseArgs := []string{`-u`, `root`, `cockroachtestdata`}
	args = append(baseArgs, args...)
	cmd := exec.Command(`mysqldump`, args...)
	cmd.Stdout = writer
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := out.Sync(); err != nil {
		t.Fatal(err)
	}
}

func descForTable(t *testing.T, create string, parent, id sqlbase.ID) *sqlbase.TableDescriptor {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.(*tree.CreateTable)
	table, err := MakeSimpleTableDescriptor(context.TODO(), nil, stmt, parent, id, testEvalCtx.StmtTimestamp.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if table.PrimaryIndex.Name == "primary" {
		table.PrimaryIndex.Name = "PRIMARY"
	}
	return table
}

func getMysqldumpTestdata(t *testing.T) ([]testRow, string) {
	testRows := getMysqlTestRows()
	dest := filepath.Join(`testdata`, `mysqldump`, `example.sql`)
	if false {
		cleanup := loadMysqlTestdata(t, testRows)
		defer cleanup()
		writeMysqldumpTestdata(t, dest, `test`)
	}
	return testRows, dest
}

func TestMysqldumpDataReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testRows, dest := getMysqldumpTestdata(t)

	ctx := context.TODO()
	table := descForTable(t, `CREATE TABLE test (i INT PRIMARY KEY, s text, b bytea)`, 10, 20)

	converter, err := newMysqldumpReader(make(chan kvBatch, 10), table, testEvalCtx)
	if err != nil {
		t.Fatal(err)
	}

	var res []tree.Datums
	converter.debugRow = func(row tree.Datums) {
		res = append(res, append(tree.Datums{}, row...))
	}

	in, err := os.Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	noop := func(_ bool) error { return nil }

	if err := converter.readFile(ctx, in, 1, "", noop); err != nil {
		t.Fatal(err)
	}
	converter.inputFinished(ctx)

	if expected, actual := len(testRows), len(res); expected != actual {
		t.Fatalf("expected %d rows, got %d: %v", expected, actual, res)
	}
	for i, expected := range testRows {
		row := res[i]
		if actual := *row[0].(*tree.DInt); expected.i != int(actual) {
			t.Fatalf("row %d: expected i = %d, got %d", i, expected.i, actual)
		}
		if expected.s != injectNull {
			if actual := *row[1].(*tree.DString); expected.s != string(actual) {
				t.Fatalf("row %d: expected s = %q, got %q", i, expected.i, actual)
			}
		} else if row[1] != tree.DNull {
			t.Fatalf("row %d: expected b = NULL, got %T: %v", i, row[1], row[1])
		}
		if expected.b != nil {
			if actual := []byte(*row[2].(*tree.DBytes)); !bytes.Equal(expected.b, actual) {
				t.Fatalf("row %d: expected b = %v, got %v", i, hex.EncodeToString(expected.b), hex.EncodeToString(actual))
			}
		} else if row[2] != tree.DNull {
			t.Fatalf("row %d: expected b = NULL, got %T: %v", i, row[2], row[2])
		}
	}
}

func readMysqlCreateFrom(t *testing.T, path, name string) *sqlbase.TableDescriptor {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := readMysqlCreateTable(f, testEvalCtx, name)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func TestMysqldumpSchemaReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	simple := filepath.Join(`testdata`, `mysqldump`, `simple-schema.sql`)
	everything := filepath.Join(`testdata`, `mysqldump`, `everything-schema.sql`)
	multi := filepath.Join(`testdata`, `mysqldump`, `multi-schema.sql`)

	if true {
		db, err := gosql.Open("mysql", "root@/cockroachtestdata")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.Exec(`DROP TABLE IF EXISTS simple`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`CREATE TABLE simple (i INT PRIMARY KEY, s text, b BINARY(200))`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`DROP TABLE IF EXISTS everything`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`CREATE TABLE everything (
			i			INT PRIMARY KEY,

			c 		CHAR(10),
			s			VARCHAR(100),
			tx		TEXT,

			bin		BINARY(100),
			vbin	VARBINARY(100),
			bl 		BLOB,

			dt 		DATETIME,
			d 		DATE,
			ts 		TIMESTAMP,
			t			TIME,
			-- TODO(dt): fix parser: for YEAR's length option
			-- y			YEAR,

			de 		DECIMAL,
			nu		NUMERIC,
			d53		DECIMAL(5,3),

			iw		INT(5),
			iz		INT ZEROFILL,
			ti 		TINYINT,
			si 		SMALLINT,
			mi 		MEDIUMINT,
			bi 		BIGINT,

			fl 		FLOAT,
			rl		REAL,
			db 		DOUBLE,

			f17		FLOAT(17),
			f47		FLOAT(47),
			f75		FLOAT(7, 5)
		)`); err != nil {
			t.Fatal(err)
		}

		writeMysqldumpTestdata(t, simple, `simple`)
		writeMysqldumpTestdata(t, everything, `everything`)
		writeMysqldumpTestdata(t, multi)

		if _, err := db.Exec(`DROP TABLE simple`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DROP TABLE everything`); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("simple", func(t *testing.T) {
		expected := descForTable(t, `CREATE TABLE simple (i INT PRIMARY KEY, s text, b bytea)`, 52, 53)
		got := readMysqlCreateFrom(t, simple, "")

		compareTables(t, expected, got)
	})

	t.Run("everything", func(t *testing.T) {
		expected := descForTable(t, `
			CREATE TABLE everything (
				i			INT PRIMARY KEY,

				c 		CHAR(10),
				s			VARCHAR(100),
				tx		TEXT,

				bin		BYTEA,
				vbin	BYTEA,
				bl 		BLOB,

				dt 		TIMESTAMPTZ,
				d 		DATE,
				ts 		TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
				t			TIME,
				-- TODO(dt): Fix year parsing.
				-- y			SMALLINT,

				de 		DECIMAL(10, 0),
				nu		NUMERIC(10, 0),
				d53		DECIMAL(5,3),

				iw		INT,
				iz		INT,
				ti 		SMALLINT,
				si 		SMALLINT,
				mi 		INT,
				bi 		BIGINT,

				fl 		FLOAT4,
				rl		DOUBLE PRECISION,
				db 		DOUBLE PRECISION,

				f17		FLOAT4,
				f47		DOUBLE PRECISION,
				f75		FLOAT4
			)`, 52, 53)

		got := readMysqlCreateFrom(t, everything, "")
		compareTables(t, expected, got)
	})
}

func compareTables(t *testing.T, expected, got *sqlbase.TableDescriptor) {
	colNames := func(cols []sqlbase.ColumnDescriptor) string {
		names := make([]string, len(cols))
		for i := range cols {
			names[i] = cols[i].Name
		}
		return strings.Join(names, ", ")
	}
	idxNames := func(indexes []sqlbase.IndexDescriptor) string {
		names := make([]string, len(indexes))
		for i := range indexes {
			names[i] = indexes[i].Name
		}
		return strings.Join(names, ", ")
	}

	// Attempt to verify the pieces individually, and return more helpful errors
	// if an individual column or index does not match. If the pieces look right
	// when compared individually, move on to compare the whole table desc as
	// rendered to a string via `%+v`, as a more comprehensive check.

	if expectedCols, gotCols := expected.Columns, got.Columns; len(gotCols) != len(expectedCols) {
		t.Fatalf("expected columns (%d):\n%v\ngot columns (%d):\n%v\n",
			len(expectedCols), colNames(expectedCols), len(gotCols), colNames(gotCols),
		)
	}
	for i := range expected.Columns {
		e, g := expected.Columns[i].SQLString(), got.Columns[i].SQLString()
		if e != g {
			t.Fatalf("column %d (%q): expected\n%s\ngot\n%s\n", i, expected.Columns[i].Name, e, g)
		}
	}

	if expectedIdx, gotIdx := expected.Indexes, got.Indexes; len(expectedIdx) != len(gotIdx) {
		t.Fatalf("expected indexes (%d):\n%v\ngot indexes (%d):\n%v\n",
			len(expectedIdx), idxNames(expectedIdx), len(gotIdx), idxNames(gotIdx),
		)
	}
	for i := range expected.Indexes {
		e, g := expected.Indexes[i].SQLString(expected.Name), got.Indexes[i].SQLString(expected.Name)
		if e != g {
			t.Fatalf("index %d: expected\n%s\ngot\n%s\n", i, e, g)
		}
	}

	// Our attempts to check parts individually (and return readable errors if
	// they didn't match) found nothing.
	expectedBytes, err := protoutil.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}

	gotBytes, err := protoutil.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expectedBytes, gotBytes) {
		t.Fatalf("expected\n%+v\n, got\n%+v\n", expected, got)
	}
}
