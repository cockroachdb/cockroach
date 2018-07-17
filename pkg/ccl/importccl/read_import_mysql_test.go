// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"encoding/hex"
	"io/ioutil"
	"os"
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

func descForTable(t *testing.T, create string, parent, id sqlbase.ID) *sqlbase.TableDescriptor {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.(*tree.CreateTable)
	table, err := MakeSimpleTableDescriptor(context.TODO(), nil, stmt, parent, id, NoFKs, testEvalCtx.StmtTimestamp.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if table.PrimaryIndex.Name == "primary" {
		table.PrimaryIndex.Name = "PRIMARY"
	}
	return table
}

func TestMysqldumpDataReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testRows, dest := getSimpleMysqlDumpTestdata(t)

	ctx := context.TODO()
	table := descForTable(t, `CREATE TABLE simple (i INT PRIMARY KEY, s text, b bytea)`, 10, 20)
	tables := map[string]*sqlbase.TableDescriptor{"simple": table}

	converter, err := newMysqldumpReader(make(chan kvBatch, 10), tables, testEvalCtx)
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

const expectedParent = 52

func readFile(t *testing.T, name string) string {
	body, err := ioutil.ReadFile(filepath.Join("testdata", "mysqldump", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func readMysqlCreateFrom(t *testing.T, path, name string) *sqlbase.TableDescriptor {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tbl, err := readMysqlCreateTable(f, testEvalCtx, expectedParent, name)
	if err != nil {
		t.Fatal(err)
	}
	return tbl[0]
}

func TestMysqldumpSchemaReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	simpleTable := descForTable(t, readFile(t, `simple.cockroach-schema.sql`), expectedParent, 53)

	t.Run("simple", func(t *testing.T) {
		expected := simpleTable
		_, testdata := getSimpleMysqlDumpTestdata(t)
		got := readMysqlCreateFrom(t, testdata, "")
		compareTables(t, expected, got)
	})

	t.Run("everything", func(t *testing.T) {
		expected := descForTable(t, readFile(t, `everything.cockroach-schema.sql`), expectedParent, 53)

		testdata := getEverythingMysqlDumpTestdata(t)
		got := readMysqlCreateFrom(t, testdata, "")
		compareTables(t, expected, got)
	})

	t.Run("simple-in-multi", func(t *testing.T) {
		expected := simpleTable
		testdata := getMultiTableMysqlDumpTestdata(t)
		got := readMysqlCreateFrom(t, testdata, "simple")
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
		tableName := tree.NewUnqualifiedTableName(tree.Name(expected.Name))
		e, g := expected.Indexes[i].SQLString(tableName), got.Indexes[i].SQLString(tableName)
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
