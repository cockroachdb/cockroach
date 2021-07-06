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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catformat"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/kr/pretty"
	mysql "vitess.io/vitess/go/vt/sqlparser"
)

func TestMysqldumpDataReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	files := getMysqldumpTestdata(t)

	ctx := context.Background()
	table := descForTable(ctx, t, `CREATE TABLE simple (i INT PRIMARY KEY, s text, b bytea)`, 100, 200, NoFKs)
	tables := map[string]*execinfrapb.ReadImportDataSpec_ImportTable{"simple": {Desc: table.TableDesc()}}
	opts := roachpb.MysqldumpOptions{}

	kvCh := make(chan row.KVBatch, 50)
	// When creating a new dump reader, we need to pass in the walltime that will be used as
	// a parameter used for generating unique rowid, random, and gen_random_uuid as default
	// expressions. Here, the parameter doesn't matter so we pass in 0.
	converter, err := newMysqldumpReader(ctx, kvCh, 0 /*walltime*/, tables, testEvalCtx, opts)

	if err != nil {
		t.Fatal(err)
	}

	var res []tree.Datums
	converter.debugRow = func(row tree.Datums) {
		res = append(res, append(tree.Datums{}, row...))
	}

	in, err := os.Open(files.simple)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	wrapped := &fileReader{Reader: in, counter: byteCounter{r: in}}

	if err := converter.readFile(ctx, wrapped, 1, 0, nil); err != nil {
		t.Fatal(err)
	}
	close(kvCh)

	if expected, actual := len(simpleTestRows), len(res); expected != actual {
		t.Fatalf("expected %d rows, got %d: %v", expected, actual, res)
	}
	for i, expected := range simpleTestRows {
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

const expectedParentID = 52

func readFile(t *testing.T, name string) string {
	body, err := ioutil.ReadFile(filepath.Join("testdata", "mysqldump", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func readMysqlCreateFrom(
	t *testing.T, path, name string, id descpb.ID, fks fkHandler,
) *descpb.TableDescriptor {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	walltime := testEvalCtx.StmtTimestamp.UnixNano()
	expectedParent := dbdesc.NewInitial(
		expectedParentID, "test", security.RootUserName(),
	)
	tbl, err := readMysqlCreateTable(context.Background(), f, testEvalCtx, nil, id, expectedParent, name, fks, map[descpb.ID]int64{}, security.RootUserName(), walltime)
	if err != nil {
		t.Fatal(err)
	}
	return tbl[len(tbl)-1].TableDesc()
}

func TestMysqldumpSchemaReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	files := getMysqldumpTestdata(t)

	simpleTable := descForTable(ctx, t, readFile(t, `simple.cockroach-schema.sql`), expectedParentID, 52, NoFKs)
	referencedSimple := descForTable(ctx, t, readFile(t, `simple.cockroach-schema.sql`), expectedParentID, 52, NoFKs)
	fks := fkHandler{
		allowed: true,
		resolver: fkResolver{
			tableNameToDesc: map[string]*tabledesc.Mutable{referencedSimple.Name: referencedSimple},
			format:          mysqlDumpFormat()},
	}

	t.Run("simple", func(t *testing.T) {
		expected := simpleTable
		got := readMysqlCreateFrom(t, files.simple, "", 51, NoFKs)
		compareTables(t, expected.TableDesc(), got)
	})

	t.Run("second", func(t *testing.T) {
		secondTable := descForTable(ctx, t, readFile(t, `second.cockroach-schema.sql`), expectedParentID, 53, fks)
		expected := secondTable
		got := readMysqlCreateFrom(t, files.second, "", 53, fks)
		compareTables(t, expected.TableDesc(), got)
	})

	t.Run("everything", func(t *testing.T) {
		expected := descForTable(ctx, t, readFile(t, `everything.cockroach-schema.sql`), expectedParentID, 53, NoFKs)
		got := readMysqlCreateFrom(t, files.everything, "", 53, NoFKs)
		compareTables(t, expected.TableDesc(), got)
	})

	t.Run("simple-in-multi", func(t *testing.T) {
		expected := simpleTable
		got := readMysqlCreateFrom(t, files.wholeDB, "simple", 51, NoFKs)
		compareTables(t, expected.TableDesc(), got)
	})

	t.Run("third-in-multi", func(t *testing.T) {
		skip := fkHandler{allowed: true, skip: true, resolver: fkResolver{
			tableNameToDesc: make(map[string]*tabledesc.Mutable),
			format:          mysqlDumpFormat(),
		}}
		expected := descForTable(ctx, t, readFile(t, `third.cockroach-schema.sql`), expectedParentID, 52, skip)
		got := readMysqlCreateFrom(t, files.wholeDB, "third", 51, skip)
		compareTables(t, expected.TableDesc(), got)
	})
}

func compareTables(t *testing.T, expected, got *descpb.TableDescriptor) {
	colNames := func(cols []descpb.ColumnDescriptor) string {
		names := make([]string, len(cols))
		for i := range cols {
			names[i] = cols[i].Name
		}
		return strings.Join(names, ", ")
	}
	idxNames := func(indexes []descpb.IndexDescriptor) string {
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
		e, g := expected.Columns[i].SQLStringNotHumanReadable(), got.Columns[i].SQLStringNotHumanReadable()
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
		ctx := context.Background()
		semaCtx := tree.MakeSemaContext()
		tableName := &descpb.AnonymousTable
		expectedDesc := tabledesc.NewBuilder(expected).BuildImmutableTable()
		gotDesc := tabledesc.NewBuilder(got).BuildImmutableTable()
		e, err := catformat.IndexForDisplay(
			ctx, expectedDesc, tableName, expectedDesc.PublicNonPrimaryIndexes()[i], "" /* partition */, "" /* interleave */, &semaCtx,
		)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		g, err := catformat.IndexForDisplay(
			ctx, gotDesc, tableName, gotDesc.PublicNonPrimaryIndexes()[i], "" /* partition */, "" /* interleave */, &semaCtx,
		)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
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
		t.Fatalf("expected\n%+v\n, got\n%+v\ndiff: %v", expected, got, pretty.Diff(expected, got))
	}
}

func TestMysqlValueToDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	date := func(s string) tree.Datum {
		d, _, err := tree.ParseDDate(nil, s)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	ts := func(s string) tree.Datum {
		d, _, err := tree.ParseDTimestamp(nil, s, time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	tests := []struct {
		raw  mysql.Expr
		typ  *types.T
		want tree.Datum
	}{
		{raw: mysql.NewStrLiteral([]byte("0000-00-00")), typ: types.Date, want: tree.DNull},
		{raw: mysql.NewStrLiteral([]byte("2010-01-01")), typ: types.Date, want: date("2010-01-01")},
		{raw: mysql.NewStrLiteral([]byte("0000-00-00 00:00:00")), typ: types.Timestamp, want: tree.DNull},
		{raw: mysql.NewStrLiteral([]byte("2010-01-01 00:00:00")), typ: types.Timestamp, want: ts("2010-01-01 00:00:00")},
	}
	evalContext := tree.NewTestingEvalContext(nil)
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v", tc.raw), func(t *testing.T) {
			got, err := mysqlValueToDatum(tc.raw, tc.typ, evalContext)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
