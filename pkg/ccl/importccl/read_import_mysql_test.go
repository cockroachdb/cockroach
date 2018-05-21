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
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	_ "github.com/go-sql-driver/mysql"
)

func writeMysqldumpTestdata(t *testing.T, dest string, rows []testRow) {
	cleanup := loadMysqlTestdata(t, rows)

	if err := os.MkdirAll(filepath.Dir(dest), 0777); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	writer := bufio.NewWriter(out)

	cmd := exec.Command(`mysqldump`, `-u`, `root`, `test`, `test`)
	cmd.Stdout = writer
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
	cleanup()
}

func descForTable(t *testing.T, create string) *sqlbase.TableDescriptor {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		t.Fatal(err)
	}
	stmt := parsed.(*tree.CreateTable)
	table, err := MakeSimpleTableDescriptor(context.TODO(), nil, stmt, 10, 20, 100)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func getMysqldumpTestdata(t *testing.T) ([]testRow, string) {
	testRows := getMysqlTestRows()
	dest := filepath.Join(`testdata`, `mysqldump`, `example.sql`)
	if false {
		writeMysqldumpTestdata(t, dest, testRows)
	}
	return testRows, dest
}
func TestMysqldumpReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testRows, dest := getMysqldumpTestdata(t)

	ctx := context.TODO()
	table := descForTable(t, `CREATE TABLE test (i INT PRIMARY KEY, s text, b bytea)`)

	evalCtx := &tree.EvalContext{SessionData: &sessiondata.SessionData{Location: time.UTC}}
	converter, err := newMysqldumpReader(make(chan kvBatch, 10), table, evalCtx)
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
