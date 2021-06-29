// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const rowsPerInsert = 100

// TestDB is the name of the database created for test tables.
const TestDB = "test"

// GenRowFn is a function that takes a (1-based) row index and returns a row of
// Datums that will be converted to strings to form part of an INSERT statement.
type GenRowFn func(row int) []tree.Datum

// genValues writes a string of generated values "(a,b,c),(d,e,f)...".
func genValues(w io.Writer, firstRow, lastRow int, fn GenRowFn, shouldPrint bool) {
	for rowIdx := firstRow; rowIdx <= lastRow; rowIdx++ {
		if rowIdx > firstRow {
			fmt.Fprint(w, ",")
		}
		row := fn(rowIdx)
		if shouldPrint {
			var strs []string
			for _, v := range row {
				strs = append(strs, v.String())
			}
			fmt.Printf("(%v),\n", strings.Join(strs, ","))
		}
		fmt.Fprintf(w, "(%s", tree.Serialize(row[0]))
		for _, v := range row[1:] {
			fmt.Fprintf(w, ",%s", tree.Serialize(v))
		}
		fmt.Fprint(w, ")")
	}
}

// CreateTable creates a table in the "test" database with the given number of
// rows and using the given row generation function.
func CreateTable(
	tb testing.TB, sqlDB *gosql.DB, tableName, schema string, numRows int, fn GenRowFn,
) {
	CreateTableInterleaved(tb, sqlDB, tableName, schema, "" /*interleaveSchema*/, numRows, fn)
}

// CreateTableDebug is identical to debug, but allows for the added option of
// printing the table and its contents upon creation.
func CreateTableDebug(
	tb testing.TB,
	sqlDB *gosql.DB,
	tableName, schema string,
	numRows int,
	fn GenRowFn,
	shouldPrint bool,
) {
	CreateTableInterleavedDebug(tb, sqlDB, tableName, schema, "" /*interleaveSchema*/, numRows, fn, shouldPrint)
}

// CreateTableInterleaved is identical to CreateTable with the added option
// of specifying an interleave schema for interleaving the table.
func CreateTableInterleaved(
	tb testing.TB,
	sqlDB *gosql.DB,
	tableName, schema, interleaveSchema string,
	numRows int,
	fn GenRowFn,
) {
	CreateTableInterleavedDebug(tb, sqlDB, tableName, schema, interleaveSchema, numRows, fn, false /* print */)
}

// CreateTableInterleavedDebug is identical to CreateTableInterleaved with the
// option of printing the table being created.
func CreateTableInterleavedDebug(
	tb testing.TB,
	sqlDB *gosql.DB,
	tableName, schema, interleaveSchema string,
	numRows int,
	fn GenRowFn,
	shouldPrint bool,
) {
	if interleaveSchema != "" {
		interleaveSchema = fmt.Sprintf(`INTERLEAVE IN PARENT %s.%s`, TestDB, interleaveSchema)
	}

	r := MakeSQLRunner(sqlDB)
	stmt := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s;`, TestDB)
	stmt += fmt.Sprintf(`CREATE TABLE %s.%s (%s) %s;`, TestDB, tableName, schema, interleaveSchema)
	r.Exec(tb, stmt)
	if shouldPrint {
		fmt.Printf("Creating table: %s\n%s\n", tableName, schema)
	}
	for i := 1; i <= numRows; {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, `INSERT INTO %s.%s VALUES `, TestDB, tableName)
		batchEnd := i + rowsPerInsert
		if batchEnd > numRows {
			batchEnd = numRows
		}
		genValues(&buf, i, batchEnd, fn, shouldPrint)

		r.Exec(tb, buf.String())
		i = batchEnd + 1
	}
}

// CreateTestInterleavedHierarchy generates the following interleaved hierarchy
// for testing:
//   <table>		  <primary index/interleave prefix>   <nrows>
//   parent1		  (pid1)			      100
//     child1		  (pid1, cid1, cid2)		      250
//       grandchild1	  (pid1, cid1, cid2, gcid1)	      1000
//     child2		  (pid1, cid3, cid4)		      50
//   parent2		  (pid1)			      20
func CreateTestInterleavedHierarchy(t *testing.T, sqlDB *gosql.DB) {
	vMod := 42
	CreateTable(t, sqlDB, "parent1",
		"pid1 INT PRIMARY KEY, v INT",
		100,
		ToRowFn(RowIdxFn, RowModuloFn(vMod)),
	)

	CreateTableInterleaved(t, sqlDB, "child1",
		"pid1 INT, cid1 INT, cid2 INT, v INT, PRIMARY KEY (pid1, cid1, cid2)",
		"parent1 (pid1)",
		250,
		ToRowFn(
			RowModuloShiftedFn(100),
			RowIdxFn,
			RowIdxFn,
			RowModuloFn(vMod),
		),
	)

	CreateTableInterleaved(t, sqlDB, "grandchild1",
		"pid1 INT, cid1 INT, cid2 INT, gcid1 INT, v INT, PRIMARY KEY (pid1, cid1, cid2, gcid1)",
		"child1 (pid1, cid1, cid2)",
		1000,
		ToRowFn(
			RowModuloShiftedFn(250, 100),
			RowModuloShiftedFn(250),
			RowModuloShiftedFn(250),
			RowIdxFn,
			RowModuloFn(vMod),
		),
	)

	CreateTableInterleaved(t, sqlDB, "child2",
		"pid1 INT, cid3 INT, cid4 INT, v INT, PRIMARY KEY (pid1, cid3, cid4)",
		"parent1 (pid1)",
		50,
		ToRowFn(
			RowModuloShiftedFn(100),
			RowIdxFn,
			RowIdxFn,
			RowModuloFn(vMod),
		),
	)

	CreateTable(t, sqlDB, "parent2",
		"pid1 INT PRIMARY KEY, v INT",
		20,
		ToRowFn(RowIdxFn, RowModuloFn(vMod)),
	)
}

// GenValueFn is a function that takes a (1-based) row index and returns a Datum
// which will be converted to a string to form part of an INSERT statement.
type GenValueFn func(row int) tree.Datum

// RowIdxFn is a GenValueFn that returns the row number as a DInt
func RowIdxFn(row int) tree.Datum {
	return tree.NewDInt(tree.DInt(row))
}

// RowModuloFn creates a GenValueFn that returns the row number modulo a given
// value as a DInt
func RowModuloFn(modulo int) GenValueFn {
	return func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % modulo))
	}
}

// RowModuloShiftedFn creates a GenValueFn that uses the following recursive
// function definition F(row, modulo), where modulo is []int
//    F(row, [])      = row
//    F(row, modulo)  = F((row - 1) % modulo[0] + 1, modulo[1:])
// and returns the result as a DInt.
func RowModuloShiftedFn(modulo ...int) GenValueFn {
	return func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(moduloShiftedRecursive(row, modulo)))
	}
}

func moduloShiftedRecursive(row int, modulo []int) int {
	if len(modulo) == 0 {
		return row
	}
	return moduloShiftedRecursive(((row-1)%modulo[0])+1, modulo[1:])
}

// IntToEnglish returns an English (pilot style) string for the given integer,
// for example:
//   IntToEnglish(135) = "one-three-five"
func IntToEnglish(val int) string {
	if val < 0 {
		panic(val)
	}
	d := []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}

	var digits []string
	digits = append(digits, d[val%10])
	for val > 9 {
		val /= 10
		digits = append(digits, d[val%10])
	}
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return strings.Join(digits, "-")
}

// RowEnglishFn is a GenValueFn which returns an English representation of the
// row number, as a DString
func RowEnglishFn(row int) tree.Datum {
	return tree.NewDString(IntToEnglish(row))
}

// ToRowFn creates a GenRowFn that returns rows of values generated by the given
// GenValueFns (one per column).
func ToRowFn(fn ...GenValueFn) GenRowFn {
	return func(row int) []tree.Datum {
		res := make([]tree.Datum, 0, len(fn))
		for _, f := range fn {
			res = append(res, f(row))
		}
		return res
	}
}
