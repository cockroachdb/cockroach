// Copyright 2018 The Cockroach Authors.
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

package build

// This file is home to TestBuilder, which is similar to the logic tests, except it
// is used for optimizer builder-specific testcases.
//
// Each testfile contains testcases of the form
//   <command>
//   <SQL statement or expression>
//   ----
//   <expected results>
//
// The supported commands are:
//
//  - build
//
//    Builds a memo structure from a SQL query and outputs a representation
//    of the "expression view" of the memo structure.
//
//  - build-scalar
//
//    Builds a memo structure from a SQL scalar expression and outputs a
//    representation of the "expression view" of the memo structure.
//

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
)

var (
	testDataGlob = flag.String("d", "testdata/[^.]*", "test data glob")
)

func TestBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	paths, err := filepath.Glob(*testDataGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found matching: %s", *testDataGlob)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			ctx := context.Background()
			catalog := createBuilderCatalog()

			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				var varTypes []types.T
				var iVarHelper tree.IndexedVarHelper

				for _, arg := range d.CmdArgs {
					key := arg
					val := ""
					if pos := strings.Index(key, "="); pos >= 0 {
						key = arg[:pos]
						val = arg[pos+1:]
					}
					if len(val) > 2 && val[0] == '(' && val[len(val)-1] == ')' {
						val = val[1 : len(val)-1]
					}
					vals := strings.Split(val, ",")
					switch key {
					case "vars":
						varTypes, err = testutils.ParseTypes(vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}

						iVarHelper = tree.MakeTypesOnlyIndexedVarHelper(varTypes)

					default:
						d.Fatalf(t, "unknown argument: %s", key)
					}
				}

				switch d.Cmd {
				case "build":
					stmt, err := parser.ParseOne(d.Input)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					o := xform.NewOptimizer(catalog, 0 /* maxSteps */)
					b := NewBuilder(ctx, o.Factory(), stmt)
					root, props, err := b.Build()
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
					exprView := o.Optimize(root, props)
					return exprView.String()

				case "build-scalar":
					typedExpr, err := testutils.ParseScalarExpr(d.Input, &iVarHelper)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					o := xform.NewOptimizer(catalog, 0 /* maxSteps */)
					b := newScalarBuilder(o.Factory(), &iVarHelper)

					group, err := buildScalar(b, typedExpr)
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
					exprView := o.Optimize(group, &opt.PhysicalProps{})
					return exprView.String()

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}

// newScalarBuilder constructs a Builder to be used for building scalar
// expressions.
func newScalarBuilder(factory opt.Factory, ivh *tree.IndexedVarHelper) *Builder {
	b := &Builder{factory: factory, colMap: make([]columnProps, 1)}

	b.semaCtx.IVarHelper = ivh
	b.semaCtx.Placeholders = tree.MakePlaceholderInfo()

	return b
}

// buildScalar is a wrapper for Builder.buildScalar which catches panics and
// converts those containing builderErrors back to errors.
func buildScalar(b *Builder, typedExpr tree.TypedExpr) (group opt.GroupID, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(builderError); ok {
				err = r.(builderError)
			} else {
				panic(r)
			}
		}
	}()

	group = b.buildScalar(typedExpr, &scope{builder: b})
	return group, err
}

func createBuilderCatalog() *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()

	// CREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)
	kv := &testutils.TestTable{Name: "kv"}
	k := &testutils.TestColumn{Name: "k", Type: types.Int}
	v := &testutils.TestColumn{Name: "v", Type: types.Int, Nullable: true}
	w := &testutils.TestColumn{Name: "w", Type: types.Int, Nullable: true}
	s := &testutils.TestColumn{Name: "s", Type: types.String, Nullable: true}
	kv.Columns = append(kv.Columns, k, v, w, s)
	cat.AddTable(kv)

	// CREATE TABLE abc (a CHAR PRIMARY KEY, b FLOAT, c BOOLEAN, d DECIMAL)
	abc := &testutils.TestTable{Name: "abc"}
	a := &testutils.TestColumn{Name: "a", Type: types.String}
	b := &testutils.TestColumn{Name: "b", Type: types.Float, Nullable: true}
	c := &testutils.TestColumn{Name: "c", Type: types.Bool, Nullable: true}
	d := &testutils.TestColumn{Name: "d", Type: types.Decimal, Nullable: true}
	abc.Columns = append(abc.Columns, a, b, c, d)
	cat.AddTable(abc)

	// CREATE TABLE intervals (a INTERVAL PRIMARY KEY)
	intervals := &testutils.TestTable{Name: "intervals"}
	a = &testutils.TestColumn{Name: "a", Type: types.Interval}
	intervals.Columns = append(intervals.Columns, a)
	cat.AddTable(intervals)

	// CREATE TABLE xyz (x INT PRIMARY KEY, y INT, z FLOAT, INDEX xy (x, y),
	//   INDEX zyx (z, y, x), FAMILY (x), FAMILY (y), FAMILY (z))
	xyz := &testutils.TestTable{Name: "xyz"}
	x := &testutils.TestColumn{Name: "x", Type: types.Int}
	y := &testutils.TestColumn{Name: "y", Type: types.Int, Nullable: true}
	z := &testutils.TestColumn{Name: "z", Type: types.Float, Nullable: true}
	xyz.Columns = append(xyz.Columns, x, y, z)
	cat.AddTable(xyz)

	// CREATE TABLE bools (b BOOL)
	bools := &testutils.TestTable{Name: "bools"}
	b = &testutils.TestColumn{Name: "b", Type: types.Bool, Nullable: true}
	rowid := &testutils.TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
	bools.Columns = append(bools.Columns, b, rowid)
	cat.AddTable(bools)

	// CREATE TABLE xor_bytes (a bytes, b int, c int)
	xorBytes := &testutils.TestTable{Name: "xor_bytes"}
	a = &testutils.TestColumn{Name: "a", Type: types.Bytes, Nullable: true}
	b = &testutils.TestColumn{Name: "b", Type: types.Int, Nullable: true}
	c = &testutils.TestColumn{Name: "c", Type: types.Int, Nullable: true}
	rowid = &testutils.TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
	xorBytes.Columns = append(xorBytes.Columns, a, b, c, rowid)
	cat.AddTable(xorBytes)

	// CREATE TABLE ab (a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b))
	ab := &testutils.TestTable{Name: "ab"}
	a = &testutils.TestColumn{Name: "a", Type: types.Int}
	b = &testutils.TestColumn{Name: "b", Type: types.Int, Nullable: true}
	ab.Columns = append(ab.Columns, a, b)
	cat.AddTable(ab)

	// CREATE TABLE xy (x STRING, y STRING)
	xy := &testutils.TestTable{Name: "xy"}
	x = &testutils.TestColumn{Name: "x", Type: types.String, Nullable: true}
	y = &testutils.TestColumn{Name: "y", Type: types.String, Nullable: true}
	rowid = &testutils.TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
	xy.Columns = append(xy.Columns, x, y, rowid)
	cat.AddTable(xy)

	// CREATE TABLE a (x INT PRIMARY KEY, y FLOAT)
	tabA := &testutils.TestTable{Name: "a"}
	x = &testutils.TestColumn{Name: "x", Type: types.Int}
	y = &testutils.TestColumn{Name: "y", Type: types.Float, Nullable: true}
	tabA.Columns = append(tabA.Columns, x, y)
	cat.AddTable(tabA)

	// CREATE TABLE b (x INT, y FLOAT)
	tabB := &testutils.TestTable{Name: "b"}
	x = &testutils.TestColumn{Name: "x", Type: types.Int, Nullable: true}
	y = &testutils.TestColumn{Name: "y", Type: types.Float, Nullable: true}
	rowid = &testutils.TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
	tabB.Columns = append(tabB.Columns, x, y, rowid)
	cat.AddTable(tabB)

	// CREATE TABLE c (x INT, y FLOAT, z VARCHAR, CONSTRAINT fk_x_ref_a FOREIGN KEY (x) REFERENCES a (x))
	tabC := &testutils.TestTable{Name: "c"}
	x = &testutils.TestColumn{Name: "x", Type: types.Int, Nullable: true}
	y = &testutils.TestColumn{Name: "y", Type: types.Float, Nullable: true}
	z = &testutils.TestColumn{Name: "z", Type: types.String, Nullable: true}
	rowid = &testutils.TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
	tabC.Columns = append(tabC.Columns, x, y, z, rowid)
	cat.AddTable(tabC)

	return cat
}
