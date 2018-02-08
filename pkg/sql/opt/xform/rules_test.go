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

package xform

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/build"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

// Rules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/bool"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/comp"
//   ...
func TestRules(t *testing.T) {
	paths, err := filepath.Glob("testdata/rules/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				// Only opt command supported.
				if d.Cmd != "opt" {
					t.FailNow()
				}

				stmt, err := parser.ParseOne(d.Input)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				o := NewOptimizer(createRulesCatalog(), OptimizeAll)
				b := build.NewBuilder(context.Background(), o.Factory(), stmt)
				root, props, err := b.Build()
				if err != nil {
					d.Fatalf(t, "%v", err)
				}
				exprView := o.Optimize(root, props)
				return exprView.String()
			})
		})
	}
}

func createRulesCatalog() *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()

	// CREATE TABLE a (k INT PRIMARY KEY, i INT, f FLOAT, s STRING, j JSON)
	a := &testutils.TestTable{Name: "a"}
	k := &testutils.TestColumn{Name: "k", Type: types.Int}
	i := &testutils.TestColumn{Name: "i", Type: types.Int, Nullable: true}
	f := &testutils.TestColumn{Name: "f", Type: types.Float, Nullable: true}
	s := &testutils.TestColumn{Name: "s", Type: types.String, Nullable: true}
	j := &testutils.TestColumn{Name: "j", Type: types.JSON, Nullable: true}
	a.Columns = append(a.Columns, k, i, f, s, j)
	cat.AddTable(a)

	return cat
}
