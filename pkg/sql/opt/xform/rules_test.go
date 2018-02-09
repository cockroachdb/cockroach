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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
			catalog := testutils.NewTestCatalog()

			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				stmt, err := parser.ParseOne(d.Input)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				switch d.Cmd {
				case "exec-ddl":
					if stmt.StatementType() != tree.DDL {
						d.Fatalf(t, "statement type is not DDL: %v", stmt.StatementType())
					}

					switch stmt := stmt.(type) {
					case *tree.CreateTable:
						tbl := catalog.CreateTable(stmt)
						return tbl.String()

					default:
						d.Fatalf(t, "expected CREATE TABLE statement but found: %v", stmt)
						return ""
					}

				case "opt":
					o := NewOptimizer(catalog, OptimizeAll)
					b := optbuilder.New(context.Background(), o.Factory(), stmt)
					root, props, err := b.Build()
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					exprView := o.Optimize(root, props)
					return exprView.String()

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}
