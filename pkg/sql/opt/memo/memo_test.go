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

package memo_test

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

func TestMemo(t *testing.T) {
	runDataDrivenTest(t, "testdata/memo")
}

// runDataDrivenTest runs data-driven testcases of the form
//   <command>
//   <SQL statement>
//   ----
//   <expected results>
//
// The supported commands are:
//
//  - exec-ddl
//
//    Runs a SQL DDL statement to build the test catalog. Only a small number
//    of DDL statements are supported, and those not fully.
//
//  - build
//
//    Builds an expression tree from a SQL query and outputs it without any
//    optimizations applied to it.
//
//  - opt
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the lowest cost tree.
//
//  - memo
//
//    Builds an expression tree from a SQL query, fully optimizes it using the
//    memo, and then outputs the memo containing the forest of trees.
//
func runDataDrivenTest(t *testing.T, testdataGlob string) {
	for _, path := range testutils.GetTestFiles(t, testdataGlob) {
		catalog := testutils.NewTestCatalog()
		t.Run(filepath.Base(path), func(t *testing.T) {
			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				tester := testutils.NewOptTester(catalog, d.Input)
				switch d.Cmd {
				case "exec-ddl":
					return testutils.ExecuteTestDDL(t, d.Input, catalog)

				case "build":
					ev, err := tester.OptBuild()
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return ev.String()

				case "opt":
					ev, err := tester.Optimize()
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return ev.String()

				case "memo":
					result, err := tester.Memo()
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return result

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}
