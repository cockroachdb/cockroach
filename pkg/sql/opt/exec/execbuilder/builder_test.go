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

package execbuilder_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// TestBuilder runs data-driven testcases of the form
//
//   <command> [<args]...
//   <SQL statement>
//   ----
//   <expected results>
//
// See OptTester.Handle for supported commands and arguments. In addition to
// those, we support:
//
//  - exec-raw
//
//    Runs a SQL statement against the database (not through the execbuilder).
//
//  - exec
//
//    Builds a memo structure from a SQL statement, then builds an
//    execution plan and runs it, outputting the results. Supported args:
//      - rowsort: if specified, the results are sorted. Used for queries where
//        result ordering can be arbitrary.
//
//      - partialsort=(x,y,z..): if specified, the results are partially sorted,
//        preserving the relative ordering of rows that differ on the specified
//        columns (1-indexed). Used for queries which guarantee a partial order.
//        See partialSort() for more information.
//
//  - exec-explain
//
//    Builds a memo structure from a SQL statement, then builds an
//    execution plan and outputs the details of that plan.
//
//  - catalog
//
//    Prints information about a table, retrieved through the Catalog interface.
//
func TestBuild(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		ctx := context.Background()
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		_, err := sqlDB.Exec("CREATE DATABASE test; SET DATABASE = test;")
		if err != nil {
			t.Fatal(err)
		}

		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			eng := s.Executor().(exec.TestEngineFactory).NewTestEngine("test")
			defer eng.Close()

			tester := testutils.NewOptTester(eng.Catalog(), d.Input)

			var rowSort bool
			var partialSortColumns []int

			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "rowsort":
					// We will sort the resulting rows before comparing with the
					// expected result.
					rowSort = true

				case "partialsort":
					// See partialSort().
					partialSortColumns = make([]int, len(arg.Vals))
					for i, colStr := range arg.Vals {
						val, err := strconv.Atoi(colStr)
						if err != nil {
							t.Fatalf("error parsing partialSort argument: %s", err)
						}
						partialSortColumns[i] = val - 1
					}

				default:
					if err := tester.Flags.Set(arg); err != nil {
						d.Fatalf(t, "%s", err)
					}
				}
			}

			switch d.Cmd {
			case "exec-raw":
				_, err := sqlDB.Exec(d.Input)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}
				return ""

			case "exec", "exec-explain":
				eng := s.Executor().(exec.TestEngineFactory).NewTestEngine("test")
				defer eng.Close()

				var columns sqlbase.ResultColumns
				var results []tree.Datums
				if d.Cmd == "exec-explain" {
					results, err = tester.Explain(eng)
				} else {
					columns, results, err = tester.Exec(eng)
				}
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				if rowSort {
					sortRows(results, &evalCtx)
				} else if partialSortColumns != nil {
					partialSort(results, partialSortColumns, &evalCtx)
				}

				// Format the results.
				var buf bytes.Buffer
				tw := tabwriter.NewWriter(
					&buf,
					2,   /* minwidth */
					1,   /* tabwidth */
					2,   /* padding */
					' ', /* padchar */
					0,   /* flags */
				)
				if columns != nil {
					for i := range columns {
						if i > 0 {
							fmt.Fprintf(tw, "\t")
						}
						fmt.Fprintf(tw, "%s:%s", columns[i].Name, columns[i].Typ)
					}
					fmt.Fprintf(tw, "\n")
				}
				for _, r := range results {
					for j, val := range r {
						if j > 0 {
							fmt.Fprintf(tw, "\t")
						}
						if d, ok := val.(*tree.DString); ok && utf8.ValidString(string(*d)) {
							str := string(*d)
							if str == "" {
								str = "·"
							}
							// Avoid the quotes on strings.
							fmt.Fprintf(tw, "%s", str)
						} else {
							fmt.Fprintf(tw, "%s", val)
						}
					}
					fmt.Fprintf(tw, "\n")
				}
				_ = tw.Flush()
				return buf.String()

			case "catalog":
				// Create the engine in order to get access to its catalog.
				eng := s.Executor().(exec.TestEngineFactory).NewTestEngine("test")
				defer eng.Close()

				parts := strings.Split(d.Input, ".")
				name := tree.NewTableName(tree.Name(parts[0]), tree.Name(parts[1]))
				tab, err := eng.Catalog().FindTable(context.Background(), name)
				if err != nil {
					d.Fatalf(t, "Catalog: %v", err)
				}

				tp := treeprinter.New()
				opt.FormatCatalogTable(tab, tp)
				return tp.String()

			default:
				return tester.RunCommand(t, d)
			}
		})
	})
}

func sortRows(rows []tree.Datums, evalCtx *tree.EvalContext) {
	sort.Slice(rows, func(i, j int) bool {
		for k := range rows[i] {
			cmp := rows[i][k].Compare(evalCtx, rows[j][k])
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
}

// partialSort rearranges consecutive rows that have the same values on a
// certain set of columns (orderedCols).
//
// More specifically: rows are partitioned into groups of consecutive rows that
// have the same values for columns orderedCols. Inside each group, the rows are
// sorted. The relative order of any two rows that differ on orderedCols is
// preserved.
//
// This is useful when comparing results for a statement that guarantees a
// partial, but not a total order. Consider:
//
//   SELECT a, b FROM ab ORDER BY a
//
// Some possible outputs for the same data:
//   1 2        1 5        1 2
//   1 5        1 4        1 4
//   1 4   or   1 2   or   1 5
//   2 3        2 2        2 3
//   2 2        2 3        2 2
//
// After a partialSort with colStrs = {"1"} all become:
//   1 2
//   1 4
//   1 5
//   2 2
//   2 3
//
// An incorrect output like:
//   1 5                          1 2
//   1 2                          1 5
//   2 3          becomes:        2 2
//   2 2                          2 3
//   1 4                          1 4
// and it is detected as different.
func partialSort(results []tree.Datums, orderedCols []int, evalCtx *tree.EvalContext) {
	if len(results) == 0 {
		return
	}

	groupStart := 0
	for rIdx := 1; rIdx < len(results); rIdx++ {
		// See if this row belongs in the group with the previous row.
		row := results[rIdx]
		start := results[groupStart]
		differs := false
		for _, i := range orderedCols {
			if start[i].Compare(evalCtx, row[i]) != 0 {
				differs = true
				break
			}
		}
		if differs {
			// Sort the group and start a new group with just this row in it.
			sortRows(results[groupStart:rIdx], evalCtx)
			groupStart = rIdx
		}
	}
	sortRows(results[groupStart:], evalCtx)
}
