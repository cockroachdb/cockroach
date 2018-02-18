// Copyright 2017 The Cockroach Authors.
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

package idxconstraint

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var (
	testDataGlob = flag.String("d", "testdata/[^.]*", "test data glob")
)

// The test files support only one command:
//
//   - index-constraints [arg | arg=val | arg=(val1,val2, ...)]...
//
//   Takes a scalar expression, builds a memo for it, and computes index
//   constraints. Arguments:
//
//     - vars=(<type>, ...)
//
//       Sets the types for the index vars in the expression.
//
//     - index=(@<index> [ascending|asc|descending|desc] [not null], ...)
//
//       Information for the index (used by index-constraints). Each column of the
//       index refers to an index var.
//
//     - inverted-index=@<index>
//
//       Information about an inverted index (used by index-constraints). The
//       one column of the inverted index refers to an index var. Only one of
//       "index" and "inverted-index" should be used.
//
//     - nonormalize
//
//       Disable the optimizer normalization rules.
//
//     - semtree-normalize
//
//       Run TypedExpr normalization before building the memo.
//
func TestIndexConstraints(t *testing.T) {
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
			catalog := testutils.NewTestCatalog()

			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				var varTypes []types.T
				var colInfos []IndexColumnInfo
				var iVarHelper tree.IndexedVarHelper
				var invertedIndex bool
				var normalizeTypedExpr bool
				normalize := true

				st := cluster.MakeTestingClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)

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

					case "index", "inverted-index":
						if varTypes == nil {
							d.Fatalf(t, "vars must precede index")
						}
						var err error
						colInfos, err = parseIndexColumns(varTypes, vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}
						if key == "inverted-index" {
							if len(colInfos) > 1 {
								d.Fatalf(t, "inverted index must be on a single column")
							}
							invertedIndex = true
						}

					case "nonormalize":
						normalize = false

					case "semtree-normalize":
						normalizeTypedExpr = true

					default:
						d.Fatalf(t, "unknown argument: %s", key)
					}
				}

				switch d.Cmd {
				case "index-constraints":
					typedExpr, err := testutils.ParseScalarExpr(d.Input, iVarHelper.Container())
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					if normalizeTypedExpr {
						typedExpr, err = evalCtx.NormalizeExpr(typedExpr)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}
					}

					varNames := make([]string, len(varTypes))
					for i := range varNames {
						varNames[i] = fmt.Sprintf("@%d", i+1)
					}
					steps := xform.OptimizeAll
					if !normalize {
						steps = xform.OptimizeNone
					}
					o := xform.NewOptimizer(catalog, steps)
					b := optbuilder.NewScalar(ctx, o.Factory(), varNames, varTypes)
					b.AllowUnsupportedExpr = true
					group, err := b.Build(typedExpr)
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
					ev := o.Optimize(group, &opt.PhysicalProps{})

					var ic IndexConstraints
					ic.Init(ev, colInfos, invertedIndex, &evalCtx)
					spans, ok := ic.Spans()

					var buf bytes.Buffer
					if !ok {
						spans = LogicalSpans{MakeFullSpan()}
					}
					for _, sp := range spans {
						fmt.Fprintf(&buf, "%s\n", sp)
					}
					//remainingFilter := ic.RemainingFilter(&iVarHelper)
					//if remainingFilter != nil {
					//	fmt.Fprintf(&buf, "Remaining filter: %s\n", remainingFilter)
					//}
					return buf.String()

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}

//func BenchmarkIndexConstraints(b *testing.B) {
//	testCases := []struct {
//		name, varTypes, indexInfo, expr string
//	}{
//		{
//			name:      "point-lookup",
//			varTypes:  "int",
//			indexInfo: "@1",
//			expr:      "@1 = 1",
//		},
//		{
//			name:      "no-constraints",
//			varTypes:  "int, int",
//			indexInfo: "@2",
//			expr:      "@1 = 1",
//		},
//		{
//			name:      "range",
//			varTypes:  "int",
//			indexInfo: "@1",
//			expr:      "@1 >= 1 AND @1 <= 10",
//		},
//		{
//			name:      "range-2d",
//			varTypes:  "int, int",
//			indexInfo: "@1, @2",
//			expr:      "@1 >= 1 AND @1 <= 10 AND @2 >= 1 AND @2 <= 10",
//		},
//		{
//			name:      "many-columns",
//			varTypes:  "int, int, int, int, int",
//			indexInfo: "@1, @2, @3, @4, @5",
//			expr:      "@1 = 1 AND @2 >= 2 AND @2 <= 4 AND (@3, @4, @5) IN ((3, 4, 5), (6, 7, 8))",
//		},
//	}
//
//	for _, tc := range testCases {
//		b.Run(tc.name, func(b *testing.B) {
//			varTypes, err := testutils.ParseTypes(strings.Split(tc.varTypes, ", "))
//			if err != nil {
//				b.Fatal(err)
//			}
//			colInfos, err := parseIndexColumns(varTypes, strings.Split(tc.indexInfo, ", "))
//			if err != nil {
//				b.Fatal(err)
//			}
//
//			iVarHelper := tree.MakeTypesOnlyIndexedVarHelper(varTypes)
//
//			typedExpr, err := testutils.ParseScalarExpr(tc.expr, &iVarHelper)
//			if err != nil {
//				b.Fatal(err)
//			}
//
//			evalCtx := tree.MakeTestingEvalContext()
//			e, err := BuildScalarExpr(typedExpr, &evalCtx)
//			if err != nil {
//				b.Fatal(err)
//			}
//
//			b.ResetTimer()
//			for i := 0; i < b.N; i++ {
//				var ic IndexConstraints
//
//				ic.Init(e, colInfos, false /*isInverted */, &evalCtx)
//				_, _ = ic.Spans()
//				// _ = ic.RemainingFilter(&iVarHelper)
//			}
//		})
//	}
//}

// parseIndexColumns parses descriptions of index columns; each
// string corresponds to an index column and is of the form:
//   <type> [ascending|descending]
func parseIndexColumns(indexVarTypes []types.T, colStrs []string) ([]IndexColumnInfo, error) {
	res := make([]IndexColumnInfo, len(colStrs))
	for i := range colStrs {
		fields := strings.Fields(colStrs[i])
		if fields[0][0] != '@' {
			return nil, fmt.Errorf("index column must start with @<index>")
		}
		idx, err := strconv.Atoi(fields[0][1:])
		if err != nil {
			return nil, err
		}
		if idx < 1 || idx > len(indexVarTypes) {
			return nil, fmt.Errorf("invalid index var @%d", idx)
		}
		res[i].VarIdx = opt.ColumnIndex(idx)
		res[i].Typ = indexVarTypes[res[i].VarIdx-1]
		res[i].Direction = encoding.Ascending
		res[i].Nullable = true
		fields = fields[1:]
		for len(fields) > 0 {
			switch strings.ToLower(fields[0]) {
			case "ascending", "asc":
				// ascending is the default.
				fields = fields[1:]
			case "descending", "desc":
				res[i].Direction = encoding.Descending
				fields = fields[1:]

			case "not":
				if len(fields) < 2 || strings.ToLower(fields[1]) != "null" {
					return nil, fmt.Errorf("unknown column attribute %s", fields)
				}
				res[i].Nullable = false
				fields = fields[2:]
			default:
				return nil, fmt.Errorf("unknown column attribute %s", fields)
			}
		}
	}
	return res, nil
}
