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
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

var (
	testDataGlob = flag.String("d", "testdata/[^.]*", "test data glob")
)

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
		res[i].Typ = indexVarTypes[res[i].VarIdx]
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
