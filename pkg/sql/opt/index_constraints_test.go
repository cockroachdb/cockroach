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

package opt

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func BenchmarkIndexConstraints(b *testing.B) {
	testCases := []struct {
		name, varTypes, indexInfo, expr string
	}{
		{
			name:      "point-lookup",
			varTypes:  "int",
			indexInfo: "@1",
			expr:      "@1 = 1",
		},
		{
			name:      "no-constraints",
			varTypes:  "int, int",
			indexInfo: "@2",
			expr:      "@1 = 1",
		},
		{
			name:      "range",
			varTypes:  "int",
			indexInfo: "@1",
			expr:      "@1 >= 1 AND @1 <= 10",
		},
		{
			name:      "range-2d",
			varTypes:  "int, int",
			indexInfo: "@1, @2",
			expr:      "@1 >= 1 AND @1 <= 10 AND @2 >= 1 AND @2 <= 10",
		},
		{
			name:      "many-columns",
			varTypes:  "int, int, int, int, int",
			indexInfo: "@1, @2, @3, @4, @5",
			expr:      "@1 = 1 AND @2 >= 2 AND @2 <= 4 AND (@3, @4, @5) IN ((3, 4, 5), (6, 7, 8))",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			varTypes, err := testutils.ParseTypes(strings.Split(tc.varTypes, ", "))
			if err != nil {
				b.Fatal(err)
			}
			colInfos, err := parseIndexColumns(varTypes, strings.Split(tc.indexInfo, ", "))
			if err != nil {
				b.Fatal(err)
			}

			iVarHelper := tree.MakeTypesOnlyIndexedVarHelper(varTypes)

			typedExpr, err := testutils.ParseScalarExpr(tc.expr, iVarHelper.Container())
			if err != nil {
				b.Fatal(err)
			}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			e, err := BuildScalarExpr(typedExpr, &evalCtx)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var ic IndexConstraints

				ic.Init(e, colInfos, false /*isInverted */, &evalCtx)
				_, _ = ic.Spans()
				_ = ic.RemainingFilter(&iVarHelper)
			}
		})
	}
}
