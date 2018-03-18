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
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func BenchmarkExprView(b *testing.B) {
	semaCtx := tree.MakeSemaContext(false /* privileged */)
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	catalog := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(b, "CREATE TABLE a (x INT PRIMARY KEY, y INT)", catalog)

	cases := []string{
		"SELECT x, y FROM a WHERE x + y * 2 - (x * y) > 0",
	}

	for testIdx, sql := range cases {
		b.Run(fmt.Sprintf("%d", testIdx), func(b *testing.B) {
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				b.Fatal(err)
			}
			o := xform.NewOptimizer(&evalCtx)
			bld := optbuilder.New(
				context.Background(), &semaCtx, &evalCtx, catalog, o.Factory(), stmt,
			)
			root, props, err := bld.Build()
			if err != nil {
				b.Fatal(err)
			}
			exprView := o.Optimize(root, props)

			stack := make([]memo.ExprView, 16)
			for i := 0; i < b.N; i++ {
				// Do a depth-first traversal of the ExprView tree. Don't use recursion
				// to minimize overhead from the benchmark code.
				stack = append(stack[:0], exprView)
				for len(stack) > 0 {
					ev := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					_ = ev.Private()
					for i, n := 0, ev.ChildCount(); i < n; i++ {
						stack = append(stack, ev.Child(i))
					}
				}
			}
		})
	}
}
