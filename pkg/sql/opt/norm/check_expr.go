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

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// checkExpr does sanity checking on an Expr. This code is called from
// onConstruct in testrace builds (which gives us test/CI coverage but elides
// this code in regular builds).
func (f *Factory) checkExpr(ev memo.ExprView) {
	// Check logical properties.
	ev.Logical().Verify()

	switch ev.Operator() {
	case opt.ProjectionsOp:
		// Check that we aren't passing through columns in projection expressions.
		n := ev.ChildCount()
		def := ev.Private().(*memo.ProjectionsOpDef)
		colList := def.SynthesizedCols
		if len(colList) != n {
			panic(fmt.Sprintf("%d projections but %d columns", n, len(colList)))
		}
		for i := 0; i < n; i++ {
			if child := ev.Child(i); child.Operator() == opt.VariableOp {
				if child.Private().(opt.ColumnID) == colList[i] {
					panic(fmt.Sprintf("projection passes through column %d", colList[i]))
				}
			}
		}

	case opt.AggregationsOp:
		// Check that we don't have any bare variables as aggregations.
		n := ev.ChildCount()
		colList := ev.Private().(opt.ColList)
		if len(colList) != n {
			panic(fmt.Sprintf("%d aggregations but %d columns", n, len(colList)))
		}
		for i := 0; i < n; i++ {
			if child := ev.Child(i); child.Operator() == opt.VariableOp {
				if ev.Operator() == opt.AggregationsOp {
					panic("aggregation contains bare variable")
				}
			}
		}

	case opt.LimitOp, opt.OffsetOp, opt.RowNumberOp, opt.GroupByOp, opt.ScalarGroupByOp:
		var ordering *props.OrderingChoice
		switch ev.Operator() {
		case opt.LimitOp, opt.OffsetOp:
			ordering = ev.Private().(*props.OrderingChoice)
		case opt.RowNumberOp:
			ordering = &ev.Private().(*memo.RowNumberDef).Ordering
		case opt.GroupByOp, opt.ScalarGroupByOp:
			ordering = &ev.Private().(*memo.GroupByDef).Ordering
		}
		if outCols := ev.Child(0).Logical().Relational.OutputCols; !ordering.SubsetOfCols(outCols) {
			panic(fmt.Sprintf("invalid ordering %v (op: %s, outcols: %v)", ordering, ev.Operator(), outCols))
		}
	}
}
