// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDesiredAggregateOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		expr     string
		ordering sqlbase.ColumnOrdering
	}{
		{`a`, nil},
		{`MIN(a)`, sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
		{`MAX(a)`, sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
		{`(MIN(a), MAX(a))`, nil},
		{`(MIN(a), AVG(a))`, nil},
		{`(MIN(a), COUNT(a))`, nil},
		{`(MIN(a), SUM(a))`, nil},
		// TODO(pmattis): This could/should return []int{1} (or perhaps []int{2}),
		// since both aggregations are for the same function and the same column.
		{`(MIN(a), MIN(a))`, nil},
		{`(MIN(a+1), MIN(a))`, nil},
		{`(COUNT(a), MIN(a))`, nil},
		{`(MIN(a+1))`, nil},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		evalCtx := &parser.EvalContext{}
		sel := makeSelectNode(t)
		expr := parseAndNormalizeExpr(t, evalCtx, d.expr, sel)
		group := &groupNode{planner: p}
		(extractAggregatesVisitor{n: group}).extract(expr)
		ordering := desiredAggregateOrdering(group.funcs, evalCtx)
		if !reflect.DeepEqual(d.ordering, ordering) {
			t.Fatalf("%s: expected %v, but found %v", d.expr, d.ordering, ordering)
		}
	}
}
