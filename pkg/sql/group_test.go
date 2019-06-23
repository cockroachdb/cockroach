// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
		{`min(a)`, sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
		{`max(a)`, sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Descending}}},
		{`min(a+1)`, sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
		{`(min(a), max(a))`, nil},
		{`(min(a), avg(a))`, nil},
		{`(min(a), count(a))`, nil},
		{`(min(a), sum(a))`, nil},
		{`(min(a), min(a))`, sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}},
		{`(min(a+1), min(a))`, nil},
		{`(count(a), min(a))`, nil},
	}
	p := makeTestPlanner()
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			p.extendedEvalCtx = makeTestingExtendedEvalContext(cluster.MakeTestingClusterSettings())
			defer p.extendedEvalCtx.Stop(context.Background())
			sel := makeSelectNode(t, p)
			expr := parseAndNormalizeExpr(t, p, d.expr, sel)
			group := &groupNode{}
			render := &renderNode{}
			postRender := &renderNode{}
			postRender.ivarHelper = tree.MakeIndexedVarHelper(postRender, len(group.funcs))
			v := extractAggregatesVisitor{
				ctx:        context.TODO(),
				groupNode:  group,
				preRender:  render,
				ivarHelper: &postRender.ivarHelper,
				planner:    p,
			}
			if _, err := v.extract(expr); err != nil {
				t.Fatal(err)
			}
			ordering := group.desiredAggregateOrdering(p.EvalContext())
			if !reflect.DeepEqual(d.ordering, ordering) {
				t.Fatalf("%s: expected %v, but found %v", d.expr, d.ordering, ordering)
			}
			// Verify we never have a desired ordering if there is a GROUP BY.
			group.groupCols = []int{0}
			ordering = group.desiredAggregateOrdering(p.EvalContext())
			if len(ordering) > 0 {
				t.Fatalf("%s: expected no ordering when there is a GROUP BY, found %v", d.expr, ordering)
			}
		})
	}
}
