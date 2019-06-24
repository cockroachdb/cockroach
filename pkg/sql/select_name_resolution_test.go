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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func testInitDummySelectNode(t *testing.T, p *planner, desc *ImmutableTableDescriptor) *renderNode {
	scan := &scanNode{}
	scan.desc = desc
	if err := scan.initDescDefaults(p.curPlan.deps, publicColumnsCfg); err != nil {
		t.Fatal(err)
	}

	sel := &renderNode{}
	sel.source.plan = scan
	testName := tree.MakeTableName("test", tree.Name(desc.Name))
	cols := planColumns(scan)
	sel.source.info = sqlbase.NewSourceInfoForSingleTable(testName, cols)
	sel.sourceInfo = sqlbase.MakeMultiSourceInfo(sel.source.info)
	sel.ivarHelper = tree.MakeIndexedVarHelper(sel, len(cols))

	return sel
}

// Test that we can resolve the names in an expression that has already been
// resolved.
func TestRetryResolveNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expr, err := parser.ParseExpr(`count(a)`)
	if err != nil {
		t.Fatal(err)
	}

	desc := testTableDesc(t, func(*MutableTableDescriptor) {})
	p := makeTestPlanner()
	s := testInitDummySelectNode(t, p, desc)

	for i := 0; i < 2; i++ {
		newExpr, _, _, err := p.resolveNamesForRender(expr, s)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for iv := 0; iv < len(s.sourceInfo[0].SourceColumns); iv++ {
			if s.ivarHelper.IndexedVarUsed(iv) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%d: expected 1 ivar, but found %d", i, count)
		}
		if newExpr.String() != "count(a)" {
			t.Fatalf("%d: newExpr: got %s, expected 'count(a)'", i, newExpr.String())
		}
		expr = newExpr
	}
}
