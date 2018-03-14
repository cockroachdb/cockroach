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

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func testInitDummySelectNode(t *testing.T, p *planner, desc *sqlbase.TableDescriptor) *renderNode {
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

	desc := testTableDesc()
	p := makeTestPlanner()
	s := testInitDummySelectNode(t, p, desc)
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

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
