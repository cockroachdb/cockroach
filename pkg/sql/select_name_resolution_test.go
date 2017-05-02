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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func testInitDummySelectNode(desc *sqlbase.TableDescriptor) *renderNode {
	p := makeTestPlanner()
	scan := &scanNode{p: p}
	scan.desc = *desc
	// Note: scan.initDescDefaults only returns an error if its 2nd argument is not nil.
	_ = scan.initDescDefaults(publicColumns, nil)

	sel := &renderNode{planner: p}
	sel.source.plan = scan
	testName := parser.TableName{TableName: parser.Name(desc.Name), DatabaseName: parser.Name("test")}
	sel.source.info = newSourceInfoForSingleTable(testName, scan.Columns())
	sel.sourceInfo = multiSourceInfo{sel.source.info}
	sel.ivarHelper = parser.MakeIndexedVarHelper(sel, len(scan.Columns()))

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
	s := testInitDummySelectNode(desc)
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		newExpr, _, err := s.resolveNames(expr)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for iv := 0; iv < len(s.sourceInfo[0].sourceColumns); iv++ {
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
