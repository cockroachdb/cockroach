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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func testInitDummySelectNode(desc *sqlbase.TableDescriptor) *selectNode {
	scan := &scanNode{}
	scan.desc = *desc
	scan.initDescDefaults(publicColumns)

	sel := &selectNode{}
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

	expr, err := parser.ParseExprTraditional(`COUNT(a)`)
	if err != nil {
		t.Fatal(err)
	}

	desc := testTableDesc()
	s := testInitDummySelectNode(desc)
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		_, err := s.resolveNames(expr)
		if err != nil {
			t.Fatal(err)
		}
		ivars := s.ivarHelper.GetIndexedVars()
		count := 0
		for iv := 0; iv < len(ivars); iv++ {
			if ivars[iv].Idx != invalidColIdx {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%d: expected 1 ivar, but found %d", i, count)
		}
		if ivars[0].Idx != 0 {
			t.Fatalf("%d: ivar not properly initialized for column 0 (a): got %d, expected 0",
				i, ivars[0].Idx)
		}
		if ivars[0].String() != "a" {
			t.Fatalf("%d: ivar name: got %s, expected 'a'", i, ivars[0].String())
		}
	}
}
