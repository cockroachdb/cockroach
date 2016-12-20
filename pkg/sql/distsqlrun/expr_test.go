// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testVarContainer struct{}

func (d testVarContainer) IndexedVarResolvedType(idx int) parser.Type {
	return parser.TypeInt
}

func (d testVarContainer) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return nil, nil
}

func (d testVarContainer) IndexedVarFormat(buf *bytes.Buffer, _ parser.FmtFlags, idx int) {
	fmt.Fprintf(buf, "var%d", idx)
}

func TestProcessExpression(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := Expression{Expr: "@1 * (@2 + @3) + @1"}
	h := parser.MakeIndexedVarHelper(testVarContainer{}, 4)
	expr, err := processExpression(e, &h)
	if err != nil {
		t.Fatal(err)
	}

	if !h.IndexedVarUsed(0) || !h.IndexedVarUsed(1) || !h.IndexedVarUsed(2) || h.IndexedVarUsed(3) {
		t.Errorf("invalid IndexedVarUsed results %t %t %t %t (expected false false false true)",
			h.IndexedVarUsed(0), h.IndexedVarUsed(1), h.IndexedVarUsed(2), h.IndexedVarUsed(3))
	}

	str := expr.String()
	expectedStr := "(var0 * (var1 + var2)) + var0"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}
}
