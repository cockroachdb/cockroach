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

package xform

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestTyping(t *testing.T) {
	runDataDrivenTest(t, "testdata/typing")
}

func TestTypingJson(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	o := NewOptimizer(&evalCtx, OptimizeNone)
	f := o.Factory()

	// (Const <json>)
	json, _ := tree.ParseDJSON("[1, 2]")
	jsonGroup := f.ConstructConst(f.InternPrivate(json))

	// (Const <int>)
	intGroup := f.ConstructConst(f.InternPrivate(tree.NewDInt(1)))

	// (Const <string-array>)
	arrGroup := f.ConstructConst(f.InternPrivate(tree.NewDArray(types.String)))

	// (FetchVal (Const <json>) (Const <int>))
	fetchValGroup := f.ConstructFetchVal(jsonGroup, intGroup)
	ev := o.Optimize(fetchValGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.JSON)

	// (FetchValPath (Const <json>) (Const <string-array>))
	fetchValPathGroup := f.ConstructFetchValPath(jsonGroup, arrGroup)
	ev = o.Optimize(fetchValPathGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.JSON)

	// (FetchText (Const <json>) (Const <int>))
	fetchTextGroup := f.ConstructFetchText(jsonGroup, intGroup)
	ev = o.Optimize(fetchTextGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.String)

	// (FetchTextPath (Const <json>) (Const <string-array>))
	fetchTextPathGroup := f.ConstructFetchTextPath(jsonGroup, arrGroup)
	ev = o.Optimize(fetchTextPathGroup, &opt.PhysicalProps{})
	testTyping(t, ev, types.String)
}

// TestTypingUnaryAssumptions ensures that unary overloads conform to certain
// assumptions we're making in the type inference code:
//   1. The return type can be inferred from the operator type and the data
//      types of its operand.
func TestTypingUnaryAssumptions(t *testing.T) {
	for name, overloads := range tree.UnaryOps {
		for i, overload := range overloads {
			op := overload.(tree.UnaryOp)

			// Check for basic ambiguity where two different unary op overloads
			// both allow equivalent operand types.
			for i2, overload2 := range overloads {
				if i == i2 {
					continue
				}

				op2 := overload2.(tree.UnaryOp)
				if op.Typ.Equivalent(op2.Typ) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
			}
		}
	}
}

// TestTypingBinaryAssumptions ensures that binary overloads conform to certain
// assumptions we're making in the type inference code:
//   1. The return type can be inferred from the operator type and the data
//      types of its operands.
//   2. When of the operands is null, and if NullableArgs is true, then the
//      return type can be inferred from just the non-null operand.
func TestTypingBinaryAssumptions(t *testing.T) {
	for name, overloads := range tree.BinOps {
		for i, overload := range overloads {
			op := overload.(tree.BinOp)

			// Check for basic ambiguity where two different binary op overloads
			// both allow equivalent operand types.
			for i2, overload2 := range overloads {
				if i == i2 {
					continue
				}

				op2 := overload2.(tree.BinOp)
				if op.LeftType.Equivalent(op2.LeftType) && op.RightType.Equivalent(op2.RightType) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
			}

			// Handle ops that allow null operands. Check for ambiguity where
			// the return type cannot be inferred from the non-null operand.
			if op.NullableArgs {
				for i2, overload2 := range overloads {
					if i == i2 {
						continue
					}

					op2 := overload2.(tree.BinOp)
					if !op2.NullableArgs {
						continue
					}

					if op.LeftType == op2.LeftType && op.ReturnType != op2.ReturnType {
						t.Errorf("found null operand ambiguity for %s:\n%+v\n%+v", name, op, op2)
					}

					if op.RightType == op2.RightType && op.ReturnType != op2.ReturnType {
						t.Errorf("found null operand ambiguity for %s:\n%+v\n%+v", name, op, op2)
					}
				}
			}
		}
	}
}

func createTypingCatalog(t *testing.T) *testutils.TestCatalog {
	cat := testutils.NewTestCatalog()
	testutils.ExecuteTestDDL(t, "CREATE TABLE a (x INT PRIMARY KEY, y INT)", cat)
	testutils.ExecuteTestDDL(t, "CREATE TABLE b (x STRING PRIMARY KEY, z DECIMAL NOT NULL)", cat)
	return cat
}

func testTyping(t *testing.T, ev ExprView, expected types.T) {
	t.Helper()

	actual := ev.Logical().Scalar.Type

	if !actual.Equivalent(expected) {
		t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
	}
}
