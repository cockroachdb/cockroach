// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	tu "github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
)

func TestTyping(t *testing.T) {
	runDataDrivenTest(t, tu.TestDataPath(t, "typing"),
		memo.ExprFmtHideMiscProps|
			memo.ExprFmtHideConstraints|
			memo.ExprFmtHideFuncDeps|
			memo.ExprFmtHideRuleProps|
			memo.ExprFmtHideStats|
			memo.ExprFmtHideCost|
			memo.ExprFmtHideQualifications|
			memo.ExprFmtHideScalars|
			memo.ExprFmtHideNotVisibleIndexInfo|
			memo.ExprFmtHideFastPathChecks,
	)
}

func TestBinaryOverloadExists(t *testing.T) {
	test := func(expected, actual bool) {
		if expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	}

	test(true, memo.BinaryOverloadExists(opt.MinusOp, types.Date, types.Int))
	test(true, memo.BinaryOverloadExists(opt.MinusOp, types.Date, types.Unknown))
	test(true, memo.BinaryOverloadExists(opt.MinusOp, types.Unknown, types.Int))
	test(false, memo.BinaryOverloadExists(opt.MinusOp, types.Int, types.Date))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.IntArray, types.Int))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.Unknown, types.IntArray))
}

func TestBinaryAllowsNullArgs(t *testing.T) {
	test := func(expected, actual bool) {
		if expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	}

	test(false, memo.BinaryAllowsNullArgs(opt.PlusOp, types.Int, types.Int))
	test(false, memo.BinaryAllowsNullArgs(opt.PlusOp, types.Int, types.Unknown))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.IntArray, types.Int))
	test(true, memo.BinaryOverloadExists(opt.ConcatOp, types.Unknown, types.IntArray))
}

// TestTypingUnaryAssumptions ensures that unary overloads conform to certain
// assumptions we're making in the type inference code:
//  1. The return type can be inferred from the operator type and the data
//     types of its operand.
func TestTypingUnaryAssumptions(t *testing.T) {
	for name, overloads := range tree.UnaryOps {
		_ = overloads.ForEachUnaryOp(func(op *tree.UnaryOp) error {
			_ = overloads.ForEachUnaryOp(func(op2 *tree.UnaryOp) error {
				if op == op2 {
					return nil
				}
				if op.Typ.Equivalent(op2.Typ) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
				return nil
			})
			return nil
		})
	}
}

// TestTypingComparisonAssumptions ensures that comparison overloads conform to
// certain assumptions we're making in the type inference code:
//  1. All comparison ops will be present in tree.CmpOps after being mapped
//     with NormalizeComparison.
//  2. The overload can be inferred from the operator type and the data
//     types of its operands.
func TestTypingComparisonAssumptions(t *testing.T) {
	for _, op := range opt.ComparisonOperators {
		newOp, _, _ := memo.NormalizeComparison(op)
		comp := opt.ComparisonOpReverseMap[newOp]
		if _, ok := tree.CmpOps[comp]; !ok {
			t.Errorf("could not find overload for %v", op)
		}
	}
	for name, overloads := range tree.CmpOps {
		_ = overloads.ForEachCmpOp(func(op *tree.CmpOp) error {
			_ = overloads.ForEachCmpOp(func(op2 *tree.CmpOp) error {
				// Check for basic ambiguity where two different comparison op overloads
				// both allow equivalent operand types.
				if op == op2 {
					return nil
				}
				if op.OverloadPreference != op2.OverloadPreference {
					return nil
				}
				if op.LeftType.Equivalent(op2.LeftType) && op.RightType.Equivalent(op2.RightType) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
				return nil
			})
			return nil
		})
	}
}

// TestTypingAggregateAssumptions ensures that aggregate overloads conform to
// certain assumptions we're making in the type inference code:
//  1. The return type can be inferred from the operator type and the data
//     types of its operand.
//  2. The return type of overloads is fixed.
//  3. The return type for min/max aggregates is same as type of argument.
func TestTypingAggregateAssumptions(t *testing.T) {
	for _, name := range builtins.AllAggregateBuiltinNames() {
		if name == builtins.AnyNotNull ||
			name == "percentile_disc" ||
			name == "percentile_cont" {
			// These are treated as special cases.
			continue
		}
		_, overloads := builtinsregistry.GetBuiltinProperties(name)
		for i, overload := range overloads {
			// Check for basic ambiguity where two different aggregate function
			// overloads both allow equivalent operand types.
			for i2, overload2 := range overloads {
				if i == i2 {
					continue
				}

				if overload.Types.Match(overload2.Types.Types()) {
					format := "found equivalent operand type ambiguity for %s: %+v"
					t.Errorf(format, name, overload.Types.Types())
				}
			}

			// Check for fixed return types.
			retType := overload.ReturnType(nil)

			// As per rule 3, max and min have slightly different rules. We allow
			// max and min to have non-fixed return types to allow defining aggregate
			// overloads that have container types as arguments.
			if name == "min" || name == "max" {
				// Evaluate the return typer.
				types := overload.Types.Types()
				retType = overload.InferReturnTypeFromInputArgTypes(types)
				if retType != types[0] {
					t.Errorf("return type differs from arg type for %s: %+v", name, overload.Types.Types())
				}
				continue
			}

			if retType == tree.UnknownReturnType {
				t.Errorf("return type is not fixed for %s: %+v", name, overload.Types.Types())
			}
		}
	}
}
