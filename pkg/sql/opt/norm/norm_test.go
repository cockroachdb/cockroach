// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/datadriven"
)

// TestNormRules tests the various Optgen normalization rules found in the rules
// directory. The tests are data-driven cases of the form:
//   <command>
//   <SQL statement>
//   ----
//   <expected results>
//
// See OptTester.Handle for supported commands.
//
// Rules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/norm TESTS="TestNormRules/bool"
//   make test PKG=./pkg/sql/opt/norm TESTS="TestNormRules/comp"
//   ...
func TestNormRules(t *testing.T) {
	const fmtFlags = memo.ExprFmtHideStats | memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps |
		memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars | memo.ExprFmtHideTypes
	datadriven.Walk(t, "testdata/rules", func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tester := opttester.New(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}

// Ensure that every binary commutative operator overload can have its operands
// switched. Patterns like CommuteConst rely on this being possible.
func TestRuleBinaryAssumption(t *testing.T) {
	fn := func(op opt.Operator) {
		for _, overload := range tree.BinOps[opt.BinaryOpReverseMap[op]] {
			binOp := overload.(*tree.BinOp)
			if !memo.BinaryOverloadExists(op, binOp.RightType, binOp.LeftType) {
				t.Errorf("could not find inverse for overload: %+v", op)
			}
		}
	}

	// Only include commutative binary operators.
	fn(opt.PlusOp)
	fn(opt.MultOp)
	fn(opt.BitandOp)
	fn(opt.BitorOp)
	fn(opt.BitxorOp)
}

// These constants are copied from sql/sem/builtins/builtins.go.
const (
	categorySystemInfo  = "System info"
	categoryDateAndTime = "Date and time"
)

// TestRuleFunctionAssumption checks that we do not fold impure functions.
// There are other functions we should not fold such as current_user() because
// they depend on context, but it is very difficult to test that we avoid all
// such cases. Instead, we rely on clues like the category.
func TestRuleFunctionAssumption(t *testing.T) {
	for name := range norm.FoldFunctionWhitelist {
		props, _ := builtins.GetBuiltinProperties(name)
		if props == nil {
			t.Errorf("could not find properties for function %s", name)
			continue
		}
		if props.Impure {
			t.Errorf("%s should not be folded because it is impure", name)
		}
		if props.Category == categorySystemInfo || props.Category == categoryDateAndTime {
			switch name {
			case "crdb_internal.locality_value":
				// OK to fold this function.

			default:
				t.Errorf("%s should not be folded because it has category %s", name, props.Category)
			}
		}
	}
}
