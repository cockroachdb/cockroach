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

package xform_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Rules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/bool"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/comp"
//   ...
func TestRules(t *testing.T) {
	runDataDrivenTest(t, "testdata/rules/*")
}

// Test the FoldNullInEmpty rule. Can't create empty tuple on right side of
// IN/NOT IN in SQL, so do it here.
func TestRuleFoldNullInEmpty(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	o := xform.NewOptimizer(&evalCtx)
	f := o.Factory()

	null := f.ConstructNull(f.InternPrivate(types.Unknown))
	empty := f.ConstructTuple(opt.EmptyList)
	in := f.ConstructIn(null, empty)
	ev := o.Optimize(in, &opt.PhysicalProps{})
	if ev.Operator() != opt.FalseOp {
		t.Errorf("expected NULL IN () to fold to False")
	}

	notIn := f.ConstructNotIn(null, empty)
	ev = o.Optimize(notIn, &opt.PhysicalProps{})
	if ev.Operator() != opt.TrueOp {
		t.Errorf("expected NULL NOT IN () to fold to True")
	}
}

// Ensure that every binary commutative operator overload can have its operands
// switched. Patterns like CommuteConst rely on this being possible.
func TestRuleBinaryAssumption(t *testing.T) {
	fn := func(op opt.Operator) {
		for _, overload := range tree.BinOps[opt.BinaryOpReverseMap[op]] {
			binOp := overload.(tree.BinOp)
			if !xform.BinaryOverloadExists(op, binOp.RightType, binOp.LeftType) {
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
