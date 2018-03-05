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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Rules files can be run separately like this:
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/bool"
//   make test PKG=./pkg/sql/opt/xform TESTS="TestRules/comp"
//   ...
func TestRules(t *testing.T) {
	runDataDrivenTest(t, "testdata/rules/*")
}

// Ensure that every Plus and Mult operator overload can have its operands
// switched. Patterns like NormalizePlusMult rely on this being possible.
func TestRulePlusMultAssumption(t *testing.T) {
	fn := func(op opt.Operator) {
		for _, overload := range tree.BinOps[opt.BinaryOpReverseMap[op]] {
			binOp := overload.(tree.BinOp)

			_, ok := findBinaryOverload(op, binOp.RightType, binOp.LeftType)
			if !ok {
				t.Errorf("could not find inverse for overload: %+v", op)
			}
		}
	}
	fn(opt.PlusOp)
	fn(opt.MultOp)
}
