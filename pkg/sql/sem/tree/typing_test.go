// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestTypingBinaryAssumptions ensures that binary overloads conform to certain
// assumptions we're making in the type inference code:
//  1. The return type can be inferred from the operator type and the data
//     types of its operands.
//  2. When of the operands is null, and if CalledOnNullInput is true, then the
//     return type can be inferred from just the non-null operand.
func TestTypingBinaryAssumptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for name, overloads := range BinOps {
		for i, op := range overloads.overloads {

			// Check for basic ambiguity where two different binary op overloads
			// both allow equivalent operand types.
			for i2, op2 := range overloads.overloads {
				if i == i2 {
					continue
				}
				if op.LeftType.Equivalent(op2.LeftType) && op.RightType.Equivalent(op2.RightType) {
					format := "found equivalent operand type ambiguity for %s:\n%+v\n%+v"
					t.Errorf(format, name, op, op2)
				}
			}

			// Handle ops that allow null operands. Check for ambiguity where
			// the return type cannot be inferred from the non-null operand.
			if op.CalledOnNullInput {
				for i2, op2 := range overloads.overloads {
					if i == i2 {
						continue
					}

					if !op2.CalledOnNullInput {
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
