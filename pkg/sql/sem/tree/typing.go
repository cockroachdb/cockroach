// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// InferBinaryType infers the return type of a binary expression, given the type
// of its inputs.
func InferBinaryType(bin treebin.BinaryOperatorSymbol, leftType, rightType *types.T) *types.T {
	o, ok := FindBinaryOverload(bin, leftType, rightType)
	if !ok {
		return nil
	}
	return o.ReturnType
}

// FindBinaryOverload finds the correct type signature overload for the
// specified binary operator, given the types of its inputs. If an overload is
// found, FindBinaryOverload returns true, plus a pointer to the overload.
// If an overload is not found, FindBinaryOverload returns false.
func FindBinaryOverload(
	bin treebin.BinaryOperatorSymbol, leftType, rightType *types.T,
) (_ *BinOp, ok bool) {

	// Find the binary op that matches the type of the expression's left and
	// right children. No more than one match should ever be found. The
	// TestTypingBinaryAssumptions test ensures this will be the case even if
	// new operators or overloads are added.
	for _, binOverloads := range BinOps[bin] {
		o := binOverloads.(*BinOp)

		if leftType.Family() == types.UnknownFamily {
			if rightType.Equivalent(o.RightType) {
				return o, true
			}
		} else if rightType.Family() == types.UnknownFamily {
			if leftType.Equivalent(o.LeftType) {
				return o, true
			}
		} else {
			if leftType.Equivalent(o.LeftType) && rightType.Equivalent(o.RightType) {
				return o, true
			}
		}
	}
	return nil, false
}
