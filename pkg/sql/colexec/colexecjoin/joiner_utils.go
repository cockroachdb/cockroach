// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecjoin

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/errors"
)

// newTwoInputNode returns an execinfra.OpNode with two Operator inputs.
func newTwoInputNode(inputOne, inputTwo colexecop.Operator) twoInputNode {
	return twoInputNode{inputOne: inputOne, inputTwo: inputTwo}
}

type twoInputNode struct {
	inputOne colexecop.Operator
	inputTwo colexecop.Operator
}

func (twoInputNode) ChildCount(verbose bool) int {
	return 2
}

func (n *twoInputNode) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		return n.inputOne
	case 1:
		return n.inputTwo
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
