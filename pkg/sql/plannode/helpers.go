// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"github.com/cockroachdb/errors"
)

// ZeroInputPlanNode is embedded in planNode implementations that have no input
// planNode. It implements the InputCount, Input, and SetInput methods of
// PlanNode.
type ZeroInputPlanNode struct{}

func (ZeroInputPlanNode) InputCount() int { return 0 }

func (ZeroInputPlanNode) Input(i int) (PlanNode, error) {
	return nil, errors.AssertionFailedf("node has no inputs")
}

func (ZeroInputPlanNode) SetInput(i int, p PlanNode) error {
	return errors.AssertionFailedf("node has no inputs")
}

// Lowercase alias for backwards compatibility within this package
type zeroInputPlanNode = ZeroInputPlanNode

// SingleInputPlanNode is embedded in planNode implementations that have a
// single input planNode. It implements the InputCount, Input, and SetInput
// methods of PlanNode.
type SingleInputPlanNode struct {
	Source PlanNode
}

func (n *SingleInputPlanNode) InputCount() int { return 1 }

func (n *SingleInputPlanNode) Input(i int) (PlanNode, error) {
	if i == 0 {
		return n.Source, nil
	}
	return nil, errors.AssertionFailedf("input index %d is out of range", i)
}

func (n *SingleInputPlanNode) SetInput(i int, p PlanNode) error {
	if i == 0 {
		n.Source = p
		return nil
	}
	return errors.AssertionFailedf("input index %d is out of range", i)
}

// Lowercase alias for backwards compatibility within this package
type singleInputPlanNode = SingleInputPlanNode
