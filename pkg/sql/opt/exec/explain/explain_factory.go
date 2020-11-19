// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
)

// Factory implements exec.ExplainFactory. It wraps another factory and forwards
// all factory calls, while also recording the calls and arguments. The product
// of the factory is the Plan returned by ConstructPlan, which can be used to
// access the wrapped plan, as well as a parallel Node tree which can be used to
// produce EXPLAIN information.
type Factory struct {
	wrappedFactory exec.Factory
}

var _ exec.ExplainFactory = &Factory{}

// Node in a plan tree; records the operation and arguments passed to the
// factory method and provides access to the corresponding exec.Node (produced
// by the wrapped factory).
type Node struct {
	f        *Factory
	op       execOperator
	args     interface{}
	columns  colinfo.ResultColumns
	ordering colinfo.ColumnOrdering

	children []*Node

	annotations map[exec.ExplainAnnotationID]interface{}

	wrappedNode exec.Node
}

var _ exec.Node = &Node{}

// ChildCount returns the number of children nodes.
func (n *Node) ChildCount() int {
	return len(n.children)
}

// Child returns the i-th child.
func (n *Node) Child(idx int) *Node {
	return n.children[idx]
}

// Columns returns the ResultColumns for this node.
func (n *Node) Columns() colinfo.ResultColumns {
	return n.columns
}

// Ordering returns the required output ordering for this node; columns
// correspond to Columns().
func (n *Node) Ordering() colinfo.ColumnOrdering {
	return n.ordering
}

// WrappedNode returns the corresponding exec.Node produced by the wrapped
// factory.
func (n *Node) WrappedNode() exec.Node {
	return n.wrappedNode
}

// Annotate annotates the node with extra information.
func (n *Node) Annotate(id exec.ExplainAnnotationID, value interface{}) {
	if n.annotations == nil {
		n.annotations = make(map[exec.ExplainAnnotationID]interface{})
	}
	n.annotations[id] = value
}

func (f *Factory) newNode(
	op execOperator, args interface{}, ordering exec.OutputOrdering, children ...*Node,
) (*Node, error) {
	inputNodeCols := make([]colinfo.ResultColumns, len(children))
	for i := range children {
		inputNodeCols[i] = children[i].Columns()
	}
	columns, err := getResultColumns(op, args, inputNodeCols...)
	if err != nil {
		return nil, err
	}
	return &Node{
		f:        f,
		op:       op,
		args:     args,
		columns:  columns,
		ordering: colinfo.ColumnOrdering(ordering),
		children: children,
	}, nil
}

// Plan is the result of ConstructPlan; provides access to the exec.Plan
// produced by the wrapped factory.
type Plan struct {
	Root        *Node
	Subqueries  []exec.Subquery
	Cascades    []exec.Cascade
	Checks      []*Node
	WrappedPlan exec.Plan
}

var _ exec.Plan = &Plan{}

// NewFactory creates a new explain factory.
func NewFactory(wrappedFactory exec.Factory) *Factory {
	return &Factory{
		wrappedFactory: wrappedFactory,
	}
}

// AnnotateNode is part of the exec.ExplainFactory interface.
func (f *Factory) AnnotateNode(execNode exec.Node, id exec.ExplainAnnotationID, value interface{}) {
	execNode.(*Node).Annotate(id, value)
}

// ConstructPlan is part of the exec.Factory interface.
func (f *Factory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, cascades []exec.Cascade, checks []exec.Node,
) (exec.Plan, error) {
	p := &Plan{
		Root:       root.(*Node),
		Subqueries: subqueries,
		Cascades:   cascades,
		Checks:     make([]*Node, len(checks)),
	}
	for i := range checks {
		p.Checks[i] = checks[i].(*Node)
	}

	wrappedSubqueries := append([]exec.Subquery(nil), subqueries...)
	for i := range wrappedSubqueries {
		wrappedSubqueries[i].Root = wrappedSubqueries[i].Root.(*Node).WrappedNode()
	}
	wrappedCascades := append([]exec.Cascade(nil), cascades...)
	for i := range wrappedCascades {
		if wrappedCascades[i].Buffer != nil {
			wrappedCascades[i].Buffer = wrappedCascades[i].Buffer.(*Node).WrappedNode()
		}
	}
	wrappedChecks := make([]exec.Node, len(checks))
	for i := range wrappedChecks {
		wrappedChecks[i] = checks[i].(*Node).WrappedNode()
	}
	var err error
	p.WrappedPlan, err = f.wrappedFactory.ConstructPlan(
		p.Root.WrappedNode(), wrappedSubqueries, wrappedCascades, wrappedChecks,
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}
