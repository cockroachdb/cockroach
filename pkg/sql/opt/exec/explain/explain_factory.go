// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package explain implements "explaining" for cockroach.
package explain

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Factory implements exec.ExplainFactory. It wraps another factory and forwards
// all factory calls, while also recording the calls and arguments. The product
// of the factory is the Plan returned by ConstructPlan, which can be used to
// access the wrapped plan, as well as a parallel Node tree which can be used to
// produce EXPLAIN information.
type Factory struct {
	wrappedFactory exec.Factory
	semaCtx        *tree.SemaContext
	evalCtx        *eval.Context
}

var _ exec.ExplainFactory = &Factory{}

// Ctx implements the Factory interface.
func (f *Factory) Ctx() context.Context {
	return f.wrappedFactory.Ctx()
}

// Node in a plan tree; records the operation and arguments passed to the
// factory method and provides access to the corresponding exec.Node (produced
// by the wrapped factory).
type Node struct {
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

func newNode(
	op execOperator, args interface{}, ordering exec.OutputOrdering, children ...*Node,
) (*Node, error) {
	nonNilChildren := make([]*Node, 0, len(children))
	for i := range children {
		if children[i] != nil {
			nonNilChildren = append(nonNilChildren, children[i])
		}
	}
	children = nonNilChildren
	inputNodeCols := make([]colinfo.ResultColumns, len(children))
	for i := range children {
		inputNodeCols[i] = children[i].Columns()
	}
	columns, err := getResultColumns(op, args, inputNodeCols...)
	if err != nil {
		return nil, err
	}
	return &Node{
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
	Gist        PlanGist
}

var _ exec.Plan = &Plan{}

// NewFactory creates a new explain factory.
func NewFactory(
	wrappedFactory exec.Factory, semaCtx *tree.SemaContext, evalCtx *eval.Context,
) *Factory {
	return &Factory{
		wrappedFactory: wrappedFactory,
		semaCtx:        semaCtx,
		evalCtx:        evalCtx,
	}
}

// AnnotateNode is part of the exec.ExplainFactory interface.
func (f *Factory) AnnotateNode(execNode exec.Node, id exec.ExplainAnnotationID, value interface{}) {
	execNode.(*Node).Annotate(id, value)
}

// ConstructPlan is part of the exec.Factory interface.
func (f *Factory) ConstructPlan(
	root exec.Node,
	subqueries []exec.Subquery,
	cascades []exec.Cascade,
	checks []exec.Node,
	rootRowCount int64,
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
		buffer := wrappedCascades[i].Buffer
		if buffer != nil {
			wrappedCascades[i].Buffer = buffer.(*Node).WrappedNode()
		}
		// cascadePlan will be populated lazily, either when PlanFn is invoked
		// during the execution or when GetExplainPlan is invoked during the
		// explain output population.
		var cascadePlan exec.Plan
		var selfReferencing bool
		// In order to capture the plan for the cascade, we'll inject some
		// additional code into the planning function.
		origPlanFn := wrappedCascades[i].PlanFn
		wrappedCascades[i].PlanFn = func(
			ctx context.Context,
			semaCtx *tree.SemaContext,
			evalCtx *eval.Context,
			execFactory exec.Factory,
			bufferRef exec.Node,
			numBufferedRows int,
			allowAutoCommit bool,
		) (exec.Plan, bool, error) {
			// Sanity check that the buffer node we captured earlier references
			// the same wrapped node as the one we're given.
			if (buffer == nil) != (bufferRef == nil) {
				return nil, false, errors.AssertionFailedf("expected both buffer %v and bufferRef %v be either nil or non-nil", buffer, bufferRef)
			}
			if buffer != nil && buffer.(*Node).WrappedNode() != bufferRef {
				return nil, false, errors.AssertionFailedf("expected captured buffer %v to wrap the provided bufferRef %v", buffer, bufferRef)
			}
			f.wrappedFactory = execFactory
			var err error
			cascadePlan, selfReferencing, err = origPlanFn(ctx, semaCtx, evalCtx, f, buffer, numBufferedRows, allowAutoCommit)
			if err != nil {
				return nil, false, err
			}
			return cascadePlan.(*Plan).WrappedPlan, selfReferencing, nil
		}
		cascades[i].GetExplainPlan = func(ctx context.Context) (exec.Plan, bool, error) {
			if cascadePlan != nil {
				// If PlanFn was already invoked (presumably because we're in
				// EXPLAIN ANALYZE context), then we'll use the stored plan.
				return cascadePlan, selfReferencing, nil
			}
			// We're in vanilla EXPLAIN context, so we need to create the
			// cascade plan ourselves. Note that cascades can create other
			// cascades, but that will be transparently handled by internal
			// recursive call to explain.Factory.ConstructPlan.
			var numBufferedRows int
			if buffer != nil {
				// TODO(yuzefovich): we should use an estimate for it.
				numBufferedRows = 100
			}
			// During the execution of the cascade, auto-commit is only allowed
			// if it's the last cascade and there are no checks, but the cascade
			// plan might contain more cascades / checks, so auto-commit should
			// never be allowed (see #117092).
			const allowAutoCommit = false
			var err error
			cascadePlan, selfReferencing, err = origPlanFn(ctx, f.semaCtx, f.evalCtx, f, buffer, numBufferedRows, allowAutoCommit)
			return cascadePlan, selfReferencing, err
		}
	}
	wrappedChecks := make([]exec.Node, len(checks))
	for i := range wrappedChecks {
		wrappedChecks[i] = checks[i].(*Node).WrappedNode()
	}
	var err error
	p.WrappedPlan, err = f.wrappedFactory.ConstructPlan(
		p.Root.WrappedNode(), wrappedSubqueries, wrappedCascades, wrappedChecks, rootRowCount,
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}
