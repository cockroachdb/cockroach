// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	Cascades    []exec.PostQuery
	Triggers    []exec.PostQuery
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
	cascades, triggers []exec.PostQuery,
	checks []exec.Node,
	rootRowCount int64,
	flags exec.PlanFlags,
) (exec.Plan, error) {
	p := &Plan{
		Root:       root.(*Node),
		Subqueries: subqueries,
		Cascades:   cascades,
		Checks:     make([]*Node, len(checks)),
		Triggers:   triggers,
	}
	for i := range checks {
		p.Checks[i] = checks[i].(*Node)
	}

	wrappedSubqueries := append([]exec.Subquery(nil), subqueries...)
	for i := range wrappedSubqueries {
		wrappedSubqueries[i].Root = wrappedSubqueries[i].Root.(*Node).WrappedNode()
	}
	wrappedCascades := append([]exec.PostQuery(nil), cascades...)
	for i := range wrappedCascades {
		f.wrapPostQuery(&cascades[i], &wrappedCascades[i])
	}
	wrappedChecks := make([]exec.Node, len(checks))
	for i := range wrappedChecks {
		wrappedChecks[i] = checks[i].(*Node).WrappedNode()
	}
	wrappedTriggers := append([]exec.PostQuery(nil), triggers...)
	for i := range wrappedTriggers {
		f.wrapPostQuery(&triggers[i], &wrappedTriggers[i])
	}
	var err error
	p.WrappedPlan, err = f.wrappedFactory.ConstructPlan(
		p.Root.WrappedNode(), wrappedSubqueries, wrappedCascades, wrappedTriggers, wrappedChecks,
		rootRowCount, flags,
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (f *Factory) wrapPostQuery(originalPostQuery, wrappedPostQuery *exec.PostQuery) {
	buffer := wrappedPostQuery.Buffer
	if buffer != nil {
		wrappedPostQuery.Buffer = buffer.(*Node).WrappedNode()
	}
	// postQueryPlan will be populated lazily, either when PlanFn is invoked
	// during the execution or when GetExplainPlan is invoked during the
	// explain output population.
	var postQueryPlan exec.Plan
	// In order to capture the plan for the post query, we'll inject some
	// additional code into the planning function.
	origPlanFn := wrappedPostQuery.PlanFn
	wrappedPostQuery.PlanFn = func(
		ctx context.Context,
		semaCtx *tree.SemaContext,
		evalCtx *eval.Context,
		execFactory exec.Factory,
		bufferRef exec.Node,
		numBufferedRows int,
		allowAutoCommit bool,
	) (exec.Plan, error) {
		// Sanity check that the buffer node we captured earlier references
		// the same wrapped node as the one we're given.
		if (buffer == nil) != (bufferRef == nil) {
			return nil, errors.AssertionFailedf("expected both buffer %v and bufferRef %v be either nil or non-nil", buffer, bufferRef)
		}
		if buffer != nil && buffer.(*Node).WrappedNode() != bufferRef {
			return nil, errors.AssertionFailedf("expected captured buffer %v to wrap the provided bufferRef %v", buffer, bufferRef)
		}
		explainFactory := NewFactory(execFactory, semaCtx, evalCtx)
		var err error
		postQueryPlan, err = origPlanFn(ctx, semaCtx, evalCtx, explainFactory, buffer, numBufferedRows, allowAutoCommit)
		if err != nil {
			return nil, err
		}
		return postQueryPlan.(*Plan).WrappedPlan, nil
	}
	originalPostQuery.GetExplainPlan = func(ctx context.Context, createPlanIfMissing bool) (exec.Plan, error) {
		if postQueryPlan != nil || !createPlanIfMissing {
			// If we already created the plan, or if we can't create a fresh
			// plan, then use the cached one (if available).
			return postQueryPlan, nil
		}
		// We're in vanilla EXPLAIN context, so we need to create the
		// cascade plan ourselves. Note that cascades/triggers can create other
		// cascades/triggers, but that will be transparently handled by internal
		// recursive call to explain.Factory.ConstructPlan.
		var numBufferedRows int
		if buffer != nil {
			// TODO(yuzefovich): we should use an estimate for it.
			numBufferedRows = 100
		}
		// We're not going to execute the plan so this value doesn't
		// actually matter.
		const allowAutoCommit = false
		var err error
		postQueryPlan, err = origPlanFn(ctx, f.semaCtx, f.evalCtx, f, buffer, numBufferedRows, allowAutoCommit)
		return postQueryPlan, err
	}
}
