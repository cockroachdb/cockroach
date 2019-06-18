// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package execbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Builder constructs a tree of execution nodes (exec.Node) from an optimized
// expression tree (opt.Expr).
type Builder struct {
	factory            exec.Factory
	mem                *memo.Memo
	e                  opt.Expr
	disableTelemetry   bool
	evalCtx            *tree.EvalContext
	fastIsConstVisitor fastIsConstVisitor

	// subqueries accumulates information about subqueries that are part of scalar
	// expressions we built. Each entry is associated with a tree.Subquery
	// expression node.
	subqueries []exec.Subquery

	// nullifyMissingVarExprs, if greater than 0, tells the builder to replace
	// VariableExprs that have no bindings with DNull. This is useful for apply
	// join, which needs to be able to create a plan that has outer columns.
	// The number indicates the depth of apply joins.
	nullifyMissingVarExprs int

	// nameGen is used to generate names for the tables that will be created for
	// each relational subexpression when evalCtx.SessionData.SaveTablesPrefix is
	// non-empty.
	nameGen *memo.ExprNameGenerator
}

// New constructs an instance of the execution node builder using the
// given factory to construct nodes. The Build method will build the execution
// node tree from the given optimized expression tree.
func New(factory exec.Factory, mem *memo.Memo, e opt.Expr, evalCtx *tree.EvalContext) *Builder {
	var nameGen *memo.ExprNameGenerator
	if evalCtx != nil && evalCtx.SessionData.SaveTablesPrefix != "" {
		nameGen = memo.NewExprNameGenerator(evalCtx.SessionData.SaveTablesPrefix)
	}
	return &Builder{factory: factory, mem: mem, e: e, evalCtx: evalCtx, nameGen: nameGen}
}

// DisableTelemetry prevents the execbuilder from updating telemetry counters.
func (b *Builder) DisableTelemetry() {
	b.disableTelemetry = true
}

// Build constructs the execution node tree and returns its root node if no
// error occurred.
func (b *Builder) Build() (_ exec.Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if e, ok := r.(error); ok {
				err = e
				return
			}
			// Other panic objects can't be considered "safe" and thus are
			// propagated as crashes that terminate the session.
			panic(r)
		}
	}()

	root, err := b.build(b.e)
	if err != nil {
		return nil, err
	}
	return b.factory.ConstructPlan(root, b.subqueries)
}

func (b *Builder) build(e opt.Expr) (exec.Node, error) {
	rel, ok := e.(memo.RelExpr)
	if !ok {
		return nil, errors.AssertionFailedf("building execution for non-relational operator %s", log.Safe(e.Op()))
	}
	plan, err := b.buildRelational(rel)
	if err != nil {
		return nil, err
	}

	return plan.root, nil
}

// BuildScalar converts a scalar expression to a TypedExpr. Variables are mapped
// according to the IndexedVarHelper.
func (b *Builder) BuildScalar(ivh *tree.IndexedVarHelper) (tree.TypedExpr, error) {
	scalar, ok := b.e.(opt.ScalarExpr)
	if !ok {
		return nil, errors.AssertionFailedf("BuildScalar cannot be called for non-scalar operator %s", log.Safe(b.e.Op()))
	}
	ctx := buildScalarCtx{ivh: *ivh}
	for i := 0; i < ivh.NumVars(); i++ {
		ctx.ivarMap.Set(i+1, i)
	}
	return b.buildScalar(&ctx, scalar)
}

func (b *Builder) decorrelationError() error {
	return errors.Errorf("could not decorrelate subquery")
}
