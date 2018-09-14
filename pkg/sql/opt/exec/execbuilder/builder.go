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

package execbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// Builder constructs a tree of execution nodes (exec.Node) from an optimized
// expression tree (memo.ExprView).
type Builder struct {
	factory            exec.Factory
	ev                 memo.ExprView
	evalCtx            *tree.EvalContext
	fastIsConstVisitor fastIsConstVisitor

	// subqueries accumulates information about subqueries that are part of scalar
	// expressions we built. Each entry is associated with a tree.Subquery
	// expression node.
	subqueries []exec.Subquery
}

// New constructs an instance of the execution node builder using the
// given factory to construct nodes. The Build method will build the execution
// node tree from the given optimized expression tree.
func New(factory exec.Factory, ev memo.ExprView, evalCtx *tree.EvalContext) *Builder {
	return &Builder{factory: factory, ev: ev, evalCtx: evalCtx}
}

// Build constructs the execution node tree and returns its root node if no
// error occurred.
func (b *Builder) Build() (exec.Plan, error) {
	root, err := b.build(b.ev)
	if err != nil {
		return nil, err
	}
	return b.factory.ConstructPlan(root, b.subqueries)
}

func (b *Builder) build(ev memo.ExprView) (exec.Node, error) {
	if !ev.IsRelational() && !ev.IsEnforcer() {
		return nil, errors.Errorf("building execution for non-relational operator %s", ev.Operator())
	}
	plan, err := b.buildRelational(ev)
	if err != nil {
		return nil, err
	}
	return plan.root, err
}

// BuildScalar converts a scalar expression to a TypedExpr. Variables are mapped
// according to the IndexedVarHelper.
func (b *Builder) BuildScalar(ivh *tree.IndexedVarHelper) (tree.TypedExpr, error) {
	ctx := buildScalarCtx{ivh: *ivh}
	for i := 0; i < ivh.NumVars(); i++ {
		ctx.ivarMap.Set(i+1, i)
	}
	return b.buildScalar(&ctx, b.ev)
}

func (b *Builder) decorrelationError() error {
	return errors.Errorf("could not decorrelate subquery")
}
