// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Builder constructs a tree of execution nodes (exec.Node) from an optimized
// expression tree (opt.Expr).
type Builder struct {
	factory            exec.Factory
	mem                *memo.Memo
	catalog            cat.Catalog
	e                  opt.Expr
	disableTelemetry   bool
	evalCtx            *tree.EvalContext
	fastIsConstVisitor fastIsConstVisitor

	// subqueries accumulates information about subqueries that are part of scalar
	// expressions we built. Each entry is associated with a tree.Subquery
	// expression node.
	subqueries []exec.Subquery

	// cascades accumulates cascades that run after the main query but before
	// checks.
	cascades []exec.Cascade

	// checks accumulates check queries that are run after the main query and
	// any cascades.
	checks []exec.Node

	// nameGen is used to generate names for the tables that will be created for
	// each relational subexpression when evalCtx.SessionData.SaveTablesPrefix is
	// non-empty.
	nameGen *memo.ExprNameGenerator

	// withExprs is the set of With expressions which may be referenced elsewhere
	// in the query.
	// TODO(justin): set this up so that we can look them up by index lookups
	// rather than scans.
	withExprs []builtWithExpr

	// allowAutoCommit is passed through to factory methods for mutation
	// operators. It allows execution to commit the transaction as part of the
	// mutation itself. See canAutoCommit().
	allowAutoCommit bool

	allowInsertFastPath bool

	// forceForUpdateLocking is conditionally passed through to factory methods
	// for scan operators that serve as the input for mutation operators. When
	// set to true, it ensures that a FOR UPDATE row-level locking mode is used
	// by scans. See forUpdateLocking.
	forceForUpdateLocking bool

	// -- output --

	// IsDDL is set to true if the statement contains DDL.
	IsDDL bool
}

// New constructs an instance of the execution node builder using the
// given factory to construct nodes. The Build method will build the execution
// node tree from the given optimized expression tree.
//
// catalog is only needed if the statement contains an EXPLAIN (OPT, CATALOG).
func New(
	factory exec.Factory, mem *memo.Memo, catalog cat.Catalog, e opt.Expr, evalCtx *tree.EvalContext,
) *Builder {
	b := &Builder{
		factory:         factory,
		mem:             mem,
		catalog:         catalog,
		e:               e,
		evalCtx:         evalCtx,
		allowAutoCommit: true,
	}
	if evalCtx != nil {
		if evalCtx.SessionData.SaveTablesPrefix != "" {
			b.nameGen = memo.NewExprNameGenerator(evalCtx.SessionData.SaveTablesPrefix)
		}
		b.allowInsertFastPath = evalCtx.SessionData.InsertFastPath
	}
	return b
}

// Build constructs the execution node tree and returns its root node if no
// error occurred.
func (b *Builder) Build() (_ exec.Plan, err error) {
	plan, err := b.build(b.e)
	if err != nil {
		return nil, err
	}
	return b.factory.ConstructPlan(plan.root, b.subqueries, b.cascades, b.checks)
}

func (b *Builder) build(e opt.Expr) (_ execPlan, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate errors without adding lots of checks
			// for `if err != nil` throughout the construction code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	rel, ok := e.(memo.RelExpr)
	if !ok {
		return execPlan{}, errors.AssertionFailedf(
			"building execution for non-relational operator %s", log.Safe(e.Op()),
		)
	}

	b.allowAutoCommit = b.allowAutoCommit && b.canAutoCommit(rel)

	return b.buildRelational(rel)
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

// builtWithExpr is metadata regarding a With expression which has already been
// added to the set of subqueries for the query.
type builtWithExpr struct {
	id opt.WithID
	// outputCols maps the output ColumnIDs of the With expression to the ordinal
	// positions they are output to. See execPlan.outputCols for more details.
	outputCols opt.ColMap
	bufferNode exec.BufferNode
}

func (b *Builder) addBuiltWithExpr(
	id opt.WithID, outputCols opt.ColMap, bufferNode exec.BufferNode,
) {
	b.withExprs = append(b.withExprs, builtWithExpr{
		id:         id,
		outputCols: outputCols,
		bufferNode: bufferNode,
	})
}

func (b *Builder) findBuiltWithExpr(id opt.WithID) *builtWithExpr {
	for i := range b.withExprs {
		if b.withExprs[i].id == id {
			return &b.withExprs[i]
		}
	}
	return nil
}
