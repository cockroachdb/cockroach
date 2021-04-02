// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// pushWithFrame pushes a new frame onto the CTE stack.
//
// The CTE stack is used to "hoist" CTEs to an appropriate boundary before
// constructing the With operators. For run-of-the-mill queries, there are no
// boundaries and all CTEs are hoisted at the top level. However, certain
// statements (e.g. EXPLAIN, CREATE VIEW, CREATE TABLE AS) must introduce a
// boundary.
func (b *Builder) pushWithFrame() {
	b.cteStack = append(b.cteStack, []cteSource{})
}

// addCTEToWithFrame adds a CTE to the current frame.
func (b *Builder) addCTEToWithFrame(cte cteSource) {
	level := len(b.cteStack) - 1
	b.cteStack[level] = append(b.cteStack[level], cte)
}

// popWithFrame pops a frame from the CTE stack and wraps the given scope's
// expression with With operators for the expressions in this frame.
func (b *Builder) popWithFrame(s *scope) {
	level := len(b.cteStack) - 1
	ctes := b.cteStack[level]
	b.cteStack = b.cteStack[:level]

	s.expr = b.buildWiths(s.expr, ctes)
}

// buildWiths adds With expressions on top of an expression.
func (b *Builder) buildWiths(expr memo.RelExpr, ctes []cteSource) memo.RelExpr {
	if len(ctes) == 0 {
		return expr
	}

	// Since later CTEs can refer to earlier ones, we want to add these in
	// reverse order.
	for i := len(ctes) - 1; i >= 0; i-- {
		expr = b.factory.ConstructWith(
			ctes[i].expr,
			expr,
			&memo.WithPrivate{
				ID:           ctes[i].id,
				Name:         string(ctes[i].name.Alias),
				Mtr:          ctes[i].mtr,
				OriginalExpr: ctes[i].originalExpr,
			},
		)
	}
	return expr
}

// processWiths is used when building a statement that has a WITH clause. It
// builds any CTEs defined by the WITH clause and calls the given function to
// build the statement itself.
func (b *Builder) processWiths(
	with *tree.With, inScope *scope, buildStmt func(inScope *scope) *scope,
) *scope {
	inScope = b.buildCTEs(with, inScope)
	prevAtRoot := inScope.atRoot
	inScope.atRoot = false
	outScope := buildStmt(inScope)
	inScope.atRoot = prevAtRoot
	return outScope
}

// buildCTEs constructs expressions for the CTEs defined by a WITH clause and
// adds them to the CTE stack.
func (b *Builder) buildCTEs(with *tree.With, inScope *scope) (outScope *scope) {
	if with == nil {
		return inScope
	}

	outScope = inScope.push()
	addedCTEs := make([]cteSource, len(with.CTEList))
	hasRecursive := false

	// Make a fake subquery to ensure that no CTEs are correlated.
	// TODO(justin): relax this restriction.
	outer := b.subquery
	defer func() { b.subquery = outer }()
	b.subquery = &subquery{}

	outScope.ctes = make(map[string]*cteSource)
	for i, cte := range with.CTEList {
		hasRecursive = hasRecursive || with.Recursive
		cteExpr, cteCols := b.buildCTE(cte, outScope, with.Recursive)

		// TODO(justin): lift this restriction when possible. WITH should be hoistable.
		if b.subquery != nil && !b.subquery.outerCols.Empty() {
			panic(pgerror.Newf(pgcode.FeatureNotSupported, "CTEs may not be correlated"))
		}

		aliasStr := cte.Name.Alias.String()
		if _, ok := outScope.ctes[aliasStr]; ok {
			panic(pgerror.Newf(
				pgcode.DuplicateAlias, "WITH query name %s specified more than once", aliasStr,
			))
		}

		id := b.factory.Memo().NextWithID()
		b.factory.Metadata().AddWithBinding(id, cteExpr)

		addedCTEs[i] = cteSource{
			name:         cte.Name,
			cols:         cteCols,
			originalExpr: cte.Stmt,
			expr:         cteExpr,
			id:           id,
			mtr:          cte.Mtr,
		}
		cte := &addedCTEs[i]
		outScope.ctes[cte.name.Alias.String()] = cte
		b.addCTEToWithFrame(*cte)

		if cteExpr.Relational().CanMutate && !inScope.atRoot {
			panic(
				pgerror.Newf(
					pgcode.FeatureNotSupported,
					"WITH clause containing a data-modifying statement must be at the top level",
				),
			)
		}

	}

	telemetry.Inc(sqltelemetry.CteUseCounter)
	if hasRecursive {
		telemetry.Inc(sqltelemetry.RecursiveCteUseCounter)
	}

	return outScope
}

// buildCTE constructs an expression for a CTE.
func (b *Builder) buildCTE(
	cte *tree.CTE, inScope *scope, isRecursive bool,
) (memo.RelExpr, physical.Presentation) {
	if !isRecursive {
		cteScope := b.buildStmt(cte.Stmt, nil /* desiredTypes */, inScope)
		cteScope.removeHiddenCols()
		b.dropOrderingAndExtraCols(cteScope)
		return cteScope.expr, b.getCTECols(cteScope, cte.Name)
	}

	// WITH RECURSIVE queries are always of the form:
	//
	//   WITH RECURSIVE name(cols) AS (
	//     initial_query
	//     UNION ALL
	//     recursive_query
	//   )
	//
	// Recursive CTE evaluation (paraphrased from postgres docs):
	//  1. Evaluate the initial query; emit the results and also save them in
	//     a "working" table.
	//  2. So long as the working table is not empty:
	//     * evaluate the recursive query, substituting the current contents of
	//       the working table for the recursive self-reference.
	//     * emit all resulting rows, and save them as the next iteration's
	//       working table.
	//
	// Note however, that a non-recursive CTE can be used even when RECURSIVE is
	// specified (particularly useful when there are multiple CTEs defined).
	// Handling this while having decent error messages is tricky.

	// Generate an id for the recursive CTE reference. This is the id through
	// which the recursive expression refers to the current working table
	// (via WithScan).
	withID := b.factory.Memo().NextWithID()

	// cteScope allows recursive references to this CTE.
	cteScope := inScope.push()
	cteSrc := &cteSource{
		id:   withID,
		name: cte.Name,
	}
	cteScope.ctes = map[string]*cteSource{cte.Name.Alias.String(): cteSrc}

	initial, recursive, isUnionAll, ok := b.splitRecursiveCTE(cte.Stmt)
	// We don't currently support the UNION form (only UNION ALL).
	if !ok || !isUnionAll {
		// Build this as a non-recursive CTE, but throw a proper error message if it
		// does have a recursive reference.
		cteSrc.onRef = func() {
			if !ok {
				panic(pgerror.Newf(
					pgcode.Syntax,
					"recursive query %q does not have the form non-recursive-term UNION ALL recursive-term",
					cte.Name.Alias,
				))
			} else {
				panic(unimplementedWithIssueDetailf(
					46642, "",
					"recursive query %q uses UNION which is not implemented (only UNION ALL is supported)",
					cte.Name.Alias,
				))
			}
		}
		return b.buildCTE(cte, cteScope, false /* recursive */)
	}

	// Set up an error if the initial part has a recursive reference.
	cteSrc.onRef = func() {
		panic(pgerror.Newf(
			pgcode.Syntax,
			"recursive reference to query %q must not appear within its non-recursive term",
			cte.Name.Alias,
		))
	}
	// If the initial statement contains CTEs, we don't want the Withs hoisted
	// above the recursive CTE.
	b.pushWithFrame()
	initialScope := b.buildStmt(initial, nil /* desiredTypes */, cteScope)
	b.popWithFrame(initialScope)

	initialScope.removeHiddenCols()
	b.dropOrderingAndExtraCols(initialScope)

	// The properties of the binding are tricky: the recursive expression is
	// invoked repeatedly and these must hold each time. We can't use the initial
	// expression's properties directly, as those only hold the first time the
	// recursive query is executed. We can't really say too much about what the
	// working table contains, except that it has at least one row (the recursive
	// query is never invoked with an empty working table).
	bindingProps := &props.Relational{}
	bindingProps.OutputCols = initialScope.colSet()
	bindingProps.Cardinality = props.AnyCardinality.AtLeast(props.OneCardinality)
	// We don't really know the input row count, except for the first time we run
	// the recursive query. We don't have anything better though.
	bindingProps.Stats.RowCount = initialScope.expr.Relational().Stats.RowCount
	// Row count must be greater than 0 or the stats code will throw an error.
	// Set it to 1 to match the cardinality.
	if bindingProps.Stats.RowCount < 1 {
		bindingProps.Stats.RowCount = 1
	}
	cteSrc.expr = b.factory.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: bindingProps,
	})
	b.factory.Metadata().AddWithBinding(withID, cteSrc.expr)

	cteSrc.cols = b.getCTECols(initialScope, cte.Name)

	outScope := inScope.push()

	initialTypes := initialScope.makeColumnTypes()

	// Synthesize new output columns (because they contain values from both the
	// initial and the recursive relations). These columns will also be used to
	// refer to the working table (from the recursive query); we can't use the
	// initial columns directly because they might contain duplicate IDs (e.g.
	// consider initial query SELECT 0, 0).
	for i, c := range cteSrc.cols {
		newCol := b.synthesizeColumn(outScope, c.Alias, initialTypes[i], nil /* expr */, nil /* scalar */)
		cteSrc.cols[i].ID = newCol.id
	}

	// We want to check if the recursive query is actually recursive. This is for
	// annoying cases like `SELECT 1 UNION ALL SELECT 2`.
	numRefs := 0
	cteSrc.onRef = func() {
		numRefs++
	}

	// If the recursive statement contains CTEs, we don't want the Withs hoisted
	// above the recursive CTE.
	b.pushWithFrame()
	recursiveScope := b.buildStmt(recursive, initialTypes /* desiredTypes */, cteScope)
	b.popWithFrame(recursiveScope)

	if numRefs == 0 {
		// Build this as a non-recursive CTE.
		cteScope := b.buildSetOp(tree.UnionOp, false /* all */, inScope, initialScope, recursiveScope)
		return cteScope.expr, b.getCTECols(cteScope, cte.Name)
	}

	if numRefs != 1 {
		// We disallow multiple recursive references for consistency with postgres.
		panic(pgerror.Newf(
			pgcode.Syntax,
			"recursive reference to query %q must not appear more than once",
			cte.Name.Alias,
		))
	}

	recursiveScope.removeHiddenCols()
	b.dropOrderingAndExtraCols(recursiveScope)

	// We allow propagation of types from the initial query to the recursive
	// query.
	outTypes, leftCastsNeeded, rightCastsNeeded := b.typeCheckSetOp(initialScope, recursiveScope, "UNION")
	if leftCastsNeeded {
		initialScope = b.addCasts(initialScope, outTypes)
	}
	if rightCastsNeeded {
		recursiveScope = b.addCasts(recursiveScope, outTypes)
	}

	private := memo.RecursiveCTEPrivate{
		Name:          string(cte.Name.Alias),
		WithID:        withID,
		InitialCols:   colsToColList(initialScope.cols),
		RecursiveCols: colsToColList(recursiveScope.cols),
		OutCols:       colsToColList(outScope.cols),
	}

	expr := b.factory.ConstructRecursiveCTE(cteSrc.expr, initialScope.expr, recursiveScope.expr, &private)
	return expr, cteSrc.cols
}

// getCTECols returns a presentation for the scope, renaming the columns to
// those provided in the AliasClause (if any). Throws an error if there is a
// mismatch in the number of columns.
func (b *Builder) getCTECols(cteScope *scope, name tree.AliasClause) physical.Presentation {
	presentation := cteScope.makePresentation()

	if len(presentation) == 0 {
		err := pgerror.Newf(
			pgcode.FeatureNotSupported,
			"WITH clause %q does not return any columns",
			tree.ErrString(&name),
		)
		panic(errors.WithHint(err, "missing RETURNING clause?"))
	}

	if name.Cols == nil {
		return presentation
	}

	if len(presentation) != len(name.Cols) {
		panic(pgerror.Newf(
			pgcode.InvalidColumnReference,
			"source %q has %d columns available but %d columns specified",
			name.Alias, len(presentation), len(name.Cols),
		))
	}
	for i := range presentation {
		presentation[i].Alias = string(name.Cols[i])
	}
	return presentation
}

// splitRecursiveCTE splits a CTE statement of the form
//   initial_query UNION ALL recursive_query
// into the initial and recursive parts. If the statement is not of this form,
// returns ok=false.
func (b *Builder) splitRecursiveCTE(
	stmt tree.Statement,
) (initial, recursive *tree.Select, isUnionAll bool, ok bool) {
	sel, ok := stmt.(*tree.Select)
	// The form above doesn't allow for "outer" WITH, ORDER BY, or LIMIT
	// clauses.
	if !ok || sel.With != nil || sel.OrderBy != nil || sel.Limit != nil {
		return nil, nil, false, false
	}
	union, ok := sel.Select.(*tree.UnionClause)
	if !ok || union.Type != tree.UnionOp {
		return nil, nil, false, false
	}
	return union.Left, union.Right, union.All, true
}
