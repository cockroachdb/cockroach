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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

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
	cteSrc.bindingProps = bindingProps

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
	_, propagateToRight := b.checkTypesMatch(initialScope, recursiveScope,
		false, /* tolerateUnknownLeft */
		true,  /* tolerateUnknownRight */
		"UNION",
	)
	if propagateToRight {
		recursiveScope = b.propagateTypes(recursiveScope /* dst */, initialScope /* src */)
	}

	private := memo.RecursiveCTEPrivate{
		Name:          string(cte.Name.Alias),
		WithID:        withID,
		InitialCols:   colsToColList(initialScope.cols),
		RecursiveCols: colsToColList(recursiveScope.cols),
		OutCols:       colsToColList(outScope.cols),
	}

	expr := b.factory.ConstructRecursiveCTE(initialScope.expr, recursiveScope.expr, &private)
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
