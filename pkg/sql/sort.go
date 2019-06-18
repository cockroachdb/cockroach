// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// sortNode represents a node that sorts the rows returned by its
// sub-node.
type sortNode struct {
	plan     planNode
	columns  sqlbase.ResultColumns
	ordering sqlbase.ColumnOrdering

	needSort bool
}

// orderBy constructs a sortNode based on the ORDER BY clause.
//
// In the general case (SELECT/UNION/VALUES), we can sort by a column index or a
// column name.
//
// However, for a SELECT, we can also sort by the pre-alias column name (SELECT
// a AS b ORDER BY b) as well as expressions (SELECT a, b, ORDER BY a+b). In
// this case, construction of the sortNode might adjust the number of render
// targets in the renderNode if any ordering expressions are specified.
//
// TODO(dan): SQL also allows sorting a VALUES or UNION by an expression.
// Support this. It will reduce some of the special casing below, but requires a
// generalization of how to add derived columns to a SelectStatement.
func (p *planner) orderBy(
	ctx context.Context, orderBy tree.OrderBy, n planNode,
) (*sortNode, error) {
	if orderBy == nil {
		return nil, nil
	}

	// Multiple tests below use renderNode as a special case.
	// So factor the cast.
	s, _ := n.(*renderNode)

	// We grab a copy of columns here because we might add new render targets
	// below. This is the set of columns requested by the query.
	columns := planColumns(n)
	numOriginalCols := len(columns)
	if s != nil {
		numOriginalCols = s.numOriginalCols
	}
	var ordering sqlbase.ColumnOrdering

	var err error
	orderBy, err = p.rewriteIndexOrderings(ctx, orderBy)
	if err != nil {
		return nil, err
	}

	// We need to reject SRFs in the ORDER BY clause until they are
	// properly handled.
	seenGenerator := p.semaCtx.Properties.Derived.SeenGenerator
	p.semaCtx.Properties.Derived.SeenGenerator = false
	defer func() {
		p.semaCtx.Properties.Derived.SeenGenerator = p.semaCtx.Properties.Derived.SeenGenerator || seenGenerator
	}()

	for _, o := range orderBy {
		direction := encoding.Ascending
		if o.Direction == tree.Descending {
			direction = encoding.Descending
		}

		// Unwrap parenthesized expressions like "((a))" to "a".
		expr := tree.StripParens(o.Expr)

		// The logical data source for ORDER BY is the list of render
		// expressions for a SELECT, as specified in the input SQL text
		// (or an entire UNION or VALUES clause).  Alas, SQL has some
		// historical baggage from SQL92 and there are some special cases:
		//
		// SQL92 rules:
		//
		// 1) if the expression is the aliased (AS) name of a render
		//    expression in a SELECT clause, then use that
		//    render as sort key.
		//    e.g. SELECT a AS b, b AS c ORDER BY b
		//    this sorts on the first render.
		//
		// 2) column ordinals. If a simple integer literal is used,
		//    optionally enclosed within parentheses but *not subject to
		//    any arithmetic*, then this refers to one of the columns of
		//    the data source. Then use the render expression at that
		//    ordinal position as sort key.
		//
		// SQL99 rules:
		//
		// 3) otherwise, if the expression is already in the render list,
		//    then use that render as sort key.
		//    e.g. SELECT b AS c ORDER BY b
		//    this sorts on the first render.
		//    (this is an optimization)
		//
		// 4) if the sort key is not dependent on the data source (no
		//    IndexedVar) then simply do not sort. (this is an optimization)
		//
		// 5) otherwise, add a new render with the ORDER BY expression
		//    and use that as sort key.
		//    e.g. SELECT a FROM t ORDER by b
		//    e.g. SELECT a, b FROM t ORDER by a+b

		// First, deal with render aliases.
		index, err := s.colIdxByRenderAlias(expr, columns, "ORDER BY")
		if err != nil {
			return nil, err
		}

		// If the expression does not refer to an alias, deal with
		// column ordinals.
		if index == -1 {
			col, err := p.colIndex(numOriginalCols, expr, "ORDER BY")
			if err != nil {
				return nil, err
			}
			if col != -1 {
				index = col
			}
		}

		// If we're ordering by using one of the existing renders, ensure it's a
		// column type we can order on.
		if index != -1 {
			if err := ensureColumnOrderable(columns[index]); err != nil {
				return nil, err
			}
		}

		// Finally, if we haven't found anything so far, we really
		// need a new render.
		// TODO(knz/dan): currently this is only possible for renderNode.
		// If we are dealing with a UNION or something else we would need
		// to fabricate an intermediate renderNode to add the new render.
		if index == -1 && s != nil {
			cols, exprs, hasStar, err := p.computeRenderAllowingStars(
				ctx, tree.SelectExpr{Expr: expr}, types.Any,
				s.sourceInfo, s.ivarHelper, autoGenerateRenderOutputName)
			if err != nil {
				return nil, err
			}
			p.curPlan.hasStar = p.curPlan.hasStar || hasStar

			if len(cols) == 0 {
				// Nothing was expanded! No order here.
				continue
			}

			// ORDER BY (a, b) -> ORDER BY a, b
			cols, exprs = flattenTuples(cols, exprs, &s.ivarHelper)

			colIdxs := s.addOrReuseRenders(cols, exprs, true)
			for i := 0; i < len(colIdxs)-1; i++ {
				// If more than 1 column were expanded, turn them into sort columns too.
				// Except the last one, which will be added below.
				ordering = append(ordering,
					sqlbase.ColumnOrderInfo{ColIdx: colIdxs[i], Direction: direction})
			}
			index = colIdxs[len(colIdxs)-1]
			// Ensure our newly rendered columns are ok to order by.
			renderCols := planColumns(s)
			for _, colIdx := range colIdxs {
				if err := ensureColumnOrderable(renderCols[colIdx]); err != nil {
					return nil, err
				}
			}
		}

		if index == -1 {
			return nil, sqlbase.NewUndefinedColumnError(expr.String())
		}
		ordering = append(ordering,
			sqlbase.ColumnOrderInfo{ColIdx: index, Direction: direction})
	}

	if p.semaCtx.Properties.Derived.SeenGenerator {
		return nil, tree.NewInvalidFunctionUsageError(tree.GeneratorClass, "ORDER BY")
	}

	if ordering == nil {
		// No ordering; simply drop the sort node.
		return nil, nil
	}
	return &sortNode{columns: columns, ordering: ordering}, nil
}

func (n *sortNode) startExec(runParams) error {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Next(params runParams) (bool, error) {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Values() tree.Datums {
	panic("sortNode cannot be run in local mode")
}

func (n *sortNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}

func ensureColumnOrderable(c sqlbase.ResultColumn) error {
	if c.Typ.Family() == types.ArrayFamily {
		return unimplemented.NewWithIssuef(32707, "can't order by column type %s", c.Typ)
	}
	if c.Typ.Family() == types.JsonFamily {
		return unimplemented.NewWithIssue(32706, "can't order by column type jsonb")
	}
	return nil
}

// flattenTuples extracts the members of tuples into a list of columns.
func flattenTuples(
	cols sqlbase.ResultColumns, exprs []tree.TypedExpr, ivarHelper *tree.IndexedVarHelper,
) (sqlbase.ResultColumns, []tree.TypedExpr) {
	// We want to avoid allocating new slices unless strictly necessary.
	var newExprs []tree.TypedExpr
	var newCols sqlbase.ResultColumns
	for i, e := range exprs {
		if t, ok := e.(*tree.Tuple); ok {
			if newExprs == nil {
				// All right, it was necessary to allocate the slices after all.
				newExprs = make([]tree.TypedExpr, i, len(exprs))
				newCols = make(sqlbase.ResultColumns, i, len(cols))
				copy(newExprs, exprs[:i])
				copy(newCols, cols[:i])
			}

			newCols, newExprs = flattenTuple(t, newCols, newExprs, ivarHelper)
		} else if newExprs != nil {
			newExprs = append(newExprs, e)
			newCols = append(newCols, cols[i])
		}
	}
	if newExprs != nil {
		return newCols, newExprs
	}
	return cols, exprs
}

// flattenTuple extracts the members of one tuple into a list of columns.
func flattenTuple(
	t *tree.Tuple,
	cols sqlbase.ResultColumns,
	exprs []tree.TypedExpr,
	ivarHelper *tree.IndexedVarHelper,
) (sqlbase.ResultColumns, []tree.TypedExpr) {
	for _, e := range t.Exprs {
		if eT, ok := e.(*tree.Tuple); ok {
			cols, exprs = flattenTuple(eT, cols, exprs, ivarHelper)
		} else {
			expr := e.(tree.TypedExpr)
			exprs = append(exprs, expr)
			cols = append(cols, sqlbase.ResultColumn{
				Name: expr.String(),
				Typ:  expr.ResolvedType(),
			})
		}
	}
	return cols, exprs
}

// rewriteIndexOrderings rewrites an ORDER BY clause that uses the
// extended INDEX or PRIMARY KEY syntax into an ORDER BY clause that
// doesn't: each INDEX or PRIMARY KEY order specification is replaced
// by the list of columns in the specified index.
// For example,
//   ORDER BY PRIMARY KEY kv -> ORDER BY kv.k ASC
//   ORDER BY INDEX kv@primary -> ORDER BY kv.k ASC
// With an index foo(a DESC, b ASC):
//   ORDER BY INDEX t@foo ASC -> ORDER BY t.a DESC, t.a ASC
//   ORDER BY INDEX t@foo DESC -> ORDER BY t.a ASC, t.b DESC
func (p *planner) rewriteIndexOrderings(
	ctx context.Context, orderBy tree.OrderBy,
) (tree.OrderBy, error) {
	// The loop above *may* allocate a new slice, but this is only
	// needed if the INDEX / PRIMARY KEY syntax is used. In case the
	// ORDER BY clause only uses the column syntax, we should reuse the
	// same slice.
	newOrderBy := orderBy
	rewrite := false
	for i, o := range orderBy {
		switch o.OrderType {
		case tree.OrderByColumn:
			// Nothing to do, just propagate the setting.
			if rewrite {
				// We only need to-reappend if we've allocated a new slice
				// (see below).
				newOrderBy = append(newOrderBy, o)
			}

		case tree.OrderByIndex:
			tn := o.Table
			desc, err := ResolveExistingObject(ctx, p, &tn, true /*required*/, ResolveRequireTableDesc)
			if err != nil {
				return nil, err
			}

			// Force the name to be fully qualified so that the column references
			// below only match against real tables in the FROM clause.
			tn.ExplicitCatalog = true
			tn.ExplicitSchema = true

			var idxDesc *sqlbase.IndexDescriptor
			if o.Index == "" || string(o.Index) == desc.PrimaryIndex.Name {
				// ORDER BY PRIMARY KEY / ORDER BY INDEX t@primary
				idxDesc = &desc.PrimaryIndex
			} else {
				// ORDER BY INDEX t@somename
				// We need to search for the index with that name.
				for i := range desc.Indexes {
					if string(o.Index) == desc.Indexes[i].Name {
						idxDesc = &desc.Indexes[i]
						break
					}
				}
				if idxDesc == nil {
					return nil, errors.Errorf("index %q not found", tree.ErrString(&o.Index))
				}
			}

			// Now, expand the ORDER BY clause by an equivalent clause using that
			// index's columns.

			if !rewrite {
				// First, make a final slice that's intelligently larger.
				newOrderBy = make(tree.OrderBy, i, len(orderBy)+len(idxDesc.ColumnNames)-1)
				copy(newOrderBy, orderBy[:i])
				rewrite = true
			}

			// Now expand the clause.
			for k, colName := range idxDesc.ColumnNames {
				newOrderBy = append(newOrderBy, &tree.Order{
					OrderType: tree.OrderByColumn,
					Expr:      tree.NewColumnItem(&tn, tree.Name(colName)),
					Direction: chooseDirection(o.Direction == tree.Descending, idxDesc.ColumnDirections[k]),
				})
			}

			for _, id := range idxDesc.ExtraColumnIDs {
				col, err := desc.FindColumnByID(id)
				if err != nil {
					return nil, errors.AssertionFailedf("column with ID %d not found", id)
				}

				newOrderBy = append(newOrderBy, &tree.Order{
					OrderType: tree.OrderByColumn,
					Expr:      tree.NewColumnItem(&tn, tree.Name(col.Name)),
					Direction: chooseDirection(o.Direction == tree.Descending, sqlbase.IndexDescriptor_ASC),
				})
			}

		default:
			return nil, errors.Errorf("unknown ORDER BY specification: %s", tree.ErrString(&orderBy))
		}
	}

	if log.V(2) {
		log.Infof(ctx, "rewritten ORDER BY clause: %s", tree.ErrString(&newOrderBy))
	}
	return newOrderBy, nil
}

// chooseDirection translates the specified IndexDescriptor_Direction
// into a tree.Direction. If invert is true, the idxDir is inverted.
func chooseDirection(invert bool, idxDir sqlbase.IndexDescriptor_Direction) tree.Direction {
	if (idxDir == sqlbase.IndexDescriptor_ASC) != invert {
		return tree.Ascending
	}
	return tree.Descending
}

// colIndex takes an expression that refers to a column using an integer, verifies it refers to a
// valid render target and returns the corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first render target "a". The returned index is 0.
func (p *planner) colIndex(numOriginalCols int, expr tree.Expr, context string) (int, error) {
	ord := int64(-1)
	switch i := expr.(type) {
	case *tree.NumVal:
		if i.ShouldBeInt64() {
			val, err := i.AsInt64()
			if err != nil {
				return -1, err
			}
			ord = val
		} else {
			return -1, pgerror.Newf(
				pgcode.Syntax,
				"non-integer constant in %s: %s", context, expr,
			)
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		return -1, pgerror.Newf(
			pgcode.Syntax, "non-integer constant in %s: %s", context, expr,
		)
	case tree.Datum:
		return -1, pgerror.Newf(
			pgcode.Syntax, "non-integer constant in %s: %s", context, expr,
		)
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			return -1, pgerror.Newf(
				pgcode.InvalidColumnReference,
				"%s position %s is not in select list", context, expr,
			)
		}
		ord--
	}
	return int(ord), nil
}
