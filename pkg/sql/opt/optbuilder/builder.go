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

package optbuilder

import (
	"context"
	"fmt"

	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type unaryFactoryFunc func(f opt.Factory, input opt.GroupID) opt.GroupID
type binaryFactoryFunc func(f opt.Factory, left, right opt.GroupID) opt.GroupID

// Map from tree.ComparisonOperator to Factory constructor function.
var comparisonOpMap = [...]binaryFactoryFunc{
	tree.EQ:                (opt.Factory).ConstructEq,
	tree.LT:                (opt.Factory).ConstructLt,
	tree.GT:                (opt.Factory).ConstructGt,
	tree.LE:                (opt.Factory).ConstructLe,
	tree.GE:                (opt.Factory).ConstructGe,
	tree.NE:                (opt.Factory).ConstructNe,
	tree.In:                (opt.Factory).ConstructIn,
	tree.NotIn:             (opt.Factory).ConstructNotIn,
	tree.Like:              (opt.Factory).ConstructLike,
	tree.NotLike:           (opt.Factory).ConstructNotLike,
	tree.ILike:             (opt.Factory).ConstructILike,
	tree.NotILike:          (opt.Factory).ConstructNotILike,
	tree.SimilarTo:         (opt.Factory).ConstructSimilarTo,
	tree.NotSimilarTo:      (opt.Factory).ConstructNotSimilarTo,
	tree.RegMatch:          (opt.Factory).ConstructRegMatch,
	tree.NotRegMatch:       (opt.Factory).ConstructNotRegMatch,
	tree.RegIMatch:         (opt.Factory).ConstructRegIMatch,
	tree.NotRegIMatch:      (opt.Factory).ConstructNotRegIMatch,
	tree.IsDistinctFrom:    (opt.Factory).ConstructIsNot,
	tree.IsNotDistinctFrom: (opt.Factory).ConstructIs,
	tree.Contains:          (opt.Factory).ConstructContains,

	// TODO(rytaft): NYI, but these need to be nil to avoid an index out of
	// range exception if any of these functions are used.
	tree.ContainedBy:    nil,
	tree.JSONExists:     nil,
	tree.JSONSomeExists: nil,
	tree.JSONAllExists:  nil,
	tree.Any:            nil,
	tree.Some:           nil,
	tree.All:            nil,
}

// Map from tree.BinaryOperator to Factory constructor function.
var binaryOpMap = [...]binaryFactoryFunc{
	tree.Bitand:   (opt.Factory).ConstructBitand,
	tree.Bitor:    (opt.Factory).ConstructBitor,
	tree.Bitxor:   (opt.Factory).ConstructBitxor,
	tree.Plus:     (opt.Factory).ConstructPlus,
	tree.Minus:    (opt.Factory).ConstructMinus,
	tree.Mult:     (opt.Factory).ConstructMult,
	tree.Div:      (opt.Factory).ConstructDiv,
	tree.FloorDiv: (opt.Factory).ConstructFloorDiv,
	tree.Mod:      (opt.Factory).ConstructMod,
	tree.Pow:      (opt.Factory).ConstructPow,
	tree.Concat:   (opt.Factory).ConstructConcat,
	tree.LShift:   (opt.Factory).ConstructLShift,
	tree.RShift:   (opt.Factory).ConstructRShift,
}

// Map from tree.UnaryOperator to Factory constructor function.
var unaryOpMap = [...]unaryFactoryFunc{
	tree.UnaryPlus:       (opt.Factory).ConstructUnaryPlus,
	tree.UnaryMinus:      (opt.Factory).ConstructUnaryMinus,
	tree.UnaryComplement: (opt.Factory).ConstructUnaryComplement,
}

// Builder holds the context needed for building a memo structure from a SQL
// statement. Builder.Build() is the top-level function to perform this build
// process. As part of the build process, it performs name resolution and
// type checking on the expressions within Builder.stmt.
//
// The memo structure is the primary data structure used for query
// optimization, so building the memo is the first step required to
// optimize a query. The memo is maintained inside Builder.factory,
// which exposes methods to construct expression groups inside the memo.
//
// A memo is essentially a compact representation of a forest of logically-
// equivalent query trees. Each tree is either a logical or a physical plan
// for executing the SQL query. After the build process is complete, the memo
// forest will contain exactly one tree: the logical query plan corresponding
// to the AST of the original SQL statement with some number of "normalization"
// transformations applied. Normalization transformations include heuristics
// such as predicate push-down that should always be applied. They do not
// include "exploration" transformations whose benefit must be evaluated with
// the optimizer's cost model (e.g., join reordering).
//
// See factory.go and memo.go inside the opt/xform package for more details
// about the memo structure.
type Builder struct {
	factory opt.Factory
	stmt    tree.Statement
	semaCtx tree.SemaContext
	ctx     context.Context

	// Skip index 0 in order to reserve it to indicate the "unknown" column.
	colMap []columnProps
}

// New creates a new Builder structure initialized with the given
// Context, Factory, and parsed SQL statement.
func New(ctx context.Context, factory opt.Factory, stmt tree.Statement) *Builder {
	b := &Builder{factory: factory, stmt: stmt, colMap: make([]columnProps, 1), ctx: ctx}

	b.semaCtx.Placeholders = tree.MakePlaceholderInfo()

	return b
}

// Build is the top-level function to build the memo structure inside
// Builder.factory from the parsed SQL statement in Builder.stmt. See the
// comment above the Builder type declaration for details.
//
// The first return value `root` is the group ID of the root memo group.
// The second return value `required` is the set of physical properties
// (e.g., row and column ordering) that are required of the root memo group.
// If any subroutines panic with a builderError as part of the build process,
// the panic is caught here and returned as an error.
func (b *Builder) Build() (root opt.GroupID, required *opt.PhysicalProps, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate builder errors without adding
			// lots of checks for `if err != nil` throughout the code. This is
			// only possible because the code does not update shared state and does
			// not manipulate locks.
			if bldErr, ok := r.(builderError); ok {
				err = bldErr
			} else {
				panic(r)
			}
		}
	}()

	out, _ := b.buildStmt(b.stmt, &scope{builder: b})
	root = out

	// TODO(rytaft): Add physical properties that are required of the root memo
	// group.
	required = &opt.PhysicalProps{}
	return root, required, nil
}

// builderError is used for semantic errors that occur during the build process
// and is passed as an argument to panic. These panics are caught and converted
// back to errors inside Builder.Build.
type builderError struct {
	error
}

// errorf formats according to a format specifier and returns the
// string as a builderError.
func errorf(format string, a ...interface{}) builderError {
	err := fmt.Errorf(format, a...)
	return builderError{err}
}

// buildStmt builds a set of memo groups that represent the given SQL
// statement.
//
// NOTE: The following description of the inScope parameter and return values
//       applies for all buildXXX() functions in this file.
//
// inScope   This parameter contains the name bindings that are visible for this
//           statement/expression (e.g., passed in from an enclosing statement).
//
// out       This return value corresponds to the top-level memo group ID for
//           this statement/expression.
//
// outScope  This return value contains the newly bound variables that will be
//           visible to enclosing statements, as well as a pointer to any
//           "parent" scope that is still visible.
func (b *Builder) buildStmt(
	stmt tree.Statement, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, inScope)

	case *tree.Select:
		return b.buildSelect(stmt, inScope)

	default:
		panic(errorf("unexpected statement: %T", stmt))
	}
}

// buildTable builds a set of memo groups that represent the given table
// expression. For example, if the tree.TableExpr consists of a single table,
// the resulting set of memo groups will consist of a single group with a
// scanOp operator. Joins will result in the construction of several groups,
// including two for the left and right table scans, at least one for the join
// condition, and one for the join itself.
// TODO(rytaft): Add support for function and join table expressions.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildTable(
	texpr tree.TableExpr, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		out, outScope = b.buildTable(source.Expr, inScope)

		// Overwrite output properties with any alias information.
		if source.As.Alias != "" {
			if n := len(source.As.Cols); n > 0 && n != len(outScope.cols) {
				panic(errorf("rename specified %d columns, but table contains %d", n, len(outScope.cols)))
			}

			for i := range outScope.cols {
				outScope.cols[i].table = optbase.TableName(source.As.Alias)
				if i < len(source.As.Cols) {
					outScope.cols[i].name = optbase.ColumnName(source.As.Cols[i])
				}
			}
		}

		return out, outScope

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			panic(builderError{err})
		}
		tbl, err := b.factory.Metadata().Catalog().FindTable(b.ctx, tn)
		if err != nil {
			panic(builderError{err})
		}

		return b.buildScan(tbl, inScope)

	case *tree.ParenTableExpr:
		return b.buildTable(source.Expr, inScope)

	case *tree.Subquery:
		return b.buildStmt(source.Select, inScope)

	default:
		panic(errorf("not yet implemented: table expr: %T", texpr))
	}
}

// buildScan builds a memo group for a scanOp expression on the given table.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildScan(tbl optbase.Table, inScope *scope) (out opt.GroupID, outScope *scope) {
	tblIndex := b.factory.Metadata().AddTable(tbl)

	outScope = inScope.push()
	for i := 0; i < tbl.NumColumns(); i++ {
		col := tbl.Column(i)
		colIndex := b.factory.Metadata().TableColumn(tblIndex, i)
		colProps := columnProps{
			index:  colIndex,
			name:   col.ColName(),
			table:  tbl.TabName(),
			typ:    col.DatumType(),
			hidden: col.IsHidden(),
		}

		b.colMap = append(b.colMap, colProps)
		outScope.cols = append(outScope.cols, colProps)
	}

	return b.factory.ConstructScan(b.factory.InternPrivate(tblIndex)), outScope
}

// buildScalar builds a set of memo groups that represent the given scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildScalar(scalar tree.TypedExpr, inScope *scope) (out opt.GroupID) {
	inGroupingContext := inScope.inGroupingContext() && !inScope.groupby.inAgg
	varsUsedIn := len(inScope.groupby.varsUsed)
	switch t := scalar.(type) {
	case *columnProps:
		if inGroupingContext && !inScope.groupby.groupingsScope.hasColumn(t.index) {
			inScope.groupby.varsUsed = append(inScope.groupby.varsUsed, t.index)
		}
		return b.factory.ConstructVariable(b.factory.InternPrivate(t.index))

	case *tree.AndExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		conditions := b.factory.InternList([]opt.GroupID{left, right})
		out = b.factory.ConstructAnd(conditions)

	case *tree.BinaryExpr:
		out = binaryOpMap[t.Operator](b.factory,
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope),
		)

	case *tree.ComparisonExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		fn := comparisonOpMap[t.Operator]
		if fn != nil {
			// Most comparison ops map directly to a factory method.
			out = fn(b.factory, left, right)
		} else {
			// Several comparison ops need special handling.
			// TODO(andyk): handle t.SubOperator. Do this by mapping Any, Some,
			// and All to various formulations of the opt Exists operator.
			switch t.Operator {
			case tree.ContainedBy:
				// This is just syntatic sugar that reverses the operands.
				out = b.factory.ConstructContains(right, left)

			default:
				// TODO(rytaft): remove this check when we are confident that
				// all operators are included in comparisonOpMap.
				panic(errorf("not yet implemented: operator %s", t.Operator.String()))
			}
		}

	case *tree.DTuple:
		list := make([]opt.GroupID, len(t.D))
		for i := range t.D {
			list[i] = b.buildScalar(t.D[i], inScope)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list))

	case *tree.FuncExpr:
		out, _ = b.buildFunction(t, "", inScope)

	case *tree.IndexedVar:
		if t.Idx < 0 || t.Idx >= len(inScope.cols) {
			panic(errorf("invalid column ordinal @%d", t.Idx))
		}
		out = b.factory.ConstructVariable(b.factory.InternPrivate(inScope.cols[t.Idx].index))
		// TODO(rytaft): Do we need to update varsUsed here?

	case *tree.NotExpr:
		out = b.factory.ConstructNot(b.buildScalar(t.TypedInnerExpr(), inScope))

	case *tree.OrExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		conditions := b.factory.InternList([]opt.GroupID{left, right})
		out = b.factory.ConstructOr(conditions)

	case *tree.ParenExpr:
		out = b.buildScalar(t.TypedInnerExpr(), inScope)

	case *tree.Placeholder:
		out = b.factory.ConstructPlaceholder(b.factory.InternPrivate(t))
		// TODO(rytaft): Do we need to update varsUsed here?

	case *tree.Tuple:
		list := make([]opt.GroupID, len(t.Exprs))
		for i := range t.Exprs {
			list[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope)
		}
		out = b.factory.ConstructTuple(b.factory.InternList(list))

	case *tree.UnaryExpr:
		out = unaryOpMap[t.Operator](b.factory, b.buildScalar(t.TypedInnerExpr(), inScope))

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		// Map True/False datums to True/False operator.
		if t == tree.DBoolTrue {
			out = b.factory.ConstructTrue()
		} else if t == tree.DBoolFalse {
			out = b.factory.ConstructFalse()
		} else {
			out = b.factory.ConstructConst(b.factory.InternPrivate(t))
		}

	default:
		panic(errorf("not yet implemented: scalar expr: %T", scalar))
	}

	// If we are in a grouping context and this expression corresponds to
	// a GROUP BY expression, truncate varsUsed.
	if inGroupingContext {
		// TODO(rytaft): This currently regenerates a string for each subexpression.
		// Change this to generate the string once for the top-level expression and
		// check the relevant slice for this subexpression.
		if _, ok := inScope.groupby.groupStrs[symbolicExprStr(scalar)]; ok {
			inScope.groupby.varsUsed = inScope.groupby.varsUsed[:varsUsedIn]
		}
	}

	return out
}

// symbolicExprStr returns a string representation of the expression using
// symbolic notation. Because the symbolic notation disambiguates columns, this
// string can be used to determine if two expressions are equivalent.
func symbolicExprStr(expr tree.Expr) string {
	return tree.AsStringWithFlags(expr, tree.FmtCheckEquivalence)
}

// buildFunction builds a set of memo groups that represent a function
// expression.
//
// f       The given function expression.
// label   If a new column is synthesized, it will be labeled with this
//         string.
//
// If the function is an aggregate, the second return value, col,
// corresponds to the columnProps that represents the aggregate.
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildFunction(
	f *tree.FuncExpr, label string, inScope *scope,
) (out opt.GroupID, col *columnProps) {
	def, err := f.Func.Resolve(b.semaCtx.SearchPath)
	if err != nil {
		panic(builderError{err})
	}

	isAgg := isAggregate(def)
	if isAgg {
		inScope.startAggFunc()
	}

	argList := make([]opt.GroupID, 0, len(f.Exprs))
	for _, pexpr := range f.Exprs {
		var arg opt.GroupID
		if _, ok := pexpr.(tree.UnqualifiedStar); ok {
			arg = b.factory.ConstructConst(b.factory.InternPrivate(tree.NewDInt(1)))
		} else {
			arg = b.buildScalar(pexpr.(tree.TypedExpr), inScope)
		}

		argList = append(argList, arg)
	}

	out = b.factory.ConstructFunction(b.factory.InternList(argList), b.factory.InternPrivate(def))

	if isAgg {
		refScope := inScope.endAggFunc()

		// If the aggregate already exists as a column, use that. Otherwise
		// create a new column and add it the list of aggregates that need to
		// be computed by the groupby expression.
		col = refScope.findAggregate(out)
		if col == nil {
			col = b.synthesizeColumn(refScope, label, f.ResolvedType())

			// Add the aggregate to the list of aggregates that need to be computed by
			// the groupby expression.
			refScope.groupby.aggs = append(refScope.groupby.aggs, out)
		}

		// Replace the function call with a reference to the column.
		out = b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
	}

	return out, col
}

// buildSelect builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(
	stmt *tree.Select, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch t := stmt.Select.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(t.Select, inScope)

	case *tree.SelectClause:
		return b.buildSelectClause(stmt, inScope)

	// TODO(rytaft): Add support for union clause and values clause.

	default:
		panic(errorf("not yet implemented: select statement: %T", stmt.Select))
	}

	// TODO(rytaft): Add support for ORDER BY expression.
	// TODO(rytaft): Support FILTER expression.
	// TODO(peter): stmt.Limit
}

// buildSelectClause builds a set of memo groups that represent the given
// select clause. We pass the entire select statement rather than just the
// select clause in order to handle ORDER BY scoping rules. ORDER BY can sort
// results using columns from the FROM/GROUP BY clause and/or from the
// projection list.
// TODO(rytaft): Add support for ORDER BY, DISTINCT and HAVING.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildSelectClause(
	stmt *tree.Select, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	sel := stmt.Select.(*tree.SelectClause)

	var fromScope, groupingsScope, projectionsScope *scope
	out, fromScope = b.buildFrom(sel.From, sel.Where, inScope)

	var groupings []opt.GroupID
	var having opt.GroupID

	// We have an aggregation if:
	//  - we have a GROUP BY, or
	//  - we have a HAVING clause, or
	//  - we have aggregate functions in the select expressions.
	if len(sel.GroupBy) > 0 || sel.Having != nil || b.hasAggregates(sel.Exprs) {
		// After GROUP BY, variables from fromScope are hidden.
		groupingsScope = fromScope.replace()
		// The "from" columns are visible to any grouping expressions.
		groupings = b.buildGroupingList(sel.GroupBy, sel.Exprs, fromScope, groupingsScope)
		fromScope.groupby.groupings = groupings
		// Set the grouping scope so that any aggregates will be added to the set
		// of grouping columns.
		fromScope.groupby.groupingsScope = groupingsScope
		projectionsScope = fromScope.replace()

		// Any "grouping" columns are visible to both the "having" and "projection"
		// expressions. The build has the side effect of extracting aggregation
		// columns.
		if sel.Having != nil {
			having = b.buildHaving(sel.Having.Expr, fromScope)
		}
	} else {
		projectionsScope = fromScope.push()
	}

	// Note: if we have an aggregation and the expressions contain aggregate
	// functions for this aggregation, this call will populate
	// groupingsScope.groupby.aggs.
	projections := b.buildProjectionList(sel.Exprs, fromScope, projectionsScope)

	// Wrap with groupby operator if groupings or aggregates exist.
	if groupingsScope != nil {
		// Any aggregate columns that were discovered would have been added to
		// the grouping scope.
		aggCols := groupingsScope.getAggregateCols()
		aggList := b.constructList(opt.AggregationsOp, groupingsScope.groupby.aggs, aggCols)

		var groupingCols []columnProps
		if len(groupings) > 0 {
			groupingCols = groupingsScope.cols
		}

		groupingList := b.constructList(opt.GroupingsOp, groupings, groupingCols)
		out = b.factory.ConstructGroupBy(out, groupingList, aggList)

		// Wrap with having filter if it exists.
		if having != 0 {
			out = b.factory.ConstructSelect(out, having)
		}

		outScope = groupingsScope
	} else {
		// No aggregates, so current output scope is the "from" scope.
		outScope = fromScope
	}

	if stmt.OrderBy != nil {
		// TODO(rytaft): Build Order By. Order By relies on the existence of
		// ordering physical properties.
		panic(errorf("ORDER BY not yet supported: %s", stmt.String()))
	}

	// Don't add an unnecessary "pass through" project expression.
	if !projectionsScope.hasSameColumns(outScope) {
		p := b.constructList(opt.ProjectionsOp, projections, projectionsScope.cols)
		out = b.factory.ConstructProject(out, p)
		outScope = projectionsScope
	}

	// Wrap with distinct operator if it exists.
	out = b.buildDistinct(out, sel.Distinct, outScope.cols, outScope)
	return out, outScope
}

// buildFrom builds a set of memo groups that represent the given FROM statement
// and WHERE clause.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildFrom(
	from *tree.From, where *tree.Where, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	var left, right opt.GroupID

	for _, table := range from.Tables {
		var rightScope *scope
		right, rightScope = b.buildTable(table, inScope)

		if left == 0 {
			left = right
			outScope = rightScope
			continue
		}

		outScope.appendColumns(rightScope)

		left = b.factory.ConstructInnerJoin(left, right, b.factory.ConstructTrue())
	}

	if left == 0 {
		// TODO(peter): This should be a table with 1 row and 0 columns to match
		// current cockroach behavior.
		rows := []opt.GroupID{b.factory.ConstructTuple(b.factory.InternList(nil))}
		out = b.factory.ConstructValues(
			b.factory.InternList(rows),
			b.factory.InternPrivate(&opt.ColList{}),
		)
		outScope = inScope
	} else {
		out = left
	}

	if where != nil {
		// All "from" columns are visible to the filter expression.
		texpr := outScope.resolveType(where.Expr, types.Bool)
		filter := b.buildScalar(texpr, outScope)
		out = b.factory.ConstructSelect(out, filter)
	}

	return out, outScope
}

// buildGroupingList builds a set of memo groups that represent a list of
// GROUP BY expressions.
//
// groupBy  The given GROUP BY expressions.
// selects  The select expressions are needed in case one of the GROUP BY
//          expressions is an index into to the select list. For example,
//              SELECT count(*), k FROM t GROUP BY 2
//          indicates that the grouping is on the second select expression, k.
//
// The first return value `groupings` is an ordered list of top-level memo
// groups corresponding to each GROUP BY expression. See Builder.buildStmt
// above for a description of the remaining input and return values.
func (b *Builder) buildGroupingList(
	groupBy tree.GroupBy, selects tree.SelectExprs, inScope *scope, outScope *scope,
) (groupings []opt.GroupID) {

	groupings = make([]opt.GroupID, 0, len(groupBy))
	inScope.groupby.groupStrs = make(groupByStrSet, len(groupBy))
	for _, e := range groupBy {
		subset := b.buildGrouping(e, selects, inScope, outScope)
		groupings = append(groupings, subset...)
	}

	return groupings
}

// buildGrouping builds a set of memo groups that represent a GROUP BY
// expression.
//
// groupBy  The given GROUP BY expression.
// selects  The select expressions are needed in case the GROUP BY expression
//          is an index into to the select list.
//
// The return value is an ordered list of top-level memo groups corresponding
// to the expression. The list generally consists of a single memo group except
// in the case of "*", where the expression is expanded to multiple columns.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildGroupingList).
func (b *Builder) buildGrouping(
	groupBy tree.Expr, selects tree.SelectExprs, inScope, outScope *scope,
) []opt.GroupID {
	// Unwrap parenthesized expressions like "((a))" to "a".
	groupBy = tree.StripParens(groupBy)

	// Check whether the GROUP BY clause refers to a column in the SELECT list
	// by index, e.g. `SELECT a, SUM(b) FROM y GROUP BY 1`.
	col := colIndex(len(selects), groupBy, "GROUP BY")
	label := ""
	if col != -1 {
		groupBy = selects[col].Expr
		label = string(selects[col].As)
	}

	// Resolve types, expand stars, and flatten tuples.
	exprs := b.expandStarAndResolveType(groupBy, inScope)
	exprs = flattenTuples(exprs)

	// Finally, build each of the GROUP BY columns.
	out := make([]opt.GroupID, 0, len(exprs))
	for _, e := range exprs {
		// Save a representation of the GROUP BY expression for validation of the
		// SELECT and HAVING expressions. This enables queries such as:
		//    SELECT x+y FROM t GROUP BY x+y
		inScope.groupby.groupStrs[symbolicExprStr(e)] = exists
		out = append(out, b.buildScalarProjection(e, label, inScope, outScope))
	}
	return out
}

// colIndex takes an expression that refers to a column using an integer,
// verifies it refers to a valid target in the SELECT list, and returns the
// corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first item in the SELECT list, "a". The returned index
// is 0.
func colIndex(numOriginalCols int, expr tree.Expr, context string) int {
	ord := int64(-1)
	switch i := expr.(type) {
	case *tree.NumVal:
		if i.ShouldBeInt64() {
			val, err := i.AsInt64()
			if err != nil {
				panic(builderError{err})
			}
			ord = val
		} else {
			panic(errorf("non-integer constant in %s: %s", context, expr))
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		panic(errorf("non-integer constant in %s: %s", context, expr))
	case tree.Datum:
		panic(errorf("non-integer constant in %s: %s", context, expr))
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			panic(errorf("%s position %s is not in select list", context, expr))
		}
		ord--
	}
	return int(ord)
}

// expandStarAndResolveType expands expr into a list of columns if expr
// corresponds to a "*" or "<table>.*". Otherwise, expandStarAndResolveType
// resolves the type of expr and returns it as a []TypedExpr.
func (b *Builder) expandStarAndResolveType(
	expr tree.Expr, inScope *scope,
) (exprs []tree.TypedExpr) {
	// NB: The case statements are sorted lexicographically.
	switch t := expr.(type) {
	case *tree.AllColumnsSelector:
		// TODO(whomever): this improperly omits the catalog/schema prefix.
		tableName := optbase.TableName(t.TableName.Parts[0])
		for i := range inScope.cols {
			col := inScope.cols[i]
			if col.table == tableName && !col.hidden {
				exprs = append(exprs, &col)
			}
		}
		if len(exprs) == 0 {
			panic(errorf("unknown table %s", t))
		}

	case tree.UnqualifiedStar:
		for i := range inScope.cols {
			col := inScope.cols[i]
			if !col.hidden {
				exprs = append(exprs, &col)
			}
		}
		if len(exprs) == 0 {
			panic(errorf("failed to expand *"))
		}

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}
		return b.expandStarAndResolveType(vn, inScope)

	default:
		texpr := inScope.resolveType(t, types.Any)
		exprs = []tree.TypedExpr{texpr}
	}

	return exprs
}

// flattenTuples extracts the members of tuples into a list of columns.
func flattenTuples(exprs []tree.TypedExpr) []tree.TypedExpr {
	// We want to avoid allocating new slices unless strictly necessary.
	var newExprs []tree.TypedExpr
	for i, e := range exprs {
		if t, ok := e.(*tree.Tuple); ok {
			if newExprs == nil {
				// All right, it was necessary to allocate the slices after all.
				newExprs = make([]tree.TypedExpr, i, len(exprs))
				copy(newExprs, exprs[:i])
			}

			newExprs = flattenTuple(t, newExprs)
		} else if newExprs != nil {
			newExprs = append(newExprs, e)
		}
	}
	if newExprs != nil {
		return newExprs
	}
	return exprs
}

// flattenTuple recursively extracts the members of a tuple into a list of
// expressions.
func flattenTuple(t *tree.Tuple, exprs []tree.TypedExpr) []tree.TypedExpr {
	for _, e := range t.Exprs {
		if eT, ok := e.(*tree.Tuple); ok {
			exprs = flattenTuple(eT, exprs)
		} else {
			expr := e.(tree.TypedExpr)
			exprs = append(exprs, expr)
		}
	}
	return exprs
}

// hasAggregates determines if any of the given select expressions contain an
// aggregate function.
func (b *Builder) hasAggregates(selects tree.SelectExprs) bool {
	exprTransformCtx := transform.ExprTransformContext{}
	for _, sel := range selects {
		// TODO(rytaft): This function does not recurse into subqueries, so this
		// will be incorrect for correlated subqueries.
		if exprTransformCtx.AggregateInExpr(sel.Expr, b.semaCtx.SearchPath) {
			return true
		}
	}

	return false
}

func (b *Builder) buildHaving(having tree.Expr, inScope *scope) opt.GroupID {
	out := b.buildScalar(inScope.resolveType(having, types.Bool), inScope)
	if len(inScope.groupby.varsUsed) > 0 {
		i := inScope.groupby.varsUsed[0]
		col := b.colMap[i]
		panic(groupingError(col.String()))
	}
	return out
}

// buildProjectionList builds a set of memo groups that represent the given
// list of select expressions.
//
// The first return value `projections` is an ordered list of top-level memo
// groups corresponding to each select expression. See Builder.buildStmt above
// for a description of the remaining input and return values.
//
// As a side-effect, the appropriate scopes are updated with aggregations
// (scope.groupby.aggs)
func (b *Builder) buildProjectionList(
	selects tree.SelectExprs, inScope *scope, outScope *scope,
) (projections []opt.GroupID) {
	projections = make([]opt.GroupID, 0, len(selects))
	for _, e := range selects {
		end := len(outScope.cols)
		subset := b.buildProjection(e.Expr, string(e.As), inScope, outScope)
		projections = append(projections, subset...)

		// Update the name of the column if there is an alias defined.
		if e.As != "" {
			for i := range outScope.cols[end:] {
				outScope.cols[i].name = optbase.ColumnName(e.As)
			}
		}
	}

	return projections
}

// buildProjection builds a set of memo groups that represent a projection
// expression.
//
// projection  The given projection expression.
// label       If a new column is synthesized (e.g., for a scalar expression),
//             it will be labeled with this string.
//
// The return value `projections` is an ordered list of top-level memo
// groups corresponding to the expression. The list generally consists of a
// single memo group except in the case of "*", where the expression is
// expanded to multiple columns.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildProjection(
	projection tree.Expr, label string, inScope, outScope *scope,
) (projections []opt.GroupID) {
	exprs := b.expandStarAndResolveType(projection, inScope)
	if len(exprs) > 1 && label != "" {
		panic(errorf("\"%s\" cannot be aliased", projection))
	}

	projections = make([]opt.GroupID, 0, len(exprs))
	for _, e := range exprs {
		projections = append(projections, b.buildScalarProjection(e, label, inScope, outScope))
	}

	return projections
}

// buildScalarProjection builds a set of memo groups that represent a scalar
// expression.
//
// texpr   The given scalar expression.
// label   If a new column is synthesized, it will be labeled with this string.
//         For example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a
//         projection with a synthesized column "x_incr".
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildScalarProjection(
	texpr tree.TypedExpr, label string, inScope, outScope *scope,
) opt.GroupID {
	// NB: The case statements are sorted lexicographically.
	switch t := texpr.(type) {
	case *columnProps:
		return b.buildVariableProjection(b.colMap[t.index], inScope, outScope)

	case *tree.FuncExpr:
		out, col := b.buildFunction(t, label, inScope)
		if col != nil {
			// Function was mapped to a column reference, such as in the case
			// of an aggregate.
			outScope.cols = append(outScope.cols, b.colMap[col.index])
		} else {
			out = b.buildDefaultScalarProjection(texpr, out, label, inScope, outScope)
		}
		return out

	case *tree.ParenExpr:
		return b.buildScalarProjection(t.TypedInnerExpr(), label, inScope, outScope)

	default:
		out := b.buildScalar(texpr, inScope)
		out = b.buildDefaultScalarProjection(texpr, out, label, inScope, outScope)
		return out
	}
}

// buildVariableProjection builds a memo group that represents the given
// column.
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildVariableProjection(col columnProps, inScope, outScope *scope) opt.GroupID {
	if inScope.inGroupingContext() && !inScope.groupby.groupingsScope.hasColumn(col.index) {
		panic(groupingError(col.String()))
	}
	out := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
	outScope.cols = append(outScope.cols, col)
	return out
}

// buildDefaultScalarProjection builds a set of memo groups that represent
// a scalar expression.
//
// texpr     The given scalar expression. The expression is any scalar
//           expression except for a bare variable or aggregate (those are
//           handled separately in buildVariableProjection and
//           buildFunction).
// group     The memo group that has already been built for the given
//           expression. It may be replaced by a variable reference if the
//           expression already exists (e.g., as a GROUP BY column).
// label     If a new column is synthesized, it will be labeled with this
//           string.
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildDefaultScalarProjection(
	texpr tree.TypedExpr, group opt.GroupID, label string, inScope, outScope *scope,
) opt.GroupID {
	if inScope.inGroupingContext() {
		if len(inScope.groupby.varsUsed) > 0 {
			if _, ok := inScope.groupby.groupStrs[symbolicExprStr(texpr)]; !ok {
				// This expression was not found among the GROUP BY expressions.
				i := inScope.groupby.varsUsed[0]
				col := b.colMap[i]
				panic(groupingError(col.String()))
			}

			// Reset varsUsed for the next projection.
			inScope.groupby.varsUsed = inScope.groupby.varsUsed[:0]
		}

		if col := inScope.findGrouping(group); col != nil {
			// The column already exists, so use that instead.
			col = &b.colMap[col.index]
			if label != "" {
				col.name = optbase.ColumnName(label)
			}
			outScope.cols = append(outScope.cols, *col)

			// Replace the expression with a reference to the column.
			return b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
		}
	}

	b.synthesizeColumn(outScope, label, texpr.ResolvedType())
	return group
}

// synthesizeColumn is used to synthesize new columns. This is needed for
// operations such as projection of scalar expressions and aggregations. For
// example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a projection with
// a synthesized column "x_incr".
//
// scope  The scope is passed in so it can can be updated with the newly bound
//        variable.
// label  This is an optional label for the new column (e.g., if specified with
//        the AS keyword).
// typ    The type of the column.
//
// The new column is returned as a columnProps object.
func (b *Builder) synthesizeColumn(scope *scope, label string, typ types.T) *columnProps {
	if label == "" {
		label = fmt.Sprintf("column%d", len(b.colMap))
	}

	colIndex := b.factory.Metadata().AddColumn(label, typ)
	col := columnProps{typ: typ, index: colIndex}
	b.colMap = append(b.colMap, col)
	scope.cols = append(scope.cols, col)
	return &scope.cols[len(scope.cols)-1]
}

// constructList invokes the factory to create one of several operators that
// contain a list of groups: ProjectionsOp, AggregationsOp, and GroupingsOp.
func (b *Builder) constructList(
	op opt.Operator, items []opt.GroupID, cols []columnProps,
) opt.GroupID {
	colList := make(opt.ColList, len(cols))
	for i := range cols {
		colList[i] = cols[i].index
	}

	list := b.factory.InternList(items)
	private := b.factory.InternPrivate(&colList)

	switch op {
	case opt.ProjectionsOp:
		return b.factory.ConstructProjections(list, private)
	case opt.AggregationsOp:
		return b.factory.ConstructAggregations(list, private)
	case opt.GroupingsOp:
		return b.factory.ConstructGroupings(list, private)
	}

	panic(fmt.Sprintf("unexpected operator: %s", op))
}

func (b *Builder) buildDistinct(
	in opt.GroupID, distinct bool, byCols []columnProps, inScope *scope,
) opt.GroupID {
	if !distinct {
		return in
	}

	// Distinct is equivalent to group by without any aggregations.
	groupings := make([]opt.GroupID, 0, len(byCols))
	for i := range byCols {
		v := b.factory.ConstructVariable(b.factory.InternPrivate(byCols[i].index))
		groupings = append(groupings, v)
	}

	groupingList := b.constructList(opt.GroupingsOp, groupings, byCols)
	aggList := b.constructList(opt.AggregationsOp, nil, nil)
	return b.factory.ConstructGroupBy(in, groupingList, aggList)
}

func isAggregate(def *tree.FunctionDefinition) bool {
	_, ok := builtins.Aggregates[strings.ToLower(def.Name)]
	return ok
}

func groupingError(colName string) error {
	return errorf("column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function", colName)
}

// ScalarBuilder is a specialized variant of Builder that can be used to create
// a scalar from a TypedExpr. This is used to build scalar expressions for
// testing. It is also used temporarily to interface with the old planning code.
//
// TypedExprs can refer to columns in the current scope using IndexedVars (@1,
// @2, etc). When we build a scalar, we have to provide information about these
// columns.
type ScalarBuilder struct {
	bld   Builder
	scope scope
}

// NewScalar creates a new ScalarBuilder. The provided columns are accessible
// from scalar expressions via IndexedVars.
// columnNames and columnTypes must have the same length.
func NewScalar(
	ctx context.Context, factory opt.Factory, columnNames []string, columnTypes []types.T,
) *ScalarBuilder {
	sb := &ScalarBuilder{
		bld: Builder{
			factory: factory,
			colMap:  make([]columnProps, 1),
			ctx:     ctx,
		},
	}
	sb.scope.builder = &sb.bld
	for i := range columnNames {
		sb.bld.synthesizeColumn(&sb.scope, columnNames[i], columnTypes[i])
	}
	return sb
}

// Build a memo structure from a TypedExpr: the root group represents a scalar
// expression equivalent to expr.
func (sb *ScalarBuilder) Build(expr tree.TypedExpr) (root opt.GroupID, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate builder errors without adding
			// lots of checks for `if err != nil` throughout the code. This is
			// only possible because the code does not update shared state and does
			// not manipulate locks.
			if bldErr, ok := r.(builderError); ok {
				err = bldErr
			} else {
				panic(r)
			}
		}
	}()

	return sb.bld.buildScalar(expr, &sb.scope), nil
}
