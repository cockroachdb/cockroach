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

package build

import (
	"context"
	"fmt"

	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type unaryFactoryFunc func(f *xform.Factory, input xform.GroupID) xform.GroupID
type binaryFactoryFunc func(f *xform.Factory, left, right xform.GroupID) xform.GroupID

// Map from tree.ComparisonOperator to Factory constructor function.
var comparisonOpMap = [...]binaryFactoryFunc{
	tree.EQ:                (*xform.Factory).ConstructEq,
	tree.LT:                (*xform.Factory).ConstructLt,
	tree.GT:                (*xform.Factory).ConstructGt,
	tree.LE:                (*xform.Factory).ConstructLe,
	tree.GE:                (*xform.Factory).ConstructGe,
	tree.NE:                (*xform.Factory).ConstructNe,
	tree.In:                (*xform.Factory).ConstructIn,
	tree.NotIn:             (*xform.Factory).ConstructNotIn,
	tree.Like:              (*xform.Factory).ConstructLike,
	tree.NotLike:           (*xform.Factory).ConstructNotLike,
	tree.ILike:             (*xform.Factory).ConstructILike,
	tree.NotILike:          (*xform.Factory).ConstructNotILike,
	tree.SimilarTo:         (*xform.Factory).ConstructSimilarTo,
	tree.NotSimilarTo:      (*xform.Factory).ConstructNotSimilarTo,
	tree.RegMatch:          (*xform.Factory).ConstructRegMatch,
	tree.NotRegMatch:       (*xform.Factory).ConstructNotRegMatch,
	tree.RegIMatch:         (*xform.Factory).ConstructRegIMatch,
	tree.NotRegIMatch:      (*xform.Factory).ConstructNotRegIMatch,
	tree.IsDistinctFrom:    (*xform.Factory).ConstructIsDistinctFrom,
	tree.IsNotDistinctFrom: (*xform.Factory).ConstructIsNotDistinctFrom,
	tree.Any:               (*xform.Factory).ConstructAny,
	tree.Some:              (*xform.Factory).ConstructSome,
	tree.All:               (*xform.Factory).ConstructAll,
}

// Map from tree.BinaryOperator to Factory constructor function.
var binaryOpMap = [...]binaryFactoryFunc{
	tree.Bitand:   (*xform.Factory).ConstructBitand,
	tree.Bitor:    (*xform.Factory).ConstructBitor,
	tree.Bitxor:   (*xform.Factory).ConstructBitxor,
	tree.Plus:     (*xform.Factory).ConstructPlus,
	tree.Minus:    (*xform.Factory).ConstructMinus,
	tree.Mult:     (*xform.Factory).ConstructMult,
	tree.Div:      (*xform.Factory).ConstructDiv,
	tree.FloorDiv: (*xform.Factory).ConstructFloorDiv,
	tree.Mod:      (*xform.Factory).ConstructMod,
	tree.Pow:      (*xform.Factory).ConstructPow,
	tree.Concat:   (*xform.Factory).ConstructConcat,
	tree.LShift:   (*xform.Factory).ConstructLShift,
	tree.RShift:   (*xform.Factory).ConstructRShift,
}

// Map from tree.UnaryOperator to Factory constructor function.
var unaryOpMap = [...]unaryFactoryFunc{
	tree.UnaryPlus:       (*xform.Factory).ConstructUnaryPlus,
	tree.UnaryMinus:      (*xform.Factory).ConstructUnaryMinus,
	tree.UnaryComplement: (*xform.Factory).ConstructUnaryComplement,
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
	factory *xform.Factory
	stmt    tree.Statement
	semaCtx tree.SemaContext
	ctx     context.Context

	// Skip index 0 in order to reserve it to indicate the "unknown" column.
	colMap []columnProps
}

// NewBuilder creates a new Builder structure initialized with the given
// Context, Factory, and parsed SQL statement.
func NewBuilder(ctx context.Context, factory *xform.Factory, stmt tree.Statement) *Builder {
	b := &Builder{factory: factory, stmt: stmt, colMap: make([]columnProps, 1), ctx: ctx}

	ivarHelper := tree.MakeIndexedVarHelper(b, 0)
	b.semaCtx.IVarHelper = &ivarHelper
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
func (b *Builder) Build() (root xform.GroupID, required *xform.PhysicalProps, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate builder errors without adding
			// lots of checks for `if err != nil` throughout the code. This is
			// only possible because the code does not update shared state and does
			// not manipulate locks.
			if _, ok := r.(builderError); ok {
				err = r.(builderError)
			} else {
				panic(r)
			}
		}
	}()

	out, _ := b.buildStmt(b.stmt, &scope{builder: b})
	root = out

	// TODO(rytaft): Add physical properties that are required of the root memo
	// group.
	required = &xform.PhysicalProps{}
	return
}

// builderError is used for semantic errors that occur during the build process
// and is passed as an argument to panic. These panics are caught and converted
// back to errors inside Builder.Build.
type builderError error

// errorf formats according to a format specifier and returns the
// string as a builderError.
func errorf(format string, a ...interface{}) builderError {
	return fmt.Errorf(format, a...)
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
) (out xform.GroupID, outScope *scope) {
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
) (out xform.GroupID, outScope *scope) {
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

		return

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			panic(builderError(err))
		}
		tbl, err := b.factory.Metadata().Catalog().FindTable(b.ctx, tn)
		if err != nil {
			panic(builderError(err))
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
func (b *Builder) buildScan(
	tbl optbase.Table, inScope *scope,
) (out xform.GroupID, outScope *scope) {
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

// buildScalar builds a set of memo groups that represent a scalar expression.
//
// scalar   The given scalar expression.
// hasAgg   Indicates whether this scalar expression is contained in a select
//          list or HAVING clause in a grouping context (i.e., aggregates,
//          GROUP BY or HAVING clauses are present). It is necessary since it
//          affects scoping rules.
// inScope  Contains the name bindings that are visible for this scalar
//          expression (e.g., passed in from an enclosing statement).
//
// The first return value, out, corresponds to the top-level memo group ID for
// this scalar expression.
//
// If hasAgg is true, the second return value, varsUsed, contains any variables
// used in this expression that are:
//   (1) not explicit GROUP BY columns,
//   (2) not part of a sub-expression that matches a GROUP BY expression, and
//   (3) not contained in an aggregate.
func (b *Builder) buildScalar(
	scalar tree.TypedExpr, hasAgg bool, inScope *scope,
) (out xform.GroupID, varsUsed []xform.ColumnIndex) {
	switch t := scalar.(type) {
	case *columnProps:
		if hasAgg && !inScope.groupby.groupingsScope.hasColumn(t.index) {
			varsUsed = []xform.ColumnIndex{t.index}
		}
		out = b.factory.ConstructVariable(b.factory.InternPrivate(t.index))
		return

	case *tree.AndExpr:
		left, varsUsedLeft := b.buildScalar(t.TypedLeft(), hasAgg, inScope)
		right, varsUsedRight := b.buildScalar(t.TypedRight(), hasAgg, inScope)
		varsUsed = append(varsUsedLeft, varsUsedRight...)
		out = b.factory.ConstructAnd(left, right)

	case *tree.BinaryExpr:
		left, varsUsedLeft := b.buildScalar(t.TypedLeft(), hasAgg, inScope)
		right, varsUsedRight := b.buildScalar(t.TypedRight(), hasAgg, inScope)
		varsUsed = append(varsUsedLeft, varsUsedRight...)
		out = binaryOpMap[t.Operator](b.factory, left, right)

	case *tree.ComparisonExpr:
		// TODO(rytaft): remove this check when we are confident that all operators
		// are included in comparisonOpMap.
		if comparisonOpMap[t.Operator] == nil {
			panic(errorf("not yet implemented: operator %s", t.Operator.String()))
		}

		// TODO(peter): handle t.SubOperator.
		left, varsUsedLeft := b.buildScalar(t.TypedLeft(), hasAgg, inScope)
		right, varsUsedRight := b.buildScalar(t.TypedRight(), hasAgg, inScope)
		varsUsed = append(varsUsedLeft, varsUsedRight...)
		out = comparisonOpMap[t.Operator](b.factory, left, right)

	case *tree.DTuple:
		list := make([]xform.GroupID, len(t.D))
		var varsUsedElem []xform.ColumnIndex
		for i := range t.D {
			list[i], varsUsedElem = b.buildScalar(t.D[i], hasAgg, inScope)
			varsUsed = append(varsUsed, varsUsedElem...)
		}
		out = b.factory.ConstructTuple(b.factory.StoreList(list))

	case *tree.FuncExpr:
		out, _, varsUsed = b.buildFunction(t, "", hasAgg, inScope)

	case *tree.IndexedVar:
		colProps := b.synthesizeColumn(inScope, fmt.Sprintf("@%d", t.Idx+1), t.ResolvedType())
		out = b.factory.ConstructVariable(b.factory.InternPrivate(colProps.index))
		// TODO(rytaft): Do we need to update varsUsed here?

	case *tree.NotExpr:
		var arg xform.GroupID
		arg, varsUsed = b.buildScalar(t.TypedInnerExpr(), hasAgg, inScope)
		out = b.factory.ConstructNot(arg)

	case *tree.OrExpr:
		left, varsUsedLeft := b.buildScalar(t.TypedLeft(), hasAgg, inScope)
		right, varsUsedRight := b.buildScalar(t.TypedRight(), hasAgg, inScope)
		varsUsed = append(varsUsedLeft, varsUsedRight...)
		out = b.factory.ConstructOr(left, right)

	case *tree.ParenExpr:
		out, varsUsed = b.buildScalar(t.TypedInnerExpr(), hasAgg, inScope)

	case *tree.Placeholder:
		out = b.factory.ConstructPlaceholder(b.factory.InternPrivate(t))
		// TODO(rytaft): Do we need to update varsUsed here?

	case *tree.Tuple:
		list := make([]xform.GroupID, len(t.Exprs))
		var varsUsedElem []xform.ColumnIndex
		for i := range t.Exprs {
			list[i], varsUsedElem = b.buildScalar(t.Exprs[i].(tree.TypedExpr), hasAgg, inScope)
			varsUsed = append(varsUsed, varsUsedElem...)
		}
		out = b.factory.ConstructTuple(b.factory.StoreList(list))

	case *tree.UnaryExpr:
		var arg xform.GroupID
		arg, varsUsed = b.buildScalar(t.TypedInnerExpr(), hasAgg, inScope)
		out = unaryOpMap[t.Operator](b.factory, arg)

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		out = b.factory.ConstructConst(b.factory.InternPrivate(t))

	default:
		panic(errorf("not yet implemented: scalar expr: %T", scalar))
	}

	// If we are not in a grouping context or if this expression corresponds to
	// a GROUP BY expression, truncate varsUsed.
	if !hasAgg || inScope.findGrouping(out) != nil {
		varsUsed = varsUsed[:0]
	}

	return
}

// buildFunction builds a set of memo groups that represent a function
// expression.
//
// f       The given function expression.
// label   If a new column is synthesized, it will be labeled with this
//         string.
// hasAgg  Indicates whether this function is contained in a select list or
//         HAVING clause in a grouping context (i.e., aggregates, GROUP BY
//         or HAVING clauses are present). It is necessary since it affects
//         scoping rules.
//
// If the function is an aggregate, the second return value, col,
// corresponds to the columnProps that represents the aggregate.
//
// If hasAgg is true, the third return value, varsUsed, contains any variables
// used in this function that are:
//   (1) not explicit GROUP BY columns,
//   (2) not part of a sub-expression that matches a GROUP BY expression, and
//   (3) not contained in an aggregate.
// (i.e., if this function is an aggregate, varsUsed will always be empty)
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildFunction(
	f *tree.FuncExpr, label string, hasAgg bool, inScope *scope,
) (out xform.GroupID, col *columnProps, varsUsed []xform.ColumnIndex) {
	def, err := f.Func.Resolve(b.semaCtx.SearchPath)
	if err != nil {
		panic(builderError(err))
	}

	isAgg := isAggregate(def)
	if isAgg {
		inScope.startAggFunc()
	}

	argList := make([]xform.GroupID, 0, len(f.Exprs))
	for _, pexpr := range f.Exprs {
		var arg xform.GroupID
		if _, ok := pexpr.(tree.UnqualifiedStar); ok {
			arg = b.factory.ConstructConst(b.factory.InternPrivate(tree.NewDInt(1)))
		} else {
			if isAgg {
				// Don't count variables used inside an aggregate.
				arg, _ = b.buildScalar(pexpr.(tree.TypedExpr), false /* hasAgg */, inScope)
			} else {
				arg, varsUsed = b.buildScalar(pexpr.(tree.TypedExpr), hasAgg, inScope)
			}
		}

		argList = append(argList, arg)
	}

	out = b.factory.ConstructFunction(b.factory.StoreList(argList), b.factory.InternPrivate(def))

	if isAgg {
		refScope := inScope.endAggFunc()

		// If the aggregate already exists as a column, use that. Otherwise
		// create a new column and add it the list of aggregates that need to
		// be computed by the groupby expression.
		// TODO(andy): turns out this doesn't really do anything because the
		//             list passed to ConstructFunction isn't interned.
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

	return
}

// buildSelect builds a set of memo groups that represent the given select
// statement.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildSelect(
	stmt *tree.Select, inScope *scope,
) (out xform.GroupID, outScope *scope) {
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
) (out xform.GroupID, outScope *scope) {
	sel := stmt.Select.(*tree.SelectClause)

	var fromScope *scope
	out, fromScope = b.buildFrom(sel.From, sel.Where, inScope)

	// The "from" columns are visible to any grouping expressions.
	groupings, groupingsScope := b.buildGroupingList(sel.GroupBy, sel.Exprs, fromScope)

	// Set the grouping scope so that any aggregates will be added to the set
	// of grouping columns.
	if groupings == nil {
		// Even though there is no groupby clause, create a grouping scope
		// anyway, since one or more aggregate functions in the projection list
		// triggers an implicit groupby.
		groupingsScope = &scope{builder: b}
	} else {
		fromScope.groupby.groupings = groupings
	}

	fromScope.groupby.groupingsScope = groupingsScope

	// Any "grouping" columns are visible to both the "having" and "projection"
	// expressions. The build has the side effect of extracting aggregation
	// columns.
	var having xform.GroupID
	if sel.Having != nil {
		// TODO(rytaft): Build HAVING.
		panic(errorf("HAVING not yet supported: %s", stmt.String()))
	}

	hasAgg := groupings != nil || having != 0 || b.hasAggregates(sel.Exprs)

	// If the projection is empty or a simple pass-through, then
	// buildProjectionList will return nil values.
	projections, projectionsScope := b.buildProjectionList(sel.Exprs, hasAgg, fromScope)

	// Wrap with groupby operator if groupings or aggregates exist.
	if hasAgg {
		// Any aggregate columns that were discovered would have been appended
		// to the end of the grouping scope.
		aggCols := groupingsScope.cols[len(groupingsScope.cols)-len(groupingsScope.groupby.aggs):]
		aggList := b.constructProjectionList(groupingsScope.groupby.aggs, aggCols)

		var groupingCols []columnProps
		if groupings != nil {
			groupingCols = groupingsScope.cols
		}

		groupingList := b.constructProjectionList(groupings, groupingCols)
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

	// Wrap with project operator if it exists.
	if projections != nil {
		out = b.factory.ConstructProject(out, b.constructProjectionList(projections, projectionsScope.cols))
		outScope = projectionsScope
	}

	return
}

// buildFrom builds a set of memo groups that represent the given FROM statement
// and WHERE clause.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildFrom(
	from *tree.From, where *tree.Where, inScope *scope,
) (out xform.GroupID, outScope *scope) {
	var left, right xform.GroupID

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
		rows := []xform.GroupID{b.factory.ConstructTuple(b.factory.StoreList(nil))}
		out = b.factory.ConstructValues(b.factory.StoreList(rows), b.factory.InternPrivate(&xform.ColSet{}))
		outScope = inScope
	} else {
		out = left
	}

	if where != nil {
		// All "from" columns are visible to the filter expression.
		texpr := outScope.resolveType(where.Expr, types.Bool)
		filter, _ := b.buildScalar(texpr, false /* hasAgg */, outScope)
		out = b.factory.ConstructSelect(out, filter)
	}

	return
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
	groupBy tree.GroupBy, selects tree.SelectExprs, inScope *scope,
) (groupings []xform.GroupID, outScope *scope) {
	if groupBy == nil {
		return
	}

	// After GROUP BY, variables from inScope are hidden
	outScope = &scope{builder: b}

	groupings = make([]xform.GroupID, 0, len(groupBy))
	for _, e := range groupBy {
		subset := b.buildGrouping(e, selects, inScope, outScope)
		groupings = append(groupings, subset...)
	}

	return
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
) []xform.GroupID {
	label := ""
	switch t := groupBy.(type) {
	// Special handling for "*" and "<name>.*" to prevent errors from the
	// TypeCheck call in scope.resolveType().
	case *tree.AllColumnsSelector:
	case tree.UnqualifiedStar:

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError(err))
		}
		return b.buildGrouping(vn, selects, inScope, outScope)

	default:
		texpr := inScope.resolveType(groupBy, types.Any)
		switch t := texpr.(type) {
		case tree.Datum:
			// Special case to allow using GROUP BY with column index into
			// SELECT list.
			if index, ok := t.(*tree.DInt); ok {
				i := (int)(*index)
				if i <= 0 || i > len(selects) {
					panic(errorf("GROUP BY position %d is not in select list", i))
				}
				groupBy = inScope.resolveType(selects[i-1].Expr, types.Any)
				label = string(selects[i-1].As)
			} else {
				panic(errorf("non-integer constant in GROUP BY"))
			}
		case *tree.Tuple:
			// Special case to flatten tuples in the GROUP BY.
			exprs := flattenTuple(t, nil)
			out := make([]xform.GroupID, 0, len(exprs))
			for _, e := range exprs {
				out = append(out, b.buildProjection(e, label, false /* hasAgg */, inScope, outScope)...)
			}
			return out
		}
	}

	return b.buildProjection(groupBy, label, false /* hasAgg */, inScope, outScope)
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
		if exprTransformCtx.AggregateInExpr(sel.Expr, b.semaCtx.SearchPath) {
			return true
		}
	}

	return false
}

// buildProjectionList builds a set of memo groups that represent a list of
// select expressions.
//
// selects  The given select expressions.
// hasAgg   Indicates whether these projections are in a grouping context
//          (i.e., aggregates, GROUP BY or HAVING clauses are present). It is
//          necessary since it affects scoping rules.
//
// The first return value `projections` is an ordered list of top-level memo
// groups corresponding to each select expression. See Builder.buildStmt above
// for a description of the remaining input and return values.
func (b *Builder) buildProjectionList(
	selects tree.SelectExprs, hasAgg bool, inScope *scope,
) (projections []xform.GroupID, outScope *scope) {
	if len(selects) == 0 {
		return nil, nil
	}

	if hasAgg {
		// In the case of aggregation, FROM columns are no longer visible.
		outScope = &scope{builder: b}
	} else {
		outScope = inScope.push()
	}

	projections = make([]xform.GroupID, 0, len(selects))
	for _, e := range selects {
		end := len(outScope.cols)
		subset := b.buildProjection(e.Expr, string(e.As), hasAgg, inScope, outScope)
		projections = append(projections, subset...)

		// Update the name of the column if there is an alias defined.
		if e.As != "" {
			for i := range outScope.cols[end:] {
				outScope.cols[i].name = optbase.ColumnName(e.As)
			}
		}
	}

	// Don't add an unnecessary "pass through" project expression.
	if hasAgg {
		// If aggregates will be projected, check against them instead.
		inScope = inScope.groupby.groupingsScope
	}

	if len(outScope.cols) == len(inScope.cols) {
		matches := true
		for i := range inScope.cols {
			if inScope.cols[i].index != outScope.cols[i].index {
				matches = false
				break
			}
		}

		if matches {
			return nil, nil
		}
	}

	return
}

// buildProjection builds a set of memo groups that represent a projection
// expression.
//
// projection  The given projection expression.
// label       If a new column is synthesized (e.g., for a scalar expression),
//             it will be labeled with this string.
// hasAgg      Indicates whether this projection is part of the select list in
//             a grouping context (i.e., aggregates, GROUP BY or HAVING clauses
//             are present). It is necessary since it affects scoping rules.
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
	projection tree.Expr, label string, hasAgg bool, inScope, outScope *scope,
) (projections []xform.GroupID) {
	// We only have to handle "*" and "<name>.*" in the switch below. Other names
	// will be handled by scope.resolveType().
	//
	// NB: The case statements are sorted lexicographically.
	switch t := projection.(type) {
	case *tree.AllColumnsSelector:
		tableName := optbase.TableName(t.TableName.Table())
		for _, col := range inScope.cols {
			if col.table == tableName && !col.hidden {
				v := b.buildVariableProjection(col, hasAgg, inScope, outScope)
				projections = append(projections, v)
			}
		}
		if len(projections) == 0 {
			panic(errorf("unknown table %s", t))
		}
		return

	case tree.UnqualifiedStar:
		for _, col := range inScope.cols {
			if !col.hidden {
				v := b.buildVariableProjection(col, hasAgg, inScope, outScope)
				projections = append(projections, v)
			}
		}
		if len(projections) == 0 {
			panic(errorf("failed to expand *"))
		}
		return

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError(err))
		}
		return b.buildProjection(vn, label, hasAgg, inScope, outScope)

	default:
		texpr := inScope.resolveType(projection, types.Any)
		return []xform.GroupID{b.buildScalarProjection(texpr, label, hasAgg, inScope, outScope)}
	}
}

// buildScalarProjection builds a set of memo groups that represent a scalar
// expression.
//
// texpr   The given scalar expression.
// label   If a new column is synthesized, it will be labeled with this string.
//         For example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a
//         projection with a synthesized column "x_incr".
// hasAgg  Indicates whether this projection is part of the select list in a
//         grouping context (i.e., aggregates, GROUP BY or HAVING clauses are
//         present). It is necessary since it affects scoping rules.
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values (outScope is passed as a parameter here rather than a return
// value because the newly bound variables are appended to a growing list to be
// returned by buildProjectionList).
func (b *Builder) buildScalarProjection(
	texpr tree.TypedExpr, label string, hasAgg bool, inScope, outScope *scope,
) xform.GroupID {
	// NB: The case statements are sorted lexicographically.
	switch t := texpr.(type) {
	case *columnProps:
		return b.buildVariableProjection(b.colMap[t.index], hasAgg, inScope, outScope)

	case *tree.FuncExpr:
		out, col, varsUsed := b.buildFunction(t, label, hasAgg, inScope)
		if col != nil {
			// Function was mapped to a column reference, such as in the case
			// of an aggregate.
			outScope.cols = append(outScope.cols, b.colMap[col.index])
		} else {
			out = b.buildDefaultScalarProjection(texpr, out, label, hasAgg, varsUsed, inScope, outScope)
		}
		return out

	case *tree.ParenExpr:
		return b.buildScalarProjection(t.TypedInnerExpr(), label, hasAgg, inScope, outScope)

	default:
		out, varsUsed := b.buildScalar(texpr, hasAgg, inScope)
		out = b.buildDefaultScalarProjection(texpr, out, label, hasAgg, varsUsed, inScope, outScope)
		return out
	}
}

// buildVariableProjection builds a set of memo groups that represent a column.
//
// col     The given column.
// hasAgg  Indicates whether this projection is part of the select list in a
//         grouping context (i.e., aggregates, GROUP BY or HAVING clauses are
//         present). If true, col must also be a GROUP BY column.
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildVariableProjection(
	col columnProps, hasAgg bool, inScope, outScope *scope,
) xform.GroupID {
	if hasAgg && !inScope.groupby.groupingsScope.hasColumn(col.index) {
		panic(errorf("column %s must appear in the GROUP BY clause or be used in an aggregate function", col.String()))
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
// hasAgg    Indicates whether this projection is part of the select list in a
//           grouping context (i.e., aggregates, GROUP BY or HAVING clauses are
//           present). It is necessary since it affects scoping rules.
// varsUsed  If hasAgg is true, contains any variables used in this expression
//           that are:
//              (1) not explicit GROUP BY columns,
//              (2) not part of a sub-expression that matches a GROUP BY
//                  expression, and
//              (3) not contained in an aggregate.
//           If varsUsed contains any values and the given scalar expression
//           does not exist already as a GROUP BY column, the function will
//           panic.
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildDefaultScalarProjection(
	texpr tree.TypedExpr,
	group xform.GroupID,
	label string,
	hasAgg bool,
	varsUsed []xform.ColumnIndex,
	inScope, outScope *scope,
) xform.GroupID {
	if hasAgg {
		// TODO(rytaft): This will fail for functions right now because the
		// list passed to ConstructFunction isn't interned. For example, this
		// query is correct:
		//   SELECT COUNT(*), UPPER(s) FROM t.kv GROUP BY UPPER(s)
		// But right now it will fail to find the UPPER(s) grouping and will
		// trigger the panic below.
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

		for _, i := range varsUsed {
			if !inScope.groupby.groupingsScope.hasColumn(i) {
				col := b.colMap[i]
				panic(errorf("column %s must appear in the GROUP BY clause or be used in an aggregate function", col.String()))
			}
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

	colIndex := b.factory.Metadata().AddColumn(label)
	col := columnProps{typ: typ, index: colIndex}
	b.colMap = append(b.colMap, col)
	scope.cols = append(scope.cols, col)
	return &scope.cols[len(scope.cols)-1]
}

func (b *Builder) constructProjectionList(items []xform.GroupID, cols []columnProps) xform.GroupID {
	return b.factory.ConstructProjections(b.factory.StoreList(items), b.factory.InternPrivate(makeColSet(cols)))
}

// Builder implements the IndexedVarContainer interface so it can be
// used as the container inside an IndexedVarHelper (specifically
// Builder.semaCtx.IVarHelper). This allows tree.TypeCheck to determine the
// correct type for any IndexedVars in Builder.stmt. These types are maintained
// inside the Builder.colMap.
var _ tree.IndexedVarContainer = &Builder{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (b *Builder) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("unimplemented: Builder.IndexedVarEval")
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (b *Builder) IndexedVarResolvedType(idx int) types.T {
	return b.colMap[xform.ColumnIndex(idx)].typ
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (b *Builder) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	panic("unimplemented: Builder.IndexedVarNodeFormatter")
}

func isAggregate(def *tree.FunctionDefinition) bool {
	_, ok := builtins.Aggregates[strings.ToLower(def.Name)]
	return ok
}

func makeColSet(cols []columnProps) *xform.ColSet {
	// Create column index list parameter to the ProjectionList op.
	var colSet xform.ColSet
	for i := range cols {
		colSet.Add(int(cols[i].index))
	}
	return &colSet
}
