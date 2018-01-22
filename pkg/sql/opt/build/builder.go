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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
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
			// lots of checks for `if err != nil` throughout the code.  This is
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

// newBuilderErrorf formats according to a format specifier and returns the
// string as a builderError.
func newBuilderErrorf(format string, a ...interface{}) builderError {
	return fmt.Errorf(format, a...)
}

// newBuilderError returns message as a builderError.
func newBuilderError(message string) builderError {
	return errors.New(message)
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
		panic(newBuilderErrorf("unexpected statement: %T", stmt))
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
				panic(newBuilderErrorf("rename specified %d columns, but table contains %d", n, len(outScope.cols)))
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
		panic(newBuilderErrorf("not yet implemented: table expr: %T", texpr))
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

// buildScalar builds a set of memo groups that represent the given scalar
// expression.
//
// inScope contains the name bindings that are visible for this scalar
// expression (e.g., passed in from an enclosing statement).
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
func (b *Builder) buildScalar(scalar tree.TypedExpr, inScope *scope) xform.GroupID {
	switch t := scalar.(type) {
	case *columnProps:
		return b.factory.ConstructVariable(b.factory.InternPrivate(t.index))

	case *tree.AndExpr:
		return b.factory.ConstructAnd(
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope),
		)

	case *tree.BinaryExpr:
		return binaryOpMap[t.Operator](b.factory,
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope),
		)

	case *tree.ComparisonExpr:
		// TODO(rytaft): remove this check when we are confident that all operators
		// are included in comparisonOpMap.
		if comparisonOpMap[t.Operator] == nil {
			panic(newBuilderErrorf("not yet implemented: operator %s", t.Operator.String()))
		}

		// TODO(peter): handle t.SubOperator.
		return comparisonOpMap[t.Operator](b.factory,
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope),
		)

	case *tree.DTuple:
		list := make([]xform.GroupID, len(t.D))
		for i := range t.D {
			list[i] = b.buildScalar(t.D[i], inScope)
		}
		return b.factory.ConstructTuple(b.factory.StoreList(list))

	case *tree.IndexedVar:
		colProps := b.synthesizeColumn(inScope, fmt.Sprintf("@%d", t.Idx+1), t.ResolvedType())
		return b.factory.ConstructVariable(b.factory.InternPrivate(colProps.index))

	case *tree.NotExpr:
		return b.factory.ConstructNot(b.buildScalar(t.TypedInnerExpr(), inScope))

	case *tree.OrExpr:
		return b.factory.ConstructOr(
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope),
		)

	case *tree.ParenExpr:
		return b.buildScalar(t.TypedInnerExpr(), inScope)

	case *tree.Placeholder:
		return b.factory.ConstructPlaceholder(b.factory.InternPrivate(t))

	case *tree.Tuple:
		list := make([]xform.GroupID, len(t.Exprs))
		for i := range t.Exprs {
			list[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope)
		}
		return b.factory.ConstructTuple(b.factory.StoreList(list))

	case *tree.UnaryExpr:
		return unaryOpMap[t.Operator](b.factory, b.buildScalar(t.TypedInnerExpr(), inScope))

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		return b.factory.ConstructConst(b.factory.InternPrivate(t))

	default:
		panic(newBuilderErrorf("not yet implemented: scalar expr: %T", scalar))
	}
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
		panic(newBuilderErrorf("not yet implemented: select statement: %T", stmt.Select))
	}

	// TODO(rytaft): Add support for order by expression.
	// TODO(peter): stmt.Limit
}

// buildSelectClause builds a set of memo groups that represent the given
// select clause. We pass the entire select statement rather than just the
// select clause in order to handle ORDER BY scoping rules. ORDER BY can sort
// results using columns from the FROM/GROUP BY clause and/or from the
// projection list.
// TODO(rytaft): Add support for groupings, having, order by, and distinct.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildSelectClause(
	stmt *tree.Select, inScope *scope,
) (out xform.GroupID, outScope *scope) {
	sel := stmt.Select.(*tree.SelectClause)
	if (sel.GroupBy != nil && len(sel.GroupBy) > 0) || stmt.OrderBy != nil || sel.Distinct {
		panic(newBuilderError("complex queries not yet supported."))
	}

	out, outScope = b.buildFrom(sel.From, sel.Where, inScope)

	// If the projection is empty or a simple pass-through, then
	// buildProjectionList will return nil values.
	projections, projectionsScope := b.buildProjectionList(sel.Exprs, outScope)

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
		filter := b.buildScalar(texpr, outScope)
		out = b.factory.ConstructSelect(out, filter)
	}

	return
}

// buildProjectionList builds a set of memo groups that represent the given
// select expressions.
//
// The first return value `projections` is an ordered list of top-level memo
// groups corresponding to each select expression. See Builder.buildStmt above
// for a description of the remaining input and return values.
func (b *Builder) buildProjectionList(
	selects tree.SelectExprs, inScope *scope,
) (projections []xform.GroupID, outScope *scope) {
	if len(selects) == 0 {
		return nil, nil
	}

	outScope = inScope.push()
	projections = make([]xform.GroupID, 0, len(selects))
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

// buildProjection builds a set of memo groups that represent the given
// projection expression. If a new column is synthesized (e.g., for a scalar
// expression), it will be labeled with the given label.
//
// The first return value `projections` is an ordered list of top-level memo
// groups corresponding to the expression. The list generally consists of a
// single memo group except in the case of "*", where the expression is
// expanded to multiple columns.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values (outScope is passed as a parameter here rather than a return
// value because the newly bound variables are appended to a growing list to be
// returned by buildProjectionList).
func (b *Builder) buildProjection(
	projection tree.Expr, label string, inScope, outScope *scope,
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
				v := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
				projections = append(projections, v)
				outScope.cols = append(outScope.cols, col)
			}
		}
		if len(projections) == 0 {
			panic(newBuilderErrorf("unknown table %s", t))
		}
		return

	case tree.UnqualifiedStar:
		for _, col := range inScope.cols {
			if !col.hidden {
				v := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
				projections = append(projections, v)
				outScope.cols = append(outScope.cols, col)
			}
		}
		if len(projections) == 0 {
			panic(newBuilderErrorf("failed to expand *"))
		}
		return

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError(err))
		}
		return b.buildProjection(vn, label, inScope, outScope)

	default:
		texpr := inScope.resolveType(projection, types.Any)
		return []xform.GroupID{b.buildScalarProjection(texpr, label, inScope, outScope)}
	}
}

// buildScalarProjection builds a set of memo groups that represent the given
// scalar expression. If a new column is synthesized, it will be labeled with
// the given label. For example, the query `SELECT (x + 1) AS "x_incr" FROM t`
// has a projection with a synthesized column "x_incr".
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values (outScope is passed as a parameter here rather than a return
// value because the newly bound variables are appended to a growing list to be
// returned by buildProjectionList).
func (b *Builder) buildScalarProjection(
	texpr tree.TypedExpr, label string, inScope, outScope *scope,
) xform.GroupID {
	// NB: The case statements are sorted lexicographically.
	switch t := texpr.(type) {
	case *columnProps:
		out := b.factory.ConstructVariable(b.factory.InternPrivate(t.index))
		outScope.cols = append(outScope.cols, b.colMap[t.index])
		return out

	case *tree.ParenExpr:
		return b.buildScalarProjection(t.TypedInnerExpr(), label, inScope, outScope)

	default:
		out := b.buildScalar(texpr, inScope)
		b.synthesizeColumn(outScope, label, texpr.ResolvedType())
		return out
	}
}

// synthesizeColumn is used to synthesize new columns. This is needed for
// operations such as projection of scalar expressions and aggregations. For
// example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a projection with
// a synthesized column "x_incr".
//
// scope is passed in so it can can be updated with the newly bound variable.
// label is an optional label for the new column (e.g., if specified with the
// AS keyword), and typ is the type of the column. The new column is returned
// as a columnProps object.
func (b *Builder) synthesizeColumn(scope *scope, label string, typ types.T) *columnProps {
	if label == "" {
		label = fmt.Sprintf("column%d", len(scope.cols)+1)
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

func makeColSet(cols []columnProps) *xform.ColSet {
	// Create column index list parameter to the ProjectionList op.
	var colSet xform.ColSet
	for i := range cols {
		colSet.Add(int(cols[i].index))
	}
	return &colSet
}
