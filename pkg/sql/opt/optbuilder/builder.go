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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type unaryFactoryFunc func(f opt.Factory, input opt.GroupID) opt.GroupID
type binaryFactoryFunc func(f opt.Factory, left, right opt.GroupID) opt.GroupID

// Map from tree.ComparisonOperator to Factory constructor function.
var comparisonOpMap = [tree.NumComparisonOperators]binaryFactoryFunc{
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
	tree.ContainedBy: func(f opt.Factory, left, right opt.GroupID) opt.GroupID {
		// This is just syntatic sugar that reverses the operands.
		return f.ConstructContains(right, left)
	},
}

// Map from tree.BinaryOperator to Factory constructor function.
var binaryOpMap = [tree.NumBinaryOperators]binaryFactoryFunc{
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
var unaryOpMap = [tree.NumUnaryOperators]unaryFactoryFunc{
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
	// AllowUnsupportedExpr is a control knob: if set, when building a scalar, the
	// builder takes any TypedExpr node that it doesn't recognize and wraps that
	// expression in an UnsupportedExpr node. This is temporary; it is used for
	// interfacing with the old planning code.
	AllowUnsupportedExpr bool

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
	b := &Builder{
		factory: factory,
		stmt:    stmt,
		colMap:  make([]columnProps, 1),
		ctx:     ctx,
	}

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

	out, outScope := b.buildStmt(b.stmt, &scope{builder: b})
	root = out
	required = b.buildPhysicalProps(outScope)
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

// buildPhysicalProps construct a set of required physical properties from the
// given scope.
func (b *Builder) buildPhysicalProps(scope *scope) *opt.PhysicalProps {
	presentation := make(opt.Presentation, len(scope.cols))
	for i := range scope.cols {
		col := &scope.cols[i]
		presentation[i] = opt.LabeledColumn{Label: string(col.name), Index: col.index}
	}
	return &opt.PhysicalProps{Presentation: presentation}
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
		b.renameSource(source.As, outScope)
		return out, outScope

	case *tree.JoinTableExpr:
		return b.buildJoin(source, inScope)

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			panic(builderError{err})
		}
		tbl, err := b.factory.Metadata().Catalog().FindTable(b.ctx, tn)
		if err != nil {
			panic(builderError{err})
		}

		return b.buildScan(tbl, tn, inScope)

	case *tree.ParenTableExpr:
		return b.buildTable(source.Expr, inScope)

	case *tree.Subquery:
		return b.buildStmt(source.Select, inScope)

	default:
		panic(errorf("not yet implemented: table expr: %T", texpr))
	}
}

// renameSource applies an AS clause to the columns in scope.
func (b *Builder) renameSource(as tree.AliasClause, scope *scope) {
	var tableAlias tree.Name
	colAlias := as.Cols

	if as.Alias != "" {
		// TODO(rytaft): Handle anonymous tables such as set-generating
		// functions with just one column.

		// If an alias was specified, use that.
		tableAlias = as.Alias
		for i := range scope.cols {
			scope.cols[i].table.TableName = tableAlias
		}
	}

	if len(colAlias) > 0 {
		// The column aliases can only refer to explicit columns.
		for colIdx, aliasIdx := 0, 0; aliasIdx < len(colAlias); colIdx++ {
			if colIdx >= len(scope.cols) {
				panic(errorf(
					"source %q has %d columns available but %d columns specified",
					tableAlias, aliasIdx, len(colAlias)))
			}
			if scope.cols[colIdx].hidden {
				continue
			}
			scope.cols[colIdx].name = colAlias[aliasIdx]
			aliasIdx++
		}
	}
}

// buildScan builds a memo group for a scanOp expression on the given table
// with the given table name.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildScan(
	tbl optbase.Table, tn *tree.TableName, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	tblIndex := b.factory.Metadata().AddTable(tbl)

	outScope = inScope.push()
	for i := 0; i < tbl.ColumnCount(); i++ {
		col := tbl.Column(i)
		colIndex := b.factory.Metadata().TableColumn(tblIndex, i)
		colProps := columnProps{
			index:  colIndex,
			name:   tree.Name(col.ColName()),
			table:  *tn,
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
		if inGroupingContext && !inScope.groupby.aggInScope.hasColumn(t.index) {
			inScope.groupby.varsUsed = append(inScope.groupby.varsUsed, t.index)
		}
		return b.factory.ConstructVariable(b.factory.InternPrivate(t.index))

	case *tree.AndExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		conditions := b.factory.InternList([]opt.GroupID{left, right})
		out = b.factory.ConstructAnd(conditions)

	case *tree.BinaryExpr:
		fn := binaryOpMap[t.Operator]
		if fn != nil {
			out = fn(b.factory,
				b.buildScalar(t.TypedLeft(), inScope),
				b.buildScalar(t.TypedRight(), inScope),
			)
		} else if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternPrivate(scalar))
		} else {
			panic(errorf("not yet implemented: operator %s", t.Operator.String()))
		}

	case *tree.CastExpr:
		arg := b.buildScalar(inScope.resolveType(t.Expr, types.Any), inScope)
		typ := coltypes.CastTargetToDatumType(t.Type)
		out = b.factory.ConstructCast(arg, b.factory.InternPrivate(typ))

	case *tree.ComparisonExpr:
		left := b.buildScalar(t.TypedLeft(), inScope)
		right := b.buildScalar(t.TypedRight(), inScope)
		fn := comparisonOpMap[t.Operator]
		if fn != nil {
			// Most comparison ops map directly to a factory method.
			out = fn(b.factory, left, right)
		} else if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternPrivate(scalar))
		} else {
			// TODO(rytaft): remove this check when we are confident that
			// all operators are included in comparisonOpMap.
			panic(errorf("not yet implemented: operator %s", t.Operator.String()))
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
		if b.AllowUnsupportedExpr {
			out = b.factory.ConstructUnsupportedExpr(b.factory.InternPrivate(scalar))
		} else {
			panic(errorf("not yet implemented: scalar expr: %T", scalar))
		}
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

	funcDef := opt.FuncDef{Name: def.Name, Type: f.ResolvedType(), Overload: f.ResolvedBuiltin()}

	if isAggregate(def) {
		return b.buildAggregateFunction(f, funcDef, label, inScope)
	}

	argList := make([]opt.GroupID, len(f.Exprs))
	for i, pexpr := range f.Exprs {
		argList[i] = b.buildScalar(pexpr.(tree.TypedExpr), inScope)
	}

	// Construct a private FuncDef that refers to a resolved function overload.
	return b.factory.ConstructFunction(
		b.factory.InternList(argList), b.factory.InternPrivate(funcDef),
	), nil
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

	var fromScope *scope
	out, fromScope = b.buildFrom(sel.From, sel.Where, inScope)
	outScope = fromScope

	var projections []opt.GroupID
	var projectionsScope *scope
	if b.needsAggregation(sel) {
		out, outScope, projections, projectionsScope = b.buildAggregation(sel, out, fromScope)
	} else {
		projectionsScope = fromScope.push()
		projections = b.buildProjectionList(sel.Exprs, fromScope, projectionsScope)
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
		tn, err := tree.NormalizeTableName(&t.TableName)
		if err != nil {
			panic(builderError{err})
		}

		numRes, src, _, err := inScope.FindSourceMatchingName(b.ctx, tn)
		if err != nil {
			panic(builderError{err})
		}
		if numRes == tree.NoResults {
			panic(builderError{pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"no data source named %q", tree.ErrString(&tn))})
		}

		for i := range inScope.cols {
			col := inScope.cols[i]
			if col.table == *src && !col.hidden {
				exprs = append(exprs, &col)
			}
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

// buildJoin builds a set of memo groups that represent the given join table
// expression.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildJoin(
	join *tree.JoinTableExpr, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	left, leftScope := b.buildTable(join.Left, inScope)
	right, rightScope := b.buildTable(join.Right, inScope)

	// Check that the same table name is not used on both sides.
	leftTables := make(map[tree.TableName]struct{})
	for _, leftCol := range leftScope.cols {
		leftTables[leftCol.table] = exists
	}

	for _, rightCol := range rightScope.cols {
		t := rightCol.table
		if t.TableName == "" {
			// Allow joins of sources that define columns with no
			// associated table name. At worst, the USING/NATURAL
			// detection code or expression analysis for ON will detect an
			// ambiguity later.
			continue
		}
		if _, ok := leftTables[t]; ok {
			panic(errorf("cannot join columns from the same source name %q (missing AS clause)", t.TableName))
		}
	}

	joinType := sqlbase.JoinTypeFromAstString(join.Join)

	switch cond := join.Cond.(type) {
	case tree.NaturalJoinCond, *tree.UsingJoinCond:
		var usingColNames tree.NameList

		switch t := cond.(type) {
		case tree.NaturalJoinCond:
			usingColNames = commonColumns(leftScope, rightScope)
		case *tree.UsingJoinCond:
			usingColNames = t.Cols
		}

		return b.buildUsingJoin(joinType, usingColNames, left, right, leftScope, rightScope, inScope)

	case *tree.OnJoinCond, nil:
		// Append columns added by the children, as they are visible to the filter.
		outScope = inScope.push()
		outScope.appendColumns(leftScope)
		outScope.appendColumns(rightScope)

		var filter opt.GroupID
		if on, ok := cond.(*tree.OnJoinCond); ok {
			filter = b.buildScalar(outScope.resolveType(on.Expr, types.Bool), outScope)
		} else {
			filter = b.factory.ConstructTrue()
		}

		return b.constructJoin(joinType, left, right, filter), outScope

	default:
		panic(fmt.Sprintf("unsupported join condition %#v", cond))
	}
}

// commonColumns returns the names of columns common on the
// left and right sides, for use by NATURAL JOIN.
func commonColumns(leftScope, rightScope *scope) (common tree.NameList) {
	for _, leftCol := range leftScope.cols {
		if leftCol.hidden {
			continue
		}
		for _, rightCol := range rightScope.cols {
			if rightCol.hidden {
				continue
			}

			if leftCol.name == rightCol.name {
				common = append(common, leftCol.name)
				break
			}
		}
	}

	return common
}

// buildUsingJoin builds a set of memo groups that represent the given join
// table expression with the given `USING` column names. It is used for both
// USING and NATURAL joins.
//
// joinType    The join type (inner, left, right or outer)
// names       The list of `USING` column names
// left        The memo group ID of the left table
// right       The memo group ID of the right table
// leftScope   The outScope from the left table
// rightScope  The outScope from the right table
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildUsingJoin(
	joinType sqlbase.JoinType,
	names tree.NameList,
	left, right opt.GroupID,
	leftScope, rightScope, inScope *scope,
) (out opt.GroupID, outScope *scope) {
	// Build the join predicate.
	mergedCols, filter, outScope := b.buildUsingJoinPredicate(
		joinType, leftScope.cols, rightScope.cols, names, inScope,
	)

	out = b.constructJoin(joinType, left, right, filter)

	if len(mergedCols) > 0 {
		// Wrap in a projection to include the merged columns.
		projections := make([]opt.GroupID, 0, len(outScope.cols))
		for _, col := range outScope.cols {
			if mergedCol, ok := mergedCols[col.index]; ok {
				projections = append(projections, mergedCol)
			} else {
				v := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
				projections = append(projections, v)
			}
		}

		p := b.constructList(opt.ProjectionsOp, projections, outScope.cols)
		out = b.factory.ConstructProject(out, p)
	}

	return out, outScope
}

// buildUsingJoinPredicate builds a set of memo groups that represent the join
// conditions for a USING join or natural join. It finds the columns in the
// left and right relations that match the columns provided in the names
// parameter, and creates equality predicate(s) with those columns. It also
// ensures that there is a single output column for each name in `names`
// (other columns with the same name are hidden).
//
// -- Merged columns --
//
// With NATURAL JOIN or JOIN USING (a,b,c,...), SQL allows us to refer to the
// columns a,b,c directly; these columns have the following semantics:
//   a = IFNULL(left.a, right.a)
//   b = IFNULL(left.b, right.b)
//   c = IFNULL(left.c, right.c)
//   ...
//
// Furthermore, a star has to resolve the columns in the following order:
// merged columns, non-equality columns from the left table, non-equality
// columns from the right table. To perform this rearrangement, we use a
// projection on top of the join. Note that the original columns must
// still be accessible via left.a, right.a (they will just be hidden).
//
// For inner or left outer joins, a is always the same as left.a.
//
// For right outer joins, a is always equal to right.a; but for some types
// (like collated strings), this doesn't mean it is the same as right.a. In
// this case we must still use the IFNULL construct.
//
// Example:
//
//  left has columns (a,b,x)
//  right has columns (a,b,y)
//
//  - SELECT * FROM left JOIN right USING(a,b)
//
//  join has columns:
//    1: left.a
//    2: left.b
//    3: left.x
//    4: right.a
//    5: right.b
//    6: right.y
//
//  projection has columns and corresponding variable expressions:
//    1: a aka left.a        @1
//    2: b aka left.b        @2
//    3: left.x              @3
//    4: right.a (hidden)    @4
//    5: right.b (hidden)    @5
//    6: right.y             @6
//
// If the join was a FULL OUTER JOIN, the columns would be:
//    1: a                   IFNULL(@1,@4)
//    2: b                   IFNULL(@2,@5)
//    3: left.a (hidden)     @1
//    4: left.b (hidden)     @2
//    5: left.x              @3
//    6: right.a (hidden)    @4
//    7: right.b (hidden)    @5
//    8: right.y             @6
//
// If new merged columns are created (as in the FULL OUTER JOIN example above),
// the return value mergedCols contains a mapping from the column index to the
// memo group ID of the IFNULL expression.
//
// See Builder.buildStmt above for a description of the remaining input and
// return values.
func (b *Builder) buildUsingJoinPredicate(
	joinType sqlbase.JoinType,
	leftCols []columnProps,
	rightCols []columnProps,
	names tree.NameList,
	inScope *scope,
) (mergedCols map[opt.ColumnIndex]opt.GroupID, out opt.GroupID, outScope *scope) {
	joined := make(map[tree.Name]*columnProps, len(names))
	conditions := make([]opt.GroupID, 0, len(names))
	mergedCols = make(map[opt.ColumnIndex]opt.GroupID)
	outScope = inScope.push()

	for _, name := range names {
		if _, ok := joined[name]; ok {
			panic(errorf("column %q appears more than once in USING clause", name))
		}

		// For every adjacent pair of tables, add an equality predicate.
		leftCol := findUsingColumn(leftCols, name, "left")
		rightCol := findUsingColumn(rightCols, name, "right")

		if !leftCol.typ.Equivalent(rightCol.typ) {
			// First, check if the comparison would even be valid.
			if _, found := tree.FindEqualComparisonFunction(leftCol.typ, rightCol.typ); !found {
				panic(errorf(
					"JOIN/USING types %s for left and %s for right cannot be matched for column %s",
					leftCol.typ, rightCol.typ, leftCol.name,
				))
			}
		}

		// Construct the predicate.
		leftVar := b.factory.ConstructVariable(b.factory.InternPrivate(leftCol.index))
		rightVar := b.factory.ConstructVariable(b.factory.InternPrivate(rightCol.index))
		eq := b.factory.ConstructEq(leftVar, rightVar)
		conditions = append(conditions, eq)

		// Add the merged column to the scope, constructing a new column if needed.
		if joinType == sqlbase.InnerJoin || joinType == sqlbase.LeftOuterJoin {
			// The merged column is the same as the corresponding column from the
			// left side.
			outScope.cols = append(outScope.cols, *leftCol)
		} else if joinType == sqlbase.RightOuterJoin &&
			!sqlbase.DatumTypeHasCompositeKeyEncoding(leftCol.typ) {
			// The merged column is the same as the corresponding column from the
			// right side.
			outScope.cols = append(outScope.cols, *rightCol)
		} else {
			// Construct a new merged column to represent IFNULL(left, right).
			col := b.synthesizeColumn(outScope, string(leftCol.name), leftCol.typ)
			merged := b.factory.ConstructCoalesce(b.factory.InternList([]opt.GroupID{leftVar, rightVar}))
			mergedCols[col.index] = merged
		}

		joined[name] = &outScope.cols[len(outScope.cols)-1]
	}

	// Hide other columns that have the same name as the merged columns.
	hideMatchingColumns(leftCols, joined, outScope)
	hideMatchingColumns(rightCols, joined, outScope)

	return mergedCols, b.constructFilter(conditions), outScope
}

// hideMatchingColumns iterates through each of the columns in cols and
// performs one of the following actions:
// (1) If the column is equal to one of the columns in `joined`, it is skipped
//     since it was one of the merged columns already added to the scope.
// (2) If the column has the same name as one of the columns in `joined` but is
//     not equal, it is marked as hidden and added to the scope.
// (3) All other columns are added to the scope without modification.
func hideMatchingColumns(cols []columnProps, joined map[tree.Name]*columnProps, scope *scope) {
	for _, col := range cols {
		if foundCol, ok := joined[col.name]; ok {
			// Hide other columns with the same name.
			if col == *foundCol {
				continue
			}
			col.hidden = true
		}
		scope.cols = append(scope.cols, col)
	}
}

// constructFilter builds a set of memo groups that represent the given
// list of filter conditions. It returns the top-level memo group ID for the
// filter.
func (b *Builder) constructFilter(conditions []opt.GroupID) opt.GroupID {
	switch len(conditions) {
	case 0:
		return b.factory.ConstructTrue()
	case 1:
		return conditions[0]
	default:
		return b.factory.ConstructAnd(b.factory.InternList(conditions))
	}
}

func (b *Builder) constructJoin(
	joinType sqlbase.JoinType, left, right, filter opt.GroupID,
) opt.GroupID {
	switch joinType {
	case sqlbase.InnerJoin:
		return b.factory.ConstructInnerJoin(left, right, filter)
	case sqlbase.LeftOuterJoin:
		return b.factory.ConstructLeftJoin(left, right, filter)
	case sqlbase.RightOuterJoin:
		return b.factory.ConstructRightJoin(left, right, filter)
	case sqlbase.FullOuterJoin:
		return b.factory.ConstructFullJoin(left, right, filter)
	default:
		panic(fmt.Errorf("unsupported JOIN type %d", joinType))
	}
}

// findUsingColumn finds the column in cols that has the given name. If the
// column exists it is returned. Otherwise, an error is thrown.
//
// context is a string ("left" or "right") used to indicate in the error
// message whether the name is missing from the left or right side of the join.
func findUsingColumn(cols []columnProps, name tree.Name, context string) *columnProps {
	for i := range cols {
		col := &cols[i]
		if !col.hidden && col.name == name {
			return col
		}
	}

	panic(errorf("column \"%s\" specified in USING clause does not exist in %s table", name, context))
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
		subset := b.buildProjection(e.Expr, string(e.As), inScope, outScope)
		projections = append(projections, subset...)
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
		return b.buildVariableProjection(t, label, inScope, outScope)

	case *tree.FuncExpr:
		out, col := b.buildFunction(t, label, inScope)
		if col != nil {
			// Function was mapped to a column reference, such as in the case
			// of an aggregate.
			outScope.cols = append(outScope.cols, *col)
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
// column. label contains an optional alias for the column (e.g., if specified
// with the AS keyword).
//
// The return value corresponds to the top-level memo group ID for this scalar
// expression.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildProjectionList).
func (b *Builder) buildVariableProjection(
	col *columnProps, label string, inScope, outScope *scope,
) opt.GroupID {
	if inScope.inGroupingContext() && !inScope.groupby.inAgg && !inScope.groupby.aggInScope.hasColumn(col.index) {
		panic(groupingError(col.String()))
	}
	out := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
	outScope.cols = append(outScope.cols, *col)

	// Update the column name with the alias if it exists, and mark the column
	// as a visible member of an anonymous table.
	col = &outScope.cols[len(outScope.cols)-1]
	if label != "" {
		col.name = tree.Name(label)
	}
	col.table.TableName = ""
	col.hidden = false
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
				col.name = tree.Name(label)
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
	col := columnProps{name: tree.Name(label), typ: typ, index: colIndex}
	b.colMap = append(b.colMap, col)
	scope.cols = append(scope.cols, col)
	return &scope.cols[len(scope.cols)-1]
}

// constructList invokes the factory to create one of the operators that contain
// a list of groups: ProjectionsOp and AggregationsOp.
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
	}

	panic(fmt.Sprintf("unexpected operator: %s", op))
}

// buildDistinct builds a set of memo groups that represent a DISTINCT
// expression.
//
// in        contains the memo group ID of the input expression.
// distinct  is true if this is a DISTINCT expression. If distinct is false,
//           we just return `in`.
// byCols    is the set of columns in the DISTINCT expression. Since
//           DISTINCT is equivalent to GROUP BY without any aggregations,
//           byCols are essentially the grouping columns.
// inScope   contains the name bindings that are visible for this DISTINCT
//           expression (e.g., passed in from an enclosing statement).
//
// The return value corresponds to the top-level memo group ID for this
// DISTINCT expression.
func (b *Builder) buildDistinct(
	in opt.GroupID, distinct bool, byCols []columnProps, inScope *scope,
) opt.GroupID {
	if !distinct {
		return in
	}

	// Distinct is equivalent to group by without any aggregations.
	var groupCols opt.ColSet
	for i := range byCols {
		groupCols.Add(int(byCols[i].index))
	}

	aggList := b.constructList(opt.AggregationsOp, nil, nil)
	return b.factory.ConstructGroupBy(in, aggList, b.factory.InternPrivate(&groupCols))
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
	Builder
	scope scope
}

// NewScalar creates a new ScalarBuilder. The provided columns are accessible
// from scalar expressions via IndexedVars.
// columnNames and columnTypes must have the same length.
func NewScalar(
	ctx context.Context, factory opt.Factory, columnNames []string, columnTypes []types.T,
) *ScalarBuilder {
	sb := &ScalarBuilder{
		Builder: Builder{
			factory: factory,
			colMap:  make([]columnProps, 1),
			ctx:     ctx,
		},
	}
	sb.scope.builder = &sb.Builder
	for i := range columnNames {
		sb.synthesizeColumn(&sb.scope, columnNames[i], columnTypes[i])
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

	return sb.buildScalar(expr, &sb.scope), nil
}
