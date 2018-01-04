// Copyright 2017 The Cockroach Authors.
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

package opt

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Map from tree.ComparisonOperator to operator.
var comparisonOpMap = [...]operator{
	tree.EQ:                eqOp,
	tree.LT:                ltOp,
	tree.GT:                gtOp,
	tree.LE:                leOp,
	tree.GE:                geOp,
	tree.NE:                neOp,
	tree.In:                inOp,
	tree.NotIn:             notInOp,
	tree.Like:              likeOp,
	tree.NotLike:           notLikeOp,
	tree.ILike:             iLikeOp,
	tree.NotILike:          notILikeOp,
	tree.SimilarTo:         similarToOp,
	tree.NotSimilarTo:      notSimilarToOp,
	tree.RegMatch:          regMatchOp,
	tree.NotRegMatch:       notRegMatchOp,
	tree.RegIMatch:         regIMatchOp,
	tree.NotRegIMatch:      notRegIMatchOp,
	tree.IsNotDistinctFrom: isOp,
	tree.IsDistinctFrom:    isNotOp,
	tree.Any:               anyOp,
	tree.Some:              someOp,
	tree.All:               allOp,
}

var comparisonOpReverseMap = [...]tree.ComparisonOperator{
	eqOp:           tree.EQ,
	ltOp:           tree.LT,
	gtOp:           tree.GT,
	leOp:           tree.LE,
	geOp:           tree.GE,
	neOp:           tree.NE,
	inOp:           tree.In,
	notInOp:        tree.NotIn,
	likeOp:         tree.Like,
	notLikeOp:      tree.NotLike,
	iLikeOp:        tree.ILike,
	notILikeOp:     tree.NotILike,
	similarToOp:    tree.SimilarTo,
	notSimilarToOp: tree.NotSimilarTo,
	regMatchOp:     tree.RegMatch,
	notRegMatchOp:  tree.NotRegMatch,
	regIMatchOp:    tree.RegIMatch,
	notRegIMatchOp: tree.NotRegIMatch,
	isOp:           tree.IsNotDistinctFrom,
	isNotOp:        tree.IsDistinctFrom,
	anyOp:          tree.Any,
	someOp:         tree.Some,
	allOp:          tree.All,
}

// Map from tree.BinaryOperator to operator.
var binaryOpMap = [...]operator{
	tree.Bitand:   bitandOp,
	tree.Bitor:    bitorOp,
	tree.Bitxor:   bitxorOp,
	tree.Plus:     plusOp,
	tree.Minus:    minusOp,
	tree.Mult:     multOp,
	tree.Div:      divOp,
	tree.FloorDiv: floorDivOp,
	tree.Mod:      modOp,
	tree.Pow:      powOp,
	tree.Concat:   concatOp,
	tree.LShift:   lShiftOp,
	tree.RShift:   rShiftOp,
}

var binaryOpReverseMap = [...]tree.BinaryOperator{
	bitandOp:   tree.Bitand,
	bitorOp:    tree.Bitor,
	bitxorOp:   tree.Bitxor,
	plusOp:     tree.Plus,
	minusOp:    tree.Minus,
	multOp:     tree.Mult,
	divOp:      tree.Div,
	floorDivOp: tree.FloorDiv,
	modOp:      tree.Mod,
	powOp:      tree.Pow,
	concatOp:   tree.Concat,
	lShiftOp:   tree.LShift,
	rShiftOp:   tree.RShift,
}

// Map from tree.UnaryOperator to operator.
var unaryOpMap = [...]operator{
	tree.UnaryPlus:       unaryPlusOp,
	tree.UnaryMinus:      unaryMinusOp,
	tree.UnaryComplement: unaryComplementOp,
}

// Map from tree.UnaryOperator to operator.
var unaryOpReverseMap = [...]tree.UnaryOperator{
	unaryPlusOp:       tree.UnaryPlus,
	unaryMinusOp:      tree.UnaryMinus,
	unaryComplementOp: tree.UnaryComplement,
}

// We allocate *Expr, *scalarProps and *relationalProps in chunks of these sizes.
const exprAllocChunk = 16
const scalarPropsAllocChunk = 16

// todo(rytaft) Increase the relationalProps chunk size after more relational
// operators are implemented.
const relationalPropsAllocChunk = 1

type buildContext struct {
	preallocScalarProps     []scalarProps
	preallocExprs           []Expr
	preallocRelationalProps []relationalProps
	catalog                 sqlbase.Catalog
	state                   *queryState
}

func (bc *buildContext) newRelationalProps() *relationalProps {
	if len(bc.preallocRelationalProps) == 0 {
		bc.preallocRelationalProps = make([]relationalProps, relationalPropsAllocChunk)
	}
	p := &bc.preallocRelationalProps[0]
	bc.preallocRelationalProps = bc.preallocRelationalProps[1:]
	return p
}

func (bc *buildContext) newScalarProps() *scalarProps {
	if len(bc.preallocScalarProps) == 0 {
		bc.preallocScalarProps = make([]scalarProps, scalarPropsAllocChunk)
	}
	p := &bc.preallocScalarProps[0]
	bc.preallocScalarProps = bc.preallocScalarProps[1:]
	return p
}

// newExpr returns a new *Expr with a new, blank scalarProps if scalar is true,
// or a new, blank relationalProps otherwise.
func (bc *buildContext) newExpr(scalar bool) *Expr {
	if len(bc.preallocExprs) == 0 {
		bc.preallocExprs = make([]Expr, exprAllocChunk)
	}
	e := &bc.preallocExprs[0]
	bc.preallocExprs = bc.preallocExprs[1:]
	if scalar {
		e.scalarProps = bc.newScalarProps()
	} else {
		e.props = bc.newRelationalProps()
	}
	return e
}

// build converts a tree.Statement to an Expr tree.
func (bc *buildContext) build(ctx context.Context, stmt tree.Statement) *Expr {
	var result *Expr
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		result = bc.buildSelect(ctx, stmt.Select)

	case *tree.Select:
		result = bc.buildSelect(ctx, stmt)

	default:
		panic(fmt.Sprintf("unexpected statement: %T", stmt))
	}

	return result
}

// buildSelect converts a tree.Select to an Expr tree. This method will
// expand significantly once we implement joins, aggregations, filters, etc.
func (bc *buildContext) buildSelect(ctx context.Context, stmt *tree.Select) *Expr {
	var result *Expr
	switch t := stmt.Select.(type) {
	case *tree.ParenSelect:
		result = bc.buildSelect(ctx, t.Select)

	case *tree.SelectClause:
		if t.Where != nil || (t.GroupBy != nil && len(t.GroupBy) > 0) || len(t.Exprs) > 1 || t.Distinct {
			panic("Complex queries not yet supported.")
		}
		result = bc.buildFrom(ctx, t.From)

	default:
		panic(fmt.Sprintf("unexpected select statement: %T", stmt.Select))
	}

	return result
}

// buildSelect converts a tree.From to an Expr tree. This method
// will expand significantly once we implement joins and filters.
func (bc *buildContext) buildFrom(ctx context.Context, from *tree.From) *Expr {
	if len(from.Tables) != 1 {
		panic("Joins not yet supported.")
	}
	return bc.buildTable(ctx, from.Tables[0])
}

// buildTable converts a tree.TableExpr to an Expr tree.
func (bc *buildContext) buildTable(ctx context.Context, texpr tree.TableExpr) *Expr {
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		result := bc.buildTable(ctx, source.Expr)
		if source.As.Alias != "" {
			if n := len(source.As.Cols); n > 0 && n != len(result.props.columns) {
				panic(fmt.Sprintf("rename specified %d columns, but table contains %d",
					n, len(result.props.columns)))
			}

			for i := range result.props.columns {
				col := &result.props.columns[i]
				if i < len(source.As.Cols) {
					col.name = columnName(source.As.Cols[i])
				}
				col.table = tableName(source.As.Alias)
			}
		}
		return result

	case *tree.FuncExpr:
		panic(fmt.Sprintf("unimplemented table expr: %T", texpr))

	case *tree.JoinTableExpr:
		panic(fmt.Sprintf("unimplemented table expr: %T", texpr))

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			panic(fmt.Sprintf("%s", err))
		}
		tab, err := bc.catalog.FindTable(ctx, tn)
		if err != nil {
			panic(fmt.Sprintf("%s", err))
		}

		return bc.buildScan(tab)

	case *tree.ParenTableExpr:
		return bc.buildTable(ctx, source.Expr)

	case *tree.StatementSource:
		panic(fmt.Sprintf("unimplemented table expr: %T", texpr))

	case *tree.Subquery:
		return bc.build(ctx, source.Select)

	case *tree.TableRef:
		panic(fmt.Sprintf("unimplemented table expr: %T", texpr))

	default:
		panic(fmt.Sprintf("unexpected table expr: %T", texpr))
	}
}

// buildScan creates an Expr with a scanOp operator for the given table.
func (bc *buildContext) buildScan(tab sqlbase.Table) *Expr {
	result := bc.newExpr(false /* scalar */)
	initScanExpr(result, tab)
	result.props.columns = make([]columnProps, 0, len(tab.TableName()))
	props := result.props

	// Every reference to a table in the query gets a new set of output column
	// indexes. Consider the query:
	//
	//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
	//
	// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not
	// equivalent to `r.y`. In order to achieve this, we need to give these
	// columns different indexes.
	base := len(bc.state.columns)
	tabName := tableName(tab.TableName())
	bc.state.tables[tabName] = append(bc.state.tables[tabName], base)

	for i := 0; i < tab.NumColumns(); i++ {
		index := base + i
		col := tab.Column(i)
		colProps := columnProps{
			index: index,
			name:  columnName(col.ColumnName()),
			table: tabName,
			typ:   col.DatumType(),
		}
		bc.state.columns = append(bc.state.columns, colProps)
		props.columns = append(props.columns, colProps)
	}

	// Initialize not-NULL columns from the table schema.
	for i := 0; i < tab.NumColumns(); i++ {
		if !tab.Column(i).IsNullable() {
			props.notNullCols.Add(props.columns[i].index)
		}
	}

	result.initProps()
	return result
}

// buildScalar converts a tree.TypedExpr to an Expr tree.
func (bc *buildContext) buildScalar(pexpr tree.TypedExpr) *Expr {
	switch t := pexpr.(type) {
	case *tree.ParenExpr:
		return bc.buildScalar(t.TypedInnerExpr())
	}

	e := bc.newExpr(true /* scalar */)
	e.scalarProps.typ = pexpr.ResolvedType()

	switch t := pexpr.(type) {
	case *tree.AndExpr:
		initBinaryExpr(
			e, andOp,
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.OrExpr:
		initBinaryExpr(
			e, orOp,
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.NotExpr:
		initUnaryExpr(e, notOp, bc.buildScalar(t.TypedInnerExpr()))

	case *tree.BinaryExpr:
		initBinaryExpr(
			e, binaryOpMap[t.Operator],
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.ComparisonExpr:
		initBinaryExpr(
			e, comparisonOpMap[t.Operator],
			bc.buildScalar(t.TypedLeft()),
			bc.buildScalar(t.TypedRight()),
		)
	case *tree.UnaryExpr:
		initUnaryExpr(e, unaryOpMap[t.Operator], bc.buildScalar(t.TypedInnerExpr()))

	// TODO(radu): for now, we pass through FuncExprs as unsupported
	// expressions.
	//case *tree.FuncExpr:
	//	children := make([]*expr, len(t.Exprs))
	//	for i, pexpr := range t.Exprs {
	//		children[i] = bc.buildScalar(pexpr.(tree.TypedExpr))
	//	}
	//	initFunctionCallExpr(e, t.ResolvedFunc(), children)

	case *tree.IndexedVar:
		initVariableExpr(e, t.Idx)

	case *tree.Tuple:
		children := make([]*Expr, len(t.Exprs))
		for i, e := range t.Exprs {
			children[i] = bc.buildScalar(e.(tree.TypedExpr))
		}
		initTupleExpr(e, children)

	case *tree.DTuple:
		children := make([]*Expr, len(t.D))
		for i, d := range t.D {
			children[i] = bc.buildScalar(d)
		}
		initTupleExpr(e, children)

	case tree.Datum:
		initConstExpr(e, t)

	default:
		initUnsupportedExpr(e, t)
	}
	return e
}

// build converts a tree.Statement to an Expr tree.
func build(ctx context.Context, stmt tree.Statement, catalog sqlbase.Catalog) (_ *Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	buildCtx := buildContext{
		catalog: catalog,
		state:   &queryState{tables: make(map[tableName][]columnIndex)},
	}
	return buildCtx.build(ctx, stmt), nil
}

// buildScalar converts a tree.TypedExpr to an Expr tree.
func buildScalar(pexpr tree.TypedExpr) (_ *Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	buildCtx := buildContext{}
	return buildCtx.buildScalar(pexpr), nil
}

var typedExprConvMap [numOperators]func(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr

func init() {
	// This code is not inline to avoid an initialization loop error (some of the
	// functions depend on scalarToTypedExpr which depends on typedExprConvMap).
	typedExprConvMap = [numOperators]func(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr{
		constOp:    constOpToTypedExpr,
		variableOp: variableOpToTypedExpr,

		andOp: boolOpToTypedExpr,
		orOp:  boolOpToTypedExpr,
		notOp: boolOpToTypedExpr,

		unaryPlusOp:       unaryOpToTypedExpr,
		unaryMinusOp:      unaryOpToTypedExpr,
		unaryComplementOp: unaryOpToTypedExpr,

		eqOp:           comparisonOpToTypedExpr,
		ltOp:           comparisonOpToTypedExpr,
		gtOp:           comparisonOpToTypedExpr,
		leOp:           comparisonOpToTypedExpr,
		geOp:           comparisonOpToTypedExpr,
		neOp:           comparisonOpToTypedExpr,
		inOp:           comparisonOpToTypedExpr,
		notInOp:        comparisonOpToTypedExpr,
		likeOp:         comparisonOpToTypedExpr,
		notLikeOp:      comparisonOpToTypedExpr,
		iLikeOp:        comparisonOpToTypedExpr,
		notILikeOp:     comparisonOpToTypedExpr,
		similarToOp:    comparisonOpToTypedExpr,
		notSimilarToOp: comparisonOpToTypedExpr,
		regMatchOp:     comparisonOpToTypedExpr,
		notRegMatchOp:  comparisonOpToTypedExpr,
		regIMatchOp:    comparisonOpToTypedExpr,
		notRegIMatchOp: comparisonOpToTypedExpr,
		isOp:           comparisonOpToTypedExpr,
		isNotOp:        comparisonOpToTypedExpr,
		anyOp:          comparisonOpToTypedExpr,
		someOp:         comparisonOpToTypedExpr,
		allOp:          comparisonOpToTypedExpr,

		bitandOp:   binaryOpToTypedExpr,
		bitorOp:    binaryOpToTypedExpr,
		bitxorOp:   binaryOpToTypedExpr,
		plusOp:     binaryOpToTypedExpr,
		minusOp:    binaryOpToTypedExpr,
		multOp:     binaryOpToTypedExpr,
		divOp:      binaryOpToTypedExpr,
		floorDivOp: binaryOpToTypedExpr,
		modOp:      binaryOpToTypedExpr,
		powOp:      binaryOpToTypedExpr,
		concatOp:   binaryOpToTypedExpr,
		lShiftOp:   binaryOpToTypedExpr,
		rShiftOp:   binaryOpToTypedExpr,

		tupleOp: tupleOpToTypedExpr,

		unsupportedScalarOp: unsupportedScalarOpToTypedExpr,
	}
}

func constOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return e.private.(tree.Datum)
}

func variableOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return ivh.IndexedVar(e.private.(int))
}

func boolOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	switch e.op {
	case andOp, orOp:
		n := scalarToTypedExpr(e.children[0], ivh)
		for _, child := range e.children[1:] {
			m := scalarToTypedExpr(child, ivh)
			if e.op == andOp {
				n = tree.NewTypedAndExpr(n, m)
			} else {
				n = tree.NewTypedOrExpr(n, m)
			}
		}
		return n

	case notOp:
		return tree.NewTypedNotExpr(scalarToTypedExpr(e.children[0], ivh))
	default:
		panic(fmt.Sprintf("invalid op %s", e.op))
	}
}

func tupleOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	if isTupleOfConstants(e) {
		datums := make(tree.Datums, len(e.children))
		for i, child := range e.children {
			datums[i] = constOpToTypedExpr(child, ivh).(tree.Datum)
		}
		return tree.NewDTuple(datums...)
	}
	children := make([]tree.TypedExpr, len(e.children))
	for i, child := range e.children {
		children[i] = scalarToTypedExpr(child, ivh)
	}
	return tree.NewTypedTuple(children)
}

func unaryOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return tree.NewTypedUnaryExpr(
		unaryOpReverseMap[e.op],
		scalarToTypedExpr(e.children[0], ivh),
		e.scalarProps.typ,
	)
}

func comparisonOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return tree.NewTypedComparisonExpr(
		comparisonOpReverseMap[e.op],
		scalarToTypedExpr(e.children[0], ivh),
		scalarToTypedExpr(e.children[1], ivh),
	)
}

func binaryOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return tree.NewTypedBinaryExpr(
		binaryOpReverseMap[e.op],
		scalarToTypedExpr(e.children[0], ivh),
		scalarToTypedExpr(e.children[1], ivh),
		e.scalarProps.typ,
	)
}

func unsupportedScalarOpToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	return e.private.(tree.TypedExpr)
}

func scalarToTypedExpr(e *Expr, ivh *tree.IndexedVarHelper) tree.TypedExpr {
	if fn := typedExprConvMap[e.op]; fn != nil {
		return fn(e, ivh)
	}
	panic(fmt.Sprintf("unsupported op %s", e.op))
}

// BuildScalarExpr converts a TypedExpr to a *Expr tree and normalizes it.
func BuildScalarExpr(typedExpr tree.TypedExpr) (*Expr, error) {
	if typedExpr == nil {
		return nil, nil
	}
	e, err := buildScalar(typedExpr)
	if err != nil {
		return nil, err
	}
	normalizeExpr(e)
	return e, nil
}
