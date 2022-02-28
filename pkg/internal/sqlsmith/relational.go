// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

func (s *Smither) makeStmt() (stmt tree.Statement, ok bool) {
	return s.stmtSampler.Next()(s)
}

func (s *Smither) makeSelectStmt(
	desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (stmt tree.SelectStatement, stmtRefs colRefs, ok bool) {
	if s.canRecurse() {
		for {
			expr, exprRefs, ok := s.selectStmtSampler.Next()(s, desiredTypes, refs, withTables)
			if ok {
				return expr, exprRefs, ok
			}
		}
	}
	return makeValuesSelect(s, desiredTypes, refs, withTables)
}

func makeSchemaTable(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	// If there's no tables, don't keep failing in this function, just make
	// a values table.
	if len(s.tables) == 0 {
		return makeValuesTable(s, refs, forJoin)
	}
	expr, _, _, exprRefs, ok := s.getSchemaTableWithIndexHint()
	return expr, exprRefs, ok
}

// getSchemaTable returns a table expression without the index hint.
func (s *Smither) getSchemaTable() (tree.TableExpr, *tree.TableName, *tableRef, colRefs, bool) {
	ate, name, tableRef, refs, ok := s.getSchemaTableWithIndexHint()
	if ate != nil {
		ate.IndexFlags = nil
	}
	return ate, name, tableRef, refs, ok
}

// getSchemaTableWithIndexHint returns a table expression that might contain an
// index hint.
func (s *Smither) getSchemaTableWithIndexHint() (
	*tree.AliasedTableExpr,
	*tree.TableName,
	*tableRef,
	colRefs,
	bool,
) {
	table, ok := s.getRandTable()
	if !ok {
		return nil, nil, nil, nil, false
	}
	alias := s.name("tab")
	name := tree.NewUnqualifiedTableName(alias)
	expr, refs := s.tableExpr(table.tableRef, name)
	return &tree.AliasedTableExpr{
		Expr:       expr,
		IndexFlags: table.indexFlags,
		As:         tree.AliasClause{Alias: alias},
	}, name, table.tableRef, refs, true
}

func (s *Smither) tableExpr(table *tableRef, name *tree.TableName) (tree.TableExpr, colRefs) {
	refs := make(colRefs, len(table.Columns))
	for i, c := range table.Columns {
		refs[i] = &colRef{
			typ: tree.MustBeStaticallyKnownType(c.Type),
			item: tree.NewColumnItem(
				name,
				c.Name,
			),
		}
	}
	return table.TableName, refs
}

var (
	mutatingStatements = []statementWeight{
		{10, makeInsert},
		{10, makeDelete},
		{10, makeUpdate},
		{1, makeAlter},
		{1, makeBegin},
		{2, makeRollback},
		{6, makeCommit},
		{1, makeBackup},
		{1, makeRestore},
		{1, makeExport},
		{1, makeImport},
	}
	nonMutatingStatements = []statementWeight{
		{10, makeSelect},
	}
	allStatements = append(mutatingStatements, nonMutatingStatements...)

	mutatingTableExprs = []tableExprWeight{
		{1, makeInsertReturning},
		{1, makeDeleteReturning},
		{1, makeUpdateReturning},
	}
	nonMutatingTableExprs = []tableExprWeight{
		{40, makeMergeJoinExpr},
		{40, makeEquiJoinExpr},
		{20, makeSchemaTable},
		{10, makeJoinExpr},
		{1, makeValuesTable},
		{2, makeSelectTable},
	}
	allTableExprs = append(mutatingTableExprs, nonMutatingTableExprs...)

	selectStmts = []selectStatementWeight{
		{1, makeValuesSelect},
		{1, makeSetOp},
		{1, makeSelectClause},
	}
)

// makeTableExpr returns a tableExpr. If forJoin is true the tableExpr is
// valid to be used as a join reference.
func makeTableExpr(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if s.canRecurse() {
		for i := 0; i < retryCount; i++ {
			expr, exprRefs, ok := s.tableExprSampler.Next()(s, refs, forJoin)
			if ok {
				return expr, exprRefs, ok
			}
		}
	}
	return makeSchemaTable(s, refs, forJoin)
}

type typedExpr struct {
	tree.TypedExpr
	typ *types.T
}

func makeTypedExpr(expr tree.TypedExpr, typ *types.T) tree.TypedExpr {
	return typedExpr{
		TypedExpr: expr,
		typ:       typ,
	}
}

func (t typedExpr) ResolvedType() *types.T {
	return t.typ
}

var joinTypes = []string{
	"",
	tree.AstFull,
	tree.AstLeft,
	tree.AstRight,
	tree.AstCross,
	tree.AstInner,
}

func makeJoinExpr(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	left, leftRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}
	right, rightRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}

	joinExpr := &tree.JoinTableExpr{
		JoinType: joinTypes[s.rnd.Intn(len(joinTypes))],
		Left:     left,
		Right:    right,
	}

	if joinExpr.JoinType != tree.AstCross {
		on := makeBoolExpr(s, refs)
		joinExpr.Cond = &tree.OnJoinCond{Expr: on}
	}
	joinRefs := leftRefs.extend(rightRefs...)

	return joinExpr, joinRefs, true
}

func makeEquiJoinExpr(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	left, leftRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}
	right, rightRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}

	// Determine overlapping types.
	var available [][2]tree.TypedExpr
	for _, leftCol := range leftRefs {
		for _, rightCol := range rightRefs {
			// You can't compare voids.
			if leftCol.typ.Oid() == oid.T_void || rightCol.typ.Oid() == oid.T_void {
				continue
			}
			if leftCol.typ.Equivalent(rightCol.typ) {
				available = append(available, [2]tree.TypedExpr{
					typedParen(leftCol.item, leftCol.typ),
					typedParen(rightCol.item, rightCol.typ),
				})
			}
		}
	}
	if len(available) == 0 {
		// left and right didn't have any columns with the same type.
		return nil, nil, false
	}

	s.rnd.Shuffle(len(available), func(i, j int) {
		available[i], available[j] = available[j], available[i]
	})

	var cond tree.TypedExpr
	for (cond == nil || s.coin()) && len(available) > 0 {
		v := available[0]
		available = available[1:]
		expr := tree.NewTypedComparisonExpr(treecmp.MakeComparisonOperator(treecmp.EQ), v[0], v[1])
		if cond == nil {
			cond = expr
		} else {
			cond = tree.NewTypedAndExpr(cond, expr)
		}
	}

	joinExpr := &tree.JoinTableExpr{
		Left:  left,
		Right: right,
		Cond:  &tree.OnJoinCond{Expr: cond},
	}
	joinRefs := leftRefs.extend(rightRefs...)
	// If we have a nil cond, then we didn't succeed in generating this join
	// expr.
	ok = cond != nil
	return joinExpr, joinRefs, ok
}

func makeMergeJoinExpr(s *Smither, _ colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	// A merge join is an equijoin where both sides are sorted in the same
	// direction. Do this by looking for two indexes that have column
	// prefixes that are the same type and direction. Identical indexes
	// are fine.

	// Start with some random index.
	leftTableName, leftIdx, _, ok := s.getRandIndex()
	if !ok {
		return nil, nil, false
	}

	leftAlias := s.name("tab")
	rightAlias := s.name("tab")
	leftAliasName := tree.NewUnqualifiedTableName(leftAlias)
	rightAliasName := tree.NewUnqualifiedTableName(rightAlias)

	// Now look for one that satisfies our constraints (some shared prefix
	// of type + direction), might end up being the same one. We rely on
	// Go's non-deterministic map iteration ordering for randomness.
	rightTableName, cols := func() (*tree.TableIndexName, [][2]colRef) {
		s.lock.RLock()
		defer s.lock.RUnlock()
		for tbl, idxs := range s.indexes {
			for idxName, idx := range idxs {
				rightTableName := &tree.TableIndexName{
					Table: tbl,
					Index: tree.UnrestrictedName(idxName),
				}
				// cols keeps track of matching column pairs.
				var cols [][2]colRef
				for _, rightColElem := range idx.Columns {
					rightCol := s.columns[tbl][rightColElem.Column]
					leftColElem := leftIdx.Columns[len(cols)]
					leftCol := s.columns[leftTableName.Table][leftColElem.Column]
					if rightColElem.Direction != leftColElem.Direction {
						break
					}
					if leftCol == nil || rightCol == nil {
						// TODO(yuzefovich): there are some cases here where
						// column references are nil, but we aren't yet sure
						// why. Rather than panicking, just break.
						break
					}
					if !tree.MustBeStaticallyKnownType(rightCol.Type).Equivalent(tree.MustBeStaticallyKnownType(leftCol.Type)) {
						break
					}
					leftType := tree.MustBeStaticallyKnownType(leftCol.Type)
					rightType := tree.MustBeStaticallyKnownType(rightCol.Type)
					// You can't compare voids.
					if leftType.Oid() == oid.T_void || rightType.Oid() == oid.T_void {
						break
					}
					cols = append(cols, [2]colRef{
						{
							typ:  leftType,
							item: tree.NewColumnItem(leftAliasName, leftColElem.Column),
						},
						{
							typ:  rightType,
							item: tree.NewColumnItem(rightAliasName, rightColElem.Column),
						},
					})
					if len(cols) >= len(leftIdx.Columns) {
						break
					}
				}
				if len(cols) > 0 {
					return rightTableName, cols
				}
			}
		}
		// Since we can always match leftIdx we should never get here.
		panic("unreachable")
	}()

	// joinRefs are limited to columns in the indexes (even if they don't
	// appear in the join condition) because non-stored columns will cause
	// a hash join.
	var joinRefs colRefs
	for _, pair := range cols {
		joinRefs = append(joinRefs, &pair[0], &pair[1])
	}

	var cond tree.TypedExpr
	// Pick some prefix of the available columns. Not randomized because it
	// needs to match the index column order.
	for (cond == nil || s.coin()) && len(cols) > 0 {
		v := cols[0]
		cols = cols[1:]
		expr := tree.NewTypedComparisonExpr(
			treecmp.MakeComparisonOperator(treecmp.EQ),
			typedParen(v[0].item, v[0].typ),
			typedParen(v[1].item, v[1].typ),
		)
		if cond == nil {
			cond = expr
		} else {
			cond = tree.NewTypedAndExpr(cond, expr)
		}
	}

	joinExpr := &tree.JoinTableExpr{
		Left: &tree.AliasedTableExpr{
			Expr: &leftTableName.Table,
			As:   tree.AliasClause{Alias: leftAlias},
		},
		Right: &tree.AliasedTableExpr{
			Expr: &rightTableName.Table,
			As:   tree.AliasClause{Alias: rightAlias},
		},
		Cond: &tree.OnJoinCond{Expr: cond},
	}
	// If we have a nil cond, then we didn't succeed in generating this join
	// expr.
	ok = cond != nil
	return joinExpr, joinRefs, ok
}

// STATEMENTS

func (s *Smither) makeWith() (*tree.With, tableRefs) {
	if s.disableWith {
		return nil, nil
	}

	// WITHs are pretty rare, so just ignore them a lot.
	if s.coin() {
		return nil, nil
	}
	ctes := make([]*tree.CTE, 0, s.d6()/2)
	if cap(ctes) == 0 {
		return nil, nil
	}
	var tables tableRefs
	for i := 0; i < cap(ctes); i++ {
		var ok bool
		var stmt tree.SelectStatement
		var stmtRefs colRefs
		stmt, stmtRefs, ok = s.makeSelectStmt(s.makeDesiredTypes(), nil /* refs */, tables)
		if !ok {
			continue
		}
		alias := s.name("with")
		tblName := tree.NewUnqualifiedTableName(alias)
		cols := make(tree.NameList, len(stmtRefs))
		defs := make([]*tree.ColumnTableDef, len(stmtRefs))
		for i, r := range stmtRefs {
			var err error
			cols[i] = r.item.ColumnName
			defs[i], err = tree.NewColumnTableDef(r.item.ColumnName, r.typ, false /* isSerial */, nil)
			if err != nil {
				panic(err)
			}
		}
		tables = append(tables, &tableRef{
			TableName: tblName,
			Columns:   defs,
		})
		ctes = append(ctes, &tree.CTE{
			Name: tree.AliasClause{
				Alias: alias,
				Cols:  cols,
			},
			Stmt: stmt,
		})
	}
	return &tree.With{
		CTEList: ctes,
	}, tables
}

var orderDirections = []tree.Direction{
	tree.DefaultDirection,
	tree.Ascending,
	tree.Descending,
}

func (s *Smither) randDirection() tree.Direction {
	return orderDirections[s.rnd.Intn(len(orderDirections))]
}

var nullabilities = []tree.Nullability{
	tree.NotNull,
	tree.Null,
	tree.SilentNull,
}

func (s *Smither) randNullability() tree.Nullability {
	return nullabilities[s.rnd.Intn(len(nullabilities))]
}

var dropBehaviors = []tree.DropBehavior{
	tree.DropDefault,
	tree.DropRestrict,
	tree.DropCascade,
}

func (s *Smither) randDropBehavior() tree.DropBehavior {
	return dropBehaviors[s.rnd.Intn(len(dropBehaviors))]
}

var stringComparisons = []treecmp.ComparisonOperatorSymbol{
	treecmp.Like,
	treecmp.NotLike,
	treecmp.ILike,
	treecmp.NotILike,
	treecmp.SimilarTo,
	treecmp.NotSimilarTo,
	treecmp.RegMatch,
	treecmp.NotRegMatch,
	treecmp.RegIMatch,
	treecmp.NotRegIMatch,
}

func (s *Smither) randStringComparison() treecmp.ComparisonOperator {
	return treecmp.MakeComparisonOperator(stringComparisons[s.rnd.Intn(len(stringComparisons))])
}

// makeSelectTable returns a TableExpr of the form `(SELECT ...)`, which
// would end up looking like `SELECT ... FROM (SELECT ...)`.
func makeSelectTable(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	stmt, stmtRefs, ok := s.makeSelect(nil /* desiredTypes */, refs)
	if !ok {
		return nil, nil, false
	}

	table := s.name("tab")
	names := make(tree.NameList, len(stmtRefs))
	clauseRefs := make(colRefs, len(stmtRefs))
	for i, ref := range stmtRefs {
		names[i] = s.name("col")
		clauseRefs[i] = &colRef{
			typ: ref.typ,
			item: tree.NewColumnItem(
				tree.NewUnqualifiedTableName(table),
				names[i],
			),
		}
	}

	return &tree.AliasedTableExpr{
		Expr: &tree.Subquery{
			Select: &tree.ParenSelect{Select: stmt},
		},
		As: tree.AliasClause{
			Alias: table,
			Cols:  names,
		},
	}, clauseRefs, true
}

func makeSelectClause(
	s *Smither, desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, bool) {
	stmt, selectRefs, _, ok := s.makeSelectClause(desiredTypes, refs, withTables)
	return stmt, selectRefs, ok
}

func (s *Smither) makeSelectClause(
	desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (clause *tree.SelectClause, selectRefs, orderByRefs colRefs, ok bool) {
	if desiredTypes == nil && s.d9() == 1 {
		return s.makeOrderedAggregate()
	}

	clause = &tree.SelectClause{}

	var fromRefs colRefs
	// Sometimes generate a SELECT with no FROM clause.
	requireFrom := s.d6() != 1
	for (requireFrom && len(clause.From.Tables) < 1) || s.canRecurse() {
		var from tree.TableExpr
		if len(withTables) == 0 || s.coin() {
			// Add a normal data source.
			source, sourceRefs, sourceOk := makeTableExpr(s, refs, false)
			if !sourceOk {
				return nil, nil, nil, false
			}
			from = source
			fromRefs = append(fromRefs, sourceRefs...)
		} else {
			// Add a CTE reference.
			table := withTables[s.rnd.Intn(len(withTables))]

			alias := s.name("cte_ref")
			name := tree.NewUnqualifiedTableName(alias)
			expr, exprRefs := s.tableExpr(table, name)

			from = &tree.AliasedTableExpr{
				Expr: expr,
				As:   tree.AliasClause{Alias: alias},
			}
			fromRefs = append(fromRefs, exprRefs...)
		}
		clause.From.Tables = append(clause.From.Tables, from)
		// Restrict so that we don't have a crazy amount of rows due to many joins.
		if len(clause.From.Tables) >= 4 {
			break
		}
	}

	selectListRefs := refs
	ctx := emptyCtx

	if len(clause.From.Tables) > 0 {
		clause.Where = s.makeWhere(fromRefs)
		orderByRefs = fromRefs
		selectListRefs = selectListRefs.extend(fromRefs...)

		if s.d6() <= 2 && s.canRecurse() {
			// Enable GROUP BY. Choose some random subset of the
			// fromRefs.
			// TODO(mjibson): Refence handling and aggregation functions
			// aren't quite handled correctly here. This currently
			// does well enough to at least find some bugs but should
			// be improved to do the correct thing wrt aggregate
			// functions. That is, the select and having exprs can
			// either reference a group by column or a non-group by
			// column in an aggregate function. It's also possible
			// the where and order by exprs are not correct.
			var groupByRefs colRefs
			for _, r := range fromRefs {
				if s.postgres && r.typ.Family() == types.Box2DFamily {
					continue
				}
				groupByRefs = append(groupByRefs, r)
			}
			s.rnd.Shuffle(len(groupByRefs), func(i, j int) {
				groupByRefs[i], groupByRefs[j] = groupByRefs[j], groupByRefs[i]
			})
			var groupBy tree.GroupBy
			for (len(groupBy) < 1 || s.coin()) && len(groupBy) < len(groupByRefs) {
				groupBy = append(groupBy, groupByRefs[len(groupBy)].item)
			}
			groupByRefs = groupByRefs[:len(groupBy)]
			clause.GroupBy = groupBy
			clause.Having = s.makeHaving(fromRefs)
			selectListRefs = groupByRefs
			orderByRefs = groupByRefs
			// TODO(mjibson): also use this context sometimes in
			// non-aggregate mode (select sum(x) from a).
			ctx = groupByCtx
		} else if s.d6() <= 1 && s.canRecurse() {
			// Enable window functions. This will enable them for all
			// exprs, but makeFunc will only let a few through.
			ctx = windowCtx
		}
	}

	selectList, selectRefs, ok := s.makeSelectList(ctx, desiredTypes, selectListRefs)
	if !ok {
		return nil, nil, nil, false
	}
	clause.Exprs = selectList

	return clause, selectRefs, orderByRefs, true
}

func (s *Smither) makeOrderedAggregate() (
	clause *tree.SelectClause,
	selectRefs, orderByRefs colRefs,
	ok bool,
) {
	// We need a SELECT with a GROUP BY on ordered columns. Choose a random
	// table and index from that table and pick a random prefix from it.
	tableExpr, tableAlias, table, tableColRefs, ok := s.getSchemaTableWithIndexHint()
	if !ok {
		return nil, nil, nil, false
	}
	_, _, idxRefs, ok := s.getRandTableIndex(*table.TableName, *tableAlias)
	if !ok {
		return nil, nil, nil, false
	}

	var groupBy tree.GroupBy
	for (len(groupBy) < 1 || s.coin()) && len(groupBy) < len(idxRefs) {
		groupBy = append(groupBy, idxRefs[len(groupBy)].item)
	}
	idxRefs = idxRefs[:len(groupBy)]
	alias := s.name("col")
	selectRefs = colRefs{
		{
			typ:  types.Int,
			item: &tree.ColumnItem{ColumnName: alias},
		},
	}
	return &tree.SelectClause{
		Exprs: tree.SelectExprs{
			{
				Expr: countStar,
				As:   tree.UnrestrictedName(alias),
			},
		},
		From: tree.From{
			Tables: tree.TableExprs{tableExpr},
		},
		Where:   s.makeWhere(tableColRefs),
		GroupBy: groupBy,
		Having:  s.makeHaving(idxRefs),
	}, selectRefs, idxRefs, true
}

var countStar = func() tree.TypedExpr {
	fn := tree.FunDefs["count"]
	typ := types.Int
	return tree.NewTypedFuncExpr(
		tree.ResolvableFunctionReference{FunctionReference: fn},
		0, /* aggQualifier */
		tree.TypedExprs{tree.UnqualifiedStar{}},
		nil, /* filter */
		nil, /* window */
		typ,
		&fn.FunctionProperties,
		fn.Definition[0].(*tree.Overload),
	)
}()

func makeSelect(s *Smither) (tree.Statement, bool) {
	stmt, refs, ok := s.makeSelect(nil, nil)
	if !ok {
		return stmt, ok
	}
	if s.outputSort {
		order := make(tree.OrderBy, len(refs))
		for i, r := range refs {
			var expr tree.Expr = r.item
			// PostGIS cannot order box2d types, so we cast to string so the
			// order is deterministic.
			if s.postgres && r.typ.Family() == types.Box2DFamily {
				expr = &tree.CastExpr{Expr: r.item, Type: types.String}
			}
			order[i] = &tree.Order{
				Expr:       expr,
				NullsOrder: tree.NullsFirst,
			}
		}
		stmt = &tree.Select{
			Select: &tree.SelectClause{
				Exprs: tree.SelectExprs{
					tree.SelectExpr{
						Expr: tree.UnqualifiedStar{},
					},
				},
				From: tree.From{
					Tables: tree.TableExprs{
						&tree.AliasedTableExpr{
							Expr: &tree.Subquery{
								Select: &tree.ParenSelect{Select: stmt},
							},
							As: tree.AliasClause{
								Alias: s.name("tab"),
							},
						},
					},
				},
			},
			OrderBy: order,
		}
	}
	return stmt, ok
}

func (s *Smither) makeSelect(desiredTypes []*types.T, refs colRefs) (*tree.Select, colRefs, bool) {
	withStmt, withTables := s.makeWith()

	clause, selectRefs, orderByRefs, ok := s.makeSelectClause(desiredTypes, refs, withTables)
	if !ok {
		return nil, nil, ok
	}

	stmt := tree.Select{
		Select:  clause,
		With:    withStmt,
		OrderBy: s.makeOrderBy(orderByRefs),
		Limit:   makeLimit(s),
	}

	return &stmt, selectRefs, true
}

// makeSelectList generates SelectExprs corresponding to
// desiredTypes. desiredTypes can be nil which causes types to be randomly
// selected from refs, improving the chance they are chosen during
// makeScalar. Especially useful with the AvoidConsts option.
func (s *Smither) makeSelectList(
	ctx Context, desiredTypes []*types.T, refs colRefs,
) (tree.SelectExprs, colRefs, bool) {
	// If we don't have any desired types, generate some from the given refs.
	if len(desiredTypes) == 0 {
		s.sample(len(refs), 6, func(i int) {
			desiredTypes = append(desiredTypes, refs[i].typ)
		})
	}
	// If we still don't have any then there weren't any refs.
	if len(desiredTypes) == 0 {
		desiredTypes = s.makeDesiredTypes()
	}
	result := make(tree.SelectExprs, len(desiredTypes))
	selectRefs := make(colRefs, len(desiredTypes))
	for i, t := range desiredTypes {
		result[i].Expr = makeScalarContext(s, ctx, t, refs)
		alias := s.name("col")
		result[i].As = tree.UnrestrictedName(alias)
		selectRefs[i] = &colRef{
			typ:  t,
			item: &tree.ColumnItem{ColumnName: alias},
		}
	}
	return result, selectRefs, true
}

func makeDelete(s *Smither) (tree.Statement, bool) {
	stmt, _, ok := s.makeDelete(nil)
	return stmt, ok
}

func (s *Smither) makeDelete(refs colRefs) (*tree.Delete, *tableRef, bool) {
	table, _, tableRef, tableRefs, ok := s.getSchemaTable()
	if !ok {
		return nil, nil, false
	}

	del := &tree.Delete{
		Table:     table,
		Where:     s.makeWhere(tableRefs),
		OrderBy:   s.makeOrderBy(tableRefs),
		Limit:     makeLimit(s),
		Returning: &tree.NoReturningClause{},
	}
	if del.Limit == nil {
		del.OrderBy = nil
	}

	return del, tableRef, true
}

func makeDeleteReturning(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	return s.makeDeleteReturning(refs)
}

func (s *Smither) makeDeleteReturning(refs colRefs) (tree.TableExpr, colRefs, bool) {
	del, delRef, ok := s.makeDelete(refs)
	if !ok {
		return nil, nil, false
	}
	var returningRefs colRefs
	del.Returning, returningRefs = s.makeReturning(delRef)
	return &tree.StatementSource{
		Statement: del,
	}, returningRefs, true
}

func makeUpdate(s *Smither) (tree.Statement, bool) {
	stmt, _, ok := s.makeUpdate(nil)
	return stmt, ok
}

func (s *Smither) makeUpdate(refs colRefs) (*tree.Update, *tableRef, bool) {
	table, _, tableRef, tableRefs, ok := s.getSchemaTable()
	if !ok {
		return nil, nil, false
	}
	cols := make(map[tree.Name]*tree.ColumnTableDef)
	for _, c := range tableRef.Columns {
		cols[c.Name] = c
	}

	update := &tree.Update{
		Table:     table,
		Where:     s.makeWhere(tableRefs),
		OrderBy:   s.makeOrderBy(tableRefs),
		Limit:     makeLimit(s),
		Returning: &tree.NoReturningClause{},
	}
	// Each row can be set at most once. Copy tableRefs to upRefs and remove
	// elements from it as we use them.
	upRefs := tableRefs.extend()
	for (len(update.Exprs) < 1 || s.coin()) && len(upRefs) > 0 {
		n := s.rnd.Intn(len(upRefs))
		ref := upRefs[n]
		upRefs = append(upRefs[:n], upRefs[n+1:]...)
		col := cols[ref.item.ColumnName]
		// Ignore computed and hidden columns.
		if col == nil || col.Computed.Computed || col.Hidden {
			continue
		}
		var expr tree.TypedExpr
		for {
			expr = makeScalar(s, ref.typ, tableRefs)
			// Make sure expr isn't null if that's not allowed.
			if col.Nullable.Nullability != tree.NotNull || expr != tree.DNull {
				break
			}
		}
		update.Exprs = append(update.Exprs, &tree.UpdateExpr{
			Names: tree.NameList{ref.item.ColumnName},
			Expr:  expr,
		})
	}
	if len(update.Exprs) == 0 {
		panic("empty")
	}
	if update.Limit == nil {
		update.OrderBy = nil
	}

	return update, tableRef, true
}

func makeUpdateReturning(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	return s.makeUpdateReturning(refs)
}

func (s *Smither) makeUpdateReturning(refs colRefs) (tree.TableExpr, colRefs, bool) {
	update, updateRef, ok := s.makeUpdate(refs)
	if !ok {
		return nil, nil, false
	}
	var returningRefs colRefs
	update.Returning, returningRefs = s.makeReturning(updateRef)
	return &tree.StatementSource{
		Statement: update,
	}, returningRefs, true
}

func makeInsert(s *Smither) (tree.Statement, bool) {
	stmt, _, ok := s.makeInsert(nil)
	return stmt, ok
}

func makeBegin(s *Smither) (tree.Statement, bool) {
	return &tree.BeginTransaction{}, true
}

const letters = "abcdefghijklmnopqrstuvwxyz"

func makeSavepoint(s *Smither) (tree.Statement, bool) {
	savepointName := randgen.RandString(s.rnd, s.d9(), letters)
	s.activeSavepoints = append(s.activeSavepoints, savepointName)
	return &tree.Savepoint{Name: tree.Name(savepointName)}, true
}

func makeReleaseSavepoint(s *Smither) (tree.Statement, bool) {
	if len(s.activeSavepoints) == 0 {
		return nil, false
	}
	idx := s.rnd.Intn(len(s.activeSavepoints))
	savepoint := s.activeSavepoints[idx]
	// Remove the released savepoint from our set of active savepoints.
	s.activeSavepoints = append(s.activeSavepoints[:idx], s.activeSavepoints[idx+1:]...)
	return &tree.ReleaseSavepoint{Savepoint: tree.Name(savepoint)}, true
}

func makeRollbackToSavepoint(s *Smither) (tree.Statement, bool) {
	if len(s.activeSavepoints) == 0 {
		return nil, false
	}
	idx := s.rnd.Intn(len(s.activeSavepoints))
	savepoint := s.activeSavepoints[idx]
	// Destroy all savepoints that come after the savepoint we rollback to.
	s.activeSavepoints = s.activeSavepoints[:idx+1]
	return &tree.RollbackToSavepoint{Savepoint: tree.Name(savepoint)}, true
}

func makeCommit(s *Smither) (tree.Statement, bool) {
	return &tree.CommitTransaction{}, true
}

func makeRollback(s *Smither) (tree.Statement, bool) {
	return &tree.RollbackTransaction{}, true
}

// makeInsert has only one valid reference: its table source, which can be
// used only in the optional returning section. Hence the irregular return
// signature.
func (s *Smither) makeInsert(refs colRefs) (*tree.Insert, *tableRef, bool) {
	table, _, tableRef, _, ok := s.getSchemaTable()
	if !ok {
		return nil, nil, false
	}

	insert := &tree.Insert{
		Table:     table,
		Rows:      &tree.Select{},
		Returning: &tree.NoReturningClause{},
	}

	// Use DEFAULT VALUES only sometimes. A nil insert.Rows.Select indicates
	// DEFAULT VALUES.
	if s.d9() == 1 {
		return insert, tableRef, true
	}

	// Use a simple INSERT...VALUES statement some of the time.
	if s.coin() {
		var names tree.NameList
		var row tree.Exprs
		for _, c := range tableRef.Columns {
			// We *must* write a column if it's writable and non-nullable.
			// We *can* write a column if it's writable and nullable.
			// We *cannot* write a column if it's computed or hidden.
			if c.Computed.Computed || c.Hidden {
				continue
			}
			notNull := c.Nullable.Nullability == tree.NotNull
			if notNull || s.coin() {
				names = append(names, c.Name)
				row = append(row, randgen.RandDatum(s.rnd, tree.MustBeStaticallyKnownType(c.Type), !notNull))
			}
		}

		if len(names) == 0 {
			return nil, nil, false
		}

		insert.Columns = names
		insert.Rows = &tree.Select{
			Select: &tree.ValuesClause{
				Rows: []tree.Exprs{row},
			},
		}
		return insert, tableRef, true
	}

	// Otherwise, build a more complex INSERT with a SELECT.
	// Grab some subset of the columns of the table to attempt to insert into.
	var desiredTypes []*types.T
	var names tree.NameList
	unnamed := s.coin()
	for _, c := range tableRef.Columns {
		// We *must* write a column if it's writable and non-nullable.
		// We *can* write a column if it's writable and nullable.
		// We *cannot* write a column if it's computed or hidden.
		//
		// We include all non-computed, non-hidden columns if we are building an
		// unnamed insert. If a column is omitted in an unnamed insert, it would
		// be unlikely that column types of the insert values match the column
		// types of the table.
		if c.Computed.Computed || c.Hidden {
			continue
		}
		if unnamed || c.Nullable.Nullability == tree.NotNull || s.coin() {
			names = append(names, c.Name)
			desiredTypes = append(desiredTypes, tree.MustBeStaticallyKnownType(c.Type))
		}
	}

	if len(desiredTypes) == 0 {
		return nil, nil, false
	}

	if !unnamed {
		insert.Columns = names
	}

	insert.Rows, _, ok = s.makeSelect(desiredTypes, refs)
	if !ok {
		return nil, nil, false
	}

	return insert, tableRef, true
}

func makeInsertReturning(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	return s.makeInsertReturning(refs)
}

func (s *Smither) makeInsertReturning(refs colRefs) (tree.TableExpr, colRefs, bool) {
	insert, insertRef, ok := s.makeInsert(refs)
	if !ok {
		return nil, nil, false
	}
	var returningRefs colRefs
	insert.Returning, returningRefs = s.makeReturning(insertRef)
	return &tree.StatementSource{
		Statement: insert,
	}, returningRefs, true
}

func makeValuesTable(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	types := s.makeDesiredTypes()
	values, valuesRefs := makeValues(s, types, refs)
	return values, valuesRefs, true
}

func makeValues(s *Smither, desiredTypes []*types.T, refs colRefs) (tree.TableExpr, colRefs) {
	numRowsToInsert := s.d6()
	values := tree.ValuesClause{
		Rows: make([]tree.Exprs, numRowsToInsert),
	}

	for i := 0; i < numRowsToInsert; i++ {
		tuple := make([]tree.Expr, len(desiredTypes))
		for j, t := range desiredTypes {
			tuple[j] = makeScalar(s, t, refs)
		}
		values.Rows[i] = tuple
	}
	table := s.name("tab")
	names := make(tree.NameList, len(desiredTypes))
	valuesRefs := make(colRefs, len(desiredTypes))
	for i, typ := range desiredTypes {
		names[i] = s.name("col")
		valuesRefs[i] = &colRef{
			typ: typ,
			item: tree.NewColumnItem(
				tree.NewUnqualifiedTableName(table),
				names[i],
			),
		}
	}

	return &tree.AliasedTableExpr{
		Expr: &tree.Subquery{
			Select: &tree.ParenSelect{
				Select: &tree.Select{
					Select: &values,
				},
			},
		},
		As: tree.AliasClause{
			Alias: table,
			Cols:  names,
		},
	}, valuesRefs
}

func makeValuesSelect(
	s *Smither, desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, bool) {
	values, valuesRefs := makeValues(s, desiredTypes, refs)

	// Returning just &values here would result in a query like `VALUES (...)` where
	// the columns are arbitrarily named by index (column1, column2, etc.). Since
	// we want to be able to reference the columns in other places we need to
	// name them deterministically. We can use `SELECT * FROM (VALUES (...)) AS
	// tbl (c1, c2, etc.)` to achieve this. There's quite a lot of indirection
	// for how to achieve exactly that syntax as tree nodes, but it works.
	return &tree.SelectClause{
		Exprs: tree.SelectExprs{tree.StarSelectExpr()},
		From: tree.From{
			Tables: tree.TableExprs{values},
		},
	}, valuesRefs, true
}

var setOps = []tree.UnionType{
	tree.UnionOp,
	tree.IntersectOp,
	tree.ExceptOp,
}

func makeSetOp(
	s *Smither, desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, bool) {
	left, leftRefs, ok := s.makeSelectStmt(desiredTypes, refs, withTables)
	if !ok {
		return nil, nil, false
	}

	right, _, ok := s.makeSelectStmt(desiredTypes, refs, withTables)
	if !ok {
		return nil, nil, false
	}

	return &tree.UnionClause{
		Type:  setOps[s.rnd.Intn(len(setOps))],
		Left:  &tree.Select{Select: left},
		Right: &tree.Select{Select: right},
		All:   s.coin(),
	}, leftRefs, true
}

func (s *Smither) makeWhere(refs colRefs) *tree.Where {
	if s.coin() {
		where := makeBoolExpr(s, refs)
		return tree.NewWhere("WHERE", where)
	}
	return nil
}

func (s *Smither) makeHaving(refs colRefs) *tree.Where {
	if s.coin() {
		where := makeBoolExprContext(s, havingCtx, refs)
		return tree.NewWhere("HAVING", where)
	}
	return nil
}

func (s *Smither) makeOrderBy(refs colRefs) tree.OrderBy {
	if len(refs) == 0 {
		return nil
	}
	var ob tree.OrderBy
	for s.coin() {
		ref := refs[s.rnd.Intn(len(refs))]
		// We don't support order by jsonb columns.
		if ref.typ.Family() == types.JsonFamily {
			continue
		}
		// PostGIS cannot order box2d types.
		if s.postgres && ref.typ.Family() == types.Box2DFamily {
			continue
		}
		ob = append(ob, &tree.Order{
			Expr:      ref.item,
			Direction: s.randDirection(),
		})
	}
	return ob
}

func makeLimit(s *Smither) *tree.Limit {
	if s.disableLimits {
		return nil
	}
	if s.d6() > 2 {
		return &tree.Limit{Count: tree.NewDInt(tree.DInt(s.d100()))}
	}
	return nil
}

func (s *Smither) makeReturning(table *tableRef) (*tree.ReturningExprs, colRefs) {
	desiredTypes := s.makeDesiredTypes()

	refs := make(colRefs, len(table.Columns))
	for i, c := range table.Columns {
		refs[i] = &colRef{
			typ:  tree.MustBeStaticallyKnownType(c.Type),
			item: &tree.ColumnItem{ColumnName: c.Name},
		}
	}

	returning := make(tree.ReturningExprs, len(desiredTypes))
	returningRefs := make(colRefs, len(desiredTypes))
	for i, t := range desiredTypes {
		returning[i].Expr = makeScalar(s, t, refs)
		alias := s.name("ret")
		returning[i].As = tree.UnrestrictedName(alias)
		returningRefs[i] = &colRef{
			typ: t,
			item: &tree.ColumnItem{
				ColumnName: alias,
			},
		}
	}
	return &returning, returningRefs
}
