// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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
		{2, makeAlter},
		{1, makeBegin},
		{1, makeRollback},
		{2, makeCommit},
		{1, makeBackup},
		{1, makeRestore},
		{1, makeExport},
		{1, makeImport},
		{1, makeCreateStats},
		{1, makeSetSessionCharacteristics},
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
	tree.AstInner,
	// Please keep AstCross as the last item as the Smither.disableCrossJoins
	// option depends on this in order to avoid cross joins.
	tree.AstCross,
}

func makeJoinExpr(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if s.disableJoins {
		return nil, nil, false
	}
	left, leftRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}
	right, rightRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}
	maxJoinType := len(joinTypes)
	if s.disableCrossJoins {
		maxJoinType = len(joinTypes) - 1
	}
	joinExpr := &tree.JoinTableExpr{
		JoinType: joinTypes[s.rnd.Intn(maxJoinType)],
		Left:     left,
		Right:    right,
	}

	if s.disableCrossJoins {
		if available, ok := getAvailablePairedColsForJoinPreds(s, leftRefs, rightRefs); ok {
			cond := makeAndedJoinCond(s, available, false /* onlyEqualityPreds */)
			joinExpr.Cond = &tree.OnJoinCond{Expr: cond}
		}
	}
	if joinExpr.Cond == nil && joinExpr.JoinType != tree.AstCross {
		var allRefs colRefs
		// We used to make an ON clause only out of projected columns.
		// Now we consider all left and right input relation columns.
		allRefs = make(colRefs, 0, len(leftRefs)+len(rightRefs))
		allRefs = append(allRefs, leftRefs...)
		allRefs = append(allRefs, rightRefs...)
		on := makeBoolExpr(s, allRefs)
		joinExpr.Cond = &tree.OnJoinCond{Expr: on}
	}
	joinRefs := leftRefs.extend(rightRefs...)

	return joinExpr, joinRefs, true
}

func getAvailablePairedColsForJoinPreds(
	s *Smither, leftRefs colRefs, rightRefs colRefs,
) (available [][2]tree.TypedExpr, ok bool) {

	// Determine overlapping types.
	for _, leftCol := range leftRefs {
		for _, rightCol := range rightRefs {
			// Don't compare non-scalar types. This avoids trying to
			// compare types like arrays of tuples, tuple[], which
			// cannot be compared. However, it also avoids comparing
			// some types that can be compared, like arrays.
			if !s.isScalarType(leftCol.typ) || !s.isScalarType(rightCol.typ) {
				continue
			}
			if s.disableCrossJoins && (leftCol.typ.Family() == types.OidFamily ||
				rightCol.typ.Family() == types.OidFamily) {
				// In smithtests, values in OID columns may have a large number of
				// duplicates. Avoid generating join predicates on these columns to
				// prevent what are essentially cross joins, which may time out.
				// TODO(msirek): Improve the Smither's ability to find and insert valid
				//               OIDs, so this rule can be removed.
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
		return nil, false
	}
	s.rnd.Shuffle(len(available), func(i, j int) {
		available[i], available[j] = available[j], available[i]
	})

	return available, true
}

// makeAndedJoinCond makes a join predicate for each left/right pair of columns
// in `available` and ANDs them together. Typically these expression pairs are
// column pairs, but really they could be any pair of expressions. If
// `onlyEqualityPreds` is true, only equijoin predicates such as `c1 = c2` are
// generated, otherwise we probabilistically choose between operators `=`, `>`,
// `<`, `>=`, `<=` and `<>`, except for the first generated predicate, which
// always uses `=`.
func makeAndedJoinCond(
	s *Smither, available [][2]tree.TypedExpr, onlyEqualityPreds bool,
) tree.TypedExpr {
	var otherOps []treecmp.ComparisonOperatorSymbol
	if !onlyEqualityPreds {
		otherOps = make([]treecmp.ComparisonOperatorSymbol, 0, 5)
		otherOps = append(otherOps, treecmp.LT)
		otherOps = append(otherOps, treecmp.GT)
		otherOps = append(otherOps, treecmp.LE)
		otherOps = append(otherOps, treecmp.GE)
		otherOps = append(otherOps, treecmp.NE)
	}
	var cond tree.TypedExpr
	for (cond == nil || s.coin()) && len(available) > 0 {
		v := available[0]
		available = available[1:]
		var expr *tree.ComparisonExpr
		_, expressionsAreComparable := tree.CmpOps[treecmp.LT].LookupImpl(v[0].ResolvedType(), v[1].ResolvedType())
		useEQ := cond == nil || onlyEqualityPreds || !expressionsAreComparable || s.coin()
		if useEQ {
			expr = tree.NewTypedComparisonExpr(treecmp.MakeComparisonOperator(treecmp.EQ), v[0], v[1])
		} else {
			idx := s.rnd.Intn(len(otherOps))
			expr = tree.NewTypedComparisonExpr(treecmp.MakeComparisonOperator(otherOps[idx]), v[0], v[1])
		}
		if cond == nil {
			cond = expr
		} else {
			cond = tree.NewTypedAndExpr(cond, expr)
		}
	}
	return cond
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

	s.lock.RLock()
	defer s.lock.RUnlock()
	// Determine overlapping types.
	available, ok := getAvailablePairedColsForJoinPreds(s, leftRefs, rightRefs)
	if !ok {
		// left and right didn't have any columns with the same type.
		return nil, nil, false
	}

	cond := makeAndedJoinCond(s, available, true /* onlyEqualityPreds */)
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
	// of type + direction), might end up being the same one.
	rightTableName, cols, ok := func() (*tree.TableIndexName, [][2]colRef, bool) {
		s.lock.RLock()
		defer s.lock.RUnlock()
		if len(s.tables) == 0 {
			return nil, nil, false
		}
		// Iterate in deterministic but random order over all tables.
		tableNames := make([]tree.TableName, 0, len(s.tables))
		for t := range s.indexes {
			tableNames = append(tableNames, t)
		}
		sort.Slice(tableNames, func(i, j int) bool {
			return strings.Compare(tableNames[i].String(), tableNames[j].String()) < 0
		})
		s.rnd.Shuffle(len(tableNames), func(i, j int) {
			tableNames[i], tableNames[j] = tableNames[j], tableNames[i]
		})
		for _, tbl := range tableNames {
			// Iterate in deterministic but random order over all indexes.
			idxs := s.getAllIndexesForTableRLocked(tbl)
			for _, idx := range idxs {
				rightTableName := &tree.TableIndexName{
					Table: tbl,
					Index: tree.UnrestrictedName(idx.Name),
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
					// Don't compare non-scalar types. This avoids trying to
					// compare types like arrays of tuples, tuple[], which
					// cannot be compared. However, it also avoids comparing
					// some types that can be compared, like arrays.
					if !s.isScalarType(leftType) || !s.isScalarType(rightType) {
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
					return rightTableName, cols, true
				}
			}
		}
		return nil, nil, false
	}()
	if !ok {
		return nil, nil, false
	}

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
		cols := make(tree.ColumnDefList, len(stmtRefs))
		defs := make([]*tree.ColumnTableDef, len(stmtRefs))
		for i, r := range stmtRefs {
			var err error
			cols[i].Name = r.item.ColumnName
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

var nullsOrders = []tree.NullsOrder{
	tree.DefaultNullsOrder,
	tree.NullsFirst,
	tree.NullsLast,
}

func (s *Smither) randNullsOrder() tree.NullsOrder {
	return nullsOrders[s.rnd.Intn(len(nullsOrders))]
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

func (s *Smither) randDropBehaviorNoCascade() tree.DropBehavior {
	if s.d6() < 3 {
		// Make RESTRICT twice less likely than DEFAULT.
		return tree.DropRestrict
	}
	return tree.DropDefault
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
	names := make(tree.ColumnDefList, len(stmtRefs))
	clauseRefs := make(colRefs, len(stmtRefs))
	for i, ref := range stmtRefs {
		names[i].Name = s.name("col")
		clauseRefs[i] = &colRef{
			typ: ref.typ,
			item: tree.NewColumnItem(
				tree.NewUnqualifiedTableName(table),
				names[i].Name,
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
	hasJoinTable := false
	for (requireFrom && len(clause.From.Tables) < 1) ||
		(!s.disableCrossJoins && s.canRecurse()) {
		var from tree.TableExpr
		if len(withTables) == 0 || s.coin() {
			// Add a normal data source.
			source, sourceRefs, sourceOk := makeTableExpr(s, refs, false)
			if !sourceOk {
				return nil, nil, nil, false
			}
			from = source
			if _, ok = source.(*tree.JoinTableExpr); ok {
				hasJoinTable = true
			}
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
		tableLimit := 4
		if s.disableJoins {
			tableLimit = 1
		}
		if len(clause.From.Tables) >= tableLimit {
			break
		}
	}

	selectListRefs := refs
	ctx := emptyCtx

	if len(clause.From.Tables) > 0 {
		clause.Where = s.makeWhere(fromRefs, hasJoinTable)
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
		Where:   s.makeWhere(tableColRefs, false /* hasJoinTable */),
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
		fn.Definition[0],
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
			if !s.isOrderable(r.typ) {
				// Cast to string so the order is deterministic.
				expr = &tree.CastExpr{Expr: r.item, Type: types.String}
			}
			order[i] = &tree.Order{
				Expr:       expr,
				Direction:  s.randDirection(),
				NullsOrder: s.randNullsOrder(),
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

	var orderBy tree.OrderBy
	limit := makeLimit(s)
	if limit != nil && s.disableNondeterministicLimits {
		// The ORDER BY clause must be fully specified with all select list columns
		// in order to make a LIMIT clause deterministic.
		orderBy, ok = s.makeOrderByWithAllCols(orderByRefs.extend(selectRefs...))
		if !ok {
			return nil, nil, false
		}
	} else {
		orderBy = s.makeOrderBy(orderByRefs)
	}

	stmt := tree.Select{
		Select:  clause,
		With:    withStmt,
		OrderBy: orderBy,
		Limit:   limit,
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

func makeCreateFunc(s *Smither) (tree.Statement, bool) {
	if s.disableUDFCreation {
		return nil, false
	}
	return s.makeCreateFunc()
}

func makeDropFunc(s *Smither) (tree.Statement, bool) {
	if s.disableUDFCreation {
		return nil, false
	}
	functions.Lock()
	defer functions.Unlock()
	class := tree.NormalClass
	if s.d6() < 3 {
		class = tree.GeneratorClass
	}
	// fns is a map from return type OID to the list of function overloads.
	fns := functions.fns[class]
	if len(fns) == 0 {
		return nil, false
	}
	retOIDs := make([]oid.Oid, 0, len(fns))
	for oid := range fns {
		retOIDs = append(retOIDs, oid)
	}
	// Sort the slice since iterating over fns is non-deterministic.
	sort.Slice(retOIDs, func(i, j int) bool {
		return retOIDs[i] < retOIDs[j]
	})
	// Pick a random starting point within the list of OIDs.
	oidShift := s.rnd.Intn(len(retOIDs))
	for i := 0; i < len(retOIDs); i++ {
		oidPos := (i + oidShift) % len(retOIDs)
		overloads := fns[retOIDs[oidPos]]
		// Pick a random starting point within the list of overloads.
		ovShift := s.rnd.Intn(len(overloads))
		for j := 0; j < len(overloads); j++ {
			fn := overloads[(j+ovShift)%len(overloads)]
			if fn.overload.Type == tree.UDFRoutine {
				routineObj := tree.RoutineObj{
					FuncName: tree.MakeRoutineNameFromPrefix(tree.ObjectNamePrefix{}, tree.Name(fn.def.Name)),
				}
				if s.coin() {
					// Sometimes simulate omitting the parameter specification,
					// sometimes specify all parameters.
					routineObj.Params = fn.overload.RoutineParams
				}
				return &tree.DropRoutine{
					IfExists:  s.d6() < 3,
					Procedure: false,
					Routines:  tree.RoutineObjs{routineObj},
					// TODO(#101380): use s.randDropBehavior() once DROP
					// FUNCTION CASCADE is implemented.
					DropBehavior: s.randDropBehaviorNoCascade(),
				}, true
			}
		}
	}
	return nil, false
}

func (s *Smither) makeCreateFunc() (cf *tree.CreateRoutine, ok bool) {
	fname := s.name("func")
	name := tree.MakeRoutineNameFromPrefix(tree.ObjectNamePrefix{}, fname)

	// There are up to 5 function options that may be applied to UDFs.
	var opts tree.RoutineOptions
	opts = make(tree.RoutineOptions, 0, 5)

	// RoutineLanguage
	lang := tree.RoutineLangSQL
	// Simulate RETURNS TABLE syntax of SQL UDFs in 10% cases.
	returnsTable := s.rnd.Float64() < 0.1
	if s.coin() {
		lang = tree.RoutineLangPLpgSQL
		returnsTable = false
	}
	opts = append(opts, lang)

	// RoutineNullInputBehavior
	// 50%: Do not specify behavior (default is RoutineCalledOnNullInput).
	// ~17%: RoutineCalledOnNullInput
	// ~17%: RoutineReturnsNullOnNullInput
	// ~17%: RoutineStrict
	switch s.d6() {
	case 1:
		opts = append(opts, tree.RoutineCalledOnNullInput)
	case 2:
		opts = append(opts, tree.RoutineReturnsNullOnNullInput)
	case 3:
		opts = append(opts, tree.RoutineStrict)
	}

	// RoutineVolatility
	// 50%: Do not specify behavior (default is volatile).
	// ~17%: RoutineVolatile
	// ~17%: RoutineImmutable
	// ~17%: RoutineStable
	funcVol := tree.RoutineVolatile
	vol := volatility.Volatile
	switch s.d6() {
	case 1:
		funcVol = tree.RoutineImmutable
		vol = volatility.Immutable
	case 2:
		funcVol = tree.RoutineStable
		vol = volatility.Stable
	}
	if funcVol != tree.RoutineVolatile || s.coin() {
		opts = append(opts, funcVol)
	}

	// RoutineLeakproof
	// Leakproof can only be used with immutable volatility. If the function is
	// immutable, also specify leakproof 50% of the time. Otherwise, specify
	// not leakproof 50% of the time (default is not leakproof).
	leakproof := false
	if funcVol == tree.RoutineImmutable {
		leakproof = s.coin()
	}
	if leakproof || s.coin() {
		if leakproof {
			vol = volatility.Leakproof
		}
		opts = append(opts, tree.RoutineLeakproof(leakproof))
	}

	// Determine the parameters before the RoutineBody, but after the language.
	paramCnt := s.rnd.Intn(10)
	if s.types == nil || len(s.types.scalarTypes) == 0 {
		paramCnt = 0
	}
	var usedDefaultExpr bool
	params := make(tree.RoutineParams, paramCnt)
	paramTypes := make(tree.ParamTypes, paramCnt)
	refs := make(colRefs, paramCnt)
	for i := 0; i < paramCnt; i++ {
		// Do not allow collated string types. These are not supported in UDFs.
		// Do not allow RECORD-type parameters for SQL routines.
		// TODO(#105713): lift the RECORD-type restriction for PL/pgSQL.
		ptyp, ptypResolvable := s.randType()
		for ptyp.Family() == types.CollatedStringFamily || ptyp.Identical(types.AnyTuple) {
			ptyp, ptypResolvable = s.randType()
		}
		pname := fmt.Sprintf("p%d", i)
		// TODO(88947): choose VARIADIC once supported too.
		var numAllowedParamClasses = 4
		if returnsTable {
			// We cannot use explicit OUT and INOUT parameters when simulating
			// RETURNS TABLE syntax.
			numAllowedParamClasses = 2
		}
		params[i] = tree.RoutineParam{
			Name:  tree.Name(pname),
			Type:  ptypResolvable,
			Class: tree.RoutineParamClass(s.rnd.Intn(numAllowedParamClasses)),
		}
		if params[i].Class != tree.RoutineParamOut {
			// OUT parameters aren't allowed to have DEFAULT expressions.
			//
			// If we already used a DEFAULT expression, then we must add one for
			// this input parameter too. If we haven't already started using
			// DEFAULT expressions, start with this parameter in 33% cases.
			if usedDefaultExpr || s.d6() < 3 {
				params[i].DefaultVal = makeScalar(s, ptyp, nil /* colRefs */)
				usedDefaultExpr = true
			}
		}
		paramTypes[i] = tree.ParamType{
			Name: pname,
			Typ:  ptyp,
		}
		refs[i] = &colRef{
			typ: ptyp,
			item: &tree.ColumnItem{
				ColumnName: tree.Name(pname),
			},
		}
	}

	// Return a record 50% of the time, which means the UDF can return any number
	// or type in its final SQL statement. Otherwise, pick a random type from
	// this smither's available types.
	rTyp, rTypResolvable := types.AnyTuple, tree.ResolvableTypeReference(types.AnyTuple)
	if s.coin() {
		// Do not allow collated string types. These are not supported in UDFs.
		rTyp, rTypResolvable = s.randType()
		for rTyp.Family() == types.CollatedStringFamily {
			rTyp, rTypResolvable = s.randType()
		}
	}
	if returnsTable {
		// Try to pick an existing tuple type for some time. If that doesn't
		// happen, create a new one.
		for i := 0; rTyp.Family() != types.TupleFamily && i < 10; i++ {
			rTyp, rTypResolvable = s.randType()
		}
		if rTyp.Family() != types.TupleFamily {
			rTyp = s.makeRandTupleType()
			rTypResolvable = rTyp
		}
	}

	// TODO(93049): Allow UDFs to create other UDFs.
	oldDisableMutations := s.disableMutations
	defer func() {
		s.disableUDFCreation = false
		s.disableMutations = oldDisableMutations
	}()
	s.disableUDFCreation = true
	s.disableMutations = (funcVol != tree.RoutineVolatile) || s.disableMutations

	// RoutineBodyStr
	var body string
	if lang == tree.RoutineLangSQL {
		var stmtRefs colRefs
		body, stmtRefs, ok = s.makeRoutineBodySQL(funcVol, refs, rTyp)
		if !ok {
			return nil, false
		}
		// If the rTyp isn't a RECORD, change it to stmtRefs or RECORD depending
		// on how many columns there are to avoid return type mismatch errors.
		if !rTyp.Equivalent(types.AnyTuple) {
			if len(stmtRefs) == 1 && s.coin() && stmtRefs[0].typ.Family() != types.CollatedStringFamily {
				rTyp, rTypResolvable = stmtRefs[0].typ, stmtRefs[0].typ
			} else {
				rTyp, rTypResolvable = types.AnyTuple, types.AnyTuple
			}
		}
	} else {
		plpgsqlRTyp := rTyp
		if plpgsqlRTyp.Identical(types.AnyTuple) {
			// If the return type is RECORD, choose a concrete type for generating
			// RETURN statements.
			plpgsqlRTyp = s.makeRandTupleType()
		}
		body = s.makeRoutineBodyPLpgSQL(paramTypes, plpgsqlRTyp, funcVol)
	}
	opts = append(opts, tree.RoutineBodyStr(body))

	// Return multiple rows with the SETOF option about 33% of the time.
	// PL/pgSQL functions do not currently support SETOF.
	setof := returnsTable
	if lang == tree.RoutineLangSQL && s.d6() < 3 {
		setof = true
	}

	stmt := &tree.CreateRoutine{
		// TODO(yuzefovich): once we start generating procedures, we'll need to
		// ensure that no OUT parameters are added after the first parameter
		// with the DEFAULT expression.
		Replace: s.coin(),
		Name:    name,
		Params:  params,
		Options: opts,
		ReturnType: &tree.RoutineReturnType{
			Type:  rTypResolvable,
			SetOf: setof,
		},
	}

	// Add this function to the functions list so that we can use it in future
	// queries. Unfortunately, if the function fails to be created, then any
	// queries that reference it will also fail.
	class := tree.NormalClass
	if setof {
		class = tree.GeneratorClass
	}

	// We only add overload fields that are necessary to generate functions.
	ov := &tree.Overload{
		Volatility: vol,
		Types:      paramTypes,
		Class:      class,
		Type:       tree.UDFRoutine,
	}

	functions.Lock()
	functions.fns[class][rTyp.Oid()] = append(functions.fns[class][rTyp.Oid()], function{
		def:      tree.NewFunctionDefinition(name.String(), &tree.FunctionProperties{}, nil /* def */),
		overload: ov,
	})
	functions.Unlock()
	return stmt, true
}

func (s *Smither) makeRoutineBodySQL(
	vol tree.RoutineVolatility, refs colRefs, rTyp *types.T,
) (body string, lastStmtRefs colRefs, ok bool) {
	// Generate SQL statements for the function body. More than one may be
	// generated, but only the result of the final statement will matter for
	// the function return type. Use the RoutineBodyStr option so the statements
	// are formatted correctly.
	stmtCnt := s.rnd.Intn(11)
	stmts := make([]string, 0, stmtCnt)
	var stmt tree.Statement
	for i := 0; i < stmtCnt-1; i++ {
		stmt, ok = s.makeSQLStmtForRoutine(vol, refs, nil /* desiredTypes */)
		if !ok {
			continue
		}
		stmts = append(stmts, tree.AsStringWithFlags(stmt, tree.FmtParsable))
	}
	// The return type of the last statement should match the function return
	// type.
	// If mutations are enabled, also use anything from mutatingTableExprs -- needs returning
	desiredTypes := []*types.T{rTyp}
	if s.disableMutations || vol != tree.RoutineVolatile || s.coin() {
		stmt, lastStmtRefs, ok = s.makeSelect(desiredTypes, refs)
		if !ok {
			return "", nil, false
		}
	} else {
		switch s.d6() {
		case 1, 2:
			stmt, lastStmtRefs, ok = s.makeInsertReturning(desiredTypes, refs)
		case 3, 4:
			stmt, lastStmtRefs, ok = s.makeDeleteReturning(desiredTypes, refs)
		case 5, 6:
			stmt, lastStmtRefs, ok = s.makeUpdateReturning(desiredTypes, refs)
		}
		if !ok {
			return "", nil, false
		}
	}
	stmts = append(stmts, tree.AsStringWithFlags(stmt, tree.FmtParsable))
	return "\n" + strings.Join(stmts, ";\n") + "\n", lastStmtRefs, true
}

func (s *Smither) makeSQLStmtForRoutine(
	vol tree.RoutineVolatility, refs colRefs, desiredTypes []*types.T,
) (stmt tree.Statement, ok bool) {
	const numRetries = 5
	for i := 0; i < numRetries; i++ {
		if s.disableMutations || vol != tree.RoutineVolatile || s.coin() {
			stmt, _, ok = s.makeSelect(desiredTypes, refs)
		} else {
			if len(desiredTypes) == 0 && s.coin() {
				// If the caller didn't request particular result types, in 50%
				// cases use the "vanilla" mutation stmts.
				switch s.d6() {
				case 1, 2:
					stmt, _, ok = s.makeInsert(refs)
				case 3, 4:
					stmt, _, ok = s.makeDelete(refs)
				case 5, 6:
					stmt, _, ok = s.makeUpdate(refs)
				}
			} else {
				switch s.d6() {
				case 1, 2:
					stmt, _, ok = s.makeInsertReturning(desiredTypes, refs)
				case 3, 4:
					stmt, _, ok = s.makeDeleteReturning(desiredTypes, refs)
				case 5, 6:
					stmt, _, ok = s.makeUpdateReturning(desiredTypes, refs)
				}
			}
		}
		if ok {
			return stmt, true
		}
	}
	return nil, false
}

func makeDelete(s *Smither) (tree.Statement, bool) {
	stmt, _, ok := s.makeDelete(nil)
	return stmt, ok
}

func (s *Smither) makeDelete(refs colRefs) (*tree.Delete, []*tableRef, bool) {
	table, _, ref, cols, ok := s.getSchemaTable()
	if !ok {
		return nil, nil, false
	}
	tRefs := []*tableRef{ref}

	var hasJoinTable bool
	var using tree.TableExprs
	// With 50% probably add another table into the USING clause.
	for s.coin() {
		t, _, tRef, c, ok2 := s.getSchemaTable()
		if !ok2 {
			break
		}
		hasJoinTable = true
		using = append(using, t)
		tRefs = append(tRefs, tRef)
		cols = append(cols, c...)
	}

	var orderBy tree.OrderBy
	limit := makeLimit(s)
	if limit != nil && s.disableNondeterministicLimits {
		// The ORDER BY clause must be fully specified with all columns in order to
		// make a LIMIT clause deterministic.
		orderBy, ok = s.makeOrderByWithAllCols(cols)
		if !ok {
			return nil, nil, false
		}
	} else {
		orderBy = s.makeOrderBy(cols)
	}

	del := &tree.Delete{
		Table:     table,
		Where:     s.makeWhere(cols, hasJoinTable),
		OrderBy:   orderBy,
		Using:     using,
		Limit:     limit,
		Returning: &tree.NoReturningClause{},
	}
	if del.Limit == nil {
		del.OrderBy = nil
	}

	return del, tRefs, true
}

func makeDeleteReturning(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	del, returningRefs, ok := s.makeDeleteReturning(nil /* desiredTypes */, refs)
	if !ok {
		return nil, nil, false
	}
	return &tree.StatementSource{Statement: del}, returningRefs, true
}

func (s *Smither) makeDeleteReturning(
	desiredTypes []*types.T, refs colRefs,
) (*tree.Delete, colRefs, bool) {
	del, delRef, ok := s.makeDelete(refs)
	if !ok {
		return nil, nil, false
	}
	var returningRefs colRefs
	del.Returning, returningRefs = s.makeReturning(desiredTypes, delRef)
	return del, returningRefs, true
}

func makeUpdate(s *Smither) (tree.Statement, bool) {
	stmt, _, ok := s.makeUpdate(nil)
	return stmt, ok
}

func (s *Smither) makeUpdate(refs colRefs) (*tree.Update, []*tableRef, bool) {
	table, _, ref, cols, ok := s.getSchemaTable()
	if !ok {
		return nil, nil, false
	}
	tRefs := []*tableRef{ref}
	// Each column can be set at most once. Copy colRefs to upRefs - we will
	// remove elements from it as we use them below.
	//
	// Note that we need to make this copy before we append columns from other
	// tables added into the FROM clause below.
	upRefs := cols.extend()

	var hasJoinTable bool
	var from tree.TableExprs
	// With 50% probably add another table into the FROM clause.
	for s.coin() {
		t, _, tRef, c, ok2 := s.getSchemaTable()
		if !ok2 {
			break
		}
		hasJoinTable = true
		from = append(from, t)
		tRefs = append(tRefs, tRef)
		cols = append(cols, c...)
	}

	var orderBy tree.OrderBy
	limit := makeLimit(s)
	if limit != nil && s.disableNondeterministicLimits {
		// The ORDER BY clause must be fully specified with all columns in order to
		// make a LIMIT clause deterministic.
		orderBy, ok = s.makeOrderByWithAllCols(cols)
		if !ok {
			return nil, nil, false
		}
	} else {
		orderBy = s.makeOrderBy(cols)
	}

	update := &tree.Update{
		Table:     table,
		From:      from,
		Where:     s.makeWhere(cols, hasJoinTable),
		OrderBy:   orderBy,
		Limit:     limit,
		Returning: &tree.NoReturningClause{},
	}
	colByName := make(map[tree.Name]*tree.ColumnTableDef)
	for _, c := range ref.Columns {
		colByName[c.Name] = c
	}
	for (len(update.Exprs) < 1 || s.coin()) && len(upRefs) > 0 {
		n := s.rnd.Intn(len(upRefs))
		ref := upRefs[n]
		upRefs = append(upRefs[:n], upRefs[n+1:]...)
		col := colByName[ref.item.ColumnName]
		// Ignore computed and hidden columns.
		if col == nil || col.Computed.Computed || col.Hidden {
			continue
		}
		var expr tree.TypedExpr
		for {
			expr = makeScalar(s, ref.typ, cols)
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

	return update, tRefs, true
}

func makeUpdateReturning(s *Smither, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	update, returningRefs, ok := s.makeUpdateReturning(nil /* desiredTypes */, refs)
	if !ok {
		return nil, nil, false
	}
	return &tree.StatementSource{Statement: update}, returningRefs, true
}

func (s *Smither) makeUpdateReturning(
	desiredTypes []*types.T, refs colRefs,
) (*tree.Update, colRefs, bool) {
	update, updateRef, ok := s.makeUpdate(refs)
	if !ok {
		return nil, nil, false
	}
	var returningRefs colRefs
	update.Returning, returningRefs = s.makeReturning(desiredTypes, updateRef)
	return update, returningRefs, true
}

func makeInsert(s *Smither) (tree.Statement, bool) {
	stmt, _, ok := s.makeInsert(nil)
	return stmt, ok
}

func makeTransactionModes(s *Smither) tree.TransactionModes {
	levels := []tree.IsolationLevel{
		tree.UnspecifiedIsolation,
		tree.ReadUncommittedIsolation,
		tree.ReadCommittedIsolation,
		tree.RepeatableReadIsolation,
		tree.SnapshotIsolation,
		tree.SerializableIsolation,
	}
	return tree.TransactionModes{Isolation: levels[s.rnd.Intn(len(levels))]}
}

func makeBegin(s *Smither) (tree.Statement, bool) {
	modes := makeTransactionModes(s)
	return &tree.BeginTransaction{Modes: modes}, true
}

const letters = "abcdefghijklmnopqrstuvwxyz"

func makeSavepoint(s *Smither) (tree.Statement, bool) {
	savepointName := util.RandString(s.rnd, s.d9(), letters)
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

func makeSetSessionCharacteristics(s *Smither) (tree.Statement, bool) {
	if s.disableIsolationChange {
		return nil, false
	}
	modes := makeTransactionModes(s)
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	modes.Format(fmtCtx)
	if fmtCtx.String() == "" {
		// If no options are specified, then we would get an invalid SET SESSION
		// CHARACTERISTICS statement.
		return nil, false
	}
	return &tree.SetSessionCharacteristics{Modes: modes}, true
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
		if tableRef.insertDefaultsAllowed() {
			return insert, tableRef, true
		}
	}

	insertValues := true
	if !s.disableInsertSelect {
		insertValues = s.coin()
	}
	// Use a simple INSERT...VALUES statement some of the time.
	if insertValues {
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
				row = append(row,
					randgen.RandDatumWithNullChance(
						s.rnd,
						tree.MustBeStaticallyKnownType(c.Type),
						randgen.NullChance(!notNull),
						s.favorCommonData,
						c.Unique.IsUnique || c.PrimaryKey.IsPrimaryKey,
					))
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
	columnSkipped := false
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
		} else {
			columnSkipped = true
		}
	}

	if len(desiredTypes) == 0 {
		return nil, nil, false
	}

	if columnSkipped {
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
	insert, returningRefs, ok := s.makeInsertReturning(nil /* desiredTypes */, refs)
	if !ok {
		return nil, nil, false
	}
	return &tree.StatementSource{Statement: insert}, returningRefs, true
}

func (s *Smither) makeInsertReturning(
	desiredTypes []*types.T, refs colRefs,
) (*tree.Insert, colRefs, bool) {
	insert, insertRef, ok := s.makeInsert(refs)
	if !ok {
		return nil, nil, false
	}
	var returningRefs colRefs
	insert.Returning, returningRefs = s.makeReturning(desiredTypes, []*tableRef{insertRef})
	return insert, returningRefs, true
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
	names := make(tree.ColumnDefList, len(desiredTypes))
	valuesRefs := make(colRefs, len(desiredTypes))
	for i, typ := range desiredTypes {
		names[i].Name = s.name("col")
		valuesRefs[i] = &colRef{
			typ: typ,
			item: tree.NewColumnItem(
				tree.NewUnqualifiedTableName(table),
				names[i].Name,
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

func (s *Smither) makeWhere(refs colRefs, hasJoinTable bool) *tree.Where {
	var generateWhere bool
	if s.lowProbWhereWithJoinTables && hasJoinTable {
		// One out of 5 chance of generating a WHERE clause with join tables.
		whereChance := 5
		generateWhere = s.rnd.Intn(whereChance) == 0
	} else {
		generateWhere = s.coin()
	}
	if generateWhere {
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

func (s *Smither) isOrderable(typ *types.T) bool {
	if s.postgres {
		// PostGIS cannot order box2d types.
		return typ.Family() != types.Box2DFamily
	}
	switch typ.Family() {
	case types.TSQueryFamily, types.TSVectorFamily:
		// We can't order by these types - see #92165.
		return false
	default:
		return true
	}
}

func (s *Smither) makeOrderBy(refs colRefs) tree.OrderBy {
	if len(refs) == 0 {
		return nil
	}
	var ob tree.OrderBy
	for s.coin() {
		ref := refs[s.rnd.Intn(len(refs))]
		if !s.isOrderable(ref.typ) {
			continue
		}
		ob = append(ob, &tree.Order{
			Expr:       ref.item,
			Direction:  s.randDirection(),
			NullsOrder: s.randNullsOrder(),
		})
	}
	return ob
}

// makeOrderByWithAllCols returns the ORDER BY that includes all reference
// columns in random order. If at least one of the columns is not orderable,
// then ok=false is returned.
func (s *Smither) makeOrderByWithAllCols(refs colRefs) (_ tree.OrderBy, ok bool) {
	if len(refs) == 0 {
		return nil, true
	}
	var ob tree.OrderBy
	for _, ref := range refs {
		if !s.isOrderable(ref.typ) {
			return nil, false
		}
		if ref.typ.Family() == types.CollatedStringFamily {
			// Some collated strings are equal, yet they differ when comparing
			// them directly as strings (e.g. e'\x00':::STRING COLLATE en_US
			// vs e'\x01':::STRING COLLATE en_US), which makes some queries
			// produce non-deterministic results even with all columns in the
			// ORDER BY clause. Fixing how we check the expected output is not
			// easy, so we simply reject stmts that require collated strings to
			// be in the ORDER BY clause.
			return nil, false
		}
		ob = append(ob, &tree.Order{
			Expr:       ref.item,
			Direction:  s.randDirection(),
			NullsOrder: s.randNullsOrder(),
		})
	}
	s.rnd.Shuffle(len(ob), func(i, j int) {
		ob[i], ob[j] = ob[j], ob[i]
	})
	return ob, true
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

func (s *Smither) makeReturning(
	desiredTypes []*types.T, tables []*tableRef,
) (*tree.ReturningExprs, colRefs) {
	if len(desiredTypes) == 0 {
		desiredTypes = s.makeDesiredTypes()
	}

	var refs colRefs
	for _, table := range tables {
		for _, c := range table.Columns {
			refs = append(refs, &colRef{
				typ:  tree.MustBeStaticallyKnownType(c.Type),
				item: &tree.ColumnItem{ColumnName: c.Name},
			})
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

func makeCreateStats(s *Smither) (tree.Statement, bool) {
	table, ok := s.getRandTable()
	if !ok {
		return nil, false
	}

	// Names slightly change behavior of statistics, so pick randomly between:
	// ~50%: __auto__ (simulating auto stats)
	// ~17%: __auto_partial__ (simulating auto partial stats)
	// ~17%: random (simulating manual CREATE STATISTICS statements)
	// ~17%: blank (simulating manual ANALYZE statements)
	var name tree.Name
	switch s.d6() {
	case 1, 2, 3:
		name = jobspb.AutoStatsName
	case 4:
		name = jobspb.AutoPartialStatsName
	case 5:
		name = s.name("stats")
	}

	// Pick specific columns ~8% of the time. We only do this for simulated
	// manual CREATE STATISTICS statements because neither auto stats nor ANALYZE
	// allow column selection.
	var columns tree.NameList
	if name != jobspb.AutoStatsName && name != jobspb.AutoPartialStatsName && name != "" && s.coin() {
		for _, col := range table.Columns {
			if !colinfo.IsSystemColumnName(string(col.Name)) {
				columns = append(columns, col.Name)
			}
		}
		s.rnd.Shuffle(len(columns), func(i, j int) {
			columns[i], columns[j] = columns[j], columns[i]
		})
		columns = columns[0:s.rnd.Intn(len(columns))]
	}

	var options tree.CreateStatsOptions
	if name == jobspb.AutoStatsName || name == jobspb.AutoPartialStatsName {
		// For auto stats we always set throttling and AOST.
		options.Throttling = 0.9
		// For auto stats we use AOST -30s by default, but this will make things
		// non-deterministic, so we use the smallest legal AOST instead.
		options.AsOf = tree.AsOfClause{Expr: tree.NewStrVal("-0.001ms")}
		// For auto partial stats we always set UsingExtremes.
		options.UsingExtremes = name == jobspb.AutoPartialStatsName
	} else if name == "" {
		// ANALYZE only sets AOST.
		options.AsOf = tree.AsOfClause{Expr: tree.NewStrVal("-0.001ms")}
	} else {
		// For CREATE STATISTICS we randomly set options.
		// Set throttling ~17% of the time.
		if s.coin() {
			options.Throttling = s.rnd.Float64()
		}
		// Set AOST ~17% of the time.
		if s.coin() {
			options.AsOf = tree.AsOfClause{Expr: tree.NewStrVal("-0.001ms")}
		}
		// Set USING EXTREMES ~17% of the time.
		options.UsingExtremes = s.coin()
		// TODO(93998): Add random predicate when we support WHERE.
	}

	return &tree.CreateStats{
		Name:        name,
		ColumnNames: columns,
		Table:       table.TableName,
		Options:     options,
	}, true
}
