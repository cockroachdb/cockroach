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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func (s *scope) makeStmt() (stmt tree.Statement, ok bool) {
	idx := s.schema.stmtSampler.Next()
	return s.schema.statements[idx].fn(s)
}

func (s *scope) makeSelectStmt(
	desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (stmt tree.SelectStatement, stmtRefs colRefs, tables tableRefs, ok bool) {
	if s.canRecurse() {
		for {
			idx := s.schema.selectStmts.Next()
			expr, exprRefs, tables, ok := selectStmts[idx].fn(s, desiredTypes, refs, withTables)
			if ok {
				return expr, exprRefs, tables, ok
			}
		}
	}
	return makeValuesSelect(s, desiredTypes, refs, withTables)
}

func makeSchemaTable(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	expr, _, exprRefs, ok := s.getSchemaTable()
	return expr, exprRefs, ok
}

func (s *scope) getSchemaTable() (tree.TableExpr, *tableRef, colRefs, bool) {
	table, ok := s.schema.getRandTable()
	if !ok {
		return nil, nil, nil, false
	}
	alias := s.schema.name("tab")
	expr, refs := s.tableExpr(table, tree.NewUnqualifiedTableName(alias))
	return &tree.AliasedTableExpr{
		Expr: expr,
		As:   tree.AliasClause{Alias: alias},
	}, table, refs, true
}

func (s *scope) tableExpr(table *tableRef, name *tree.TableName) (tree.TableExpr, colRefs) {
	refs := make(colRefs, len(table.Columns))
	for i, c := range table.Columns {
		refs[i] = &colRef{
			typ: c.Type,
			item: tree.NewColumnItem(
				name,
				c.Name,
			),
		}
	}
	return table.TableName, refs
}

var (
	mutatingStatements = statementWeights{
		{10, makeInsert},
		{10, makeDelete},
		{10, makeUpdate},
		{1, makeAlter},
	}
	nonMutatingStatements = statementWeights{
		{10, makeSelect},
	}
	allStatements = append(mutatingStatements, nonMutatingStatements...)

	mutatingTableExprs = tableExprWeights{
		{1, makeInsertReturning},
		{1, makeDeleteReturning},
		{1, makeUpdateReturning},
	}
	nonMutatingTableExprs = tableExprWeights{
		{20, makeJoinExpr},
		{10, makeSchemaTable},
		{1, makeValuesTable},
		{2, makeSelectTable},
	}
	allTableExprs = append(mutatingTableExprs, nonMutatingTableExprs...)

	selectStmts       []selectStmtWeight
	selectStmtWeights []int
)

func init() {
	selectStmts = []selectStmtWeight{
		{1, makeValuesSelect},
		{1, makeSetOp},
		{1, makeSelectClause},
	}
	selectStmtWeights = func() []int {
		m := make([]int, len(selectStmts))
		for i, s := range selectStmts {
			m[i] = s.weight
		}
		return m
	}()
}

func (ws statementWeights) Weights() []int {
	m := make([]int, len(ws))
	for i, w := range ws {
		m[i] = w.weight
	}
	return m
}

func (ws tableExprWeights) Weights() []int {
	m := make([]int, len(ws))
	for i, w := range ws {
		m[i] = w.weight
	}
	return m
}

type (
	statementWeight struct {
		weight int
		fn     func(s *scope) (tree.Statement, bool)
	}
	statementWeights []statementWeight
	tableExprWeight  struct {
		weight int
		fn     func(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool)
	}
	tableExprWeights []tableExprWeight
	selectStmtWeight struct {
		weight int
		fn     selectStmt
	}
	// selectStmt is a func that returns something that can be used in a Select. It
	// accepts a list of tables generated from WITH expressions. Since cockroach
	// has a limitation that CTE tables can only be used once, it returns a
	// possibly modified list of those same withTables, where used tables have
	// been removed.
	selectStmt func(s *scope, desiredTypes []*types.T, refs colRefs, withTables tableRefs) (tree.SelectStatement, colRefs, tableRefs, bool)
)

// makeTableExpr returns a tableExpr. If forJoin is true the tableExpr is
// valid to be used as a join reference.
func makeTableExpr(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if s.canRecurse() {
		for i := 0; i < retryCount; i++ {
			idx := s.schema.tableExprSampler.Next()
			expr, exprRefs, ok := s.schema.tableExprs[idx].fn(s, refs, forJoin)
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

func makeJoinExpr(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	left, leftRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}
	right, rightRefs, ok := makeTableExpr(s, refs, true)
	if !ok {
		return nil, nil, false
	}

	joinExpr := &tree.JoinTableExpr{
		JoinType: joinTypes[s.schema.rnd.Intn(len(joinTypes))],
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

// STATEMENTS

func (s *scope) makeWith() (*tree.With, tableRefs) {
	if s.schema.disableWith {
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
		stmt, stmtRefs, tables, ok = s.makeSelectStmt(makeDesiredTypes(s), nil /* refs */, tables)
		if !ok {
			continue
		}
		alias := s.schema.name("with")
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

func makeDesiredTypes(s *scope) []*types.T {
	var typs []*types.T
	for {
		typs = append(typs, sqlbase.RandType(s.schema.rnd))
		if s.d6() < 2 {
			break
		}
	}
	return typs
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

var stringComparisons = []tree.ComparisonOperator{
	tree.Like,
	tree.NotLike,
	tree.ILike,
	tree.NotILike,
	tree.SimilarTo,
	tree.NotSimilarTo,
	tree.RegMatch,
	tree.NotRegMatch,
	tree.RegIMatch,
	tree.NotRegIMatch,
}

func (s *Smither) randStringComparison() tree.ComparisonOperator {
	return stringComparisons[s.rnd.Intn(len(stringComparisons))]
}

// makeSelectTable returns a TableExpr of the form `(SELECT ...)`, which
// would end up looking like `SELECT ... FROM (SELECT ...)`.
func makeSelectTable(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	stmt, stmtRefs, ok := s.makeSelect(nil /* desiredTypes */, refs)
	if !ok {
		return nil, nil, false
	}

	table := s.schema.name("tab")
	names := make(tree.NameList, len(stmtRefs))
	clauseRefs := make(colRefs, len(stmtRefs))
	for i, ref := range stmtRefs {
		names[i] = s.schema.name("col")
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
	s *scope, desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, tableRefs, bool) {
	stmt, selectRefs, _, tables, ok := s.makeSelectClause(desiredTypes, refs, withTables)
	return stmt, selectRefs, tables, ok
}

func (s *scope) makeSelectClause(
	desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (clause *tree.SelectClause, selectRefs, orderByRefs colRefs, tables tableRefs, ok bool) {
	clause = &tree.SelectClause{}

	var fromRefs colRefs
	// Sometimes generate a SELECT with no FROM clause.
	requireFrom := s.d6() != 1
	for (requireFrom && len(clause.From.Tables) < 1) || s.coin() {
		var from tree.TableExpr
		if len(withTables) == 0 || s.coin() {
			// Add a normal data source.
			source, sourceRefs, sourceOk := makeTableExpr(s, refs, false)
			if !sourceOk {
				return nil, nil, nil, nil, false
			}
			from = source
			fromRefs = append(fromRefs, sourceRefs...)
		} else {
			// Add a CTE reference.
			var table *tableRef
			table, withTables = withTables.Pop()
			expr, exprRefs := s.tableExpr(table, table.TableName)
			from = expr
			fromRefs = append(fromRefs, exprRefs...)
		}
		clause.From.Tables = append(clause.From.Tables, from)
	}

	selectListRefs := refs
	ctx := emptyCtx

	if len(clause.From.Tables) > 0 {
		clause.Where = s.makeWhere(fromRefs)
		orderByRefs = fromRefs
		selectListRefs = selectListRefs.extend(fromRefs...)

		if s.d6() <= 2 {
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
			groupByRefs := fromRefs.extend()
			s.schema.rnd.Shuffle(len(groupByRefs), func(i, j int) {
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
		} else if s.d6() <= 1 {
			// Enable window functions. This will enable them for all
			// exprs, but makeFunc will only let a few through.
			ctx = windowCtx
		}
	}

	selectList, selectRefs, ok := s.makeSelectList(ctx, desiredTypes, selectListRefs)
	if !ok {
		return nil, nil, nil, nil, false
	}
	clause.Exprs = selectList

	if s.d100() == 1 {
		clause.Distinct = true
		// For SELECT DISTINCT, ORDER BY expressions must appear in select list.
		orderByRefs = selectRefs
	}

	return clause, selectRefs, orderByRefs, withTables, true
}

func makeSelect(s *scope) (tree.Statement, bool) {
	stmt, _, ok := s.makeSelect(makeDesiredTypes(s), nil)
	return stmt, ok
}

func (s *scope) makeSelect(desiredTypes []*types.T, refs colRefs) (*tree.Select, colRefs, bool) {
	withStmt, withTables := s.makeWith()
	// Table references to CTEs can only be referenced once (cockroach
	// limitation). Shuffle the tables and only pick each once.
	// TODO(mjibson): remove this when cockroach supports full CTEs.
	s.schema.rnd.Shuffle(len(withTables), func(i, j int) {
		withTables[i], withTables[j] = withTables[j], withTables[i]
	})

	clause, selectRefs, orderByRefs, _, ok := s.makeSelectClause(desiredTypes, refs, withTables)
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
func (s *scope) makeSelectList(
	ctx Context, desiredTypes []*types.T, refs colRefs,
) (tree.SelectExprs, colRefs, bool) {
	// If we don't have any desired types, generate some from the given refs.
	if len(desiredTypes) == 0 {
		s.schema.sample(len(refs), 6, func(i int) {
			desiredTypes = append(desiredTypes, refs[i].typ)
		})
	}
	// If we still don't have any then there weren't any refs.
	if len(desiredTypes) == 0 {
		desiredTypes = makeDesiredTypes(s)
	}
	result := make(tree.SelectExprs, len(desiredTypes))
	selectRefs := make(colRefs, len(desiredTypes))
	for i, t := range desiredTypes {
		result[i].Expr = makeScalarContext(s, ctx, t, refs)
		alias := s.schema.name("col")
		result[i].As = tree.UnrestrictedName(alias)
		selectRefs[i] = &colRef{
			typ:  t,
			item: &tree.ColumnItem{ColumnName: alias},
		}
	}
	return result, selectRefs, true
}

func makeDelete(s *scope) (tree.Statement, bool) {
	stmt, _, ok := s.makeDelete(nil)
	return stmt, ok
}

func (s *scope) makeDelete(refs colRefs) (*tree.Delete, *tableRef, bool) {
	table, tableRef, tableRefs, ok := s.getSchemaTable()
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

func makeDeleteReturning(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	return s.makeDeleteReturning(refs)
}

func (s *scope) makeDeleteReturning(refs colRefs) (tree.TableExpr, colRefs, bool) {
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

func makeUpdate(s *scope) (tree.Statement, bool) {
	stmt, _, ok := s.makeUpdate(nil)
	return stmt, ok
}

func (s *scope) makeUpdate(refs colRefs) (*tree.Update, *tableRef, bool) {
	table, tableRef, tableRefs, ok := s.getSchemaTable()
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
		n := s.schema.rnd.Intn(len(upRefs))
		ref := upRefs[n]
		upRefs = append(upRefs[:n], upRefs[n+1:]...)
		col := cols[ref.item.ColumnName]
		// Ignore computed columns.
		if col == nil || col.Computed.Computed {
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

func makeUpdateReturning(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	return s.makeUpdateReturning(refs)
}

func (s *scope) makeUpdateReturning(refs colRefs) (tree.TableExpr, colRefs, bool) {
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

func makeInsert(s *scope) (tree.Statement, bool) {
	stmt, _, ok := s.makeInsert(nil)
	return stmt, ok
}

// makeInsert has only one valid reference: its table source, which can be
// used only in the optional returning section. Hence the irregular return
// signature.
func (s *scope) makeInsert(refs colRefs) (*tree.Insert, *tableRef, bool) {
	table, tableRef, _, ok := s.getSchemaTable()
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
	if s.d9() != 1 {
		var desiredTypes []*types.T
		var names tree.NameList

		unnamed := s.coin()

		// Grab some subset of the columns of the table to attempt to insert into.
		for _, c := range tableRef.Columns {
			// We *must* write a column if it's writable and non-nullable.
			// We *can* write a column if it's writable and nullable.
			if c.Computed.Computed {
				continue
			}
			if unnamed || c.Nullable.Nullability == tree.NotNull || s.coin() {
				desiredTypes = append(desiredTypes, c.Type)
				names = append(names, c.Name)
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
	}

	return insert, tableRef, true
}

func makeInsertReturning(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if forJoin {
		return nil, nil, false
	}
	return s.makeInsertReturning(refs)
}

func (s *scope) makeInsertReturning(refs colRefs) (tree.TableExpr, colRefs, bool) {
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

func makeValuesTable(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	types := makeDesiredTypes(s)
	values, valuesRefs := makeValues(s, types, refs)
	return values, valuesRefs, true
}

func makeValues(s *scope, desiredTypes []*types.T, refs colRefs) (tree.TableExpr, colRefs) {
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
	table := s.schema.name("tab")
	names := make(tree.NameList, len(desiredTypes))
	valuesRefs := make(colRefs, len(desiredTypes))
	for i, typ := range desiredTypes {
		names[i] = s.schema.name("col")
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
	s *scope, desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, tableRefs, bool) {
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
	}, valuesRefs, withTables, true
}

var setOps = []tree.UnionType{
	tree.UnionOp,
	tree.IntersectOp,
	tree.ExceptOp,
}

func makeSetOp(
	s *scope, desiredTypes []*types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, tableRefs, bool) {
	left, leftRefs, withTables, ok := s.makeSelectStmt(desiredTypes, refs, withTables)
	if !ok {
		return nil, nil, nil, false
	}

	right, _, withTables, ok := s.makeSelectStmt(desiredTypes, refs, withTables)
	if !ok {
		return nil, nil, nil, false
	}

	return &tree.UnionClause{
		Type:  setOps[s.schema.rnd.Intn(len(setOps))],
		Left:  &tree.Select{Select: left},
		Right: &tree.Select{Select: right},
		All:   s.coin(),
	}, leftRefs, withTables, true
}

func (s *scope) makeWhere(refs colRefs) *tree.Where {
	if s.coin() {
		where := makeBoolExpr(s, refs)
		return tree.NewWhere("WHERE", where)
	}
	return nil
}

func (s *scope) makeHaving(refs colRefs) *tree.Where {
	if s.coin() {
		where := makeBoolExprContext(s, havingCtx, refs)
		return tree.NewWhere("HAVING", where)
	}
	return nil
}

func (s *scope) makeOrderBy(refs colRefs) tree.OrderBy {
	if len(refs) == 0 {
		return nil
	}
	var ob tree.OrderBy
	for s.coin() {
		ref := refs[s.schema.rnd.Intn(len(refs))]
		// We don't support order by jsonb columns.
		if ref.typ.Family() == types.JsonFamily {
			continue
		}
		ob = append(ob, &tree.Order{
			Expr:      ref.item,
			Direction: s.schema.randDirection(),
		})
	}
	return ob
}

func makeLimit(s *scope) *tree.Limit {
	if s.schema.disableLimits {
		return nil
	}
	if s.d6() > 2 {
		return &tree.Limit{Count: tree.NewDInt(tree.DInt(s.d100()))}
	}
	return nil
}

func (s *scope) makeReturning(table *tableRef) (*tree.ReturningExprs, colRefs) {
	desiredTypes := makeDesiredTypes(s)

	refs := make(colRefs, len(table.Columns))
	for i, c := range table.Columns {
		refs[i] = &colRef{
			typ:  c.Type,
			item: &tree.ColumnItem{ColumnName: c.Name},
		}
	}

	returning := make(tree.ReturningExprs, len(desiredTypes))
	returningRefs := make(colRefs, len(desiredTypes))
	for i, t := range desiredTypes {
		returning[i].Expr = makeScalar(s, t, refs)
		alias := s.schema.name("ret")
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
