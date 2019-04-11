// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

import (
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func (s *scope) makeStmt() (stmt tree.Statement, ok bool) {
	idx := s.schema.stmts.Next()
	return statements[idx].fn(s)
}

func (s *scope) makeSelectStmt(
	desiredTypes []types.T, refs colRefs, withTables tableRefs,
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
	return makeValues(s, desiredTypes, refs, withTables)
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
			typ: coltypes.CastTargetToDatumType(c.Type),
			item: tree.NewColumnItem(
				name,
				c.Name,
			),
		}
	}
	return table.TableName, refs
}

var (
	statements                          []statementWeight
	tableExprs                          []tableExprWeight
	selectStmts                         []selectStmtWeight
	statementWeights                    []int
	tableExprWeights, selectStmtWeights []int
)

func init() {
	statements = []statementWeight{
		{10, makeInsert},
		{10, makeSelect},
		{10, makeDelete},
		{10, makeUpdate},
		{1, makeAlter},
	}
	statementWeights = func() []int {
		m := make([]int, len(statements))
		for i, s := range statements {
			m[i] = s.weight
		}
		return m
	}()
	tableExprs = []tableExprWeight{
		{2, makeJoinExpr},
		{1, makeInsertReturning},
		{1, makeDeleteReturning},
		{1, makeUpdateReturning},
		{3, makeSchemaTable},
	}
	tableExprWeights = func() []int {
		m := make([]int, len(tableExprs))
		for i, s := range tableExprs {
			m[i] = s.weight
		}
		return m
	}()
	selectStmts = []selectStmtWeight{
		{1, makeValues},
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

type (
	statementWeight struct {
		weight int
		fn     func(s *scope) (tree.Statement, bool)
	}
	tableExprWeight struct {
		weight int
		fn     func(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool)
	}
	selectStmtWeight struct {
		weight int
		fn     selectStmt
	}
	// selectStmt is a func that returns something that can be used in a Select. It
	// accepts a list of tables generated from WITH expressions. Since cockroach
	// has a limitation that CTE tables can only be used once, it returns a
	// possibly modified list of those same withTables, where used tables have
	// been removed.
	selectStmt func(s *scope, desiredTypes []types.T, refs colRefs, withTables tableRefs) (tree.SelectStatement, colRefs, tableRefs, bool)
)

// makeTableExpr returns a tableExpr. If forJoin is true the tableExpr is
// valid to be used as a join reference.
func makeTableExpr(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if s.canRecurse() {
		for i := 0; i < retryCount; i++ {
			idx := s.schema.tableExprs.Next()
			expr, exprRefs, ok := tableExprs[idx].fn(s, refs, forJoin)
			if ok {
				return expr, exprRefs, ok
			}
		}
	}
	return makeSchemaTable(s, refs, forJoin)
}

type typedExpr struct {
	tree.TypedExpr
	typ types.T
}

func makeTypedExpr(expr tree.TypedExpr, typ types.T) tree.TypedExpr {
	return typedExpr{
		TypedExpr: expr,
		typ:       typ,
	}
}

func (t typedExpr) ResolvedType() types.T {
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
	// WITHs are pretty rare, so just ignore them a lot.
	if coin() {
		return nil, nil
	}
	ctes := make([]*tree.CTE, 0, d6()/2)
	if cap(ctes) == 0 {
		return nil, nil
	}
	var tables tableRefs
	for i := 0; i < cap(ctes); i++ {
		var ok bool
		var stmt tree.SelectStatement
		var stmtRefs colRefs
		stmt, stmtRefs, tables, ok = s.makeSelectStmt(makeDesiredTypes(), nil /* refs */, tables)
		if !ok {
			continue
		}
		alias := s.schema.name("with")
		tblName := tree.NewUnqualifiedTableName(alias)
		cols := make(tree.NameList, len(stmtRefs))
		defs := make([]*tree.ColumnTableDef, len(stmtRefs))
		for i, r := range stmtRefs {
			cols[i] = r.item.ColumnName
			coltype, err := coltypes.DatumTypeToColumnType(r.typ)
			if err != nil {
				panic(err)
			}
			defs[i], err = tree.NewColumnTableDef(r.item.ColumnName, coltype, nil)
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

func makeDesiredTypes() []types.T {
	var typs []types.T
	for {
		typs = append(typs, getRandType())
		if d6() < 2 {
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

func makeSelectClause(
	s *scope, desiredTypes []types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, tableRefs, bool) {
	stmt, selectRefs, _, tables, ok := s.makeSelectClause(desiredTypes, refs, withTables)
	return stmt, selectRefs, tables, ok
}

func (s *scope) makeSelectClause(
	desiredTypes []types.T, refs colRefs, withTables tableRefs,
) (clause *tree.SelectClause, selectRefs, orderByRefs colRefs, tables tableRefs, ok bool) {
	clause = &tree.SelectClause{
		From: &tree.From{},
	}

	var fromRefs colRefs
	for len(clause.From.Tables) < 1 || coin() {
		var from tree.TableExpr
		if len(withTables) == 0 || coin() {
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
	clause.Where = s.makeWhere(fromRefs)
	orderByRefs = fromRefs
	selectListRefs := fromRefs
	var ctx Context

	if d6() <= 2 {
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
		for (len(groupBy) < 1 || coin()) && len(groupBy) < len(groupByRefs) {
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
	}

	selectList, selectRefs, ok := s.makeSelectList(ctx, desiredTypes, selectListRefs)
	if !ok {
		return nil, nil, nil, nil, false
	}
	clause.Exprs = selectList

	if d100() == 1 {
		clause.Distinct = true
		// For SELECT DISTINCT, ORDER BY expressions must appear in select list.
		orderByRefs = selectRefs
	}

	return clause, selectRefs, orderByRefs, withTables, true
}

func makeSelect(s *scope) (tree.Statement, bool) {
	stmt, _, ok := s.makeSelect(makeDesiredTypes(), nil)
	return stmt, ok
}

func (s *scope) makeSelect(desiredTypes []types.T, refs colRefs) (*tree.Select, colRefs, bool) {
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
		Limit:   makeLimit(),
	}

	return &stmt, selectRefs, true
}

func (s *scope) makeSelectList(
	ctx Context, desiredTypes []types.T, refs colRefs,
) (tree.SelectExprs, colRefs, bool) {
	if len(desiredTypes) == 0 {
		panic("expected desiredTypes")
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
		Limit:     makeLimit(),
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
		Limit:     makeLimit(),
		Returning: &tree.NoReturningClause{},
	}
	// Each row can be set at most once. Copy tableRefs to upRefs and remove
	// elements from it as we use them.
	upRefs := tableRefs.extend()
	for (len(update.Exprs) < 1 || coin()) && len(upRefs) > 0 {
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
	if d9() != 1 {
		var desiredTypes []types.T
		var names tree.NameList

		unnamed := coin()

		// Grab some subset of the columns of the table to attempt to insert into.
		for _, c := range tableRef.Columns {
			// We *must* write a column if it's writable and non-nullable.
			// We *can* write a column if it's writable and nullable.
			if c.Computed.Computed {
				continue
			}
			if unnamed || c.Nullable.Nullability == tree.NotNull || coin() {
				desiredTypes = append(desiredTypes, coltypes.CastTargetToDatumType(c.Type))
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

func makeValues(
	s *scope, desiredTypes []types.T, refs colRefs, withTables tableRefs,
) (tree.SelectStatement, colRefs, tableRefs, bool) {
	numRowsToInsert := d6()
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

	// Returing just &values here would result in a query like `VALUES (...)` where
	// the columns are arbitrarily named by index (column1, column2, etc.). Since
	// we want to be able to reference the columns in other places we need to
	// name them deterministically. We can use `SELECT * FROM (VALUES (...)) AS
	// tbl (c1, c2, etc.)` to achieve this. There's quite a lot of indirection
	// for how to achieve exactly that syntax as tree nodes, but it works.
	return &tree.SelectClause{
		Exprs: tree.SelectExprs{tree.StarSelectExpr()},
		From: &tree.From{
			Tables: tree.TableExprs{&tree.AliasedTableExpr{
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
			}},
		},
	}, valuesRefs, withTables, true
}

var setOps = []tree.UnionType{
	tree.UnionOp,
	tree.IntersectOp,
	tree.ExceptOp,
}

func makeSetOp(
	s *scope, desiredTypes []types.T, refs colRefs, withTables tableRefs,
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
		All:   coin(),
	}, leftRefs, withTables, true
}

func (s *scope) makeWhere(refs colRefs) *tree.Where {
	if coin() {
		where := makeBoolExpr(s, refs)
		return tree.NewWhere("WHERE", where)
	}
	return nil
}

func (s *scope) makeHaving(refs colRefs) *tree.Where {
	if coin() {
		where := makeBoolExprContext(s, groupByCtx, refs)
		return tree.NewWhere("HAVING", where)
	}
	return nil
}

func (s *scope) makeOrderBy(refs colRefs) tree.OrderBy {
	var ob tree.OrderBy
	for coin() {
		ref := refs[s.schema.rnd.Intn(len(refs))]
		// We don't support order by jsonb columns.
		if ref.typ.SemanticType() == types.JSON {
			continue
		}
		ob = append(ob, &tree.Order{
			Expr:      ref.item,
			Direction: s.schema.randDirection(),
		})
	}
	return ob
}

func makeLimit() *tree.Limit {
	if d6() > 2 {
		return &tree.Limit{Count: tree.NewDInt(tree.DInt(d100()))}
	}
	return nil
}

func (s *scope) makeReturning(table *tableRef) (*tree.ReturningExprs, colRefs) {
	desiredTypes := makeDesiredTypes()

	refs := make(colRefs, len(table.Columns))
	for i, c := range table.Columns {
		refs[i] = &colRef{
			typ:  coltypes.CastTargetToDatumType(c.Type),
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
