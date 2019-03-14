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
	if d6() < 3 {
		stmt, _, ok = s.makeInsert(nil)
	} else {
		stmt, _, ok = s.makeSelectStmt(makeDesiredTypes(), nil)
	}
	return stmt, ok
}

func (s *scope) makeSelectStmt(
	desiredTypes []types.T, refs colRefs,
) (stmt tree.SelectStatement, stmtRefs colRefs, ok bool) {
	if desiredTypes == nil {
		panic("expected desiredTypes")
	}
	if s.canRecurse() {
		for {
			idx := s.schema.selectStmts.Next()
			expr, exprRefs, ok := selectStmts[idx].fn(s, desiredTypes, refs)
			if ok {
				return expr, exprRefs, ok
			}
		}
	}
	return makeValues(s, desiredTypes, refs)
}

func getTableExpr(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	expr, _, exprRefs, ok := s.getTableExpr()
	return expr, exprRefs, ok
}

func (s *scope) getTableExpr() (tree.TableExpr, *tableRef, colRefs, bool) {
	if len(s.schema.tables) == 0 {
		return nil, nil, nil, false
	}
	table := s.schema.tables[s.schema.rnd.Intn(len(s.schema.tables))]
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
	tableExprs                          []tableExprWeight
	selectStmts                         []selectStmtWeight
	tableExprWeights, selectStmtWeights []int
)

func init() {
	tableExprs = []tableExprWeight{
		{2, makeJoinExpr},
		{1, makeInsertReturning},
		{3, getTableExpr},
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
	tableExprWeight struct {
		weight int
		fn     func(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool)
	}
	selectStmtWeight struct {
		weight int
		fn     func(s *scope, desiredTypes []types.T, refs colRefs) (tree.SelectStatement, colRefs, bool)
	}
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
	return getTableExpr(s, refs, forJoin)
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

func (s *scope) makeWith(refs colRefs) (*tree.With, []*tableRef) {
	ctes := make([]*tree.CTE, 0, d6()/2)
	if cap(ctes) == 0 {
		return nil, nil
	}
	// TODO(mjibson): when cockroach supports CTEs referencing other CTEs,
	// update refs in between each loop.
	var tables []*tableRef
	for i := 0; i < cap(ctes); i++ {
		stmt, stmtRefs, ok := s.makeSelectStmt(makeDesiredTypes(), refs)
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
	tree.Ascending,
	tree.Descending,
}

func makeSelectClause(
	s *scope, desiredTypes []types.T, refs colRefs,
) (tree.SelectStatement, colRefs, bool) {
	stmt, selectRefs, _, ok := s.makeSelectClause(desiredTypes, refs, nil /* withTables */)
	return stmt, selectRefs, ok
}

func (s *scope) makeSelectClause(
	desiredTypes []types.T, refs colRefs, withTables []*tableRef,
) (clause *tree.SelectClause, selectRefs, orderByRefs colRefs, ok bool) {
	clause = &tree.SelectClause{
		From: &tree.From{},
	}

	var fromRefs colRefs
	for len(clause.From.Tables) < 1 || coin() {
		var from tree.TableExpr
		if len(withTables) == 0 || coin() {
			// Add a normal data source.
			source, sourceRefs, ok := makeTableExpr(s, refs, false)
			if !ok {
				return nil, nil, nil, false
			}
			from = source
			fromRefs = append(fromRefs, sourceRefs...)
		} else {
			// Add a CTE reference.
			table := withTables[0]
			withTables = withTables[1:]
			expr, exprRefs := s.tableExpr(table, table.TableName)
			from = expr
			fromRefs = append(fromRefs, exprRefs...)
		}
		clause.From.Tables = append(clause.From.Tables, from)
	}

	selectList, selectRefs, ok := s.makeSelectList(desiredTypes, fromRefs)
	if !ok {
		return nil, nil, nil, false
	}
	clause.Exprs = selectList

	if coin() {
		where := makeBoolExpr(s, fromRefs)
		clause.Where = tree.NewWhere("WHERE", where)
	}

	if d100() == 1 {
		// For SELECT DISTINCT, ORDER BY expressions must appear in select list.
		clause.Distinct = true
		fromRefs = selectRefs
	}

	return clause, selectRefs, fromRefs, true
}

func (s *scope) makeSelect(desiredTypes []types.T, refs colRefs) (*tree.Select, colRefs, bool) {
	withStmt, withTables := s.makeWith(refs)
	// Table references to CTEs can only be referenced once (cockroach
	// limitation). Shuffle the tables and only pick each once.
	// TODO(mjibson): remove this when cockroach supports full CTEs.
	s.schema.rnd.Shuffle(len(withTables), func(i, j int) {
		withTables[i], withTables[j] = withTables[j], withTables[i]
	})

	clause, selectRefs, orderByRefs, ok := s.makeSelectClause(desiredTypes, refs, withTables)
	if !ok {
		return nil, nil, ok
	}

	stmt := tree.Select{
		Select: clause,
		With:   withStmt,
	}

	for coin() {
		stmt.OrderBy = append(stmt.OrderBy, &tree.Order{
			Expr:      orderByRefs[s.schema.rnd.Intn(len(orderByRefs))].item,
			Direction: orderDirections[s.schema.rnd.Intn(len(orderDirections))],
		})
	}

	if d6() > 2 {
		stmt.Limit = &tree.Limit{Count: tree.NewDInt(tree.DInt(d100()))}
	}

	return &stmt, selectRefs, true
}

func (s *scope) makeSelectList(
	desiredTypes []types.T, refs colRefs,
) (tree.SelectExprs, colRefs, bool) {
	if len(desiredTypes) == 0 {
		panic("expected desiredTypes")
	}
	result := make(tree.SelectExprs, len(desiredTypes))
	selectRefs := make(colRefs, len(desiredTypes))
	for i, t := range desiredTypes {
		result[i].Expr = makeScalar(s, t, refs)
		alias := s.schema.name("col")
		result[i].As = tree.UnrestrictedName(alias)
		selectRefs[i] = &colRef{
			typ:  t,
			item: &tree.ColumnItem{ColumnName: alias},
		}
	}
	return result, selectRefs, true
}

// makeInsert has only one valid reference: its table source, which can be
// used only in the optional returning section. Hence the irregular return
// signature.
func (s *scope) makeInsert(refs colRefs) (*tree.Insert, *tableRef, bool) {
	table, tableRef, _, ok := s.getTableExpr()
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

		input, _, ok := s.makeSelectStmt(desiredTypes, refs)
		if !ok {
			return nil, nil, false
		}
		if !unnamed {
			insert.Columns = names
		}
		insert.Rows.Select = input
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
	desiredTypes := makeDesiredTypes()

	insert, insertRef, ok := s.makeInsert(refs)
	if !ok {
		return nil, nil, false
	}
	insertRefs := make(colRefs, len(insertRef.Columns))
	for i, c := range insertRef.Columns {
		insertRefs[i] = &colRef{
			typ:  coltypes.CastTargetToDatumType(c.Type),
			item: &tree.ColumnItem{ColumnName: c.Name},
		}
	}

	returning := make(tree.ReturningExprs, len(desiredTypes))
	returningRefs := make(colRefs, len(desiredTypes))
	for i, t := range desiredTypes {
		returning[i].Expr = makeScalar(s, t, insertRefs)
		alias := s.schema.name("col")
		returning[i].As = tree.UnrestrictedName(alias)
		returningRefs[i] = &colRef{
			typ: t,
			item: &tree.ColumnItem{
				ColumnName: alias,
			},
		}
	}
	insert.Returning = &returning
	return &tree.StatementSource{
		Statement: insert,
	}, returningRefs, true
}

func makeValues(
	s *scope, desiredTypes []types.T, refs colRefs,
) (tree.SelectStatement, colRefs, bool) {
	if len(desiredTypes) == 0 {
		panic("expected desiredTypes")
	}

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
	}, valuesRefs, true
}

var setOps = []tree.UnionType{
	tree.UnionOp,
	tree.IntersectOp,
	tree.ExceptOp,
}

func makeSetOp(
	s *scope, desiredTypes []types.T, refs colRefs,
) (tree.SelectStatement, colRefs, bool) {
	if len(desiredTypes) == 0 {
		panic("expected desiredTypes")
	}

	left, leftRefs, ok := s.makeSelectStmt(desiredTypes, refs)
	if !ok {
		return nil, nil, false
	}

	right, _, ok := s.makeSelectStmt(desiredTypes, refs)
	if !ok {
		return nil, nil, false
	}

	return &tree.UnionClause{
		Type:  setOps[s.schema.rnd.Intn(len(setOps))],
		Left:  &tree.Select{Select: left},
		Right: &tree.Select{Select: right},
		All:   coin(),
	}, leftRefs, true
}
