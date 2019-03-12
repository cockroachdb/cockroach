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
		stmt, _, ok = s.makeReturningStmt(nil, nil)
	}
	return stmt, ok
}

func (s *scope) makeReturningStmt(
	desiredTypes []types.T, refs colRefs,
) (stmt tree.SelectStatement, stmtRefs colRefs, ok bool) {
	if s.canRecurse() {
		for {
			idx := s.schema.returnings.Next()
			expr, exprRefs, ok := returnings[idx].fn(s, desiredTypes, refs)
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

func (s *scope) getTableExpr() (*tree.AliasedTableExpr, *tableRef, colRefs, bool) {
	if len(s.schema.tables) == 0 {
		return nil, nil, nil, false
	}
	table := s.schema.tables[s.schema.rnd.Intn(len(s.schema.tables))]
	alias := s.schema.name("tab")
	refs := make(colRefs, len(table.Columns))
	for i, c := range table.Columns {
		refs[i] = &colRef{
			typ: coltypes.CastTargetToDatumType(c.Type),
			item: tree.NewColumnItem(
				tree.NewUnqualifiedTableName(alias),
				c.Name,
			),
		}
	}
	return &tree.AliasedTableExpr{
		Expr: table.TableName,
		As:   tree.AliasClause{Alias: alias},
	}, table, refs, true
}

var (
	dataSources                     []sourceWeight
	returnings                      []returningWeight
	sourceWeights, returningWeights []int
)

func init() {
	dataSources = []sourceWeight{
		{2, makeJoinExpr},
		{1, makeInsertReturning},
		{3, getTableExpr},
	}
	sourceWeights = func() []int {
		m := make([]int, len(dataSources))
		for i, s := range dataSources {
			m[i] = s.weight
		}
		return m
	}()
	returnings = []returningWeight{
		{1, makeValues},
		{1, makeSetOp},
		{1, makeSelect},
	}
	returningWeights = func() []int {
		m := make([]int, len(returnings))
		for i, s := range returnings {
			m[i] = s.weight
		}
		return m
	}()
}

type (
	sourceWeight struct {
		weight int
		fn     func(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool)
	}
	returningWeight struct {
		weight int
		fn     func(s *scope, desiredTypes []types.T, refs colRefs) (tree.SelectStatement, colRefs, bool)
	}
)

// makeDataSource returns a tableExpr. If forJoin is true the tableExpr is
// valid to be used as a join reference.
func makeDataSource(s *scope, refs colRefs, forJoin bool) (tree.TableExpr, colRefs, bool) {
	if s.canRecurse() {
		for i := 0; i < retryCount; i++ {
			idx := s.schema.sources.Next()
			expr, exprRefs, ok := dataSources[idx].fn(s, refs, forJoin)
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
	left, leftRefs, ok := makeDataSource(s, refs, true)
	if !ok {
		return nil, nil, false
	}
	right, rightRefs, ok := makeDataSource(s, refs, true)
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

var orderDirections = []tree.Direction{
	tree.Ascending,
	tree.Descending,
}

func makeSelect(
	s *scope, desiredTypes []types.T, refs colRefs,
) (tree.SelectStatement, colRefs, bool) {
	stmt, stmtRefs, ok := s.makeSelect(desiredTypes, refs)
	return stmt.Select, stmtRefs, ok
}

func (s *scope) makeSelect(desiredTypes []types.T, refs colRefs) (*tree.Select, colRefs, bool) {
	var clause tree.SelectClause
	var ok bool
	stmt := tree.Select{
		Select: &clause,
	}

	var from tree.TableExpr
	var fromRefs colRefs
	from, fromRefs, ok = makeDataSource(s, refs, false)
	if !ok {
		return nil, nil, false
	}
	clause.From = &tree.From{Tables: tree.TableExprs{from}}

	selectList, selectRefs, ok := s.makeSelectList(desiredTypes, fromRefs)
	if !ok {
		return nil, nil, false
	}
	clause.Exprs = selectList

	if coin() {
		where := makeBoolExpr(s, fromRefs)
		clause.Where = tree.NewWhere("WHERE", where)
	}

	orderByRefs := fromRefs
	if d100() == 1 {
		// For SELECT DISTINCT, ORDER BY expressions must appear in select list.
		clause.Distinct = true
		orderByRefs = selectRefs
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
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() == 1 {
				break
			}
		}
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

		input, _, ok := s.makeReturningStmt(desiredTypes, refs)
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
	return s.makeInsertReturning(nil, refs)
}

func (s *scope) makeInsertReturning(
	desiredTypes []types.T, refs colRefs,
) (tree.TableExpr, colRefs, bool) {
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

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
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
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
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

	left, leftRefs, ok := s.makeReturningStmt(desiredTypes, refs)
	if !ok {
		return nil, nil, false
	}

	right, _, ok := s.makeReturningStmt(desiredTypes, refs)
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
