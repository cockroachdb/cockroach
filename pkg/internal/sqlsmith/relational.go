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
	s = s.push()

	for i := 0; i < retryCount; i++ {
		if s.level < d6() && d6() < 3 {
			stmt, stmtRefs, ok = s.makeValues(desiredTypes, refs)
		} else if s.level < d6() && d6() < 3 {
			stmt, stmtRefs, ok = s.makeSetOp(desiredTypes, refs)
		} else {
			var inner *tree.Select
			inner, stmtRefs, ok = s.makeSelect(desiredTypes, refs)
			if ok {
				stmt = inner.Select
			}
		}
		if ok {
			return stmt, stmtRefs, ok
		}
	}
	return nil, nil, false
}

func (s *scope) getTableExpr() (*tree.AliasedTableExpr, *tableRef, colRefs, bool) {
	s = s.push()

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

func (s *scope) makeDataSource(refs colRefs) (tree.TableExpr, colRefs, bool) {
	s = s.push()

	if s.level < 3+d6() {
		if d6() > 4 {
			return s.makeJoinExpr(refs)
		}
	}
	if s.level < 3+d6() && coin() {
		return s.makeInsertReturning(nil, refs)
	}
	expr, _, refs, ok := s.getTableExpr()
	return expr, refs, ok
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

func (s *scope) makeJoinExpr(refs colRefs) (*tree.JoinTableExpr, colRefs, bool) {
	left, leftRefs, ok := s.makeDataSource(refs)
	if !ok {
		return nil, nil, false
	}
	right, rightRefs, ok := s.makeDataSource(refs)
	if !ok {
		return nil, nil, false
	}

	joinExpr := &tree.JoinTableExpr{
		JoinType: joinTypes[s.schema.rnd.Intn(len(joinTypes))],
		Left:     left,
		Right:    right,
	}

	if joinExpr.JoinType != tree.AstCross {
		on, ok := s.makeBoolExpr(refs)
		if !ok {
			return nil, nil, false
		}
		joinExpr.Cond = &tree.OnJoinCond{Expr: on}
	}
	joinRefs := leftRefs.extend(rightRefs...)

	return joinExpr, joinRefs, true
}

// STATEMENTS

func (s *scope) makeSelect(desiredTypes []types.T, refs colRefs) (*tree.Select, colRefs, bool) {
	var clause tree.SelectClause
	var ok bool
	stmt := tree.Select{
		Select: &clause,
	}

	var from tree.TableExpr
	var fromRefs colRefs
	from, fromRefs, ok = s.makeDataSource(refs)
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
		where, ok := s.makeBoolExpr(refs)
		if !ok {
			return nil, nil, false
		}
		clause.Where = tree.NewWhere("WHERE", where)
	}

	// TODO(justin): This can error a lot because it will often generate ORDER
	// BY's like `order by 'foo'`, which is invalid. The only constant that can
	// appear in ORDER BY is an integer and it must refer to a column ordinal. We
	// should make it so the only constants it generates are integers less than
	// the number of columns (or just disallow constants).
	//for coin() {
	//	expr, ok := outScope.makeScalar(anyType)
	//	if !ok {
	//		return nil, false
	//	}
	//	out.orderBy = append(out.orderBy, expr)
	//}

	clause.Distinct = d100() == 1

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
		next, ok := s.makeScalar(t, refs)
		if !ok {
			return nil, nil, false
		}
		result[i].Expr = next
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
	s = s.push()

	table, tableRef, _, ok := s.getTableExpr()
	if !ok {
		return nil, nil, false
	}

	var desiredTypes []types.T
	var names tree.NameList

	// Grab some subset of the columns of the table to attempt to insert into.
	// TODO(justin): also support the non-named variant.
	for _, c := range tableRef.Columns {
		// We *must* write a column if it's writable and non-nullable.
		// We *can* write a column if it's writable and nullable.
		if !c.Computed.Computed && (c.Nullable.Nullability == tree.NotNull || coin()) {
			desiredTypes = append(desiredTypes, coltypes.CastTargetToDatumType(c.Type))
			names = append(names, c.Name)
		}
	}

	input, _, ok := s.makeReturningStmt(desiredTypes, refs)
	if !ok {
		return nil, nil, false
	}

	return &tree.Insert{
		Table:   table,
		Columns: names,
		Rows: &tree.Select{
			Select: input,
		},
		Returning: &tree.NoReturningClause{},
	}, tableRef, true
}

func (s *scope) makeInsertReturning(
	desiredTypes []types.T, refs colRefs,
) (*tree.StatementSource, colRefs, bool) {
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

	insert, _, ok := s.makeInsert(refs)
	if !ok {
		return nil, nil, false
	}

	returning := make(tree.ReturningExprs, len(desiredTypes))
	returningRefs := make(colRefs, len(desiredTypes))
	for i, t := range desiredTypes {
		e, ok := s.makeScalar(t, refs)
		if !ok {
			return nil, nil, false
		}
		returning[i].Expr = e
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

func (s *scope) makeValues(
	desiredTypes []types.T, refs colRefs,
) (*tree.SelectClause, colRefs, bool) {
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
			e, ok := s.makeScalar(t, refs)
			if !ok {
				return nil, nil, false
			}
			tuple[j] = e
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

func (s *scope) makeSetOp(desiredTypes []types.T, refs colRefs) (*tree.UnionClause, colRefs, bool) {
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
