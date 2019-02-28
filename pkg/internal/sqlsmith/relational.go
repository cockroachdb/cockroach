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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func (s *scope) makeStmt() (tree.Statement, bool) {
	if d6() < 3 {
		return s.makeInsert()
	}
	return s.makeReturningStmt(nil)
}

func (s *scope) makeReturningStmt(desiredTypes []types.T) (tree.SelectStatement, bool) {
	s = s.push()
	var ret tree.SelectStatement
	var ok bool
	for i := 0; i < retryCount; i++ {
		if s.level < d6() && d6() < 3 {
			ret, ok = s.makeValues(desiredTypes)
		} else if s.level < d6() && d6() < 3 {
			ret, ok = s.makeSetOp(desiredTypes)
		} else {
			var inner *tree.Select
			inner, ok = s.makeSelect(desiredTypes)
			if ok {
				ret = inner.Select
			}
		}
		if ok {
			return ret, true
		}
	}
	return nil, false
}

func (s *scope) getTableExpr() (*tree.AliasedTableExpr, tableRef, bool) {
	if len(s.schema.tables) == 0 {
		return nil, tableRef{}, false
	}
	table := s.schema.tables[rand.Intn(len(s.schema.tables))]
	alias := tree.Name(s.name("tab"))
	ref := tableRef{
		TableName: tree.NewUnqualifiedTableName(alias),
		Columns:   table.Columns,
	}
	s.refs = append(s.refs, ref)
	return &tree.AliasedTableExpr{
		Expr: table.TableName,
		As:   tree.AliasClause{Alias: alias},
	}, ref, true
}

func (s *scope) makeDataSource() (tree.TableExpr, bool) {
	s = s.push()
	if s.level < 3+d6() {
		if d6() > 4 {
			return s.makeJoinExpr()
		}
	}

	if s.level < 3+d6() && coin() {
		return s.makeInsertReturning(nil)
	}

	expr, _, ok := s.getTableExpr()
	return expr, ok
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

// TODO(justin): also do outer joins.

func (s *scope) makeJoinExpr() (*tree.JoinTableExpr, bool) {
	s = s.push()
	left, ok := s.makeDataSource()
	if !ok {
		return nil, false
	}
	right, ok := s.makeDataSource()
	if !ok {
		return nil, false
	}

	on, ok := s.makeBoolExpr()
	if !ok {
		return nil, false
	}

	return &tree.JoinTableExpr{
		Left:  left,
		Right: right,
		Cond:  &tree.OnJoinCond{Expr: on},
	}, true
}

// STATEMENTS

func (s *scope) makeSelect(desiredTypes []types.T) (*tree.Select, bool) {
	s = s.push()
	var clause tree.SelectClause
	stmt := tree.Select{
		Select: &clause,
	}

	{
		from, ok := s.makeDataSource()
		if !ok {
			return nil, false
		}
		clause.From = &tree.From{Tables: tree.TableExprs{from}}
	}

	selectList, ok := s.makeSelectList(desiredTypes)
	if !ok {
		return nil, false
	}
	clause.Exprs = selectList

	if coin() {
		where, ok := s.makeBoolExpr()
		if !ok {
			return nil, false
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

	return &stmt, true
}

func (s *scope) makeSelectList(desiredTypes []types.T) (tree.SelectExprs, bool) {
	s = s.push()
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() == 1 {
				break
			}
		}
	}
	result := make(tree.SelectExprs, len(desiredTypes))
	for i, t := range desiredTypes {
		next, ok := s.makeScalar(t)
		if !ok {
			return nil, false
		}
		result[i].Expr = next
	}
	return result, true
}

func (s *scope) makeInsert() (*tree.Insert, bool) {
	s = s.push()

	table, ref, ok := s.getTableExpr()
	if !ok {
		return nil, false
	}

	var desiredTypes []types.T
	var names tree.NameList

	// Grab some subset of the columns of the table to attempt to insert into.
	// TODO(justin): also support the non-named variant.
	for _, c := range ref.Columns {
		// We *must* write a column if it's writable and non-nullable.
		// We *can* write a column if it's writable and nullable.
		if !c.Computed.Computed && (c.Nullable.Nullability == tree.NotNull || coin()) {
			desiredTypes = append(desiredTypes, coltypes.CastTargetToDatumType(c.Type))
			names = append(names, c.Name)
		}
	}

	input, ok := s.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	return &tree.Insert{
		Table:   table,
		Columns: names,
		Rows: &tree.Select{
			Select: input,
		},
		Returning: &tree.NoReturningClause{},
	}, true
}

func (s *scope) makeInsertReturning(desiredTypes []types.T) (*tree.StatementSource, bool) {
	s = s.push()
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

	insert, ok := s.makeInsert()
	if !ok {
		return nil, false
	}

	returning := make(tree.ReturningExprs, len(desiredTypes))
	for i, t := range desiredTypes {
		e, ok := s.makeScalar(t)
		if !ok {
			return nil, false
		}
		returning[i].Expr = e
	}
	insert.Returning = &returning
	return &tree.StatementSource{
		Statement: insert,
	}, true
}

func (s *scope) makeValues(desiredTypes []types.T) (*tree.ValuesClause, bool) {
	s = s.push()
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
			e, ok := s.makeScalar(t)
			if !ok {
				return nil, false
			}
			tuple[j] = e
		}
		values.Rows[i] = tuple
	}

	return &values, true
}

var setOps = []tree.UnionType{
	tree.UnionOp,
	tree.IntersectOp,
	tree.ExceptOp,
}

func (s *scope) makeSetOp(desiredTypes []types.T) (*tree.UnionClause, bool) {
	s = s.push()
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

	left, ok := s.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	right, ok := s.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	return &tree.UnionClause{
		Type:  setOps[rand.Intn(len(setOps))],
		Left:  &tree.Select{Select: left},
		Right: &tree.Select{Select: right},
		All:   coin(),
	}, true
}
