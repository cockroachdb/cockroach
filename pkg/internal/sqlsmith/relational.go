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
	"bytes"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func (s *scope) makeStmt() (*scope, bool) {
	if d6() < 3 {
		return s.makeInsert()
	}
	return s.makeReturningStmt(nil)
}

func (s *scope) makeReturningStmt(desiredTypes []types.T) (*scope, bool) {
	for i := 0; i < retryCount; i++ {
		var outScope *scope
		var ok bool
		if s.level < d6() && d6() < 3 {
			outScope, ok = s.makeValues(desiredTypes)
		} else if s.level < d6() && d6() < 3 {
			outScope, ok = s.makeSetOp(desiredTypes)
		} else {
			outScope, ok = s.makeSelect(desiredTypes)
		}
		if ok {
			return outScope, true
		}
	}
	return nil, false
}

func (s *scope) getTableExpr() (*scope, bool) {
	if len(s.schema.tables) == 0 {
		return nil, false
	}
	outScope := s.push()
	table := s.schema.tables[rand.Intn(len(s.schema.tables))]
	outScope.expr = &tableExpr{
		rel:   table,
		alias: s.name("tab"),
	}
	return outScope, true
}

func (s *scope) makeDataSource() (*scope, bool) {
	s = s.push()
	if s.level < 3+d6() {
		if d6() > 4 {
			return s.makeJoinExpr()
		}
	}

	if s.level < 3+d6() && coin() {
		return s.makeInsertReturning(nil)
	}

	return s.getTableExpr()
}

// Format defines a type that can write to a buffer.
type Format interface {
	Format(*bytes.Buffer)
}

type scalarExpr interface {
	Format
	Type() types.T
}

// REL OPS

type relExpr interface {
	Format

	Cols() []column
}

type tableRef interface {
	relExpr

	Name() string
	Refs() []tableRef
}

////////
// TABLE
////////

type tableExpr struct {
	alias string
	rel   namedRelation
}

func (t tableExpr) Name() string {
	return t.alias
}

func (t tableExpr) Format(buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%s as %s", t.rel.name, t.alias)
}

func (t tableExpr) Cols() []column {
	return t.rel.cols
}

func (t tableExpr) Refs() []tableRef {
	return []tableRef{t}
}

///////
// JOIN
///////

type join struct {
	lhs  relExpr
	rhs  relExpr
	on   scalarExpr
	cols []column
}

// TODO(justin): also do outer joins.

func (s *scope) makeJoinExpr() (*scope, bool) {
	outScope := s.push()
	leftScope, ok := s.makeDataSource()
	if !ok {
		return nil, false
	}
	rightScope, ok := s.makeDataSource()
	if !ok {
		return nil, false
	}

	lhs := leftScope.expr
	rhs := rightScope.expr

	var cols []column
	cols = append(cols, lhs.Cols()...)
	cols = append(cols, rhs.Cols()...)

	outScope.refs = append(outScope.refs, leftScope.refs...)
	outScope.refs = append(outScope.refs, rightScope.refs...)
	on, ok := s.makeBoolExpr()
	if !ok {
		return nil, false
	}

	outScope.expr = &join{
		lhs:  lhs,
		rhs:  rhs,
		cols: cols,
		on:   on,
	}

	return outScope, true
}

func (j *join) Format(buf *bytes.Buffer) {
	j.lhs.Format(buf)
	buf.WriteString(" join ")
	j.rhs.Format(buf)
	buf.WriteString(" on ")
	j.on.Format(buf)
}

func (j *join) Cols() []column {
	return j.cols
}

// STATEMENTS

/////////
// SELECT
/////////

type selectExpr struct {
	fromClause []relExpr
	selectList []scalarExpr
	filter     scalarExpr
	limit      string
	distinct   bool
	orderBy    []scalarExpr
}

func (s *selectExpr) Format(buf *bytes.Buffer) {
	buf.WriteString("select ")
	if s.distinct {
		buf.WriteString("distinct ")
	}
	comma := ""
	for _, v := range s.selectList {
		buf.WriteString(comma)
		v.Format(buf)
		comma = ", "
	}
	buf.WriteString(" from ")
	comma = ""
	for _, v := range s.fromClause {
		buf.WriteString(comma)
		v.Format(buf)
		comma = ", "
	}

	if s.filter != nil {
		buf.WriteString(" where ")
		s.filter.Format(buf)
	}

	if s.orderBy != nil {
		buf.WriteString(" order by ")
		comma = ""
		for _, v := range s.orderBy {
			buf.WriteString(comma)
			v.Format(buf)
			comma = ", "
		}
	}

	if s.limit != "" {
		buf.WriteString(" ")
		buf.WriteString(s.limit)
	}
}

func (s *scope) makeSelect(desiredTypes []types.T) (*scope, bool) {
	var outScope *scope

	var out selectExpr
	out.fromClause = []relExpr{}
	{
		fromScope, ok := s.makeDataSource()
		if !ok {
			return nil, false
		}
		out.fromClause = append(out.fromClause, fromScope.expr)
		outScope = fromScope
	}

	selectList, ok := outScope.makeSelectList(desiredTypes)
	if !ok {
		return nil, false
	}

	out.selectList = selectList

	if coin() {
		out.filter, ok = outScope.makeBoolExpr()
		if !ok {
			return nil, false
		}
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

	out.distinct = d100() == 1

	if d6() > 2 {
		out.limit = fmt.Sprintf("limit %d", d100())
	}

	outScope.expr = &out

	return outScope, true
}

func (s *scope) makeSelectList(desiredTypes []types.T) ([]scalarExpr, bool) {
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() == 1 {
				break
			}
		}
	}
	var result []scalarExpr
	for _, t := range desiredTypes {
		next, ok := s.makeScalar(t)
		if !ok {
			return nil, false
		}
		result = append(result, next)
	}
	return result, true
}

func (s *selectExpr) Cols() []column {
	return nil
}

/////////
// INSERT
/////////

type insert struct {
	target  string
	targets []column
	input   relExpr
}

func (i *insert) Format(buf *bytes.Buffer) {
	buf.WriteString("insert into ")
	buf.WriteString(i.target)
	buf.WriteString(" (")
	comma := ""
	for _, c := range i.targets {
		buf.WriteString(comma)
		buf.WriteString(c.name)
		comma = ", "
	}
	buf.WriteString(") ")
	i.input.Format(buf)
}

func (s *scope) makeInsert() (*scope, bool) {
	outScope := s.push()
	out, ok := s.getTableExpr()
	if !ok {
		return nil, false
	}
	target := out.expr.(*tableExpr)

	var desiredTypes []types.T
	var targets []column

	// Grab some subset of the columns of the table to attempt to insert into.
	// TODO(justin): also support the non-named variant.
	for _, c := range target.Cols() {
		// We *must* write a column if it's writable and non-nullable.
		// We *can* write a column if it's writable and nullable.
		if c.writability == writable && (!c.nullable || coin()) {
			targets = append(targets, c)
			desiredTypes = append(desiredTypes, c.typ)
		}
	}

	input, ok := s.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	// HACK: the ref here needs to have the original table name, not whatever
	// alias we gave it.
	outScope.refs = append(outScope.refs, &tableExpr{
		alias: target.rel.name,
		rel:   target.rel,
	})

	outScope.expr = &insert{
		target:  target.rel.name,
		targets: targets,
		input:   input.expr,
	}

	return outScope, true
}

func (i *insert) Cols() []column {
	return nil
}

///////////////////////
// INSERT ... RETURNING
///////////////////////

// INSERT...RETURNING is only treated as a data source for now. That means
// it's always the [...] variant.

type insertReturning struct {
	insert

	returning []scalarExpr
}

func (s *scope) makeInsertReturning(desiredTypes []types.T) (*scope, bool) {
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

	insertScope, ok := s.makeInsert()
	if !ok {
		return nil, false
	}

	outScope := s.push()

	var returning []scalarExpr
	for _, t := range desiredTypes {
		e, ok := insertScope.makeScalar(t)
		if !ok {
			return nil, false
		}
		returning = append(returning, e)
	}

	outScope.expr = &insertReturning{
		insert:    *insertScope.expr.(*insert),
		returning: returning,
	}
	return outScope, true
}

func (i *insertReturning) Format(buf *bytes.Buffer) {
	buf.WriteByte('[')
	i.insert.Format(buf)
	buf.WriteString(" returning ")
	comma := ""
	for _, r := range i.returning {
		buf.WriteString(comma)
		r.Format(buf)
		comma = ", "
	}
	buf.WriteByte(']')
}

/////////
// VALUES
/////////

type values struct {
	values [][]scalarExpr
}

func (s *scope) makeValues(desiredTypes []types.T) (*scope, bool) {
	outScope := s.push()
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

	numRowsToInsert := d6()
	vals := make([][]scalarExpr, numRowsToInsert)
	for i := 0; i < numRowsToInsert; i++ {
		tuple := make([]scalarExpr, len(desiredTypes))
		for j, t := range desiredTypes {
			e, ok := outScope.makeScalar(t)
			if !ok {
				return nil, false
			}
			tuple[j] = e
		}
		vals[i] = tuple
	}

	outScope.expr = &values{vals}
	return outScope, true
}

func (v *values) Cols() []column {
	return nil
}

func (v *values) Format(buf *bytes.Buffer) {
	buf.WriteString("values ")
	comma := ""
	for _, t := range v.values {
		buf.WriteString(comma)
		buf.WriteByte('(')
		comma2 := ""
		for _, d := range t {
			buf.WriteString(comma2)
			d.Format(buf)
			comma2 = ", "
		}
		buf.WriteByte(')')
		comma = ", "
	}
}

//////////////////////////////////
// SET OP (UNION/INTERSECT/EXCEPT)
//////////////////////////////////

type setOp struct {
	op    string
	left  relExpr
	right relExpr
}

var setOps = []string{"union", "union all", "except", "except all", "intersect", "intersect all"}

func (s *scope) makeSetOp(desiredTypes []types.T) (*scope, bool) {
	outScope := s.push()
	if desiredTypes == nil {
		for {
			desiredTypes = append(desiredTypes, getRandType())
			if d6() < 2 {
				break
			}
		}
	}

	leftScope, ok := outScope.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	rightScope, ok := outScope.makeReturningStmt(desiredTypes)
	if !ok {
		return nil, false
	}

	outScope.expr = &setOp{
		op:    setOps[rand.Intn(len(setOps))],
		left:  leftScope.expr,
		right: rightScope.expr,
	}

	return outScope, true
}

func (s *setOp) Cols() []column {
	return s.left.Cols()
}

func (s *setOp) Format(buf *bytes.Buffer) {
	buf.WriteByte('(')
	s.left.Format(buf)
	buf.WriteByte(')')
	buf.WriteByte(' ')
	buf.WriteString(s.op)
	buf.WriteByte(' ')
	buf.WriteByte('(')
	s.right.Format(buf)
	buf.WriteByte(')')
}
