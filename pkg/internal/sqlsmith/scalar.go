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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// makeScalar attempts to construct a scalar expression of the requested type.
// If it was unsuccessful, it will return false.
func (s *scope) makeScalar(typ types.T) (scalarExpr, bool) {
	pickedType := typ
	if typ == types.Any {
		pickedType = getRandType()
	}
	s = s.push()

	for i := 0; i < retryCount; i++ {
		var result scalarExpr
		var ok bool
		// TODO(justin): this is how sqlsmith chooses what to do, but it feels
		// to me like there should be a more clean/principled approach here.
		if s.level < d6() && d9() == 1 {
			result, ok = s.makeCaseExpr(pickedType)
		} else if s.level < d6() && d42() == 1 {
			result, ok = s.makeCoalesceExpr(pickedType)
		} else if len(s.refs) > 0 && d20() > 1 {
			result, ok = s.makeColRef(typ)
		} else if s.level < d6() && d9() == 1 {
			result, ok = s.makeBinOp(typ)
		} else if s.level < d6() && d9() == 1 {
			result, ok = s.makeFunc(typ)
		} else if s.level < d6() && d6() == 1 {
			result, ok = s.makeScalarSubquery(typ)
		} else {
			result, ok = s.makeConstExpr(pickedType), true
		}
		if ok {
			return result, ok
		}
	}

	// Retried enough times, give up.
	return nil, false
}

// TODO(justin): sqlsmith separated this out from the general case for
// some reason - I think there must be a clean way to unify the two.
func (s *scope) makeBoolExpr() (scalarExpr, bool) {
	s = s.push()

	for i := 0; i < retryCount; i++ {
		var result scalarExpr
		var ok bool

		if d6() < 4 {
			result, ok = s.makeBinOp(types.Bool)
		} else if d6() < 4 {
			result, ok = s.makeScalar(types.Bool)
		} else {
			result, ok = s.makeExists()
		}

		if ok {
			return result, ok
		}
	}

	// Retried enough times, give up.
	return nil, false
}

///////
// CASE
///////

type caseExpr struct {
	condition scalarExpr
	trueExpr  scalarExpr
	falseExpr scalarExpr
}

func (c *caseExpr) Type() types.T {
	return c.trueExpr.Type()
}

func (c *caseExpr) Format(buf *bytes.Buffer) {
	buf.WriteString("case when ")
	c.condition.Format(buf)
	buf.WriteString(" then ")
	c.trueExpr.Format(buf)
	buf.WriteString(" else ")
	c.falseExpr.Format(buf)
	buf.WriteString(" end")
}

func (s *scope) makeCaseExpr(typ types.T) (scalarExpr, bool) {
	condition, ok := s.makeScalar(types.Bool)
	if !ok {
		return nil, false
	}

	trueExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	falseExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	return &caseExpr{condition, trueExpr, falseExpr}, true
}

///////////
// COALESCE
///////////

type coalesceExpr struct {
	firstExpr  scalarExpr
	secondExpr scalarExpr
}

func (c *coalesceExpr) Type() types.T {
	return c.firstExpr.Type()
}

func (c *coalesceExpr) Format(buf *bytes.Buffer) {
	buf.WriteString("cast(coalesce(")
	c.firstExpr.Format(buf)
	buf.WriteString(", ")
	c.secondExpr.Format(buf)
	buf.WriteString(") as ")
	buf.WriteString(c.firstExpr.Type().SQLName())
	buf.WriteString(")")
}

func (s *scope) makeCoalesceExpr(typ types.T) (scalarExpr, bool) {
	firstExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	secondExpr, ok := s.makeScalar(typ)
	if !ok {
		return nil, false
	}

	return &coalesceExpr{firstExpr, secondExpr}, true
}

////////
// CONST
////////

type constExpr struct {
	typ  types.T
	expr string
}

func (c *constExpr) Type() types.T {
	return c.typ
}

func (c *constExpr) Format(buf *bytes.Buffer) {
	buf.WriteString(c.expr)
}

func (s *scope) makeConstExpr(typ types.T) scalarExpr {
	var datum tree.Datum
	col, err := sqlbase.DatumTypeToColumnType(typ)
	if err != nil {
		datum = tree.DNull
	} else {
		s.schema.lock.Lock()
		datum = sqlbase.RandDatumWithNullChance(s.schema.rnd, col, 6)
		s.schema.lock.Unlock()
	}

	// TODO(justin): maintain context and see if we're in an INSERT, and maybe use
	// DEFAULT (which is a legal "value" in such a context).

	return &constExpr{typ, datum.String()}
}

/////////
// COLREF
/////////

type colRefExpr struct {
	ref string
	typ types.T
}

func (c *colRefExpr) Type() types.T {
	return c.typ
}

func (c *colRefExpr) Format(buf *bytes.Buffer) {
	buf.WriteString(c.ref)
}

func (s *scope) makeColRef(typ types.T) (scalarExpr, bool) {
	ref := s.refs[rand.Intn(len(s.refs))]
	col := ref.Cols()[rand.Intn(len(ref.Cols()))]
	if typ != types.Any && col.typ != typ {
		return nil, false
	}

	return &colRefExpr{
		ref: ref.Name() + "." + col.name,
		typ: col.typ,
	}, true
}

/////////
// BIN OP
/////////

type opExpr struct {
	outTyp types.T

	left  scalarExpr
	right scalarExpr
	op    string
}

func (o *opExpr) Type() types.T {
	return o.outTyp
}

func (o *opExpr) Format(buf *bytes.Buffer) {
	buf.WriteByte('(')
	o.left.Format(buf)
	buf.WriteByte(' ')
	buf.WriteString(o.op)
	buf.WriteByte(' ')
	o.right.Format(buf)
	buf.WriteByte(')')
}

func (s *scope) makeBinOp(typ types.T) (scalarExpr, bool) {
	if typ == types.Any {
		typ = getRandType()
	}
	ops := s.schema.GetOperatorsByOutputType(typ)
	if len(ops) == 0 {
		return nil, false
	}
	op := ops[rand.Intn(len(ops))]

	left, ok := s.makeScalar(op.left)
	if !ok {
		return nil, false
	}
	right, ok := s.makeScalar(op.right)
	if !ok {
		return nil, false
	}

	return &opExpr{
		outTyp: typ,
		left:   left,
		right:  right,
		op:     op.name,
	}, true
}

//////////
// FUNC OP
//////////

type funcExpr struct {
	outTyp types.T

	name   string
	inputs []scalarExpr
}

func (f *funcExpr) Type() types.T {
	return f.outTyp
}

func (f *funcExpr) Format(buf *bytes.Buffer) {
	buf.WriteString(f.name)
	buf.WriteByte('(')
	comma := ""
	for _, a := range f.inputs {
		buf.WriteString(comma)
		a.Format(buf)
		comma = ", "
	}
	buf.WriteByte(')')
}

func (s *scope) makeFunc(typ types.T) (scalarExpr, bool) {
	if typ == types.Any {
		typ = getRandType()
	}
	ops := s.schema.GetFunctionsByOutputType(typ)
	if len(ops) == 0 {
		return nil, false
	}
	op := ops[rand.Intn(len(ops))]

	args := make([]scalarExpr, 0)
	for i := range op.inputs {
		in, ok := s.makeScalar(op.inputs[i])
		if !ok {
			return nil, false
		}
		args = append(args, in)
	}

	return &funcExpr{
		outTyp: typ,
		name:   op.name,
		inputs: args,
	}, true
}

/////////
// EXISTS
/////////

type exists struct {
	subquery relExpr
}

func (e *exists) Format(buf *bytes.Buffer) {
	buf.WriteString("exists(")
	e.subquery.Format(buf)
	buf.WriteString(")")
}

func (e *exists) Type() types.T {
	return types.Bool
}

func (s *scope) makeExists() (scalarExpr, bool) {
	outScope, ok := s.makeSelect(nil)
	if !ok {
		return nil, false
	}

	return &exists{outScope.expr}, true
}

//////////////////
// SCALAR SUBQUERY
//////////////////

type scalarSubq struct {
	subquery relExpr
}

func (s *scalarSubq) Format(buf *bytes.Buffer) {
	buf.WriteString("(")
	s.subquery.Format(buf)
	buf.WriteString(")")
}

func (s *scalarSubq) Type() types.T {
	return s.subquery.(*selectExpr).selectList[0].Type()
}

func (s *scope) makeScalarSubquery(typ types.T) (scalarExpr, bool) {
	outScope, ok := s.makeSelect([]types.T{typ})
	if !ok {
		return nil, false
	}

	outScope.expr.(*selectExpr).limit = "limit 1"
	return &scalarSubq{outScope.expr}, true
}
