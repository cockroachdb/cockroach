// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package parser

// Helper function to deep copy a list of expressions.
func deepCopyExprs(exprs []Expr) Exprs {
	if len(exprs) == 0 {
		return nil
	}
	res := make(Exprs, len(exprs))
	for i, e := range exprs {
		res[i] = e.DeepCopy()
	}
	return res
}

// DeepCopy is part of the Expr interface.
func (expr *AndExpr) DeepCopy() Expr {
	return &AndExpr{Left: expr.Left.DeepCopy(), Right: expr.Right.DeepCopy()}
}

// DeepCopy is part of the Expr interface.
func (expr *BinaryExpr) DeepCopy() Expr {
	return &BinaryExpr{
		Operator: expr.Operator,
		Left:     expr.Left.DeepCopy(),
		Right:    expr.Right.DeepCopy(),
		fn:       expr.fn,
		ltype:    expr.ltype,
		rtype:    expr.rtype,
	}
}

// DeepCopy is part of the Expr interface.
func (expr *CaseExpr) DeepCopy() Expr {
	n := &CaseExpr{}

	if expr.Expr != nil {
		n.Expr = expr.Expr.DeepCopy()
	}
	n.Whens = make([]*When, len(expr.Whens))
	for i, w := range expr.Whens {
		n.Whens[i] = &When{Cond: w.Cond.DeepCopy(), Val: w.Val.DeepCopy()}
	}
	if expr.Else != nil {
		n.Else = expr.Else.DeepCopy()
	}
	return n
}

// DeepCopy is part of the Expr interface.
func (expr *CastExpr) DeepCopy() Expr {
	return &CastExpr{Expr: expr.Expr.DeepCopy(), Type: expr.Type}
}

// DeepCopy is part of the Expr interface.
func (expr *CoalesceExpr) DeepCopy() Expr {
	return &CoalesceExpr{Name: expr.Name, Exprs: deepCopyExprs(expr.Exprs)}
}

// DeepCopy is part of the Expr interface.
func (expr *ComparisonExpr) DeepCopy() Expr {
	return &ComparisonExpr{
		Operator: expr.Operator,
		Left:     expr.Left.DeepCopy(),
		Right:    expr.Right.DeepCopy(),
		fn:       expr.fn,
	}
}

// DeepCopy is part of the Expr interface.
func (expr *ExistsExpr) DeepCopy() Expr {
	return &ExistsExpr{Subquery: expr.Subquery.DeepCopy()}
}

// DeepCopy is part of the Expr interface.
func (expr *FuncExpr) DeepCopy() Expr {
	return &FuncExpr{
		Name:    expr.Name,
		Type:    expr.Type,
		Exprs:   deepCopyExprs(expr.Exprs),
		fn:      expr.fn,
		fnFound: expr.fnFound,
	}
}

// DeepCopy is part of the Expr interface.
func (expr *IfExpr) DeepCopy() Expr {
	return &IfExpr{
		Cond: expr.Cond.DeepCopy(),
		True: expr.True.DeepCopy(),
		Else: expr.Else.DeepCopy(),
	}
}

// DeepCopy is part of the Expr interface.
func (expr *IsOfTypeExpr) DeepCopy() Expr {
	return &IsOfTypeExpr{
		Not:  expr.Not,
		Expr: expr.Expr.DeepCopy(),
		// XXX(radu): do we need to copy the types or can we treat them as immutable?
		Types: expr.Types,
	}
}

// DeepCopy is part of the Expr interface.
func (expr *NotExpr) DeepCopy() Expr {
	return &NotExpr{Expr: expr.Expr.DeepCopy()}
}

// DeepCopy is part of the Expr interface.
func (expr *NullIfExpr) DeepCopy() Expr {
	return &NullIfExpr{Expr1: expr.Expr1.DeepCopy(), Expr2: expr.Expr2.DeepCopy()}
}

// DeepCopy is part of the Expr interface.
func (expr *OrExpr) DeepCopy() Expr {
	return &OrExpr{Left: expr.Left.DeepCopy(), Right: expr.Right.DeepCopy()}
}

// DeepCopy is part of the Expr interface.
func (expr *ParenExpr) DeepCopy() Expr {
	return &ParenExpr{Expr: expr.Expr.DeepCopy()}
}

// DeepCopy is part of the Expr interface.
func (t *QualifiedName) DeepCopy() Expr {
	q := &QualifiedName{}
	*q = *t
	return q
}

// DeepCopy is part of the Expr interface.
func (expr *RangeCond) DeepCopy() Expr {
	return &RangeCond{
		Not:  expr.Not,
		Left: expr.Left.DeepCopy(),
		From: expr.From.DeepCopy(),
		To:   expr.To.DeepCopy(),
	}
}

// DeepCopy is part of the Expr interface.
func (expr *Subquery) DeepCopy() Expr {
	panic("deep copy for subqueries not implemented")
}

// DeepCopy is part of the Expr interface.
func (expr *UnaryExpr) DeepCopy() Expr {
	return &UnaryExpr{
		Operator: expr.Operator,
		Expr:     expr.Expr.DeepCopy(),
		fn:       expr.fn,
		dtype:    expr.dtype,
	}
}

// DeepCopy is part of the Expr interface.
func (expr Array) DeepCopy() Expr {
	return Array(deepCopyExprs(expr))
}

// DeepCopy is part of the Expr interface.
func (t DefaultVal) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t *IntVal) DeepCopy() Expr { return &IntVal{Val: t.Val, Str: t.Str} }

// DeepCopy is part of the Expr interface.
func (t NumVal) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (expr Row) DeepCopy() Expr {
	return Row(deepCopyExprs(expr))
}

// DeepCopy is part of the Expr interface.
func (expr Tuple) DeepCopy() Expr {
	return Tuple(deepCopyExprs(expr))
}

// DeepCopy is part of the Expr interface.
func (t ValArg) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DBool) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DBytes) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DDate) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DFloat) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t *DDecimal) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DInt) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DInterval) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t dNull) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DString) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DTimestamp) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DTuple) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t DValArg) DeepCopy() Expr { return t }
