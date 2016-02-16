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
	exprCopy := *expr
	exprCopy.Left = exprCopy.Left.DeepCopy()
	exprCopy.Right = exprCopy.Right.DeepCopy()
	return &exprCopy
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
	exprCopy := *expr
	exprCopy.Expr = exprCopy.Expr.DeepCopy()
	return &exprCopy
}

// DeepCopy is part of the Expr interface.
func (expr *CoalesceExpr) DeepCopy() Expr {
	exprCopy := *expr
	exprCopy.Exprs = deepCopyExprs(exprCopy.Exprs)
	return &exprCopy
}

// DeepCopy is part of the Expr interface.
func (expr *ComparisonExpr) DeepCopy() Expr {
	exprCopy := *expr
	exprCopy.Left = exprCopy.Left.DeepCopy()
	exprCopy.Right = exprCopy.Right.DeepCopy()
	return &exprCopy
}

// DeepCopy is part of the Expr interface.
func (expr *ExistsExpr) DeepCopy() Expr {
	return &ExistsExpr{Subquery: expr.Subquery.DeepCopy()}
}

// DeepCopy is part of the Expr interface.
func (expr *FuncExpr) DeepCopy() Expr {
	exprCopy := *expr
	exprCopy.Exprs = deepCopyExprs(exprCopy.Exprs)
	return &exprCopy
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
	q := *t
	return &q
}

// DeepCopy is part of the Expr interface.
func (expr *RangeCond) DeepCopy() Expr {
	exprCopy := *expr
	exprCopy.Left = exprCopy.Left.DeepCopy()
	exprCopy.From = exprCopy.From.DeepCopy()
	exprCopy.To = exprCopy.To.DeepCopy()
	return &exprCopy
}

// DeepCopy is part of the Expr interface.
func (expr *Subquery) DeepCopy() Expr {
	// TODO(radu): the code currently does not modify in-place any part of a subquery. We should
	// however implement this properly at some point.
	return expr
}

// DeepCopy is part of the Expr interface.
func (expr *UnaryExpr) DeepCopy() Expr {
	exprCopy := *expr
	exprCopy.Expr = exprCopy.Expr.DeepCopy()
	return &exprCopy
}

// DeepCopy is part of the Expr interface.
func (expr Array) DeepCopy() Expr {
	return Array(deepCopyExprs(expr))
}

// DeepCopy is part of the Expr interface.
func (t DefaultVal) DeepCopy() Expr { return t }

// DeepCopy is part of the Expr interface.
func (t *IntVal) DeepCopy() Expr {
	tCopy := *t
	return &tCopy
}

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
