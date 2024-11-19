// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// isVar returns true if the expression's value can vary during plan
// execution. The parameter allowConstPlaceholders should be true
// in the common case of scalar expressions that will be evaluated
// in the context of the execution of a prepared query, where the
// placeholder will have the same value for every row processed.
// It is set to false for scalar expressions that are not
// evaluated as part of query execution, eg. DEFAULT expressions.
func isVar(expr Expr) bool {
	switch expr.(type) {
	case VariableExpr:
		return true
	case *Placeholder:
		return true
	}
	return false
}

type containsVarsVisitor struct {
	containsVars bool
}

var _ Visitor = &containsVarsVisitor{}

func (v *containsVarsVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if !v.containsVars && isVar(expr) {
		v.containsVars = true
	}
	if v.containsVars {
		return false, expr
	}
	return true, expr
}

func (*containsVarsVisitor) VisitPost(expr Expr) Expr { return expr }

// ContainsVars returns true if the expression contains any variables.
// (variables = sub-expressions, placeholders, indexed vars, etc.)
func ContainsVars(expr Expr) bool {
	v := containsVarsVisitor{containsVars: false}
	WalkExprConst(&v, expr)
	return v.containsVars
}

// DecimalOne represents the constant 1 as DECIMAL.
var DecimalOne DDecimal

func init() {
	DecimalOne.SetInt64(1)
}
