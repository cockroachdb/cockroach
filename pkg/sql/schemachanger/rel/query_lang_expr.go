// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

// expr is an expression value in rel.
type expr interface {
	expr() // marker

	// encoded returns a value for use in serialization.
	encoded() interface{}
}

// Var is a variable name. Everything is convention, but, when you create
// clauses and use variable names which are not part of the defined scope of
// the rule, the new variableSlots which will be created will have a scope
// prefix to try to ensure that they are unique. Given that, don't put `:` in
// your variable names.
type Var string

// Blank is a special variable which, when used, means that the value should
// be bound to something. Different occurrences of Blank in different parts
// of the query are not related to each other. It's as though each occurrence
// is its own variable, but that variable is not named and its binding cannot
// be retrieved.
//
// At the time when this was created, blank is not allowed to be used as an
// entity binding.
const Blank Var = "_"

// Var is an expr.
func (Var) expr() {}

type valueExpr struct {
	value interface{}
}

func (v valueExpr) expr() {}

type notValueExpr struct {
	value interface{}
}

func (v notValueExpr) expr() {}

type anyExpr []interface{}

func (a anyExpr) expr() {}

type containsExpr struct {
	v expr
}

func (c containsExpr) expr() {}
