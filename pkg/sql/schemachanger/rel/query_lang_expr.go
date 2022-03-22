// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// Var is an expr.
func (Var) expr() {}

type valueExpr struct {
	value interface{}
}

func (v valueExpr) expr() {}

type anyExpr []interface{}

func (a anyExpr) expr() {}
