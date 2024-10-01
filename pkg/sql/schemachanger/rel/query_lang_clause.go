// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

// tripleDecl is the primary syntactic element of the query language.
// The content indicates that the entity to be bound has an attribute
// value which conforms to the specified value.
//
// It declares that the provided attribute of the referenced variable
// must be the provided value. The value can be a constant, a Var, or a
// set of constants as returned from Any.
type tripleDecl struct {
	entity    Var
	attribute Attr
	value     expr
}

func (f tripleDecl) clause() {}

var _ Clause = (*tripleDecl)(nil)

// eqDecl allows for the expression of a relationship between a variable
// and an expression. A key distinction between eqDecl and tripleDecl is
// that it allows for the introduction of independently constrained
// non-entity variables. Internally, a eqDecl internally can be viewed as
// introducing two triples: tripleDecl{v, Self, v}, triple{v, Self, expr}
// to enable unification. The key distinction is that the variable is not
// assumed to be an entity.
type eqDecl struct {
	v    Var
	expr expr
}

func (e eqDecl) clause() {}

// and is a useful conjunctive construct which exists primarily as a tool
// for libraries to write functions which emit clauses. At build time, the
// clauses are flattened to remove any and clauses.
//
// If other disjunctive features are added later, and may become more
// meaningful.
type and []Clause

func (a and) clause() {}

// filterDecl exposes user-defined predicates to the query language. The
// predicateFunc should be a function value which takes arguments
// corresponding to vars which returns a boolean value. Note that the types
// of the variables will be enforced at runtime; if the values bound to the
// specified vars do not conform the types of the function inputs, the
// predicate is determined to have failed. This is in contrast to returning
// an error.
type filterDecl struct {
	name          string
	vars          []Var
	predicateFunc interface{}
}

func (f filterDecl) clause() {}

type ruleInvocation struct {
	args []Var
	rule *RuleDef
}

func (f ruleInvocation) clause() {}
