// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import "github.com/cockroachdb/cockroach/pkg/sql/opt"

// PlanGram is a regular tree grammar that describes the approved optimizer
// plans for a statement.
//
// For example, consider the following SELECT statement:
//
//	CREATE TABLE abc (a INT, b INT, c INT, INDEX (a), INDEX (b), INDEX (c));
//	SELECT * FROM abc WHERE a = 1 AND b = 2 AND c = 3;
//
// Say we want to allow either of these two optimizer plans:
//
//	select
//	 ├── index-join abc
//	 │    └── scan abc@abc_b_idx
//	 │         └── constraint: /2/4: [/2 - /2]
//	 └── filters
//	      ├── a = 1
//	      └── c = 3
//
//	select
//	 ├── index-join abc
//	 │    └── scan abc@abc_c_idx
//	 │         └── constraint: /3/4: [/3 - /3]
//	 └── filters
//	      ├── a = 1
//	      └── b = 2
//
// A PlanGram that matches either of these two optimizer plans would be:
//
//	root: (SelectOp (IndexJoinOp scan));
//	scan: (ScanOp Index=abc_b_idx) | (ScanOp Index=abc_c_idx);
//
// The grammar is stored as a linked graph of terms, starting with the "root"
// nonterminal. The graph could contain cycles (for example to match a plan with
// a FK cascade cycle).
//
// PlanGrams are equivalent to top-down non-deterministic finite tree
// automatons. We could compile the PlanGram into a state-table-based NFTA, but
// for now we simply walk and interpret it during planning.
//
// For some background see https://en.wikipedia.org/wiki/Regular_tree_grammar
// and https://en.wikipedia.org/wiki/Tree_automaton.
type PlanGram struct {
	root planGramTerm
}

// planGramTerm represents a right-hand-side term in the grammar, which could be
// either the name of a nonterminal or an expression matching an optimizer
// expression.
type planGramTerm interface {
}

// planGramProduction represents a left-hand-side nonterminal in the grammar,
// which has a name and zero or more production rules. The nonterminal matches
// the current optimizer sub-tree if any of its alternate production rules match
// the current sub-tree.
type planGramProduction struct {
	name       string
	alternates []planGramTerm
}

// planGramExpr represents a right-hand-side expression in the grammar, matching
// an optimizer expression. If the optimizer expression has children
// (e.g. JoinOp) then those children are each represented by a term, and the
// planGramExpr is a nonterminal. If the optimizer expression is nullary / a
// leaf (e.g. ScanOp) then the planGramExpr is a terminal.
type planGramExpr struct {
	op       opt.Operator
	fields   map[string]string
	children []planGramTerm
}

var _ planGramTerm = &planGramProduction{}
var _ planGramTerm = &planGramExpr{}

// anyPlanGramTerm is "any": a special nonterminal matching any optimizer
// sub-tree. For efficiency it is the nil planGramTerm (and the nil planGramTerm
// can also be used directly to mean "any").
var anyPlanGramTerm planGramTerm

// nonePlanGramTerm is "none": a special nonterminal matching no optimizer
// sub-trees. It is a nonterminal with zero production rules.
var nonePlanGramTerm planGramTerm = &planGramProduction{name: "none"}

// AnyPlanGram is the PlanGram: "root: any". It matches any optimizer plan. For
// efficiency it is the zero value of PlanGram (and the zero value of PlanGram
// can also be used directly to mean "root: any").
var AnyPlanGram = PlanGram{anyPlanGramTerm}

// NonePlanGram is the PlanGram: "root: none". It matches no optimizer plans.
var NonePlanGram = PlanGram{nonePlanGramTerm}

// Any is true if this PlanGram matches any optimizer sub-tree.
func (p PlanGram) Any() bool {
	return p.root == nil
}
