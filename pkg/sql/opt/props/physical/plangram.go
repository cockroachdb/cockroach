// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bytes"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

//lint:file-ignore U1000 While this is stubbed out, ignore unused code.

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
//	root: (Select (IndexJoin scan));
//	scan: (Scan Index="abc_b_idx") | (Scan Index="abc_c_idx");
//
// The grammar is stored as a linked graph of terms, starting with the "root"
// term. The graph could contain cycles (for example to match a plan with a FK
// cascade cycle).
//
// PlanGrams are equivalent to top-down non-deterministic finite tree
// automatons. We could compile the PlanGram into a state-table-based NFTA, but
// for now we simply walk and interpret it during planning.
//
// For some background see https://en.wikipedia.org/wiki/Regular_tree_grammar
// and https://en.wikipedia.org/wiki/Tree_automaton.
type PlanGram struct {
	// root is the starting term, and can also be considered a "pseudo
	// nonterminal" in the grammar consisting of a single production rule. But
	// because of its construction this "root" nonterminal cannot be referenced
	// from any other production rule.
	root planGramTerm
}

// planGramTerm represents a right-hand-side term in the grammar, which could be
// either the name of a nonterminal or an expression matching an optimizer
// expression.
type planGramTerm interface {
	// visitProductions visits all left-hand-side nonterminals reachable from this
	// term exactly once, using the visited map to avoid cycles. No particular
	// traversal order is guaranteed. If the special "any" or "none" nonterminals
	// are encountered, they are not visited.
	visitProductions(visited map[*planGramProduction]struct{}, visit func(*planGramProduction))
}

// planGramProduction represents a left-hand-side nonterminal in the grammar,
// which has an identifying name and one or more production rules. The
// nonterminal matches the current optimizer sub-tree if any of its production
// rules match the current sub-tree.
type planGramProduction struct {
	// name is the symbol identifying the nonterminal in the grammar, which must
	// be unique in the grammar, and must not be "root", "any", or "none". It must
	// not contain any of the characters "'\:=()|; or whitespace. For clarity it
	// is recommended that the name not match any optimizer operator name, but
	// syntactically there's no problem with re-using an operator name.
	name string
	// rules are the set of production rules for this nonterminal. There must be
	// at least one rule (which could be anyPlanGramTerm or nonePlanGramTerm if
	// the nonterminal should match any optimizer sub-tree or no optimizer
	// sub-tree, respectively).
	rules []planGramTerm
}

// planGramExpr represents a right-hand-side expression in the grammar, matching
// an optimizer expression. If the optimizer expression has children
// (e.g. InnerJoin) then those children are each represented by a term, and the
// planGramExpr is a nonterminal. If the optimizer expression is nullary / a
// leaf (e.g. Scan) then the planGramExpr is a terminal.
type planGramExpr struct {
	op       opt.Operator
	fields   []planGramExprField
	children []planGramTerm
}

// planGramExprField represents a single field of an optimizer expression. How
// this matches the corresponding optgen field depends on the specific
// expression and field.
type planGramExprField struct {
	key, val string
}

var _ planGramTerm = &planGramProduction{}
var _ planGramTerm = &planGramExpr{}

// anyPlanGramTerm is "any": a special nonterminal matching any optimizer
// sub-tree. For efficiency it is the nil planGramTerm (and the nil planGramTerm
// can also be used directly to mean "any").
var anyPlanGramTerm planGramTerm

// nonePlanGramTerm is "none": a special nonterminal matching no optimizer
// sub-trees. It is the only nonterminal with zero production rules.
var nonePlanGramTerm planGramTerm = &planGramProduction{name: "none"}

// AnyPlanGram is the PlanGram: "root: any;". It matches any optimizer plan. For
// efficiency it is the zero value of PlanGram (and the zero value of PlanGram
// can also be used directly to mean "root: any;").
var AnyPlanGram = PlanGram{anyPlanGramTerm}

// NonePlanGram is the PlanGram: "root: none;". It matches no optimizer plans.
var NonePlanGram = PlanGram{nonePlanGramTerm}

// Any is true if this PlanGram matches any optimizer sub-tree.
func (p PlanGram) Any() bool {
	return p.root == nil
}

// FormatPretty writes the full PlanGram grammar to the buffer, starting with
// the root, optionally with multiple lines.
func (p PlanGram) FormatPretty(b *bytes.Buffer, newlines bool) {
	if newlines {
		defer b.WriteRune('\n')
	}
	if p.root == nil {
		b.WriteString("root: any;")
		return
	}
	if p.root == nonePlanGramTerm {
		b.WriteString("root: none;")
		return
	}

	// formatTerm writes a RHS term to the buffer.
	var formatTerm func(planGramTerm)
	formatTerm = func(term planGramTerm) {
		if term == nil {
			b.WriteString("any")
			return
		}
		if term == nonePlanGramTerm {
			b.WriteString("none")
			return
		}
		switch t := term.(type) {
		case *planGramProduction:
			// Assume nonterminal names don't need to be quoted.
			b.WriteString(t.name)
		case *planGramExpr:
			b.WriteRune('(')
			b.WriteString(t.op.CamelCase())
			for _, field := range t.fields {
				b.WriteRune(' ')
				// Assume field names don't need to be quoted.
				b.WriteString(field.key)
				b.WriteRune('=')
				b.WriteString(strconv.Quote(field.val))
			}
			for _, child := range t.children {
				b.WriteRune(' ')
				formatTerm(child)
			}
			b.WriteRune(')')
		}
	}

	b.WriteString("root: ")
	formatTerm(p.root)
	b.WriteRune(';')

	// Visit each LHS nonterminal reachable from the root, and for each, write all
	// the production rules to the buffer.
	visited := make(map[*planGramProduction]struct{})
	p.root.visitProductions(visited, func(pp *planGramProduction) {
		if newlines {
			b.WriteRune('\n')
		} else {
			b.WriteRune(' ')
		}
		b.WriteString(pp.name)
		for i, rule := range pp.rules {
			if i == 0 {
				b.WriteString(": ")
			} else {
				b.WriteString(" | ")
			}
			formatTerm(rule)
		}
		b.WriteRune(';')
	})
}

// Format writes the full PlanGram grammar to the buffer, starting with the
// root.
func (p PlanGram) Format(b *bytes.Buffer) {
	p.FormatPretty(b, false /* newlines */)
}

// String implements the fmt.Stringer interface.
func (p PlanGram) String() string {
	var b bytes.Buffer
	p.FormatPretty(&b, false /* newlines */)
	return b.String()
}

// visitProductions implements the planGramTerm interface.
func (pp *planGramProduction) visitProductions(
	visited map[*planGramProduction]struct{}, visit func(*planGramProduction),
) {
	if _, ok := visited[pp]; ok {
		return
	}
	visited[pp] = struct{}{}
	visit(pp)
	for _, rule := range pp.rules {
		if rule != nil && rule != nonePlanGramTerm {
			rule.visitProductions(visited, visit)
		}
	}
}

// visitProductions implements the planGramTerm interface.
func (pe *planGramExpr) visitProductions(
	visited map[*planGramProduction]struct{}, visit func(*planGramProduction),
) {
	for _, child := range pe.children {
		if child != nil && child != nonePlanGramTerm {
			child.visitProductions(visited, visit)
		}
	}
}
