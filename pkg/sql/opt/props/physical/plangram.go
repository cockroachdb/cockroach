// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/errors"
)

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
// Use "_" as a wildcard operator to match any expression, e.g. "root: (_);".
//
// The grammar is stored as a linked graph of terms, starting with the "root"
// nonterminal. The graph could contain cycles (for example to match a plan with
// a FK cascade cycle).
//
// PlanGrams are equivalent to top-down non-deterministic finite tree
// automatons. We could compile the PlanGram into a state-table-based NFTA, but
// for now we simply walk and interpret it during planning.
//
// PlanGrams are constructed via PlanGramBuilder or ParsePlanGram. The internal
// term representation is hidden so that the package retains freedom to change
// it (e.g., to intern shared subtrees or compile to an NFTA).
//
// For some background see https://en.wikipedia.org/wiki/Regular_tree_grammar
// and https://en.wikipedia.org/wiki/Tree_automaton.
//
// PlanGram equality and hashing use pointer identity of the root term, which
// requires that all PlanGram values being compared point into the same graph of
// terms.
//
// TODO(michae2): PlanGram matching currently only affects relational expressions
// (via ComputeCost). Scalar children (e.g. Filters, Projections, ON conditions)
// are not checked even if explicitly specified in the grammar, because scalar
// optimization does not consult the PlanGram.
//
// TODO(michae2): support NOT within PlanGrams.
type PlanGram struct {
	// root is the starting term. "root" can also be considered a "pseudo
	// nonterminal" in the grammar with a single production rule, but because of
	// its construction, this "root" nonterminal cannot be referenced from any
	// other production rule.
	root planGramTerm
}

// planGramTerm represents a right-hand-side term in the grammar, which could be
// either the name of a nonterminal or an expression matching an optimizer
// expression.
type planGramTerm interface {
	// visitProductions visits all left-hand-side nonterminals reachable from this
	// term exactly once, using the visited map to avoid cycles. No particular
	// traversal order is guaranteed. If the special "any" or "none" nonterminals
	// are encountered, they are not visited (but their parent is).
	visitProductions(visited map[*planGramProduction]struct{}, visit func(*planGramProduction))
	// visitAlternateExprs visits right-hand-side expressions that could be
	// alternates for the current level. It does not recurse to visit all RHS
	// expressions in the grammar. It uses the visited map to avoid cycles. No
	// particular traversal order is guaranteed. If the special "any" or "none"
	// nonterminals are encountered, they are also visited. All visited PlanGrams
	// are guaranteed to have HasAlternates() == false. True is returned if at
	// least one alternate was visited.
	visitAlternateExprs(visited map[*planGramProduction]struct{}, visit func(PlanGram)) bool
}

// planGramProduction represents a left-hand-side nonterminal in the grammar,
// which has an identifying name and one or more production rules. The
// nonterminal matches the current optimizer sub-tree if any of its production
// rules match the current sub-tree.
type planGramProduction struct {
	// name is the nonterminal symbol in the grammar, which must be unique in the
	// graph. "any" and "none" are reserved, as are names starting with
	// underscores. "root" is the required entry-point production and cannot be
	// referenced from other productions. The name must not be empty, must not
	// contain any of the characters "'\,():;=| or whitespace, and must not start
	// with a digit.
	name string
	// rules are the set of production rules for this nonterminal. There must be
	// at least one rule (which could be anyPlanGramTerm or nonePlanGramTerm if
	// the nonterminal should match any optimizer sub-tree or no optimizer
	// sub-tree, respectively).
	rules []planGramTerm
}

// planGramExpr represents a right-hand-side expression in the grammar matching
// an optimizer expression. The optimizer expression could have children, in
// which case those children are each represented by a term (e.g. InnerJoin), or
// the optimizer expression could be nullary / a leaf (e.g. Scan).
//
// If the op is UnknownOp this is a wildcard expr.
type planGramExpr struct {
	op       opt.Operator
	fields   []PlanGramField
	children []planGramTerm
}

// PlanGramField represents a single field of an optimizer expression. How this
// matches the corresponding optgen field depends on the specific expression and
// field.
type PlanGramField struct {
	Key, Val string
}

// PlanGramFieldMatchableExpr is an optional interface that optimizer expression
// types can implement to support field-based matching in PlanGrams. Expressions
// that implement this interface report their matchable fields (e.g. Table,
// Index) so that PlanGram field constraints like (Scan Table="abc") can be
// checked during matching.
type PlanGramFieldMatchableExpr interface {
	PlanGramMatchableFields(md *opt.Metadata) []PlanGramField
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

var errInvalidUTF8 = errors.New("invalid UTF-8")

// Any is true if this PlanGram matches any optimizer sub-tree.
func (p PlanGram) Any() bool {
	// There are other PlanGrams that could be proven to match any optimizer
	// sub-tree. This function does not exhaustively identify every such PlanGram.
	return p.root == nil
}

// None is true if this PlanGram does not match any optimizer sub-tree.
func (p PlanGram) None() bool {
	if p.root == nonePlanGramTerm {
		return true
	}
	if pp, ok := p.root.(*planGramProduction); ok && len(pp.rules) == 0 {
		return true
	}
	// There are other PlanGrams that could be proven to not match any optimizer
	// sub-tree. This function does not exhaustively identify every such PlanGram.
	return false
}

// WithNoneFallback wraps this PlanGram in a production that includes
// NonePlanGram as a fallback alternate. This ensures the optimizer also
// considers the unconstrained (default) plan, so that if the PlanGram cannot be
// fully matched, the optimizer falls back to its natural cost-based
// selection. This should be called on the root PlanGram before optimization
// starts.
func (p PlanGram) WithNoneFallback() PlanGram {
	if p.Any() || p.None() {
		return p
	}
	prod := &planGramProduction{
		name:  "_fallback",
		rules: []planGramTerm{p.root, nonePlanGramTerm},
	}
	return PlanGram{root: prod}
}

// FormatPretty writes the full PlanGram grammar to the buffer, starting with
// the root. If newlines is true, each production is on its own line and
// nested parenthesized expressions are indented to their paren depth. If
// newlines is false, the entire grammar is written on a single line.
func (p PlanGram) FormatPretty(b *bytes.Buffer, newlines bool) {
	if newlines {
		defer b.WriteRune('\n')
	}
	if p.Any() {
		b.WriteString("root: any;")
		return
	}
	if p.None() {
		b.WriteString("root: none;")
		return
	}

	// hasNestedExpr reports whether term is an expression with at least one
	// nested expression child. Such terms are broken across multiple lines
	// in the indented form; expressions whose children are all refs/any/none
	// stay on a single line.
	hasNestedExpr := func(term planGramTerm) bool {
		expr, ok := term.(*planGramExpr)
		if !ok {
			return false
		}
		for _, child := range expr.children {
			if _, isExpr := child.(*planGramExpr); isExpr {
				return true
			}
		}
		return false
	}

	writeIndent := func(depth int) {
		for i := 0; i < depth*2; i++ {
			b.WriteRune(' ')
		}
	}

	// formatTerm writes a RHS term to the buffer. depth is the current paren
	// depth, used to indent broken expressions when newlines is true.
	var formatTerm func(term planGramTerm, depth int)
	formatTerm = func(term planGramTerm, depth int) {
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
			if t.op == opt.UnknownOp {
				b.WriteRune('_')
			} else {
				b.WriteString(t.op.CamelCase())
			}
			for _, field := range t.fields {
				b.WriteRune(' ')
				// Assume field names don't need to be quoted.
				b.WriteString(field.Key)
				b.WriteRune('=')
				b.WriteString(strconv.Quote(field.Val))
			}
			breakChildren := newlines && hasNestedExpr(t)
			for _, child := range t.children {
				if breakChildren {
					b.WriteRune('\n')
					writeIndent(depth + 1)
				} else {
					b.WriteRune(' ')
				}
				formatTerm(child, depth+1)
			}
			b.WriteRune(')')
		default:
			panic(errors.AssertionFailedf("unexpected planGramTerm type %T", t))
		}
	}

	// formatProduction emits a production. If newlines is true and the
	// production has multiple rules or a single rule that needs breaking,
	// the rules go on their own indented lines (with `|` prefixing alts);
	// otherwise the production stays on a single line.
	formatProduction := func(name string, rules []planGramTerm) {
		b.WriteString(name)
		b.WriteRune(':')
		breakLines := newlines && (len(rules) > 1 || hasNestedExpr(rules[0]))
		for i, rule := range rules {
			if breakLines {
				b.WriteRune('\n')
				writeIndent(1)
				if i > 0 {
					b.WriteString("| ")
				}
			} else if i == 0 {
				b.WriteRune(' ')
			} else {
				b.WriteString(" | ")
			}
			formatTerm(rule, 1)
		}
		b.WriteRune(';')
	}

	formatProduction("root", []planGramTerm{p.root})

	// Write each production reachable from the root.
	visited := make(map[*planGramProduction]struct{})
	p.root.visitProductions(visited, func(pp *planGramProduction) {
		if newlines {
			b.WriteRune('\n')
		} else {
			b.WriteRune(' ')
		}
		formatProduction(pp.name, pp.rules)
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

// PlanGramBuilder builds a PlanGram via a stateful nested-context API,
// modeled on explain.OutputBuilder. The builder maintains an implicit
// "current position" stack: Enter* pushes a new context, Leave* pops it.
// AddField and the Ref* methods apply to whatever's on top.
//
// Production references are by name. Forward references are permitted: a
// production may be referenced before EnterProduction has declared it; the
// reference is resolved at Build time. The same idiom supports cycles —
// reference a production from within its own rules.
//
// Nested EnterProduction (declaring a sub-production while another context is
// open) is allowed; the inner production is not connected to the outer
// context except via explicit RefProduction calls. This matches the
// requirement of decompile, where an alternation production is declared
// inline at the spot where it is referenced.
//
// The zero value is ready to use. Build does not mutate state, so it may be
// called multiple times (returning equivalent PlanGrams) or interleaved with
// further construction calls. To construct a new grammar, call Reset —
// following the strings.Builder convention.
//
// A PlanGramBuilder is not safe for concurrent use.
type PlanGramBuilder struct {
	// productions tracks all named productions, including forward-reference
	// stubs (which have empty rules until their EnterProduction call
	// completes). Lazily allocated on first use; cleared by Reset.
	productions map[string]*planGramProduction
	// stack is the nested context stack. Each frame is the in-progress
	// production or expression at that nesting depth. Cleared by Reset.
	stack []planGramTerm
}

// EnterProduction starts a new named production. Subsequent EnterExpr / Ref*
// calls add rules to this production until the matching LeaveProduction. The
// "root" production is required and must contain exactly one rule.
//
// See planGramProduction.name for the constraints on name. Calling
// EnterProduction with a name that already has rules (i.e., was previously
// completed via LeaveProduction) is a duplicate error.
func (b *PlanGramBuilder) EnterProduction(name string) error {
	if name == "any" || name == "none" {
		return errors.Newf("%q cannot be used as a production name", name)
	}
	pp, err := b.getOrCreateProduction(name)
	if err != nil {
		return err
	}
	if len(pp.rules) > 0 {
		return errors.Newf("duplicate production %q", name)
	}
	// Disallow re-entering an already-in-progress production.
	for _, frame := range b.stack {
		if frame == pp {
			return errors.Newf("production %q already in progress", name)
		}
	}
	b.stack = append(b.stack, pp)
	return nil
}

// LeaveProduction pops the current production. The production must have at
// least one rule. Pairs with EnterProduction.
func (b *PlanGramBuilder) LeaveProduction() error {
	if len(b.stack) == 0 {
		return errors.New("LeaveProduction without matching EnterProduction")
	}
	top, ok := b.stack[len(b.stack)-1].(*planGramProduction)
	if !ok {
		return errors.New("LeaveProduction called with expression on top")
	}
	if len(top.rules) == 0 {
		return errors.Newf("production %q has no rules", top.name)
	}
	b.stack = b.stack[:len(b.stack)-1]
	return nil
}

// EnterExpr starts a new expression with the given op. Subsequent AddField,
// EnterExpr, and Ref* calls add fields and children. On LeaveExpr, the
// completed expression becomes either a child of the enclosing expression or
// a rule of the enclosing production, depending on context.
func (b *PlanGramBuilder) EnterExpr(op opt.Operator) error {
	if len(b.stack) == 0 {
		return errors.New("EnterExpr on empty builder")
	}
	if !VisibleToPlanGram(op) {
		return errors.AssertionFailedf("op %s is invisible to PlanGram matching", op)
	}
	b.stack = append(b.stack, &planGramExpr{op: op})
	return nil
}

// VisibleToPlanGram reports whether op participates in PlanGram matching.
// Invisible operators (e.g. Explain, Barrier, Distribute) are optimizer-internal
// or are dropped before a plan is costed; the matcher skips them, so a PlanGram
// must never contain one. Invisible operators must be unary so that the required
// PlanGram term can be passed down to the child group during matching.
func VisibleToPlanGram(op opt.Operator) bool {
	switch op {
	case opt.ExplainOp, opt.BarrierOp, opt.DistributeOp,
		opt.NormCycleTestRelOp, opt.MemoCycleTestRelOp:
		return false
	default:
		return true
	}
}

// LeaveExpr pops the current expression and appends it to the now-current
// frame. Pairs with EnterExpr.
func (b *PlanGramBuilder) LeaveExpr() error {
	if len(b.stack) == 0 {
		return errors.New("LeaveExpr without matching EnterExpr")
	}
	top, ok := b.stack[len(b.stack)-1].(*planGramExpr)
	if !ok {
		return errors.New("LeaveExpr called with production on top")
	}
	b.stack = b.stack[:len(b.stack)-1]
	return b.appendTerm(top)
}

// AddField adds a field constraint to the current expression. Must precede
// any child added via EnterExpr or Ref*.
func (b *PlanGramBuilder) AddField(field PlanGramField) error {
	if len(b.stack) == 0 {
		return errors.New("AddField outside expression")
	}
	top, ok := b.stack[len(b.stack)-1].(*planGramExpr)
	if !ok {
		return errors.New("AddField called with production on top")
	}
	if len(top.children) > 0 {
		return errors.New("fields must come before children in expression")
	}
	if err := validateFieldKey(field.Key); err != nil {
		return err
	}
	top.fields = append(top.fields, field)
	return nil
}

// RefProduction appends a reference to a named production at the current
// position. Forward references are permitted; the named production is
// resolved at Build. The names "any" and "none" resolve to anyPlanGramTerm
// and nonePlanGramTerm respectively (callers may also use RefAny / RefNone
// for clarity).
func (b *PlanGramBuilder) RefProduction(name string) error {
	if len(b.stack) == 0 {
		return errors.New("RefProduction on empty builder")
	}
	switch name {
	case "any":
		return b.appendTerm(anyPlanGramTerm)
	case "none":
		return b.appendTerm(nonePlanGramTerm)
	case "root":
		return errors.New(`"root" cannot be used as a nonterminal reference`)
	}
	pp, err := b.getOrCreateProduction(name)
	if err != nil {
		return err
	}
	return b.appendTerm(pp)
}

// RefAny appends anyPlanGramTerm at the current position.
func (b *PlanGramBuilder) RefAny() error {
	if len(b.stack) == 0 {
		return errors.New("RefAny on empty builder")
	}
	return b.appendTerm(anyPlanGramTerm)
}

// RefNone appends nonePlanGramTerm at the current position.
func (b *PlanGramBuilder) RefNone() error {
	if len(b.stack) == 0 {
		return errors.New("RefNone on empty builder")
	}
	return b.appendTerm(nonePlanGramTerm)
}

// Build validates and returns the PlanGram for the grammar constructed so
// far. It does not mutate the builder, so it may be called multiple times
// (returning equivalent PlanGrams) or interleaved with further construction
// calls (e.g., to inspect the in-progress grammar). To construct a new
// grammar, call Reset.
//
// PlanGrams previously returned by Build remain valid across further
// construction and across Reset: the builder API prevents re-entering a
// completed production (so no production reachable from a returned root can
// gain rules), and Reset only clears the builder's references to the
// underlying production nodes.
func (b *PlanGramBuilder) Build() (PlanGram, error) {
	if len(b.stack) != 0 {
		return PlanGram{}, errors.Newf("unmatched Enter without Leave (%d open)", len(b.stack))
	}
	root, ok := b.productions["root"]
	if !ok {
		return PlanGram{}, errors.New(`missing "root" production`)
	}
	if len(root.rules) != 1 {
		return PlanGram{}, errors.New("root must have exactly one term, not alternates")
	}
	// Report any undefined nonterminals deterministically. Allocation is
	// confined to the error path: a nil slice with no appends costs nothing.
	var undefined []string
	for name, pp := range b.productions {
		if len(pp.rules) == 0 {
			undefined = append(undefined, name)
		}
	}
	if len(undefined) > 0 {
		slices.Sort(undefined)
		return PlanGram{}, errors.Newf("undefined nonterminal(s): %s", strings.Join(undefined, ", "))
	}
	return PlanGram{root: root.rules[0]}, nil
}

// Reset clears the builder so it can be used to construct another grammar.
// The underlying map and slice memory are retained. PlanGrams previously
// returned by Build are unaffected — they hold their own references to the
// production nodes.
func (b *PlanGramBuilder) Reset() {
	clear(b.productions)
	// Nil stack entries before reslicing so the builder doesn't retain
	// references to popped frames.
	clear(b.stack)
	b.stack = b.stack[:0]
}

// appendTerm appends a term as a child of the top expression or as a rule of
// the top production. Returns an error if the stack is empty.
func (b *PlanGramBuilder) appendTerm(term planGramTerm) error {
	if len(b.stack) == 0 {
		return errors.New("term added outside production or expression")
	}
	switch top := b.stack[len(b.stack)-1].(type) {
	case *planGramExpr:
		top.children = append(top.children, term)
	case *planGramProduction:
		top.rules = append(top.rules, term)
	default:
		return errors.AssertionFailedf("unexpected stack frame type %T", top)
	}
	return nil
}

// getOrCreateProduction returns the production for the given name, creating
// a forward-reference stub if one does not yet exist. Validates the name and
// lazily allocates the productions map on first use.
func (b *PlanGramBuilder) getOrCreateProduction(name string) (*planGramProduction, error) {
	if err := validateNonterminalName(name); err != nil {
		return nil, err
	}
	if pp, ok := b.productions[name]; ok {
		return pp, nil
	}
	if b.productions == nil {
		b.productions = make(map[string]*planGramProduction)
	}
	pp := &planGramProduction{name: name}
	b.productions[name] = pp
	return pp, nil
}

// validateNonterminalName checks the syntactic constraints on production /
// reference names.
func validateNonterminalName(name string) error {
	if len(name) == 0 {
		return errors.New("name cannot be empty")
	}
	if name[0] >= '0' && name[0] <= '9' {
		return errors.Newf("name %q must not start with a digit", name)
	}
	return validateNameChars(name)
}

// validateFieldKey checks the syntactic constraints on a field key. Field keys
// share the same character restrictions as nonterminal names so that the
// grammar remains tokenizable.
func validateFieldKey(key string) error {
	if len(key) == 0 {
		return errors.New("field key cannot be empty")
	}
	return validateNameChars(key)
}

// validateNameChars rejects strings containing characters that would break
// tokenization: the reserved punctuation `"'\,():;=|` and any whitespace.
func validateNameChars(s string) error {
	if strings.ContainsAny(s, `"'\,():;=|`) {
		return errors.Newf("%q contains invalid character", s)
	}
	if strings.IndexFunc(s, unicode.IsSpace) >= 0 {
		return errors.Newf("%q contains whitespace", s)
	}
	return nil
}

// ParsePlanGram parses a PlanGram grammar from the given io.Reader, or
// returns an error if it cannot.
func ParsePlanGram(r io.Reader) (PlanGram, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(tokenizePlanGram)

	p := planGramParser{
		scanner: scanner,
		builder: &PlanGramBuilder{},
	}

	for {
		if _, ok := p.peek(); !ok {
			break
		}
		if err := p.parseProduction(); err != nil {
			return PlanGram{}, err
		}
	}

	if err := p.scanner.Err(); err != nil {
		return PlanGram{}, err
	}

	return p.builder.Build()
}

// planGramParser is a recursive-descent parser for plangram grammars with
// 1-token lookahead. It drives a PlanGramBuilder for construction and
// validation.
type planGramParser struct {
	scanner *bufio.Scanner
	tok     string
	hasTok  bool
	builder *PlanGramBuilder
}

// next consumes and returns the next token.
func (p *planGramParser) next() (string, error) {
	if p.hasTok {
		p.hasTok = false
		return p.tok, nil
	}
	if !p.scanner.Scan() {
		if err := p.scanner.Err(); err != nil {
			return "", err
		}
		return "", errors.New("unexpected end of input")
	}
	return p.scanner.Text(), nil
}

// peek returns the next token without consuming it.
func (p *planGramParser) peek() (string, bool) {
	if p.hasTok {
		return p.tok, true
	}
	if !p.scanner.Scan() {
		return "", false
	}
	p.tok = p.scanner.Text()
	p.hasTok = true
	return p.tok, true
}

// expect consumes the next token and asserts it equals want.
func (p *planGramParser) expect(want string) error {
	got, err := p.next()
	if err != nil {
		return errors.Wrapf(err, "expected %q", want)
	}
	if got != want {
		return errors.Newf("expected %q, got %q", want, got)
	}
	return nil
}

// parseProduction parses: NAME ":" term { "|" term } ";".
func (p *planGramParser) parseProduction() error {
	name, err := p.next()
	if err != nil {
		return err
	}
	if err := p.builder.EnterProduction(name); err != nil {
		return err
	}
	if err := p.expect(":"); err != nil {
		return err
	}
	for {
		if err := p.parseTerm(); err != nil {
			return err
		}
		tok, ok := p.peek()
		if !ok || tok != "|" {
			break
		}
		if _, err = p.next(); err != nil { // consume "|"
			return err
		}
	}
	if err := p.expect(";"); err != nil {
		return err
	}
	return p.builder.LeaveProduction()
}

// parseTerm parses a single term: either an expression (starting with "(") or
// a nonterminal reference.
func (p *planGramParser) parseTerm() error {
	tok, ok := p.peek()
	if !ok {
		if err := p.scanner.Err(); err != nil {
			return err
		}
		return errors.New("unexpected end of input: expected term")
	}
	if tok == "(" {
		return p.parseExpr()
	}
	// Must be a nonterminal name.
	name, err := p.next()
	if err != nil {
		return err
	}
	return p.builder.RefProduction(name)
}

// parseExpr parses: "(" OP_NAME { field } { child } ")".
func (p *planGramParser) parseExpr() error {
	if err := p.expect("("); err != nil {
		return err
	}
	opName, err := p.next()
	if err != nil {
		return err
	}
	var op opt.Operator
	if opName == "_" {
		op = opt.UnknownOp
	} else {
		var ok bool
		op, ok = opt.OperatorByCamelCase(opName)
		if !ok {
			return errors.Newf("unknown operator %q", opName)
		}
	}
	if err := p.builder.EnterExpr(op); err != nil {
		return err
	}

	for {
		tok, ok := p.peek()
		if !ok {
			if err := p.scanner.Err(); err != nil {
				return err
			}
			return errors.New("unexpected end of input: expected \")\"")
		}
		if tok == ")" {
			if _, err := p.next(); err != nil { // consume ")"
				return err
			}
			break
		}
		if tok == "(" {
			// Inline child expression.
			if err := p.parseExpr(); err != nil {
				return err
			}
			continue
		}
		if len(tok) == 1 && strings.IndexByte(`":;=|`, tok[0]) >= 0 {
			return errors.Newf("unexpected %q in expression", tok)
		}
		// Consume the name. Then peek to decide if it's a field (followed by
		// "=") or a nonterminal child.
		name, err := p.next()
		if err != nil {
			return err
		}
		if next, ok := p.peek(); !ok || next != "=" {
			// Nonterminal child.
			if err := p.builder.RefProduction(name); err != nil {
				return err
			}
			continue
		}
		// Field: Name="value".
		if _, err = p.next(); err != nil { // consume "="
			return err
		}
		val, err := p.next()
		if err != nil {
			return err
		}
		if len(val) == 0 || val[0] != '"' {
			return errors.Newf("expected quoted string for field value, got %q", val)
		}
		unquoted, err := strconv.Unquote(val)
		if err != nil {
			return errors.Wrapf(err, "invalid field value %s", val)
		}
		if err := p.builder.AddField(PlanGramField{Key: name, Val: unquoted}); err != nil {
			return err
		}
	}
	// TODO(michae2): check for correct number of children.
	return p.builder.LeaveExpr()
}

// tokenizePlanGram is a bufio.SplitFunc that tokenizes a byte stream for
// parsing as a PlanGram.
func tokenizePlanGram(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Skip leading whitespace.
	i := 0
	for i < len(data) {
		r, size := utf8.DecodeRune(data[i:])
		if r == utf8.RuneError && size <= 1 {
			if !atEOF && len(data)-i < utf8.UTFMax {
				// Might need more data to complete the UTF-8 character.
				return 0, nil, nil
			}
			return 0, nil, errInvalidUTF8
		}
		if !unicode.IsSpace(r) {
			break
		}
		i += size
	}
	if i == len(data) {
		if atEOF {
			return i, nil, nil
		}
		return 0, nil, nil
	}

	switch data[i] {
	case '"':
		// Quoted string: use strconv.QuotedPrefix to find the end.
		quoted, err := strconv.QuotedPrefix(string(data[i:]))
		if err != nil {
			if atEOF {
				return 0, nil, err
			}
			// Might need more data to complete the quoted string.
			return 0, nil, nil
		}
		return i + len(quoted), data[i : i+len(quoted)], nil
	case '(', ')', ':', ';', '=', '|':
		// Single-character punctuation token.
		return i + 1, data[i : i+1], nil
	default:
		// Word token: scan until whitespace or punctuation.
		j := i
		for j < len(data) {
			r, size := utf8.DecodeRune(data[j:])
			if r == utf8.RuneError && size <= 1 {
				if !atEOF && len(data)-j < utf8.UTFMax {
					// Might need more data to complete the UTF-8 character.
					return 0, nil, nil
				}
				return 0, nil, errInvalidUTF8
			}
			if unicode.IsSpace(r) || strings.IndexByte(`"():;=|`, data[j]) >= 0 {
				break
			}
			j += size
		}
		if j == len(data) && !atEOF {
			return 0, nil, nil
		}
		return j, data[i:j], nil
	}
}

// Equals reports whether two PlanGrams point to the same grammar node. This
// uses pointer equality of the root term, which is correct because the grammar
// is parsed once into a single graph and all operations (Child, VisitAlternates)
// return PlanGrams pointing into that same graph.
func (p PlanGram) Equals(other PlanGram) bool {
	return p.root == other.root
}

// RootHash returns a hash of the root pointer, for use by the interner.
func (p PlanGram) RootHash() uint64 {
	if p.root == nil {
		return 0
	}
	return uint64(reflect.ValueOf(p.root).Pointer())
}

// Matches reports whether this PlanGram term matches the given optimizer
// expression. Any matches everything, None matches nothing. A concrete PlanGram
// expression matches if the operator matches (or is a wildcard), the child
// count matches, and all specified fields match. This should only be called
// after alternates have been expanded, so the root is always a planGramExpr or
// Any or None. It panics if called on a production.
func (p PlanGram) Matches(e opt.Expr, md *opt.Metadata) bool {
	if p.Any() {
		return true
	}
	if p.None() {
		return false
	}
	switch t := p.root.(type) {
	case *planGramExpr:
		if (t.op != opt.UnknownOp && t.op != e.Op()) || len(t.children) > e.ChildCount() {
			return false
		}
		if len(t.fields) > 0 {
			fm, ok := e.(PlanGramFieldMatchableExpr)
			if !ok {
				return false
			}
			exprFields := fm.PlanGramMatchableFields(md)
			for _, f := range t.fields {
				if !containsPlanGramField(exprFields, f) {
					return false
				}
			}
		}
		return true
	default:
		panic(errors.AssertionFailedf("called Matches(%v) on non-expression PlanGram term %v", e, t))
	}
}

func containsPlanGramField(fields []PlanGramField, f PlanGramField) bool {
	ci := caseInsensitivePlanGramField(f.Key)
	for _, ef := range fields {
		if ef.Key == f.Key &&
			(ci && strings.EqualFold(ef.Val, f.Val) || !ci && ef.Val == f.Val) {
			return true
		}
	}
	return false
}

// caseInsensitivePlanGramField returns true for field keys whose values should
// be matched case-insensitively (e.g. boolean and enum fields). Table and Index
// names remain exact-match because quoted identifiers can be case-sensitive.
func caseInsensitivePlanGramField(key string) bool {
	switch key {
	case "JoinType", "HasConstraint", "HasLimit":
		return true
	default:
		return false
	}
}

// Child returns the PlanGram for the nth child of this term. If this PlanGram
// is Any, all children are Any. If the child index is beyond the children in
// the planGramExpr, the child is implicitly Any (unspecified children are
// unconstrained). This should only be called after alternates have been
// expanded, so the root is always a planGramExpr or Any or None. It panics if
// called on a production.
func (p PlanGram) Child(nth int) PlanGram {
	if p.Any() {
		return AnyPlanGram
	}
	if p.None() {
		return NonePlanGram
	}
	switch t := p.root.(type) {
	case *planGramExpr:
		if len(t.children) > nth {
			return PlanGram{t.children[nth]}
		}
		return AnyPlanGram
	default:
		panic(errors.AssertionFailedf("called Child(%d) on non-expression PlanGram term %v", nth, t))
	}
}

// HasAlternates returns true if this PlanGram points to a production rather
// than a concrete expression.
func (p PlanGram) HasAlternates() bool {
	if p.Any() || p.None() {
		return false
	}
	_, isExpr := p.root.(*planGramExpr)
	return !isExpr
}

// VisitAlternates calls visit for each concrete alternate of this PlanGram.
// Each visited PlanGram is guaranteed to have HasAlternates() == false.
func (p PlanGram) VisitAlternates(visit func(alternate PlanGram)) {
	if p.Any() || p.None() {
		visit(p)
		return
	}
	visited := make(map[*planGramProduction]struct{})
	if !p.root.visitAlternateExprs(visited, visit) {
		// If we didn't visit any alternates, at least visit NonePlanGram.
		visit(NonePlanGram)
	}
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

// visitAlternateExprs implements the planGramTerm interface.
func (pp *planGramProduction) visitAlternateExprs(
	visited map[*planGramProduction]struct{}, visit func(PlanGram),
) (visitedOne bool) {
	if _, ok := visited[pp]; ok {
		return visitedOne
	}
	visited[pp] = struct{}{}
	for _, rule := range pp.rules {
		if rule == nil {
			visit(AnyPlanGram)
			visitedOne = true
			continue
		}
		if rule == nonePlanGramTerm {
			visit(NonePlanGram)
			visitedOne = true
			continue
		}
		if rule.visitAlternateExprs(visited, visit) {
			visitedOne = true
		}
	}
	return visitedOne
}

// visitAlternateExprs implements the planGramTerm interface.
func (pe *planGramExpr) visitAlternateExprs(
	_ map[*planGramProduction]struct{}, visit func(PlanGram),
) (visitedOne bool) {
	visit(PlanGram{pe})
	// We don't recurse here, because expression children are down a level, not
	// alternates for the current level.
	return true
}
