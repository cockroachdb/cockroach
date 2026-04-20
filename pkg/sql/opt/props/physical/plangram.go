// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
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
// nonterminal. The graph could contain cycles (for example to match a plan with
// a FK cascade cycle or a recursive UDF).
//
// PlanGrams are equivalent to top-down non-deterministic finite tree
// automatons. We could compile the PlanGram into a state-table-based NFTA, but
// for now we simply walk and interpret it during planning.
//
// For some background see https://en.wikipedia.org/wiki/Regular_tree_grammar
// and https://en.wikipedia.org/wiki/Tree_automaton.
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
	// are encountered, they are not visited.
	visitProductions(visited map[*planGramProduction]struct{}, visit func(*planGramProduction))
}

// planGramProduction represents a left-hand-side nonterminal in the grammar,
// which has an identifying name and one or more production rules. The
// nonterminal matches the current optimizer sub-tree if any of its production
// rules match the current sub-tree.
type planGramProduction struct {
	// name is the nonterminal symbol in the grammar, which must be unique.
	// "any" and "none" are reserved; "root" is the required entry-point
	// production and cannot be referenced from other productions. The name
	// must not contain any of the characters "'\,():;=| or whitespace, and
	// must not start with a digit.
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

var errInvalidUTF8 = errors.New("invalid UTF-8")

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
	if p.Any() {
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
		default:
			if buildutil.CrdbTestBuild {
				panic(errors.AssertionFailedf("unexpected planGramTerm type %T", t))
			}
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

// ParsePlanGram parses a PlanGram grammar from the given io.Reader, or returns
// an error if it cannot.
func ParsePlanGram(r io.Reader) (PlanGram, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(tokenizePlanGram)

	p := planGramParser{
		scanner:     scanner,
		productions: make(map[string]*planGramProduction),
	}

	// Parse all productions, including "root".
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

	// Validate that root exists and has exactly one term (no alternates).
	root, ok := p.productions["root"]
	if !ok {
		return PlanGram{}, errors.New("missing \"root\" production")
	}
	if len(root.rules) != 1 {
		return PlanGram{}, errors.New("root must have exactly one term, not alternates")
	}

	// Validate all forward references were resolved (i.e. every production has
	// at least one rule).
	for name, pp := range p.productions {
		if len(pp.rules) == 0 {
			return PlanGram{}, errors.Newf("undefined nonterminal %q", name)
		}
	}

	return PlanGram{root: root.rules[0]}, nil
}

// planGramParser is a recursive-descent parser for plangram grammars with
// 1-token lookahead.
type planGramParser struct {
	scanner     *bufio.Scanner
	tok         string
	hasTok      bool
	productions map[string]*planGramProduction
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

// getOrCreateProduction returns the production for the given name, creating a
// forward-reference stub if one does not yet exist. The name must not be empty,
// punctuation, or a quoted string.
func (p *planGramParser) getOrCreateProduction(name string) (*planGramProduction, error) {
	if len(name) == 0 {
		return nil, errors.New("name cannot be empty")
	}
	if name[0] >= '0' && name[0] <= '9' {
		return nil, errors.Newf("name %q must not start with a digit", name)
	}
	if strings.ContainsAny(name, `"'\,():;=|`) {
		return nil, errors.Newf("name %q contains invalid character", name)
	}
	pp, ok := p.productions[name]
	if !ok {
		pp = &planGramProduction{name: name}
		p.productions[name] = pp
	}
	return pp, nil
}

// resolveNonterminal maps a nonterminal name to its term. "any" and "none" are
// resolved to their special terms; "root" is an error (root cannot be
// referenced from other productions).
func (p *planGramParser) resolveNonterminal(name string) (planGramTerm, error) {
	switch name {
	case "any":
		return anyPlanGramTerm, nil
	case "none":
		return nonePlanGramTerm, nil
	case "root":
		return nil, errors.New("\"root\" cannot be used as a nonterminal reference")
	default:
		pp, err := p.getOrCreateProduction(name)
		if err != nil {
			return nil, err
		}
		return pp, nil
	}
}

// parseProduction parses: NAME ":" term { "|" term } ";".
func (p *planGramParser) parseProduction() error {
	name, err := p.next()
	if err != nil {
		return err
	}
	if name == "any" || name == "none" {
		return errors.Newf("%q cannot be used as a production name", name)
	}
	pp, err := p.getOrCreateProduction(name)
	if err != nil {
		return err
	}
	if len(pp.rules) > 0 {
		return errors.Newf("duplicate production %q", name)
	}
	if err := p.expect(":"); err != nil {
		return err
	}
	for {
		term, err := p.parseTerm()
		if err != nil {
			return err
		}
		pp.rules = append(pp.rules, term)
		tok, ok := p.peek()
		if !ok || tok != "|" {
			break
		}
		if _, err = p.next(); err != nil { // consume "|"
			return err
		}
	}
	return p.expect(";")
}

// parseTerm parses a single term: either an expression (starting with "(") or
// a nonterminal name.
func (p *planGramParser) parseTerm() (planGramTerm, error) {
	tok, ok := p.peek()
	if !ok {
		if err := p.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("unexpected end of input: expected term")
	}
	if tok == "(" {
		return p.parseExpr()
	}
	// Must be a nonterminal name.
	name, err := p.next()
	if err != nil {
		return nil, err
	}
	return p.resolveNonterminal(name)
}

// parseExpr parses: "(" OP_NAME { field } { child } ")".
func (p *planGramParser) parseExpr() (planGramTerm, error) {
	if err := p.expect("("); err != nil {
		return nil, err
	}
	opName, err := p.next()
	if err != nil {
		return nil, err
	}
	op, ok := opt.OperatorByCamelCase(opName)
	if !ok {
		return nil, errors.Newf("unknown operator %q", opName)
	}
	expr := &planGramExpr{op: op}

	for {
		tok, ok := p.peek()
		if !ok {
			if err := p.scanner.Err(); err != nil {
				return nil, err
			}
			return nil, errors.New("unexpected end of input: expected \")\"")
		}
		if tok == ")" {
			if _, err := p.next(); err != nil { // consume ")"
				return nil, err
			}
			break
		}
		if tok == "(" {
			// Inline child expression.
			child, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			expr.children = append(expr.children, child)
			continue
		}
		if len(tok) == 1 && strings.IndexByte(`":;=|`, tok[0]) >= 0 {
			return nil, errors.Newf("unexpected %q in expression", tok)
		}
		// Consume the name. Then peek to decide if it's a field (followed by "=")
		// or a nonterminal child.
		name, err := p.next()
		if err != nil {
			return nil, err
		}
		if next, ok := p.peek(); !ok || next != "=" {
			// Nonterminal child.
			term, err := p.resolveNonterminal(name)
			if err != nil {
				return nil, err
			}
			expr.children = append(expr.children, term)
			continue
		}
		// Field: Name="value".
		if len(expr.children) > 0 {
			return nil, errors.New("fields must come before children in expression")
		}
		if _, err = p.next(); err != nil { // consume "="
			return nil, err
		}
		val, err := p.next()
		if err != nil {
			return nil, err
		}
		if len(val) == 0 || val[0] != '"' {
			return nil, errors.Newf("expected quoted string for field value, got %q", val)
		}
		unquoted, err := strconv.Unquote(val)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid field value %s", val)
		}
		expr.fields = append(expr.fields, planGramExprField{key: name, val: unquoted})
	}
	return expr, nil
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
