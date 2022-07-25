// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsearch

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keysbase"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// tsOperator is an enum that represents the different operators within a
// TSQuery.
type tsOperator int

const (
	// Parentheses can be used to control nesting of the TSQuery operators.
	// Without parentheses, | binds least tightly,
	// then &, then <->, and ! most tightly.

	invalid tsOperator = iota
	// and is the & operator, which requires both of its operands to exist in
	// the searched document.
	and
	// or is the | operator, which requires one or more of its operands to exist
	// in the searched document.
	or
	// not is the ! operator, which requires that its single operand doesn't exist
	// in the searched document.
	not
	// followedby is the <-> operator. It can also be specified with a number like
	// <1> or <2> or <3>. It requires that the left operand is followed by the right
	// operand. The <-> and <1> forms mean that they should be directly followed
	// by each other. A number indicates how many terms away the operands should be.
	followedby
	// lparen and rparen are grouping operators. They're just used in parsing and
	// don't appear in the TSQuery tree.
	lparen
	rparen
)

// precedence returns the parsing precedence of the receiver. A higher
// precedence means that the operator binds more tightly.
func (o tsOperator) precedence() int {
	switch o {
	case not:
		return 4
	case followedby:
		return 3
	case and:
		return 2
	case or:
		return 1
	}
	panic(errors.AssertionFailedf("no precedence for operator %d", o))
}

func (o tsOperator) pgwireEncoding() byte {
	switch o {
	case not:
		return 1
	case and:
		return 2
	case or:
		return 3
	case followedby:
		return 4
	}
	panic(errors.AssertionFailedf("no pgwire encoding for operator %d", o))
}

func tsOperatorFromPgwireEncoding(b byte) (tsOperator, error) {
	switch b {
	case 1:
		return not, nil
	case 2:
		return and, nil
	case 3:
		return or, nil
	case 4:
		return followedby, nil
	}
	return invalid, errors.AssertionFailedf("no operator for pgwire byte %d", b)
}

// tsNode represents a single AST node within the tree of a TSQuery.
type tsNode struct {
	// Only one of term or op will be set.
	// If term is set, this is a leaf node containing a lexeme.
	term tsTerm
	// If op is set, this is an operator node: either not, and, or, or followedby.
	op tsOperator
	// set only when op is followedby. Indicates the number n within the <n>
	// operator, which means the number of terms separating the left and the right
	// argument.
	// At most 16384.
	followedN uint16

	// l is the left child of the node if op is set, or the only child if
	// op is set to "not".
	l *tsNode
	// r is the right child of the node if op is set.
	r *tsNode
}

func (n tsNode) String() string {
	return n.infixString(0)
}

func (n tsNode) infixString(parentPrecedence int) string {
	if n.op == invalid {
		return n.term.String()
	}
	var s strings.Builder
	prec := n.op.precedence()
	needParen := prec < parentPrecedence
	if needParen {
		s.WriteString("( ")
	}
	switch n.op {
	case not:
		fmt.Fprintf(&s, "!%s", n.l.infixString(prec))
	default:
		fmt.Fprintf(&s, "%s %s %s",
			n.l.infixString(prec),
			tsTerm{operator: n.op, followedN: n.followedN},
			n.r.infixString(prec),
		)
	}
	if needParen {
		s.WriteString(" )")
	}
	return s.String()
}

// UnambiguousString returns a string representation of this tsNode that wraps
// all expressions with parentheses. It's just for testing.
func (n tsNode) UnambiguousString() string {
	switch n.op {
	case invalid:
		return n.term.lexeme
	case not:
		return fmt.Sprintf("!%s", n.l.UnambiguousString())
	}
	return fmt.Sprintf("[%s%s%s]", n.l.UnambiguousString(), tsTerm{operator: n.op, followedN: n.followedN}, n.r.UnambiguousString())
}

// TSQuery represents a tsNode AST root. A TSQuery is a tree of text search
// operators that can be run against a TSVector to produce a predicate of
// whether the query matched.
type TSQuery struct {
	root *tsNode
}

func (q TSQuery) String() string {
	if q.root == nil {
		return ""
	}
	return q.root.String()
}

// GetInvertedExpr returns the inverted expression that can be used to search
// an index.
func (q TSQuery) GetInvertedExpr() (expr inverted.Expression, err error) {
	return q.root.getInvertedExpr()
}

func (n *tsNode) getInvertedExpr() (inverted.Expression, error) {
	switch n.op {
	case invalid:
		// We're looking at a lexeme match.
		// There are 3 options:
		// 1. Normal match.
		//    In this case, we make a tight and unique span.
		// 2. Prefix match.
		//    In this case, we make a non-unique, tight span that starts with the
		//    prefix.
		// 3. Weighted match.
		//    In this case, we make the match non-tight, because we don't store the
		//    weights of the lexemes in the index, and are forced to re-check
		//    once we get the result from the inverted index.
		// Note that options 2 and 3 can both be present.
		var weight tsWeight
		if len(n.term.positions) > 0 {
			weight = n.term.positions[0].weight
		}
		key := EncodeInvertedIndexKey(nil /* inKey */, n.term.lexeme)
		var span inverted.Span

		prefixMatch := weight&weightStar != 0
		if prefixMatch {
			span = inverted.Span{
				Start: key,
				End:   EncodeInvertedIndexKey(nil /* inKey */, string(keysbase.PrefixEnd([]byte(n.term.lexeme)))),
			}
		} else {
			span = inverted.MakeSingleValSpan(key)
		}
		invertedExpr := inverted.ExprForSpan(span, true /* tight */)
		if !prefixMatch {
			// If we don't have a prefix match we also can set unique=true.
			invertedExpr.Unique = true
		}

		if weight != 0 && weight != weightStar {
			// Some weights are set.
			invertedExpr.SetNotTight()
		}
		return invertedExpr, nil
	case followedby:
		fallthrough
	case and:
		l, lErr := n.l.getInvertedExpr()
		r, rErr := n.r.getInvertedExpr()
		if lErr != nil && rErr != nil {
			// We need a positive match on at least one side.
			return nil, lErr
		} else if lErr != nil {
			// An error on one side means we have to re-check that side's condition
			// later.
			r.SetNotTight()
			//nolint:returnerrcheck
			return r, nil
		} else if rErr != nil {
			// Ditto above.
			l.SetNotTight()
			//nolint:returnerrcheck
			return l, nil
		}
		expr := inverted.And(l, r)
		if n.op == followedby {
			// If we have a followedby match, we have to re-check the results of the
			// match after we get them from the inverted index - just because both
			// terms are present doesn't mean they're properly next to each other,
			// and the index doesn't store position information at all.
			expr.SetNotTight()
		}
		// We never have a unique match when combining terms, since a single primary
		// key could be represented in both terms.
		expr.(*inverted.SpanExpression).Unique = false
		return expr, nil
	case or:
		l, lErr := n.l.getInvertedExpr()
		r, rErr := n.r.getInvertedExpr()
		if lErr != nil {
			// We need a positive match on both sides, so we return an error here.
			// For example, searching for a | !b would require a full scan, since some
			// documents could match that contain neither a nor b.
			return nil, lErr
		} else if rErr != nil {
			return nil, rErr
		}
		return inverted.Or(l, r), nil
	case not:
		// A not would require more advanced machinery than we have, so for now
		// we'll just assume we can't perform an inverted expression search on a
		// not. Note that a nested not would make it possible, but we are ignoring
		// this case for now as it seems marginal.
		return nil, errors.New("unable to create inverted expr for not")
	}
	return nil, errors.AssertionFailedf("invalid operator %d", n.op)
}

func lexTSQuery(input string) (TSVector, error) {
	parser := tsVectorLexer{
		input:   input,
		state:   expectingTerm,
		tsQuery: true,
	}

	return parser.lex()
}

// ParseTSQuery produces a TSQuery from an input string.
func ParseTSQuery(input string) (TSQuery, error) {
	terms, err := lexTSQuery(input)
	if err != nil {
		return TSQuery{}, err
	}

	// Now create the operator tree.
	queryParser := tsQueryParser{terms: terms, input: input}
	return queryParser.parse()
}

// tsQueryParser is a parser that operates on a set of lexed tokens, represented
// as the tsTerms in a TSVector.
type tsQueryParser struct {
	input string
	terms TSVector
}

func (p tsQueryParser) peek() (*tsTerm, bool) {
	if len(p.terms) == 0 {
		return nil, false
	}
	return &p.terms[0], true
}

func (p *tsQueryParser) nextTerm() (*tsTerm, bool) {
	if len(p.terms) == 0 {
		return nil, false
	}
	ret := &p.terms[0]
	p.terms = p.terms[1:]
	return ret, true
}

func (p *tsQueryParser) parse() (TSQuery, error) {
	expr, err := p.parseTSExpr(0)
	if err != nil {
		return TSQuery{}, err
	}
	if len(p.terms) > 0 {
		_, err := p.syntaxError()
		return TSQuery{}, err
	}
	return TSQuery{root: expr}, nil
}

// parseTSExpr is a "Pratt parser" which constructs a query tree out of the
// lexed tsTerms, respecting the precedence of the tsOperators.
// See this nice article about Pratt parsing, which this parser was adapted from:
// https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html
func (p *tsQueryParser) parseTSExpr(minBindingPower int) (*tsNode, error) {
	t, ok := p.nextTerm()
	if !ok {
		return nil, pgerror.Newf(pgcode.Syntax, "text-search query doesn't contain lexemes: %s", p.input)
	}

	// First section: grab either atoms, nots, or parens.
	var lExpr *tsNode
	switch t.operator {
	case invalid:
		lExpr = &tsNode{term: *t}
	case lparen:
		expr, err := p.parseTSExpr(0)
		if err != nil {
			return nil, err
		}
		t, ok := p.nextTerm()
		if !ok || t.operator != rparen {
			return p.syntaxError()
		}
		lExpr = expr
	case not:
		t, ok := p.nextTerm()
		if !ok {
			return p.syntaxError()
		}
		switch t.operator {
		case invalid:
			lExpr = &tsNode{op: not, l: &tsNode{term: *t}}
		case lparen:
			expr, err := p.parseTSExpr(0)
			if err != nil {
				return nil, err
			}
			lExpr = &tsNode{op: not, l: expr}
			t, ok := p.nextTerm()
			if !ok || t.operator != rparen {
				return p.syntaxError()
			}
		default:
			return p.syntaxError()
		}
	default:
		return p.syntaxError()
	}

	// Now we do our "Pratt parser loop".
	for {
		next, ok := p.peek()
		if !ok {
			return lExpr, nil
		}
		switch next.operator {
		case and, or, followedby:
		default:
			return lExpr, nil
		}
		precedence := next.operator.precedence()
		if precedence < minBindingPower {
			break
		}
		p.nextTerm()
		rExpr, err := p.parseTSExpr(precedence)
		if err != nil {
			return nil, err
		}
		lExpr = &tsNode{op: next.operator, followedN: next.followedN, l: lExpr, r: rExpr}
	}
	return lExpr, nil
}

func (p *tsQueryParser) syntaxError() (*tsNode, error) {
	return nil, pgerror.Newf(pgcode.Syntax, "syntax error in TSQuery: %s", p.input)
}
