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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// tsNode represents a single AST node within the tree of a TSQuery.
type tsNode struct {
	// Only one of term, op, or paren will be set.
	// If term is set, this is a leaf node containing a lexeme.
	term tsTerm
	// If op is set, this is an operator node: either not, and, or, or followedby.
	op tsoperator
	// set only when op is followedby. Indicates the number n within the <n>
	// operator, which means the number of terms separating the left and the right
	// argument.
	followedN int
	// paren is set if this is a node representing an expression surrounded by
	// parentheses.
	paren *tsNode

	// l is the left child of the node if op is set, or the only child if
	// op is set to "not".
	l *tsNode
	// r is the right child of the node if op is set.
	r *tsNode
}

func (n tsNode) String() string {
	if n.paren != nil {
		return fmt.Sprintf("( %s )", n.paren.String())
	}
	switch n.op {
	case invalid:
		return n.term.String()
	case not:
		return fmt.Sprintf("!%s", n.l.String())
	}
	return fmt.Sprintf("%s %s %s", n.l.String(), tsTerm{operator: n.op, followedN: n.followedN}, n.r.String())
}

// UnambiguousString returns a string representation of this tsNode that wraps
// all expressions with parentheses. It's just for testing.
func (n tsNode) UnambiguousString() string {
	if n.paren != nil {
		return fmt.Sprintf("(%s)", n.paren.UnambiguousString())
	}
	switch n.op {
	case invalid:
		return n.term.lexeme
	case not:
		return fmt.Sprintf("!%s", n.l.UnambiguousString())
	}
	return fmt.Sprintf("[%s%s%s]", n.l.UnambiguousString(), tsTerm{operator: n.op, followedN: n.followedN}, n.r.UnambiguousString())
}

// TSQuery represents a tsquery AST root. A tsquery is a tree of text search
// operators that can be run against a tsvector to produce a predicate of
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

func lexTSQuery(input string) (TSVector, error) {
	parser := tsVectorLexer{
		input:   input,
		state:   expectingTerm,
		tsquery: true,
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
		lExpr = &tsNode{paren: expr}
	case not:
		// Buggy: not needs to associate more tightly!
		// Instead of running parseTSExpr here, we need to run:
		// parseAtom
		// parseParenExpr
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
			lExpr = &tsNode{op: not, l: &tsNode{paren: expr}}
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

func (p *tsQueryParser) parseTSOperator() (*tsTerm, error) {
	t, ok := p.nextTerm()
	if !ok || t.operator == invalid {
		return nil, pgerror.Newf(pgcode.Syntax, "syntax error in TSQuery: %s", p.input)
	}
	return t, nil
}

func (p *tsQueryParser) syntaxError() (*tsNode, error) {
	return nil, pgerror.Newf(pgcode.Syntax, "syntax error in TSQuery: %s", p.input)
}

// ToTSQuery implements the to_tsquery builtin, which lexes an input, performs
// stopwording and normalization on the tokens, and returns a parsed query.
func ToTSQuery(config string, input string) (TSQuery, error) {
	return toTSQuery(config, invalid, input)
}

// PlainToTSQuery implements the plainto_tsquery builtin, which lexes an input,
// performs stopwording and normalization on the tokens, and returns a parsed
// query, interposing the & operator between each token.
func PlainToTSQuery(config string, input string) (TSQuery, error) {
	return toTSQuery(config, and, input)
}

// PhraseToTSQuery implements the phraseto_tsquery builtin, which lexes an input,
// performs stopwording and normalization on the tokens, and returns a parsed
// query, interposing the <-> operator between each token.
func PhraseToTSQuery(config string, input string) (TSQuery, error) {
	return toTSQuery(config, followedby, input)
}

// toTSQuery implements the to_tsquery builtin, which lexes an input,
// performs stopwording and normalization on the tokens, and returns a parsed
// query. If the interpose operator is not invalid, it's interposed between each
// token in the input.
func toTSQuery(config string, interpose tsoperator, input string) (TSQuery, error) {
	switch config {
	case "simple":
	default:
		return TSQuery{}, pgerror.Newf(pgcode.UndefinedObject, "text search configuration %q does not exist")
	}
	vector, err := lexTSQuery(input)
	if err != nil {
		return TSQuery{}, err
	}
	tokens := make(TSVector, 0, len(vector))
	for i := range vector {
		tok := vector[i]
		if interpose != invalid {
			// Remove all operator tokens.
			if tok.operator != invalid {
				continue
			}
			if i > 0 {
				term := tsTerm{operator: interpose}
				if interpose == followedby {
					term.followedN = 1
				}
				tokens = append(tokens, term)
			}
		}
		// When we support more than just the simple configuration, we'll also
		// want to remove stopwords, which will affect the interposing, but we can
		// worry about that later.
		// Additionally, if we're doing phraseto_tsquery, if we remove a stopword,
		// we need to make sure to increase the "followedN" of the followedby
		// operator. For example, phraseto_tsquery('hello a deer') will return
		// 'hello <2> deer', since the a stopword would be removed.
		tok.lexeme = strings.ToLower(tok.lexeme)
		tokens = append(tokens, tok)
	}

	// Now create the operator tree.
	queryParser := tsQueryParser{terms: tokens, input: input}
	return queryParser.parse()
}
