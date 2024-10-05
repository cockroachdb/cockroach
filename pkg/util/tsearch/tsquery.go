// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

func (o tsOperator) String() string {
	switch o {
	case not:
		return "!"
	case and:
		return "&"
	case or:
		return "|"
	case followedby:
		return "<->"
	case lparen:
		return "("
	case rparen:
		return ")"
	}
	panic(errors.AssertionFailedf("no string for operator %d", o))
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
	var buf strings.Builder
	n.writeInfixString(&buf, 0)
	return buf.String()
}

func (n tsNode) writeInfixString(buf *strings.Builder, parentPrecedence int) {
	if n.op == invalid {
		n.term.writeString(buf)
		return
	}
	prec := n.op.precedence()
	needParen := prec < parentPrecedence
	if needParen {
		buf.WriteString("( ")
	}
	switch n.op {
	case not:
		buf.WriteString("!")
		n.l.writeInfixString(buf, prec)
	default:
		n.l.writeInfixString(buf, prec)
		buf.WriteString(" ")
		tsTerm{operator: n.op, followedN: n.followedN}.writeString(buf)
		buf.WriteString(" ")
		n.r.writeInfixString(buf, prec)
	}
	if needParen {
		buf.WriteString(" )")
	}
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
	var buf strings.Builder
	tsTerm{operator: n.op, followedN: n.followedN}.writeString(&buf)
	return fmt.Sprintf("[%s%s%s]", n.l.UnambiguousString(), buf.String(), n.r.UnambiguousString())
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
		nextTerm, ok := p.nextTerm()
		if !ok || nextTerm.operator != rparen {
			return p.syntaxError()
		}
		lExpr = expr
	case not:
		expr, err := p.parseTSExpr(t.operator.precedence())
		if err != nil {
			return nil, err
		}
		lExpr = &tsNode{op: not, l: expr}
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
func toTSQuery(config string, interpose tsOperator, input string) (TSQuery, error) {
	vector, err := lexTSQuery(input)
	if err != nil {
		return TSQuery{}, err
	}
	tokens := make(TSVector, 0, len(vector))
	foundStopwords := false
	for i := range vector {
		tok := vector[i]

		foundOperator := tok.operator != invalid
		var lexemeTokens []string

		if !foundOperator {
			// Try parsing the token.
			lexemeTokens = TSParse(tok.lexeme)
		}

		// If we found an operator or were able to parse lexemes from the token,
		// add the interpose operator if there is one.
		if interpose != invalid && i > 0 && (foundOperator || len(lexemeTokens) > 0) {
			term := tsTerm{operator: interpose}
			if interpose == followedby {
				term.followedN = 1
			}
			tokens = append(tokens, term)
		}

		if foundOperator {
			tokens = append(tokens, tok)
			continue
		}

		if len(lexemeTokens) == 0 {
			// We ate some whitespace or whitespace-like text with no tokens.
			continue
		}

		// When we support more than just the simple configuration, we'll also
		// want to remove stopwords, which will affect the interposing, but we can
		// worry about that later.
		// Additionally, if we're doing phraseto_tsquery, if we remove a stopword,
		// we need to make sure to increase the "followedN" of the followedby
		// operator. For example, phraseto_tsquery('hello a deer') will return
		// 'hello <2> deer', since the a stopword would be removed.

		tokInterpose := interpose
		if tokInterpose == invalid {
			tokInterpose = followedby
		}
		for j := range lexemeTokens {
			if j > 0 {
				// We found more than one lexeme in our token, so we need to add all of them
				// to the query, connected by our interpose operator.
				// If we aren't running with an interpose, like in to_tsquery, Postgres
				// uses the <-> operator to connect multiple lexemes from a single token.
				term := tsTerm{operator: tokInterpose}
				if tokInterpose == followedby {
					term.followedN = 1
				}
				tokens = append(tokens, term)
			}
			lexeme, stopWord, err := TSLexize(config, lexemeTokens[j])
			if err != nil {
				return TSQuery{}, err
			}
			if stopWord {
				foundStopwords = true
			}
			tokens = append(tokens, tsTerm{lexeme: lexeme, positions: tok.positions})
		}
	}

	// Now create the operator tree.
	queryParser := tsQueryParser{terms: tokens, input: input}
	query, err := queryParser.parse()
	if err != nil {
		return query, err
	}

	if foundStopwords {
		query = cleanupStopwords(query)
		if query.root == nil {
			return query, pgerror.Newf(pgcode.Syntax, "text-search query doesn't contain lexemes: %s", input)
		}
	}
	return query, err
}

func cleanupStopwords(query TSQuery) TSQuery {
	query.root, _, _ = cleanupStopword(query.root)
	if query.root == nil {
		return TSQuery{}
	}
	return query
}

// cleanupStopword cleans up a query tree by removing stop words and adjusting
// the width of the followedby operators to account for removed stop words.
// It returns the new root of the tree, and the amount to add to a followedBy
// distance to the left and right of the input node.
//
// This function parallels the clean_stopword_intree function in Postgres.
// What follows is a reproduction of the explanation of this function in
// Postgres.

// When we remove a phrase operator due to removing one or both of its
// arguments, we might need to adjust the distance of a parent phrase
// operator.  For example, 'a' is a stopword, so:
//
//	(b <-> a) <-> c  should become	b <2> c
//	b <-> (a <-> c)  should become	b <2> c
//	(b <-> (a <-> a)) <-> c  should become	b <3> c
//	b <-> ((a <-> a) <-> c)  should become	b <3> c
//
// To handle that, we define two output parameters:
//
//	ladd: amount to add to a phrase distance to the left of this node
//	radd: amount to add to a phrase distance to the right of this node
//
// We need two outputs because we could need to bubble up adjustments to two
// different parent phrase operators. Consider
//
//	w <-> (((a <-> x) <2> (y <3> a)) <-> z)
//
// After we've removed the two a's and are considering the <2> node (which is
// now just x <2> y), we have an ladd distance of 1 that needs to propagate
// up to the topmost (leftmost) <->, and an radd distance of 3 that needs to
// propagate to the rightmost <->, so that we'll end up with
//
//	w <2> ((x <2> y) <4> z)
//
// Near the bottom of the tree, we may have subtrees consisting only of
// stopwords.  The distances of any phrase operators within such a subtree are
// summed and propagated to both ladd and radd, since we don't know which side
// of the lowest surviving phrase operator we are in.  The rule is that any
// subtree that degenerates to NULL must return equal values of ladd and radd,
// and the parent node dealing with it should incorporate only one of those.
//
// Currently, we only implement this adjustment for adjacent phrase operators.
// Thus for example 'x <-> ((a <-> y) | z)' will become 'x <-> (y | z)', which
// isn't ideal, but there is no way to represent the really desired semantics
// without some redesign of the tsquery structure.  Certainly it would not be
// any better to convert that to 'x <2> (y | z)'.  Since this is such a weird
// corner case, let it go for now.  But we can fix it in cases where the
// intervening non-phrase operator also gets removed, for example
// '((x <-> a) | a) <-> y' will become 'x <2> y'.
func cleanupStopword(node *tsNode) (ret *tsNode, lAdd int, rAdd int) {
	if node.op == invalid {
		if node.term.lexeme == "" {
			// Found a stop word.
			return nil, 0, 0
		}
		return node, 0, 0
	}
	if node.op == not {
		// Not doesn't change the pattern width, so just report child distances.
		node.l, lAdd, rAdd = cleanupStopword(node.l)
		if node.l == nil {
			return nil, lAdd, rAdd
		}
		return node, lAdd, rAdd
	}

	var llAdd, lrAdd, rlAdd, rrAdd int
	node.l, llAdd, lrAdd = cleanupStopword(node.l)
	node.r, rlAdd, rrAdd = cleanupStopword(node.r)
	isPhrase := node.op == followedby
	followedN := node.followedN
	if node.l == nil && node.r == nil {
		// Removing an entire node. Propagate its distance into both lAdd and rAdd;
		// it is the responsibility of the parent to count it only once.
		if isPhrase {
			// If we're a followed by, sum up the children lengths and propagate.
			// Distances coming from children are summed and propagated up to the
			// parent (we assume llAdd == lrAdd and rlAdd == rrAdd, else rule was
			// broken at a lower level).
			lAdd = llAdd + int(followedN) + rlAdd
			rAdd = lAdd
		} else {
			// If not, we take the max. This corresponds to the logic in evalWithinFollowedBy.
			lAdd = llAdd
			if rlAdd > lAdd {
				lAdd = rlAdd
			}
			rAdd = lAdd
		}
		return nil, lAdd, rAdd
	} else if node.l == nil {
		// Remove this operator and the left node.
		if isPhrase {
			// Operator's own distance must propagate to the left.
			return node.r, llAdd + int(followedN) + rlAdd, rrAdd
		} else {
			// At non-followedby op, just forget the left node entirely.
			return node.r, rlAdd, rrAdd
		}
	} else if node.r == nil {
		// Remove this operator and the right node.
		if isPhrase {
			// Operator's own distance must propagate to the right.
			return node.l, llAdd, lrAdd + int(followedN) + rrAdd
		} else {
			// At non-followedby op, just forget the right node entirely.
			return node.l, llAdd, lrAdd
		}
	} else if isPhrase {
		// Add the adjusted values to this operator.
		node.followedN += uint16(lrAdd + rlAdd)
		// Continue to propagate unaccounted-for adjustments.
		return node, llAdd, rrAdd
	}
	// Otherwise we found a non-phrase operator; keep it as-is.
	return node, 0, 0
}
