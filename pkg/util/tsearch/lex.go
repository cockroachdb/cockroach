// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"sort"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

type tsVectorParseState int

const (
	// Waiting for term (whitespace, ', or any other char)
	expectingTerm tsVectorParseState = iota
	// Inside of a normal term (single quotes are processed as normal chars)
	insideNormalTerm
	// Inside of a ' term
	insideQuoteTerm
	// Finished with ' term (waiting for : or space)
	finishedQuoteTerm
	// Found a colon (or comma) and expecting a position
	expectingPosList
	// Finished parsing a position, expecting a comma or whitespace
	expectingPosDelimiter
)

// tsVectorLexer is a lexing state machine for the TSVector and TSQuery input
// formats. See the comment above lex() for more details.
type tsVectorLexer struct {
	input   string
	lastLen int
	pos     int
	state   tsVectorParseState

	// If true, we're in "TSQuery lexing mode"
	tsQuery bool
}

func (p *tsVectorLexer) back() {
	p.pos -= p.lastLen
	p.lastLen = 0
}

func (p *tsVectorLexer) advance() rune {
	r, n := utf8.DecodeRuneInString(p.input[p.pos:])
	p.pos += n
	p.lastLen = n
	return r
}

const (
	// The maximum number of bytes in a TSVector.
	maxTSVectorLen = (1 << 20) - 1
	// The maximum number of positions in a TSVector position list.
	maxTSVectorPositions = 256
	// The maximum number within a <> followed-by declaration.
	maxTSVectorFollowedBy = 1 << 14
	// The maximum size of a TSVector lexeme.
	maxTSVectorLexemeLen = (1 << 14) - 1
	// The maximum position within a TSVector position list.
	maxTSVectorPosition = (1 << 14) - 1
)

// lex lexes the input in the receiver according to the TSVector "grammar", or
// according the TSQuery "grammar" if tsQuery is set to true.
//
// A simple TSVector input could look like this:
//
// foo bar:3 baz:3A 'blah :blah'
//
// A TSVector is a list of terms.
//
// Each term is a word and an optional "position list".
//
// A word may be single-quote wrapped, in which case the next term may begin
// without any whitespace in between (if there is no position list on the word).
// In a single-quote wrapped word, the word must terminate with a single quote.
// All other characters are treated as literals. Backlashes can be used to
// escape single quotes, and are otherwise skipped, allowing the following
// character to be included as a literal (such as the backslash character itself).
//
// If a word is not single-quote wrapped, the next term will begin if there is
// whitespace after the word. Whitespace and colons may be entered by escaping
// them with backslashes. All other uses of backslashes are skipped, allowing
// the following character to be included as a literal.
//
// A word is delimited from its position list with a colon.
//
// A position list is made up of a comma-delimited list of numbers, each
// of which may have an optional "strength" which is a letter from A-D.
//
// In TSQuery mode, there are a few differences:
//   - Terms must be separated with tsOperators (!, <->, |, &), not just spaces.
//   - Terms may be surrounded by the ( ) grouping tokens.
//   - Terms cannot include multiple positions.
//   - Terms can include more than one "strength", as well as the * prefix search
//     operator. For example, foo:3AC*
//
// See examples in tsvector_test.go and tsquery_test.go, and see the
// documentation in tsvector.go for more information and a link to the Postgres
// documentation that is the spec for all of this behavior.
func (p tsVectorLexer) lex() (TSVector, error) {
	// termBuf will be reused as a temporary buffer to assemble each term before
	// copying into the vector.
	termBuf := make([]rune, 0, 32)
	ret := TSVector{}

	if len(p.input) >= maxTSVectorLen {
		typ := "tsvector"
		if p.tsQuery {
			typ = "tsquery"
		}
		return nil, pgerror.Newf(pgcode.ProgramLimitExceeded,
			"string is too long for %s (%d bytes, max %d bytes)",
			typ, len(p.input), maxTSVectorLen)
	}

	for p.pos < len(p.input) {
		r := p.advance()
		switch p.state {
		case expectingTerm:
			// Expect either a single quote, a whitespace, or anything else.
			if r == '\'' {
				p.state = insideQuoteTerm
				continue
			}
			if unicode.IsSpace(r) {
				continue
			}

			if p.tsQuery {
				// Check for &, |, !, and <-> (or <number>)
				switch r {
				case '&':
					ret = append(ret, tsTerm{operator: and})
					continue
				case '|':
					ret = append(ret, tsTerm{operator: or})
					continue
				case '!':
					ret = append(ret, tsTerm{operator: not})
					continue
				case '(':
					ret = append(ret, tsTerm{operator: lparen})
					continue
				case ')':
					ret = append(ret, tsTerm{operator: rparen})
					continue
				case '<':
					r = p.advance()
					n := 1
					if r == '-' {
						r = p.advance()
					} else {
						for unicode.IsNumber(r) {
							termBuf = append(termBuf, r)
							r = p.advance()
						}
						var err error
						n, err = strconv.Atoi(string(termBuf))
						if n > maxTSVectorFollowedBy || n < 0 {
							return nil, pgerror.Newf(pgcode.InvalidParameterValue,
								"distance in phrase operator must be an integer value between zero and %d inclusive", maxTSVectorFollowedBy)
						}
						termBuf = termBuf[:0]
						if err != nil {
							return p.syntaxError()
						}
					}
					if r != '>' {
						return p.syntaxError()
					}
					ret = append(ret, tsTerm{operator: followedby, followedN: uint16(n)})
					continue
				}
			}

			p.state = insideNormalTerm
			// Need to consume the rune we just found again.
			p.back()
			continue

		case insideQuoteTerm:
			// If escaped, eat character and continue.
			switch r {
			case '\\':
				r = p.advance()
				termBuf = append(termBuf, r)
				continue
			case '\'':
				term, err := newLexemeTerm(string(termBuf))
				if err != nil {
					return nil, err
				}
				ret = append(ret, term)
				termBuf = termBuf[:0]
				p.state = finishedQuoteTerm
				continue
			}
			termBuf = append(termBuf, r)
		case finishedQuoteTerm:
			if unicode.IsSpace(r) {
				p.state = expectingTerm
			} else if r == ':' {
				lastTerm := &ret[len(ret)-1]
				lastTerm.positions = append(lastTerm.positions, tsPosition{})
				p.state = expectingPosList
			} else {
				p.state = expectingTerm
				p.back()
			}
		case insideNormalTerm:
			// If escaped, eat character and continue.
			if r == '\\' {
				r = p.advance()
				termBuf = append(termBuf, r)
				continue
			}

			if p.tsQuery {
				switch r {
				case '&', '!', '|', '<', '(', ')':
					// These are all "operators" in the TSQuery language. End the current
					// term and start a new one.
					term, err := newLexemeTerm(string(termBuf))
					if err != nil {
						return nil, err
					}
					ret = append(ret, term)
					termBuf = termBuf[:0]
					p.state = expectingTerm
					p.back()
					continue
				}
			}

			// Colon that comes first is an ordinary character.
			space := unicode.IsSpace(r)
			if space || r == ':' && len(termBuf) > 0 {
				// Found a terminator.
				// Copy the termBuf into the vector, resize the termBuf, continue on.
				term, err := newLexemeTerm(string(termBuf))
				if err != nil {
					return nil, err
				}
				if r == ':' {
					term.positions = append(term.positions, tsPosition{})
				}
				ret = append(ret, term)
				termBuf = termBuf[:0]
				if space {
					p.state = expectingTerm
				} else {
					p.state = expectingPosList
				}
				continue
			}
			if p.tsQuery && r == ':' {
				return p.syntaxError()
			}
			termBuf = append(termBuf, r)
		case expectingPosList:
			var pos int
			if !p.tsQuery {
				// If we have nothing in our termBuf, we need to see at least one number.
				if unicode.IsNumber(r) {
					termBuf = append(termBuf, r)
					continue
				}
				if len(termBuf) == 0 {
					return p.syntaxError()
				}
				var err error
				pos, err = strconv.Atoi(string(termBuf))
				if err != nil {
					return p.syntaxError()
				}
				if pos == 0 {
					return ret, pgerror.Newf(pgcode.Syntax, "wrong position info in TSVector", p.input)
				} else if pos > maxTSVectorPosition {
					// Postgres silently truncates positions larger than 16383 to 16383.
					pos = maxTSVectorPosition
				}
				termBuf = termBuf[:0]
			}
			lastTerm := &ret[len(ret)-1]
			lastTermPos := len(lastTerm.positions) - 1
			lastTerm.positions[lastTermPos].position = uint16(pos)
			if unicode.IsSpace(r) {
				// Done with our term. Advance to next term!
				p.state = expectingTerm
				continue
			}
			switch r {
			case ',':
				if p.tsQuery {
					// Not valid! No , allowed in position lists in tsqueries.
					return ret, pgerror.Newf(pgcode.Syntax, "syntax error in TSVector: %s", p.input)
				}
				lastTerm.positions = append(lastTerm.positions, tsPosition{})
				// Expecting another number next.
				continue
			case '*':
				if p.tsQuery {
					lastTerm.positions[lastTermPos].weight |= weightStar
				} else {
					p.state = expectingPosDelimiter
					lastTerm.positions[lastTermPos].weight |= weightA
				}
			case 'a', 'A':
				if !p.tsQuery {
					p.state = expectingPosDelimiter
				}
				lastTerm.positions[lastTermPos].weight |= weightA
			case 'b', 'B':
				if !p.tsQuery {
					p.state = expectingPosDelimiter
				}
				lastTerm.positions[lastTermPos].weight |= weightB
			case 'c', 'C':
				if !p.tsQuery {
					p.state = expectingPosDelimiter
				}
				lastTerm.positions[lastTermPos].weight |= weightC
			case 'd', 'D':
				// Weight D is handled differently in TSQuery parsing than TSVector. In
				// TSVector parsing, the default is already D - so we don't record any
				// weight at all. This matches Postgres behavior - a default D weight is
				// not printed or stored. In TSQuery, we have to record it explicitly.
				if p.tsQuery {
					lastTerm.positions[lastTermPos].weight |= weightD
				} else {
					p.state = expectingPosDelimiter
				}
			default:
				return p.syntaxError()
			}
		case expectingPosDelimiter:
			if r == ',' {
				p.state = expectingPosList
				lastTerm := &ret[len(ret)-1]
				lastTerm.positions = append(lastTerm.positions, tsPosition{})
			} else if unicode.IsSpace(r) {
				p.state = expectingTerm
			} else {
				return p.syntaxError()
			}
		default:
			panic("invalid TSVector lex state")
		}
	}
	// Reached the end of the string.
	switch p.state {
	case insideQuoteTerm:
		// Unfinished quote term.
		return p.syntaxError()
	case insideNormalTerm:
		// Finish normal term.
		term, err := newLexemeTerm(string(termBuf))
		if err != nil {
			return nil, err
		}
		ret = append(ret, term)
	case expectingPosList:
		// Finish number.
		if !p.tsQuery {
			if len(termBuf) == 0 {
				return p.syntaxError()
			}
			pos, err := strconv.Atoi(string(termBuf))
			if err != nil {
				return p.syntaxError()
			}
			if pos == 0 {
				return ret, pgerror.Newf(pgcode.Syntax, "wrong position info in TSVector", p.input)
			} else if pos > maxTSVectorPosition {
				// Postgres silently truncates positions larger than 16383 to 16383.
				pos = maxTSVectorPosition
			}
			lastTerm := &ret[len(ret)-1]
			lastTerm.positions[len(lastTerm.positions)-1].position = uint16(pos)
		}
	case expectingTerm, finishedQuoteTerm:
		// We are good to go, we just finished a term and nothing needs to be cleaned up.
	case expectingPosDelimiter:
		// We are good to go, we just finished a position and nothing needs to be cleaned up.
	default:
		panic("invalid TSVector lex state")
	}
	for _, t := range ret {
		sort.Slice(t.positions, func(i, j int) bool {
			return t.positions[i].position < t.positions[j].position
		})
	}
	return ret, nil
}

func (p *tsVectorLexer) syntaxError() (TSVector, error) {
	typ := "TSVector"
	if p.tsQuery {
		typ = "TSQuery"
	}
	return TSVector{}, pgerror.Newf(pgcode.Syntax, "syntax error in %s: %s", typ, p.input)
}
