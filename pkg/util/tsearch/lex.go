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

type tsVectorLexer struct {
	input   string
	lastLen int
	pos     int
	state   tsVectorParseState

	// If true, we're in "TSQuery lexing mode"
	tsquery bool
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

// lex lexes the input in the receiver according to the TSVector "grammar".
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
// escape single quotes, and are otherwise no-ops.
//
// If a word is not single-quote wrapped, the next term will begin if there is
// whitespace after the word. Whitespace and colons may be entered by escaping
// them with backslashes. All other uses of backslashes are no-ops.
//
// A word is delimited from its position list with a colon.
//
// A position list is made up of a comma-delimited list of numbers, each
// of which may have an optional "strength" which is a letter from A-D.
//
// See examples in tsvector_test.go.
func (p tsVectorLexer) lex() (TSVector, error) {
	// termBuf will be reused as a temporary buffer to assemble each term before
	// copying into the vector.
	termBuf := make([]rune, 0, 32)
	ret := TSVector{}

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

			if p.tsquery {
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
						termBuf = termBuf[:0]
						if err != nil {
							return p.syntaxError()
						}
					}
					if r != '>' {
						return p.syntaxError()
					}
					ret = append(ret, tsTerm{operator: followedby, followedN: n})
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
				ret = append(ret, tsTerm{lexeme: string(termBuf)})
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
				lastTerm.positions = append(lastTerm.positions, tsposition{})
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

			if p.tsquery {
				switch r {
				case '&', '!', '|', '<', '(', ')':
					// These are all "operators" in the TSQuery language. End the current
					// term and start a new one.
					ret = append(ret, tsTerm{lexeme: string(termBuf)})
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
				term := tsTerm{lexeme: string(termBuf)}
				if r == ':' {
					term.positions = append(term.positions, tsposition{})
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
			if p.tsquery && r == ':' {
				return p.syntaxError()
			}
			termBuf = append(termBuf, r)
		case expectingPosList:
			var pos int
			if !p.tsquery {
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
				}
				termBuf = termBuf[:0]
			}
			lastTerm := &ret[len(ret)-1]
			lastTermPos := len(lastTerm.positions) - 1
			lastTerm.positions[lastTermPos].position = pos
			if unicode.IsSpace(r) {
				// Done with our term. Advance to next term!
				p.state = expectingTerm
				continue
			}
			switch r {
			case ',':
				if p.tsquery {
					// Not valid! No , allowed in position lists in tsqueries.
					return ret, pgerror.Newf(pgcode.Syntax, "syntax error in TSVector: %s", p.input)
				}
				lastTerm.positions = append(lastTerm.positions, tsposition{})
				// Expecting another number next.
				continue
			case '*':
				if p.tsquery {
					lastTerm.positions[lastTermPos].weight |= weightStar
				} else {
					p.state = expectingPosDelimiter
					lastTerm.positions[lastTermPos].weight |= weightA
				}
			case 'a', 'A':
				if !p.tsquery {
					p.state = expectingPosDelimiter
				}
				lastTerm.positions[lastTermPos].weight |= weightA
			case 'b', 'B':
				if !p.tsquery {
					p.state = expectingPosDelimiter
				}
				lastTerm.positions[lastTermPos].weight |= weightB
			case 'c', 'C':
				if !p.tsquery {
					p.state = expectingPosDelimiter
				}
				lastTerm.positions[lastTermPos].weight |= weightC
			case 'd', 'D':
				if p.tsquery {
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
				lastTerm.positions = append(lastTerm.positions, tsposition{})
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
		ret = append(ret, tsTerm{lexeme: string(termBuf)})
	case expectingPosList:
		// Finish number.
		if !p.tsquery {
			if len(termBuf) == 0 {
				return p.syntaxError()
			}
			pos, err := strconv.Atoi(string(termBuf))
			if err != nil {
				return p.syntaxError()
			}
			if pos == 0 {
				return ret, pgerror.Newf(pgcode.Syntax, "wrong position info in TSVector", p.input)
			}
			lastTerm := &ret[len(ret)-1]
			lastTerm.positions[len(lastTerm.positions)-1].position = pos
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
	if p.tsquery {
		typ = "TSQuery"
	}
	return TSVector{}, pgerror.Newf(pgcode.Syntax, "syntax error in %s: %s", typ, p.input)
}
