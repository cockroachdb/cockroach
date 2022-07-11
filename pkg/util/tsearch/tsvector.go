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
	"sort"
	"strconv"
	"strings"
)

// tsweight is a string A, B, C, or D.
type tsweight int

const (
	weightD tsweight = 1 << iota
	weightC
	weightB
	weightA
	weightStar
)

func (w tsweight) String() string {
	var ret strings.Builder
	if w&weightStar != 0 {
		ret.WriteByte('*')
	}
	if w&weightA != 0 {
		ret.WriteByte('A')
	}
	if w&weightB != 0 {
		ret.WriteByte('B')
	}
	if w&weightC != 0 {
		ret.WriteByte('C')
	}
	if w&weightD != 0 {
		ret.WriteByte('D')
	}
	return ret.String()
}

type tsposition struct {
	position int
	weight   tsweight
}

type tsoperator int

const (
	// Parentheses can be used to control nesting of the TSQuery operators.
	// Without parentheses, | binds least tightly,
	// then &, then <->, and ! most tightly.

	invalid    tsoperator = iota
	and                   // binary
	or                    // binary
	not                   // unary prefix
	followedby            // binary
	lparen                // grouping
	rparen                // grouping
)

func (o tsoperator) precedence() int {
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
	panic(fmt.Sprintf("no precdence for operator %d", o))
}

type tsTerm struct {
	// A term is either a lexeme and position list, or an operator.
	lexeme    string
	positions []tsposition

	operator tsoperator
	// Set only when operator = followedby
	followedN int
}

func (t tsTerm) String() string {
	if t.operator != 0 {
		switch t.operator {
		case and:
			return "&"
		case or:
			return "|"
		case not:
			return "!"
		case lparen:
			return "("
		case rparen:
			return ")"
		case followedby:
			if t.followedN == 1 {
				return "<->"
			}
			return fmt.Sprintf("<%d>", t.followedN)
		}
	}

	var buf strings.Builder
	buf.WriteByte('\'')
	for _, r := range t.lexeme {
		if r == '\'' {
			// Single quotes are escaped as double single quotes inside of a TSVector.
			buf.WriteString(`''`)
		} else {
			buf.WriteRune(r)
		}
	}
	buf.WriteByte('\'')
	for i, pos := range t.positions {
		if i > 0 {
			buf.WriteByte(',')
		} else {
			buf.WriteByte(':')
		}
		if pos.position > 0 {
			buf.WriteString(strconv.Itoa(pos.position))
		}
		buf.WriteString(pos.weight.String())
	}
	return buf.String()
}

// TSVector is a sorted list of terms, each of which is a lexeme that might have
// an associated position within an original document.
type TSVector []tsTerm

func (t TSVector) String() string {
	var buf strings.Builder
	for i, term := range t {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(term.String())
	}
	return buf.String()
}

// ParseTSVector produces a TSVector from an input string. The input will be
// sorted by lexeme, but will not be automatically stemmed or stop-worded.
func ParseTSVector(input string) (TSVector, error) {
	parser := tsVectorLexer{
		input: input,
		state: expectingTerm,
	}
	ret, err := parser.lex()
	if err != nil {
		return ret, err
	}

	if len(ret) > 1 {
		// Sort and de-duplicate the resultant TSVector.
		sort.Slice(ret, func(i, j int) bool {
			return ret[i].lexeme < ret[j].lexeme
		})
		// Then distinct: (wouldn't it be nice if Go had generics?)
		lastUniqueIdx := 0
		for j := 1; j < len(ret); j++ {
			if ret[j].lexeme != ret[lastUniqueIdx].lexeme {
				// We found a unique entry, at index i. The last unique entry in the
				// array was at lastUniqueIdx, so set the entry after that one to our
				// new unique entry, and bump lastUniqueIdx for the next loop iteration.
				// First, sort and unique the position list now that we've collapsed all
				// of the identical lexemes.
				ret[lastUniqueIdx].positions = sortAndUniqTSPositions(ret[lastUniqueIdx].positions)
				lastUniqueIdx++
				ret[lastUniqueIdx] = ret[j]
			} else {
				// The last entries were not unique. Collapse their positions into the
				// first entry's list.
				ret[lastUniqueIdx].positions = append(ret[lastUniqueIdx].positions, ret[j].positions...)
			}
		}
		ret = ret[:lastUniqueIdx+1]
	}
	if len(ret) >= 1 {
		// Make sure to sort and uniq the position list even if there's only 1
		// entry.
		latsIdx := len(ret) - 1
		ret[latsIdx].positions = sortAndUniqTSPositions(ret[latsIdx].positions)
	}
	return ret, nil
}
