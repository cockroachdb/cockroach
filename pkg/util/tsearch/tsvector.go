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
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// This file defines the TSVector data structure, which is used to implement
// Postgres's tsvector text search mechanism.
// See https://www.postgresql.org/docs/current/datatype-textsearch.html for
// context on what each of the pieces do.
//
// TSVector is ultimately used to represent a document as a posting list - a
// list of lexemes in the doc (typically without stop words like common or short
// words, and typically stemmed using a stemming algorithm like snowball
// stemming (https://snowballstem.org/)), along with an associated set of
// positions that those lexemes occur within the document.
//
// Typically, this posting list is then stored in an inverted index within the
// database to accelerate searches of terms within the document.
//
// The key structures are:
// - tsTerm is a document term (also referred to as lexeme) along with a
//   position list, which contains the positions within a document that a term
//   appeared, along with an optional weight, which controls matching.
// - tsTerm is also used during parsing of both TSQueries and TSVectors, so a
//   tsTerm also can represent an TSQuery operator.
// - tsWeight represents the weight of a given lexeme. It's also used for
//   queries, when a "star" weight is available that matches any weight.
// - TSVector is a list of tsTerms, ordered by their lexeme.

// tsWeight is a bitfield that represents the weight of a given term. When
// stored in a TSVector, only 1 of the bits will be set. The default weight is
// D - as a result, we store 0 for the weight of terms with weight D or no
// specified weight. The weightStar value is never set in a TSVector weight.
//
// tsWeight is also used inside of TSQueries, to specify the weight to search.
// Within TSQueries, the absence of a weight is the default, and indicates that
// the search term should match any matching term, regardless of its weight. If
// one or more of the weights are set in a search term, it indicates that the
// query should match only terms with the given weights.
type tsWeight byte

const (
	// These enum values are a bitfield and must be kept in order.
	weightD tsWeight = 1 << iota
	weightC
	weightB
	weightA
	// weightStar is a special "weight" that can be specified only in a search
	// term. It indicates prefix matching, which will allow the term to match any
	// document term that begins with the search term.
	weightStar
	invalidWeight

	weightAny = weightA | weightB | weightC | weightD
)

func (w tsWeight) String() string {
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

// TSVectorPGEncoding returns the PG-compatible wire protocol encoding for a
// given weight. Note that this is only allowable for TSVector tsweights, which
// can't have more than one weight set at the same time. In a TSQuery, you might
// have more than one weight per lexeme, which is not encodable using this
// scheme.
func (w tsWeight) TSVectorPGEncoding() (byte, error) {
	switch w {
	case weightA:
		return 3, nil
	case weightB:
		return 2, nil
	case weightC:
		return 1, nil
	case weightD, 0:
		return 0, nil
	}
	return 0, errors.Errorf("invalid tsvector weight %d", w)
}

// matches returns true if the receiver is matched by the input tsquery weight.
func (w tsWeight) matches(queryWeight tsWeight) bool {
	if queryWeight == weightAny {
		return true
	}
	if w&queryWeight > 0 {
		return true
	}
	// If we're querying for D, and the receiver has no weight, that's also a
	// match.
	return queryWeight&weightD > 0 && w == 0
}

func tsWeightFromVectorPGEncoding(b byte) (tsWeight, error) {
	switch b {
	case 3:
		return weightA, nil
	case 2:
		return weightB, nil
	case 1:
		return weightC, nil
	case 0:
		// We don't explicitly return weightD, since it's the default.
		return 0, nil
	}
	return 0, errors.Errorf("invalid encoded tsvector weight %d", b)
}

// tsPosition is a position within a document, along with an optional weight.
type tsPosition struct {
	position uint16
	weight   tsWeight
}

// tsTerm is either a lexeme and position list, or an operator (when parsing a
// a TSQuery).
type tsTerm struct {
	// lexeme is at most 2046 characters.
	lexeme    string
	positions []tsPosition

	// The operator and followedN fields are only used when parsing a TSQuery.
	operator tsOperator
	// Set only when operator = followedby
	// At most 16384.
	followedN uint16
}

func newLexemeTerm(lexeme string) (tsTerm, error) {
	if len(lexeme) > 2046 {
		return tsTerm{}, pgerror.Newf(pgcode.ProgramLimitExceeded, "word is too long (%d bytes, max 2046 bytes)", len(lexeme))
	}
	return tsTerm{lexeme: lexeme}, nil
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
			buf.WriteString(strconv.Itoa(int(pos.position)))
		}
		buf.WriteString(pos.weight.String())
	}
	return buf.String()
}

func (t tsTerm) matchesWeight(targetWeight tsWeight) bool {
	if targetWeight == weightAny {
		return true
	}
	if len(t.positions) == 0 {
		// A "stripped" tsvector (no associated positions) always matches any input
		// weight.
		return true
	}
	for _, pos := range t.positions {
		if pos.weight.matches(targetWeight) {
			return true
		}
	}
	return false
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

	return normalizeTSVector(ret)
}

func normalizeTSVector(ret TSVector) (TSVector, error) {
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
		lastIdx := len(ret) - 1
		ret[lastIdx].positions = sortAndUniqTSPositions(ret[lastIdx].positions)
	}
	return ret, nil
}

var validCharTables = []*unicode.RangeTable{unicode.Letter, unicode.Number}

// parseForTextSearch is the function that splits an input text into a list of
// tokens. For now, the parser that we use is very simple: it merely lowercases
// the input and splits it into tokens based on assuming that non-letter,
// non-number characters are whitespace.
//
// The Postgres text search parser is much, much more sophisticated. The
// documentation (https://www.postgresql.org/docs/current/textsearch-parsers.html)
// gives more information, but roughly, each token is categorized into one of
// about 20 different buckets, such as asciiword, url, email, host, float, int,
// version, tag, etc. It uses very specific rules to produce these outputs.
// Another interesting transformation is returning multiple tokens for a
// hyphenated word, including a token that represents the entire hyphenated word,
// as well as one for each of the hyphenated components.
//
// It's not clear whether we need to exactly mimic this functionality. Likely,
// we will eventually want to do this.
func parseForTextSearch(input string) []string {
	lower := strings.ToLower(input)
	return strings.FieldsFunc(lower, func(r rune) bool {
		return !unicode.IsOneOf(validCharTables, r)
	})
}

// DocumentToTSVector parses an input document into lexemes, removes stop words,
// stems and normalizes the lexemes, and returns a TSVector annotated with
// lexeme positions according to a text search configuration passed by name.
func DocumentToTSVector(config string, input string) (TSVector, error) {
	if config != "simple" {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "text search configuration %q does not exist", config)
	}

	tokens := parseForTextSearch(input)
	vector := make(TSVector, len(tokens))
	for i := range tokens {
		vector[i].lexeme = tokens[i]
		pos := i + 1
		if i > maxTSVectorPosition {
			pos = maxTSVectorPosition
		}
		vector[i].positions = []tsPosition{{position: uint16(pos)}}
	}
	return normalizeTSVector(vector)
}
