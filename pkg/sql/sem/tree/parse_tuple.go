// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var enclosingRecordError = pgerror.Newf(pgcode.InvalidTextRepresentation, "record must be enclosed in ( and )")
var extraTextRecordError = pgerror.Newf(pgcode.InvalidTextRepresentation, "extra text after closing right paren")
var malformedRecordError = pgerror.Newf(pgcode.InvalidTextRepresentation, "malformed record literal")
var unsupportedRecordError = pgerror.Newf(pgcode.FeatureNotSupported, "cannot parse anonymous record type")

var isTupleControlChar = func(ch byte) bool {
	return ch == '(' || ch == ')' || ch == ',' || ch == '"'
}

var isTupleElementChar = func(r rune) bool {
	return r != '(' && r != ')' && r != ','
}

// gobbleString advances the parser for the remainder of the current string
// until it sees a non-escaped termination character, as specified by
// isTerminatingChar, returning the resulting string, not including the
// termination character.
func (p *tupleParseState) gobbleString(
	isTerminatingChar func(ch byte) bool,
) (out string, err error) {
	var result bytes.Buffer
	start := 0
	i := 0
	for i < len(p.s) && !isTerminatingChar(p.s[i]) {
		// In these strings, we just encode directly the character following a
		// '\', even if it would normally be an escape sequence.
		if i < len(p.s) && p.s[i] == '\\' {
			result.WriteString(p.s[start:i])
			i++
			if i < len(p.s) {
				result.WriteByte(p.s[i])
				i++
			}
			start = i
		} else {
			i++
		}
	}
	if i >= len(p.s) {
		return "", malformedRecordError
	}
	result.WriteString(p.s[start:i])
	p.s = p.s[i:]
	return result.String(), nil
}

type tupleParseState struct {
	s                string
	tupleIdx         int
	ctx              ParseTimeContext
	dependsOnContext bool
	result           *DTuple
	t                *types.T
}

func (p *tupleParseState) advance() {
	_, l := utf8.DecodeRuneInString(p.s)
	p.s = p.s[l:]
}

func (p *tupleParseState) eatWhitespace() {
	for unicode.IsSpace(p.peek()) {
		p.advance()
	}
}

func (p *tupleParseState) peek() rune {
	r, _ := utf8.DecodeRuneInString(p.s)
	return r
}

func (p *tupleParseState) eof() bool {
	return len(p.s) == 0
}

func (p *tupleParseState) parseQuotedString() (string, error) {
	return p.gobbleString(isQuoteChar)
}

func (p *tupleParseState) parseUnquotedString() (string, error) {
	out, err := p.gobbleString(isTupleControlChar)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func (p *tupleParseState) parseElement() error {
	if p.tupleIdx >= len(p.t.TupleContents()) {
		return errors.WithDetail(malformedRecordError, "Too many columns.")
	}
	var next string
	var err error
	r := p.peek()
	switch r {
	case '"':
		p.advance()
		next, err = p.parseQuotedString()
		if err != nil {
			return err
		}
		p.advance()
	default:
		if !isTupleElementChar(r) {
			return malformedRecordError
		}
		next, err = p.parseUnquotedString()
		if err != nil {
			return err
		}
		if strings.EqualFold(next, "null") {
			p.result.D[p.tupleIdx] = DNull
			p.tupleIdx++
			return nil
		}
	}

	d, dependsOnContext, err := ParseAndRequireString(
		p.t.TupleContents()[p.tupleIdx],
		next,
		p.ctx,
	)
	if err != nil {
		return err
	}
	if dependsOnContext {
		p.dependsOnContext = true
	}
	p.result.D[p.tupleIdx] = d
	p.tupleIdx++
	return nil
}

// ParseDTupleFromString parses the string-form of constructing tuples, handling
// cases such as `'(1,2,3)'::record`. The input type t is the type of the
// tuple to parse.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseTimeContext (either for the time or the local timezone).
func ParseDTupleFromString(
	ctx ParseTimeContext, s string, t *types.T,
) (_ *DTuple, dependsOnContext bool, _ error) {
	ret, dependsOnContext, err := doParseDTupleFromString(ctx, s, t)
	if err != nil {
		return ret, false, MakeParseError(s, t, err)
	}
	return ret, dependsOnContext, nil
}

// doParseDTupleFromString does most of the work of ParseDTupleFromString,
// except the error it returns isn't prettified as a parsing error.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseTimeContext (either for the time or the local timezone).
func doParseDTupleFromString(
	ctx ParseTimeContext, s string, t *types.T,
) (_ *DTuple, dependsOnContext bool, _ error) {
	if t.TupleContents() == nil {
		return nil, false, errors.AssertionFailedf("not a tuple type %s (%T)", t, t)
	}
	if t == types.AnyTuple {
		return nil, false, unsupportedRecordError
	}
	parser := tupleParseState{
		s:      s,
		ctx:    ctx,
		result: NewDTupleWithLen(t, len(t.TupleContents())),
		t:      t,
	}

	parser.eatWhitespace()
	if parser.peek() != '(' {
		return nil, false, enclosingRecordError
	}
	parser.advance()
	parser.eatWhitespace()
	if parser.peek() != ')' {
		if err := parser.parseElement(); err != nil {
			return nil, false, err
		}
		parser.eatWhitespace()
		for parser.peek() == ',' {
			parser.advance()
			parser.eatWhitespace()
			if err := parser.parseElement(); err != nil {
				return nil, false, err
			}
		}
	}
	parser.eatWhitespace()
	if parser.eof() {
		return nil, false, enclosingRecordError
	}
	if parser.peek() != ')' {
		return nil, false, malformedRecordError
	}
	if parser.tupleIdx < len(parser.t.TupleContents()) {
		return nil, false, errors.WithDetail(malformedRecordError, "Too few columns.")
	}
	parser.advance()
	parser.eatWhitespace()
	if !parser.eof() {
		return nil, false, extraTextRecordError
	}

	return parser.result, parser.dependsOnContext, nil
}
